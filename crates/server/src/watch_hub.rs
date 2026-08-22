//! Watch fan-out hub (ADR 0029).
//!
//! Naive watching spawns one store-poller per subscriber: 10k watchers on a
//! hot key = 100k probes/sec. The hub collapses that to **one poller per
//! key**, broadcasting state transitions to any number of subscribers over
//! bounded channels. Polling cost becomes O(distinct keys), not O(watchers).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use palisade_proto::{Acquired, Freed, LockEvent};
use tokio::sync::{mpsc, watch};

use tonic::Status;

use palisade_redis::RedisLockManager;

/// Store-probe cadence for each key's single poller.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Per-subscriber channel depth; slow consumers lag rather than stall the
/// shared poller (watch semantics are level-triggered, so a lagged consumer
/// simply sees the next transition).
const SUBSCRIBER_DEPTH: usize = 16;

#[derive(Clone)]
pub struct WatchHub {
    inner: Arc<HubInner>,
}

struct HubInner {
    manager: RedisLockManager,
    /// key -> broadcast state; `None` until the first probe completes.
    keys: Mutex<HashMap<String, watch::Sender<Option<bool>>>>,
}

impl WatchHub {
    pub fn new(manager: RedisLockManager) -> Self {
        Self {
            inner: Arc::new(HubInner {
                manager,
                keys: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Subscribes to anonymized state changes for one key. The first
    /// subscriber spawns the key's single poller; the last departure stops it.
    pub async fn subscribe(&self, key: &str) -> mpsc::Receiver<Result<LockEvent, Status>> {
        let state_rx = {
            let mut keys = self.inner.keys.lock().expect("hub lock");
            if let Some(tx) = keys.get(key) {
                tx.subscribe()
            } else {
                let (tx, rx) = watch::channel::<Option<bool>>(None);
                keys.insert(key.to_owned(), tx.clone());
                drop(keys);
                self.spawn_poller(key.to_owned(), tx);
                rx
            }
        };

        let (event_tx, event_rx) = mpsc::channel(SUBSCRIBER_DEPTH);
        tokio::spawn(async move {
            let mut rx = state_rx;
            loop {
                let current = *rx.borrow_and_update();
                if let Some(held) = current {
                    let event = transition_event(held);
                    if event_tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
                if rx.changed().await.is_err() {
                    // Poller retired: no subscribers left anywhere.
                    return;
                }
            }
        });
        event_rx
    }

    fn spawn_poller(&self, key: String, tx: watch::Sender<Option<bool>>) {
        let manager = self.inner.manager.clone();
        let hub = self.inner.clone();
        tokio::spawn(async move {
            loop {
                // Retire when nobody is listening anymore.
                if tx.receiver_count() == 0 {
                    let mut keys = hub.keys.lock().expect("hub lock");
                    match keys.get(&key) {
                        Some(entry) if entry.receiver_count() == 0 => {
                            keys.remove(&key);
                            metrics::gauge!("palisade_watch_keys_active").decrement(1.0);
                            return;
                        }
                        _ => {}
                    }
                }

                if let Ok(held) = manager.probe_held(&key).await {
                    tx.send_if_modified(|cur| {
                        if *cur != Some(held) {
                            *cur = Some(held);
                            true
                        } else {
                            false
                        }
                    });
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
    }
}

fn transition_event(held: bool) -> LockEvent {
    if held {
        LockEvent {
            event: Some(palisade_proto::lock_event::Event::Acquired(Acquired {})),
        }
    } else {
        LockEvent {
            event: Some(palisade_proto::lock_event::Event::Freed(Freed {})),
        }
    }
}
