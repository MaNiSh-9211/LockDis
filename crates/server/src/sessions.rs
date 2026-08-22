//! Server-authoritative sessions (ADR 0027).
//!
//! A session is a server-tracked liveness domain: locks acquired under a
//! session token are recorded here, heartbeats refresh `last_seen`, and the
//! sweeper releases every lock of any session whose heartbeat budget has
//! lapsed. A crashed client's locks therefore die within
//! `ttl + sweep_interval` â€” decided by the server, not by lease arithmetic.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use palisade_redis::RedisLockManager;

/// Sweep cadence; worst-case detection = ttl + SWEEP_INTERVAL.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct SessionBook {
    inner: Arc<Mutex<HashMap<String, SessionEntry>>>,
    manager: Arc<RedisLockManager>,
}

#[derive(Clone)]
pub(crate) struct SessionEntry {
    pub client_id: String,
    pub ttl: Duration,
    pub last_seen: Instant,
    /// Locks bound to this session: (key, owner token).
    pub locks: Vec<(String, String)>,
}

impl SessionEntry {
    fn expired(&self) -> bool {
        self.last_seen.elapsed() > self.ttl
    }
}

impl SessionBook {
    pub fn new(manager: Arc<RedisLockManager>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            manager,
        }
    }

    pub fn register(&self, client_id: String, ttl: Duration) -> String {
        let token = palisade_core::OwnerId::generate().as_uuid().to_string();
        self.inner.lock().expect("session book").insert(
            token.clone(),
            SessionEntry {
                client_id,
                ttl,
                last_seen: Instant::now(),
                locks: Vec::new(),
            },
        );
        token
    }

    pub fn heartbeat(&self, token: &str) -> bool {
        let mut book = self.inner.lock().expect("session book");
        match book.get_mut(token) {
            Some(entry) => {
                entry.last_seen = Instant::now();
                true
            }
            None => false,
        }
    }

    /// Binds an acquired lock to a live session.
    pub fn bind(&self, token: &str, key: &str, lock_token: &str) -> bool {
        let mut book = self.inner.lock().expect("session book");
        match book.get_mut(token) {
            Some(entry) => {
                entry.last_seen = Instant::now();
                entry.locks.push((key.to_owned(), lock_token.to_owned()));
                true
            }
            None => false,
        }
    }

    /// Explicit close: release everything bound, drop the session.
    /// Returns how many locks were released.
    pub(crate) async fn close(self: &Arc<Self>, token: &str) -> u32 {
        let locks = self.remove(token);
        self.release_all(&locks).await
    }

    fn remove(&self, token: &str) -> Vec<(String, String)> {
        self.inner
            .lock()
            .expect("session book")
            .remove(token)
            .map(|e| e.locks)
            .unwrap_or_default()
    }

    async fn release_all(&self, locks: &[(String, String)]) -> u32 {
        let mut n = 0u32;
        for (key, lock_token) in locks {
            if self
                .manager
                .unlock_with_token(key, lock_token)
                .await
                .is_ok()
            {
                n += 1;
            }
        }
        if n > 0 {
            tracing::warn!(released = n, "session sweep/close released bound locks");
        }
        n
    }

    /// One sweep pass: release every expired session's locks.
    pub async fn sweep_once(self: &Arc<Self>) {
        let expired: Vec<String> = {
            let book = self.inner.lock().expect("session book");
            book.iter()
                .filter(|(_, e)| e.expired())
                .map(|(t, _)| t.clone())
                .collect()
        };
        for token in expired {
            let client = {
                let book = self.inner.lock().expect("session book");
                book.get(&token).map(|e| e.client_id.clone())
            };
            if client.is_none() {
                continue; // raced with explicit close; fine
            }
            let locks = self.remove(&token);
            let n = self.release_all(&locks).await;
            metrics::counter!("palisade_sessions_expired_total").increment(1);
            tracing::warn!(
                client = ?client,
                released = n,
                "expired session: server released its locks"
            );
        }
    }

    /// Active session count for introspection.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().expect("session book").len()
    }
}

/// Spawns the periodic sweeper; keep the returned guard alive for the
/// service's lifetime (dropping it stops sweeping).
pub(crate) fn spawn_sweeper(book: Arc<SessionBook>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            book.sweep_once().await;
        }
    })
}
