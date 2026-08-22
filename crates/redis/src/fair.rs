//! Fair (FIFO) mutex: waiters queue explicitly and are served in arrival
//! order; barging past a non-empty queue is impossible.
//!
//! Waiters heartbeat while queued; dead waiters are skipped at handoff.
//! The handoff writes the winner's token directly into the lock, and the
//! winner discovers it on its next poll (see ADR 0018).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::Script;
use redis::aio::ConnectionManager;

use palisade_core::{Error, FencingToken, LockOptions, OwnerId, Result};

use crate::single::{POLL_INTERVAL, RedisLockManager, fence_key_for};

/// A fairly-acquired lock. Clones share one hold.
#[derive(Clone)]
pub struct FairLockHandle {
    shared: Arc<FairShared>,
}

impl std::fmt::Debug for FairLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FairLockHandle")
            .field("key", &self.shared.key)
            .field("owner", &self.shared.owner)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

struct FairShared {
    conn: ConnectionManager,
    release_script: Script,
    extend_script: Script,
    key: String,
    queue_key: String,
    hb_prefix: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    gone: AtomicBool,
}

impl FairShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }
}

impl Drop for FairShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mut conn = self.conn.clone();
            let script = self.release_script.clone();
            let key = self.key.clone();
            let token = self.token.clone();
            let hb_prefix = self.hb_prefix.clone();
            std::mem::drop(runtime.spawn(async move {
                let _: std::result::Result<i64, redis::RedisError> = script
                    .key(&key)
                    .arg(&token)
                    .arg(&hb_prefix)
                    .invoke_async(&mut conn)
                    .await;
            }));
        }
    }
}

impl FairLockHandle {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Identity of this acquisition.
    pub fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    /// Fence token from the grant.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Releases and hands the lock to the oldest live waiter, if any.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let ok: i64 = self
            .shared
            .release_script
            .key(&self.shared.key)
            .key(&self.shared.queue_key)
            .key(fence_key_for(&self.shared.key))
            .arg(&self.shared.token)
            .arg(30_000_i64)
            .arg(&self.shared.hb_prefix)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("fair release failed: {e}")))?;
        if ok < 0 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }

    /// Refreshes the lease (plain ownership-checked extend â€” the fair lock's
    /// value is a plain string like the standard mutex).
    pub async fn extend(&self, ttl: Duration) -> Result<()> {
        if ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "extend ttl {:?} is below the {:?} floor",
                ttl,
                palisade_core::MIN_TTL
            )));
        }
        if self.shared.gone.load(Ordering::Acquire) {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        let mut conn = self.shared.conn.clone();
        let ok: i64 = self
            .shared
            .extend_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .arg(ttl.as_millis() as u64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("fair extend failed: {e}")))?;
        if ok != 1 {
            self.shared.gone.store(true, Ordering::Release);
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }
}

impl RedisLockManager {
    /// Attempts fair acquisition: grants only if the lock is free AND no
    /// waiter is queued ahead; otherwise joins the queue.
    pub async fn try_lock_fair(&self, key: &str, options: &LockOptions) -> Result<FairLockHandle> {
        options.validate()?;
        let owner = OwnerId::generate();
        let token = owner.as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let queue_key = format!("{{{key}}}:q");
        let hb_key = format!("{{{key}}}:hb:{token}");
        let hb_prefix = format!("{{{key}}}:hb:");

        let mut conn = self.conn.clone();
        let (status, fence): (i64, i64) = self
            .fair_acquire_script
            .key(key)
            .key(&queue_key)
            .key(fence_key_for(key))
            .key(&hb_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(ttl_ms * 10)
            .arg(ttl_ms.max(2_000))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("fair acquire failed: {e}")))?;

        match status {
            1 | 2 => Ok(FairLockHandle {
                shared: Arc::new(FairShared {
                    conn: self.conn.clone(),
                    release_script: self.fair_release_script.clone(),
                    extend_script: self.extend_script.clone(),
                    key: key.to_owned(),
                    queue_key,
                    hb_prefix,
                    token,
                    owner,
                    fence: FencingToken::new(fence as u64),
                    gone: AtomicBool::new(false),
                }),
            }),
            _ => Err(Error::Held {
                key: key.to_owned(),
            }),
        }
    }

    /// Polls until granted or `wait` elapses. Each poll refreshes this
    /// waiter's queue entry and heartbeat.
    pub async fn try_lock_fair_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<FairLockHandle> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_lock_fair(key, options).await {
                Ok(h) => return Ok(h),
                Err(Error::Held { .. }) => {}
                Err(other) => return Err(other),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: key.to_owned(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
