//! Counting semaphore with server-side per-holder leases.
//!
//! Permits live in a ZSET scored by their Redis-side expiry instant, so a
//! crashed holder's slot is reclaimed automatically without trusting any
//! client clock (see ADR 0017).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::Script;
use redis::aio::ConnectionManager;

use palisade_core::{Error, FencingToken, LockOptions, Result};

use crate::single::{POLL_INTERVAL, RedisLockManager, fence_key_for};

/// A fixed-capacity semaphore bound to one key.
#[derive(Clone)]
pub struct RedisSemaphore {
    manager: RedisLockManager,
    key: String,
    max_permits: u32,
}

/// One held permit. Clones share the permit.
#[derive(Clone)]
pub struct SemaphorePermit {
    shared: Arc<PermitShared>,
}

impl std::fmt::Debug for SemaphorePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemaphorePermit")
            .field("key", &self.shared.key)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

struct PermitShared {
    conn: ConnectionManager,
    release_script: Script,
    extend_script: Script,
    key: String,
    token: String,
    fence: FencingToken,
    gone: AtomicBool,
}

impl PermitShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }
}

impl Drop for PermitShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mut conn = self.conn.clone();
            let script = self.release_script.clone();
            let key = self.key.clone();
            let token = self.token.clone();
            std::mem::drop(runtime.spawn(async move {
                let _: std::result::Result<i64, redis::RedisError> =
                    script.key(&key).arg(&token).invoke_async(&mut conn).await;
            }));
        }
    }
}

impl SemaphorePermit {
    /// The semaphore key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Fence token from the grant.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Returns the permit. Idempotent per handle.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let ok: i64 = self
            .shared
            .release_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("semaphore release failed: {e}")))?;
        if ok != 1 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }

    /// Refreshes this permit's lease.
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
            .map_err(|e| Error::Backend(format!("semaphore extend failed: {e}")))?;
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

impl RedisSemaphore {
    /// Attempts immediate acquisition of one permit.
    pub async fn try_acquire(&self, options: &LockOptions) -> Result<SemaphorePermit> {
        options.validate()?;
        let mgr = &self.manager;
        let token = palisade_core::OwnerId::generate().as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_key = fence_key_for(&self.key);

        let mut conn = mgr.conn.clone();
        let (status, fence): (i64, i64) = mgr
            .semaphore_acquire_script
            .key(&self.key)
            .key(&fence_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(i64::from(self.max_permits))
            .arg(ttl_ms * 10)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("semaphore acquire failed: {e}")))?;

        if status != 1 {
            return Err(Error::Held {
                key: self.key.clone(),
            });
        }
        Ok(SemaphorePermit {
            shared: Arc::new(PermitShared {
                conn: mgr.conn.clone(),
                release_script: mgr.semaphore_release_script.clone(),
                extend_script: mgr.semaphore_extend_script.clone(),
                key: self.key.clone(),
                token,
                fence: FencingToken::new(fence as u64),
                gone: AtomicBool::new(false),
            }),
        })
    }

    /// Polls until a permit frees up or `wait` elapses.
    pub async fn try_acquire_for(
        &self,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<SemaphorePermit> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_acquire(options).await {
                Ok(p) => return Ok(p),
                Err(Error::Held { .. }) => {}
                Err(other) => return Err(other),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: self.key.clone(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

impl RedisLockManager {
    /// Binds a semaphore of `max_permits` concurrent holders to `key`.
    pub fn semaphore(&self, key: impl Into<String>, max_permits: u32) -> Result<RedisSemaphore> {
        if max_permits == 0 {
            return Err(Error::InvalidConfig(
                "semaphore capacity must be at least 1".into(),
            ));
        }
        Ok(RedisSemaphore {
            manager: self.clone(),
            key: key.into(),
            max_permits,
        })
    }
}
