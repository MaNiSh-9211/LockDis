//! Reentrant mutex: the same owner can acquire repeatedly; a hold count
//! tracks nesting and the lock frees only at zero.
//!
//! The owner identity is caller-supplied (share one `OwnerId` across the
//! call stack that needs reentrancy). Each handle represents ONE hold;
//! clones of a handle share that hold.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::Script;
use redis::aio::ConnectionManager;

use palisade_core::{Error, FencingToken, LockOptions, OwnerId, Result};

use crate::single::{POLL_INTERVAL, RedisLockManager, fence_key_for};

/// A single hold on a reentrant lock. Cheap to clone; clones share the hold.
#[derive(Clone)]
pub struct ReentrantLockHandle {
    shared: Arc<ReentrantShared>,
}

impl std::fmt::Debug for ReentrantLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReentrantLockHandle")
            .field("key", &self.shared.key)
            .field("owner", &self.shared.owner)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

struct ReentrantShared {
    conn: ConnectionManager,
    release_script: Script,
    release_all_script: Script,
    extend_script: Script,
    key: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    gone: AtomicBool,
}

impl ReentrantShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }
}

impl Drop for ReentrantShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            std::mem::drop(runtime.spawn(detached_release_one(
                self.conn.clone(),
                self.release_script.clone(),
                self.key.clone(),
                self.token.clone(),
            )));
        }
    }
}

async fn detached_release_one(
    mut conn: ConnectionManager,
    script: Script,
    key: String,
    token: String,
) {
    let _: std::result::Result<i64, redis::RedisError> = script
        .key(&key)
        .arg(&token)
        .arg(0_i64)
        .invoke_async(&mut conn)
        .await;
}

impl ReentrantLockHandle {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Identity used for reentrancy.
    pub fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    /// Fence token from the most recent grant/reentry.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Drops one hold. Fully releases when the count reaches zero.
    /// Idempotent per handle. [`Error::Lost`] if the lease expired first.
    pub async fn release_one(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let remaining: i64 = self
            .shared
            .release_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .arg(0_i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("reentrant release failed: {e}")))?;
        if remaining < 0 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }

    /// Releases every hold this owner has on the key, regardless of how
    /// many handles exist.
    pub async fn release_all(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let ok: i64 = self
            .shared
            .release_all_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("reentrant release-all failed: {e}")))?;
        if ok < 0 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }

    /// Refreshes the lease if this owner still holds it.
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
            .map_err(|e| Error::Backend(format!("reentrant extend failed: {e}")))?;
        if ok < 0 {
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
    /// Attempts immediate reentrant acquisition for `owner`.
    pub async fn try_lock_reentrant(
        &self,
        key: &str,
        owner: OwnerId,
        options: &LockOptions,
    ) -> Result<ReentrantLockHandle> {
        options.validate()?;
        let token = owner.as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_ttl_ms = ttl_ms * 10;
        let fence_key = fence_key_for(key);

        let mut conn = self.conn.clone();
        let (status, fence): (i64, i64) = self
            .reentrant_acquire_script
            .key(key)
            .key(&fence_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(fence_ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("reentrant acquire failed: {e}")))?;

        match status {
            1 | 2 => Ok(ReentrantLockHandle {
                shared: Arc::new(ReentrantShared {
                    conn: self.conn.clone(),
                    release_script: self.reentrant_release_script.clone(),
                    release_all_script: self.reentrant_release_all_script.clone(),
                    extend_script: self.reentrant_extend_script.clone(),
                    key: key.to_owned(),
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

    /// Polls until `owner` acquires or `wait` elapses.
    pub async fn try_lock_reentrant_for(
        &self,
        key: &str,
        owner: OwnerId,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<ReentrantLockHandle> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_lock_reentrant(key, owner.clone(), options).await {
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
