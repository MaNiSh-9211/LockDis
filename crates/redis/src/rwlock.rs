//! Read-write lock: shared readers, one exclusive writer.
//!
//! Reader-preferring by construction (new readers join while mode is `r`);
//! writers starve only under sustained read pressure â€” documented trade-off
//! for v1 (see ADR 0016).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::Script;
use redis::aio::ConnectionManager;

use palisade_core::{Error, FencingToken, LockOptions, Result};

use crate::single::{POLL_INTERVAL, RedisLockManager, fence_key_for};

/// Shared state behind both guard kinds.
struct RwShared {
    conn: ConnectionManager,
    read_release_script: Script,
    write_release_script: Script,
    extend_script: Script,
    key: String,
    token: String,
    fence: FencingToken,
    ttl: Duration,
    gone: AtomicBool,
    write_mode: bool,
}

impl RwShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }
}

impl Drop for RwShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut conn = self.conn.clone();
        let key = self.key.clone();
        let task = if self.write_mode {
            let script = self.write_release_script.clone();
            let token = self.token.clone();
            runtime.spawn(async move {
                let _: std::result::Result<i64, redis::RedisError> =
                    script.key(&key).arg(&token).invoke_async(&mut conn).await;
            })
        } else {
            let script = self.read_release_script.clone();
            let token = self.token.clone();
            runtime.spawn(async move {
                let _: std::result::Result<i64, redis::RedisError> = script
                    .key(&key)
                    .arg(token)
                    .arg(0_u64)
                    .invoke_async(&mut conn)
                    .await;
            })
        };
        std::mem::drop(task);
    }
}

fn validate_ttl(ttl: Duration) -> Result<()> {
    if ttl < palisade_core::MIN_TTL {
        return Err(Error::InvalidConfig(format!(
            "ttl {:?} is below the {:?} floor",
            ttl,
            palisade_core::MIN_TTL
        )));
    }
    Ok(())
}

/// Shared read guard. Clones share one reader slot.
#[derive(Clone)]
pub struct RwReadHandle {
    shared: Arc<RwShared>,
}

impl std::fmt::Debug for RwReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwReadHandle")
            .field("key", &self.shared.key)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

/// Exclusive write guard. Clones share one writer slot.
#[derive(Clone)]
pub struct RwWriteHandle {
    shared: Arc<RwShared>,
}

impl std::fmt::Debug for RwWriteHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwWriteHandle")
            .field("key", &self.shared.key)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

macro_rules! rw_common_methods {
    () => {
        /// The lock key.
        pub fn key(&self) -> &str {
            &self.shared.key
        }

        /// Fence token from the most recent grant.
        pub fn fence(&self) -> FencingToken {
            self.shared.fence
        }

        /// Refreshes the lease (any live reader or the writer may).
        pub async fn extend(&self, ttl: Duration) -> Result<()> {
            validate_ttl(ttl)?;
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
                .map_err(|e| Error::Backend(format!("rw extend failed: {e}")))?;
            if ok < 0 {
                self.shared.gone.store(true, Ordering::Release);
                return Err(Error::Lost {
                    key: self.shared.key.clone(),
                    fence: self.shared.fence.value(),
                });
            }
            Ok(())
        }
    };
}

impl RwReadHandle {
    rw_common_methods!();

    /// Releases this reader slot. Idempotent per handle.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let remaining: i64 = self
            .shared
            .read_release_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .arg(self.shared.ttl.as_millis() as u64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("rw read-release failed: {e}")))?;
        if remaining < 0 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }
}

impl RwWriteHandle {
    rw_common_methods!();

    /// Releases the write lock. Idempotent per handle.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        let mut conn = self.shared.conn.clone();
        let ok: i64 = self
            .shared
            .write_release_script
            .key(&self.shared.key)
            .arg(&self.shared.token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("rw write-release failed: {e}")))?;
        if ok < 0 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }
}

impl RedisLockManager {
    /// Attempts immediate shared read access.
    pub async fn try_read(&self, key: &str, options: &LockOptions) -> Result<RwReadHandle> {
        options.validate()?;
        let token = palisade_core::OwnerId::generate().as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_key = fence_key_for(key);

        let mut conn = self.conn.clone();
        let (status, fence): (i64, i64) = self
            .rw_read_acquire_script
            .key(key)
            .key(&fence_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(ttl_ms * 10)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("rw read-acquire failed: {e}")))?;

        if status != 1 {
            return Err(Error::Held {
                key: key.to_owned(),
            });
        }
        Ok(RwReadHandle {
            shared: Arc::new(RwShared {
                conn: self.conn.clone(),
                read_release_script: self.rw_read_release_script.clone(),
                write_release_script: self.rw_write_release_script.clone(),
                extend_script: self.rw_extend_script.clone(),
                key: key.to_owned(),
                token,
                fence: FencingToken::new(fence as u64),
                ttl: options.ttl,
                gone: AtomicBool::new(false),
                write_mode: false,
            }),
        })
    }

    /// Attempts immediate exclusive write access.
    pub async fn try_write(&self, key: &str, options: &LockOptions) -> Result<RwWriteHandle> {
        options.validate()?;
        let token = palisade_core::OwnerId::generate().as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_key = fence_key_for(key);

        let mut conn = self.conn.clone();
        let (status, fence): (i64, i64) = self
            .rw_write_acquire_script
            .key(key)
            .key(&fence_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(ttl_ms * 10)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("rw write-acquire failed: {e}")))?;

        if status == 0 {
            return Err(Error::Held {
                key: key.to_owned(),
            });
        }
        Ok(RwWriteHandle {
            shared: Arc::new(RwShared {
                conn: self.conn.clone(),
                read_release_script: self.rw_read_release_script.clone(),
                write_release_script: self.rw_write_release_script.clone(),
                extend_script: self.rw_extend_script.clone(),
                key: key.to_owned(),
                token,
                fence: FencingToken::new(fence as u64),
                ttl: options.ttl,
                gone: AtomicBool::new(false),
                write_mode: true,
            }),
        })
    }

    /// Polls for shared read access until `wait` elapses.
    pub async fn try_read_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<RwReadHandle> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_read(key, options).await {
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

    /// Polls for exclusive write access until `wait` elapses.
    pub async fn try_write_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<RwWriteHandle> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_write(key, options).await {
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
