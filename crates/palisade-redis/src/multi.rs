//! Multi-lock: acquire several keys all-or-nothing, in globally sorted
//! order so concurrent multi-lockers can never deadlock (see ADR 0019).

use std::time::Duration;

use palisade_core::{Error, FencingToken, LockHandle, LockOptions, Result};

use crate::single::{POLL_INTERVAL, RedisLockHandle, RedisLockManager};

/// All-or-nothing acquisition over K keys.
pub struct MultiLockHandle {
    handles: Vec<RedisLockHandle>,
}

impl std::fmt::Debug for MultiLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiLockHandle")
            .field("keys", &self.keys())
            .field("fences", &self.fences())
            .finish()
    }
}

impl MultiLockHandle {
    /// Fence tokens, one per key, in the same order as the (sorted) keys.
    pub fn fences(&self) -> Vec<FencingToken> {
        self.handles.iter().map(|h| h.fence()).collect()
    }

    /// The acquired keys in acquisition (sorted) order.
    pub fn keys(&self) -> Vec<String> {
        self.handles.iter().map(|h| h.key().to_owned()).collect()
    }

    /// Releases every key in reverse acquisition order. Idempotent.
    pub async fn release_all(&self) -> Result<()> {
        let mut first_err = None;
        for h in self.handles.iter().rev() {
            if let Err(e) = h.release().await {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl RedisLockManager {
    /// Acquires `keys` in sorted order; on any failure rolls back what was
    /// taken and retries until `wait` elapses. Duplicate keys are rejected.
    pub async fn try_lock_all(
        &self,
        keys: &[String],
        options: &LockOptions,
        wait: Duration,
    ) -> Result<MultiLockHandle> {
        options.validate()?;
        let mut sorted: Vec<String> = keys.to_vec();
        sorted.sort();
        let dup = sorted.windows(2).any(|w| w[0] == w[1]);
        if dup {
            return Err(Error::InvalidConfig(
                "multi-lock received duplicate keys".into(),
            ));
        }

        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let mut handles = Vec::with_capacity(sorted.len());
            let mut rollback_err = None;
            for key in &sorted {
                match self.try_lock_with(key, options).await {
                    Ok(h) => handles.push(h),
                    Err(e @ (Error::Held { .. } | Error::Backend(_))) => {
                        for h in handles.iter().rev() {
                            let _ = h.release().await;
                        }
                        rollback_err = Some(e);
                        break;
                    }
                    Err(other) => return Err(other),
                }
            }

            match rollback_err {
                None => return Ok(MultiLockHandle { handles }),
                Some(Error::Held { .. }) => {}
                Some(other) => return Err(other),
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: format!("multi:{sorted:?}"),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
