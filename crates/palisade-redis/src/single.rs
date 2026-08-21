//! Single-instance Redis mutex: the foundation everything else builds on.
//!
//! Safety argument (see PLAN.md §4.1): grant, fence allocation, release,
//! and extension are each a single Lua script, so no interleaving of Redis
//! commands can produce two holders or a stale-token release. The lease TTL
//! bounds how long a crashed holder can block progress; fencing tokens
//! cover the pause-past-TTL case that TTLs alone cannot.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;

use palisade_core::{Error, FencingToken, LockHandle, LockManager, LockOptions, OwnerId, Result};

use crate::config::RedisConfig;
use crate::scripts;

/// Poll cadence while waiting for a contended lock.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Fence counters live `FENCE_TTL_MULTIPLIER ×` longer than the lease so a
/// counter never expires while its lock could still be held.
const FENCE_TTL_MULTIPLIER: u32 = 10;

/// [`LockManager`] implementation over one Redis instance.
///
/// Clone-cheap: all handles share one multiplexed connection.
#[derive(Clone)]
pub struct RedisLockManager {
    conn: ConnectionManager,
    default_ttl: Duration,
    acquire_script: Script,
    release_script: Script,
    extend_script: Script,
}

impl RedisLockManager {
    /// Connects and pre-parses the Lua scripts.
    pub async fn connect(config: RedisConfig) -> Result<Self> {
        config.validate()?;
        let client = redis::Client::open(config.url())
            .map_err(|e| Error::InvalidConfig(format!("bad redis url `{}`: {e}", config.url())))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| Error::Backend(format!("connect {}: {e}", config.url())))?;
        Ok(Self {
            conn,
            default_ttl: config.default_ttl(),
            acquire_script: Script::new(scripts::ACQUIRE),
            release_script: Script::new(scripts::RELEASE),
            extend_script: Script::new(scripts::EXTEND),
        })
    }

    /// Attempts immediate acquisition using the backend's default lease.
    pub async fn try_lock(&self, key: &str) -> Result<RedisLockHandle> {
        self.try_lock_with(
            key,
            &LockOptions {
                ttl: self.default_ttl,
            },
        )
        .await
    }

    /// Attempts immediate acquisition with explicit options.
    pub async fn try_lock_with(&self, key: &str, options: &LockOptions) -> Result<RedisLockHandle> {
        options.validate()?;
        let started = Instant::now();

        let owner = OwnerId::generate();
        let token = owner.as_uuid().to_string();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_ttl_ms = ttl_ms * u64::from(FENCE_TTL_MULTIPLIER);
        let fence_key = fence_key_for(key);

        let mut conn = self.conn.clone();
        let (status, fence): (i64, i64) = self
            .acquire_script
            .key(key)
            .key(&fence_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(fence_ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("acquire script failed: {e}")))?;

        metrics::histogram!("palisade_acquire_seconds").record(started.elapsed().as_secs_f64());

        match status {
            1 | 2 => {
                metrics::counter!("palisade_grants_total").increment(1);
                Ok(RedisLockHandle {
                    shared: Arc::new(HandleShared {
                        conn: self.conn.clone(),
                        release_script: self.release_script.clone(),
                        extend_script: self.extend_script.clone(),
                        key: key.to_owned(),
                        fence_key,
                        token,
                        owner,
                        fence: FencingToken::new(fence as u64),
                        released: AtomicBool::new(false),
                    }),
                })
            }
            _ => Err(Error::Held {
                key: key.to_owned(),
            }),
        }
    }

    /// Polls until acquired or `wait` elapses.
    pub async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<RedisLockHandle> {
        let deadline = Instant::now().checked_add(wait);
        loop {
            match self.try_lock_with(key, options).await {
                Ok(handle) => return Ok(handle),
                Err(Error::Held { .. }) => {}
                Err(other) => return Err(other),
            }
            let sleep_for = match deadline {
                Some(d) => {
                    let remaining = d.checked_duration_since(Instant::now()).unwrap_or_default();
                    if remaining.is_zero() {
                        return Err(Error::Timeout {
                            key: key.to_owned(),
                        });
                    }
                    POLL_INTERVAL.min(remaining)
                }
                None => POLL_INTERVAL,
            };
            tokio::time::sleep(sleep_for).await;
        }
    }
}

#[async_trait]
impl LockManager for RedisLockManager {
    async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(self.try_lock_with(key, options).await?))
    }

    async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(self.try_lock_for(key, options, wait).await?))
    }
}

/// A held lock backed by one Redis instance.
#[derive(Clone)]
pub struct RedisLockHandle {
    shared: Arc<HandleShared>,
}

impl std::fmt::Debug for RedisLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisLockHandle")
            .field("key", &self.shared.key)
            .field("owner", &self.shared.owner)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

struct HandleShared {
    conn: ConnectionManager,
    release_script: Script,
    extend_script: Script,
    key: String,
    fence_key: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    released: AtomicBool,
}

impl HandleShared {
    fn mark_released(&self) -> bool {
        !self.released.swap(true, Ordering::AcqRel)
    }
}

impl Drop for HandleShared {
    fn drop(&mut self) {
        if !self.mark_released() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            // Dropping the JoinHandle detaches the task by design: the
            // release must outlive this drop, fire-and-forget.
            std::mem::drop(runtime.spawn(detached_release(
                self.conn.clone(),
                self.release_script.clone(),
                self.key.clone(),
                self.fence_key.clone(),
                self.token.clone(),
            )));
        }
    }
}

async fn detached_release(
    mut conn: ConnectionManager,
    script: Script,
    key: String,
    fence_key: String,
    token: String,
) {
    let _: std::result::Result<i64, redis::RedisError> = script
        .key(&key)
        .key(&fence_key)
        .arg(&token)
        .invoke_async(&mut conn)
        .await;
}

impl RedisLockHandle {
    async fn run_release(&self) -> Result<i64> {
        let mut conn = self.shared.conn.clone();
        self.shared
            .release_script
            .key(&self.shared.key)
            .key(&self.shared.fence_key)
            .arg(&self.shared.token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("release script failed: {e}")))
    }
}

#[async_trait]
impl LockHandle for RedisLockHandle {
    fn key(&self) -> &str {
        &self.shared.key
    }

    fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    async fn extend(&self, ttl: Duration) -> Result<()> {
        if ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "extend ttl {:?} is below the {:?} floor",
                ttl,
                palisade_core::MIN_TTL
            )));
        }
        if self.shared.released.load(Ordering::Acquire) {
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
            .key(&self.shared.fence_key)
            .arg(&self.shared.token)
            .arg(ttl.as_millis() as u64)
            .arg(ttl.as_millis() as u64 * u64::from(FENCE_TTL_MULTIPLIER))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("extend script failed: {e}")))?;
        if ok != 1 {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        Ok(())
    }

    async fn release(&self) -> Result<()> {
        if !self.shared.mark_released() {
            return Ok(());
        }
        let released = self.run_release().await?;
        if released == 1 {
            metrics::counter!("palisade_releases_total").increment(1);
            Ok(())
        } else {
            Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            })
        }
    }
}

/// Lock and fence counter must share a cluster hash slot (Lua scripts are
/// single-slot); the `{key}` tag achieves that on both standalone and cluster.
fn fence_key_for(key: &str) -> String {
    format!("{{{key}}}:fence")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_key_shares_hash_slot() {
        assert_eq!(fence_key_for("orders:42"), "{orders:42}:fence");
    }
}
