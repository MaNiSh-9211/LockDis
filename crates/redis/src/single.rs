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
use tokio::sync::watch;

use palisade_core::{Error, FencingToken, LockHandle, LockManager, LockOptions, OwnerId, Result};

use crate::config::RedisConfig;
use crate::scripts;

/// Poll cadence while waiting for a contended lock.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The watchdog renews every `RENEWAL_DIVISOR`-th of the lease; two renewal
/// opportunities must fail before a normal TTL boundary is at risk.
const RENEWAL_DIVISOR: u32 = 3;

/// Transient (backend) renewal errors tolerated before declaring the lease
/// lost. A definitive not-owner answer poisons immediately — there is
/// nothing transient about losing ownership.
const MAX_CONSECUTIVE_RENEW_FAILURES: u32 = 2;

/// Fence counters live `FENCE_TTL_MULTIPLIER ×` longer than the lease so a
/// counter never expires while its lock could still be held.
const FENCE_TTL_MULTIPLIER: u32 = 10;

/// [`LockManager`] implementation over one Redis instance.
///
/// Clone-cheap: all handles share one multiplexed connection.
#[derive(Clone)]
pub struct RedisLockManager {
    pub(crate) conn: ConnectionManager,
    default_ttl: Duration,
    watchdog_default: bool,
    pub(crate) acquire_script: Script,
    pub(crate) release_script: Script,
    pub(crate) extend_script: Script,
    pub(crate) reentrant_acquire_script: Script,
    pub(crate) reentrant_release_script: Script,
    pub(crate) reentrant_release_all_script: Script,
    pub(crate) reentrant_extend_script: Script,
    pub(crate) rw_read_acquire_script: Script,
    pub(crate) rw_read_release_script: Script,
    pub(crate) rw_write_acquire_script: Script,
    pub(crate) rw_write_release_script: Script,
    pub(crate) rw_extend_script: Script,
    pub(crate) semaphore_acquire_script: Script,
    pub(crate) semaphore_release_script: Script,
    pub(crate) semaphore_extend_script: Script,
    pub(crate) fair_acquire_script: Script,
    pub(crate) fair_release_script: Script,
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
            watchdog_default: config.watchdog(),
            acquire_script: Script::new(scripts::ACQUIRE),
            release_script: Script::new(scripts::RELEASE),
            extend_script: Script::new(scripts::EXTEND),
            reentrant_acquire_script: Script::new(scripts::REENTRANT_ACQUIRE),
            reentrant_release_script: Script::new(scripts::REENTRANT_RELEASE),
            reentrant_release_all_script: Script::new(scripts::REENTRANT_RELEASE_ALL),
            reentrant_extend_script: Script::new(scripts::REENTRANT_EXTEND),
            rw_read_acquire_script: Script::new(scripts::RW_READ_ACQUIRE),
            rw_read_release_script: Script::new(scripts::RW_READ_RELEASE),
            rw_write_acquire_script: Script::new(scripts::RW_WRITE_ACQUIRE),
            rw_write_release_script: Script::new(scripts::RW_WRITE_RELEASE),
            rw_extend_script: Script::new(scripts::RW_EXTEND),
            semaphore_acquire_script: Script::new(scripts::SEMAPHORE_ACQUIRE),
            semaphore_release_script: Script::new(scripts::SEMAPHORE_RELEASE),
            semaphore_extend_script: Script::new(scripts::SEMAPHORE_EXTEND),
            fair_acquire_script: Script::new(scripts::FAIR_ACQUIRE),
            fair_release_script: Script::new(scripts::FAIR_RELEASE),
        })
    }

    /// Attempts immediate acquisition using the backend's default lease.
    pub async fn try_lock(&self, key: &str) -> Result<RedisLockHandle> {
        self.try_lock_with(key, &LockOptions::default().with_ttl(self.default_ttl))
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

        let elapsed = started.elapsed();
        metrics::histogram!("palisade_acquire_seconds").record(elapsed.as_secs_f64());
        tracing::debug!(
            key,
            fence,
            status,
            elapsed_ms = elapsed.as_millis() as u64,
            "acquire complete"
        );

        match status {
            1 | 2 => {
                metrics::counter!("palisade_grants_total").increment(1);
                let shared = Arc::new(HandleShared {
                    conn: self.conn.clone(),
                    release_script: self.release_script.clone(),
                    extend_script: self.extend_script.clone(),
                    key: key.to_owned(),
                    fence_key,
                    token,
                    owner,
                    fence: FencingToken::new(fence as u64),
                    released: AtomicBool::new(false),
                    poisoned: AtomicBool::new(false),
                    lost: watch::channel(false).0,
                    ttl: options.ttl,
                });
                if options.watchdog.unwrap_or(self.watchdog_default) {
                    spawn_watchdog(&shared);
                }
                Ok(RedisLockHandle { shared })
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
    /// Non-acquiring existence probe: is the key currently held by anyone?
    /// Used by watch streams; never mutates state.
    pub async fn probe_held(&self, key: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let n: i64 = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("probe failed: {e}")))?;
        Ok(n > 0)
    }

    /// Admin introspection (ADR 0030): enumerate held keys under a prefix
    /// with their remaining TTLs. Uses SCAN (non-blocking) + PTTL.
    pub async fn scan_held(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let mut conn = self.conn.clone();
        let pattern = format!("{prefix}*");
        let mut out = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::Backend(format!("scan failed: {e}")))?;
            cursor = next;
            for key in batch {
                let ttl: i64 = redis::cmd("PTTL")
                    .arg(&key)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(-2);
                if ttl > 0 {
                    out.push((key, ttl as u64));
                }
            }
            if cursor == 0 {
                break;
            }
        }
        Ok(out)
    }

    /// Admin break-glass: deletes the key with NO ownership check.
    /// Caller must be authorized upstream (ACL admin + audit).
    pub async fn force_unlock(&self, key: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let n: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("force unlock failed: {e}")))?;
        Ok(n > 0)
    }

    /// Token-level release for remote callers (gRPC pass-through): runs the
    /// ownership-checked release script for an arbitrary token without a
    /// local handle. `Ok(false)` means the token no longer owns the lock.
    pub async fn unlock_with_token(&self, key: &str, token: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .release_script
            .key(key)
            .key(fence_key_for(key))
            .arg(token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("unlock failed: {e}")))?;
        Ok(ok == 1)
    }

    /// Token-level lease refresh for remote callers (gRPC pass-through).
    /// `Ok(false)` means the token no longer owns the lock.
    pub async fn extend_with_token(&self, key: &str, token: &str, ttl: Duration) -> Result<bool> {
        if ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "extend ttl {:?} is below the {:?} floor",
                ttl,
                palisade_core::MIN_TTL
            )));
        }
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .extend_script
            .key(key)
            .key(fence_key_for(key))
            .arg(token)
            .arg(ttl.as_millis() as u64)
            .arg(ttl.as_millis() as u64 * u64::from(FENCE_TTL_MULTIPLIER))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("extend failed: {e}")))?;
        Ok(ok == 1)
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
    poisoned: AtomicBool,
    lost: watch::Sender<bool>,
    ttl: Duration,
}

impl HandleShared {
    fn mark_released(&self) -> bool {
        !self.released.swap(true, Ordering::AcqRel)
    }

    /// Runs the ownership-checked extend script. `Ok(false)` means Redis
    /// says we are no longer the owner — definitive loss.
    async fn fire_extend(&self, ttl: Duration) -> Result<bool> {
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .extend_script
            .key(&self.key)
            .key(&self.fence_key)
            .arg(&self.token)
            .arg(ttl.as_millis() as u64)
            .arg(ttl.as_millis() as u64 * u64::from(FENCE_TTL_MULTIPLIER))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("extend script failed: {e}")))?;
        Ok(ok == 1)
    }

    fn mark_lost(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.lost.send_replace(true);
    }
}

/// Renews the lease every `ttl / RENEWAL_DIVISOR` until the handle is
/// released or dropped (detected via the weak reference), or ownership is
/// definitively gone — which poisons the handle and wakes `until_lost`
/// waiters instead of letting the critical section run blind.
fn spawn_watchdog(shared: &Arc<HandleShared>) {
    let weak = Arc::downgrade(shared);
    let ttl = shared.ttl;
    tokio::spawn(async move {
        let mut transient_failures = 0u32;
        loop {
            tokio::time::sleep(ttl / RENEWAL_DIVISOR).await;
            let Some(s) = weak.upgrade() else {
                return;
            };
            if s.released.load(Ordering::Acquire) {
                return;
            }
            match s.fire_extend(ttl).await {
                Ok(true) => {
                    transient_failures = 0;
                    metrics::counter!("palisade_renewals_total").increment(1);
                }
                Ok(false) => {
                    metrics::counter!("palisade_renewal_failures_total").increment(1);
                    s.mark_lost();
                    return;
                }
                Err(_) => {
                    metrics::counter!("palisade_renewal_failures_total").increment(1);
                    transient_failures += 1;
                    if transient_failures >= MAX_CONSECUTIVE_RENEW_FAILURES {
                        s.mark_lost();
                        return;
                    }
                }
            }
        }
    });
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
        if self.shared.fire_extend(ttl).await? {
            Ok(())
        } else {
            self.shared.mark_lost();
            Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            })
        }
    }

    async fn release(&self) -> Result<()> {
        if !self.shared.mark_released() {
            return Ok(());
        }
        let released_ok = self.run_release().await?;
        if released_ok == 1 {
            metrics::counter!("palisade_releases_total").increment(1);
            tracing::debug!(key = %self.shared.key, fence = self.shared.fence.value(), "released");
            Ok(())
        } else {
            Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            })
        }
    }

    fn is_lost(&self) -> bool {
        self.shared.poisoned.load(Ordering::Acquire)
    }

    async fn until_lost(&self) {
        let mut rx = self.shared.lost.subscribe();
        while !*rx.borrow_and_update() {
            // Sender dropped => last handle gone; nothing left to wait for.
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Lock and fence counter must share a cluster hash slot (Lua scripts are
/// single-slot); the `{key}` tag achieves that on both standalone and cluster.
pub(crate) fn fence_key_for(key: &str) -> String {
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
