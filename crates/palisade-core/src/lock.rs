//! Backend-neutral locking API.
//!
//! Backends implement [`LockManager`] (acquisition) and hand out
//! [`LockHandle`]s. The trait boundary is what keeps ADR 0003's promise
//! that etcd/Postgres adapters can be added later without API churn.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::fencing::FencingToken;
use crate::owner::OwnerId;

/// Floor for lease TTLs; below this, renewal races become unwinnable.
pub const MIN_TTL: Duration = Duration::from_millis(10);

/// Per-acquisition options.
#[derive(Clone, Debug)]
pub struct LockOptions {
    /// Lease duration. The holder must finish (or renew) within it.
    pub ttl: Duration,
    /// Watchdog auto-renewal override. `None` inherits the backend's
    /// default; `Some(true)`/`Some(false)` forces it for this acquisition.
    pub watchdog: Option<bool>,
}

impl Default for LockOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            watchdog: None,
        }
    }
}

impl LockOptions {
    /// Default options (30 s lease, backend watchdog default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the lease duration.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Forces watchdog auto-renewal on or off for this acquisition.
    pub fn with_watchdog(mut self, watchdog: bool) -> Self {
        self.watchdog = Some(watchdog);
        self
    }

    /// Rejects nonsensical configurations before they reach a backend.
    pub fn validate(&self) -> Result<()> {
        if self.ttl < MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "ttl {:?} is below the {:?} floor",
                self.ttl, MIN_TTL
            )));
        }
        Ok(())
    }
}

/// Acquires distributed locks against one storage backend.
#[async_trait]
pub trait LockManager: Send + Sync + 'static {
    /// Attempts immediate acquisition. Returns [`Error::Held`] if the lock
    /// is currently held by another owner.
    async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<Box<dyn LockHandle>>;

    /// Polls until acquired or `wait` elapses ([`Error::Timeout`]).
    async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<Box<dyn LockHandle>>;

    /// Acquires, waiting indefinitely. Prefer bounded [`Self::try_lock_for`]
    /// in production paths so stalls surface as timeouts instead of hangs.
    async fn lock(&self, key: &str, options: &LockOptions) -> Result<Box<dyn LockHandle>> {
        self.try_lock_for(key, options, Duration::MAX).await
    }
}

/// A held lock. Cheap to clone; all clones share one lease.
///
/// Dropping the last clone performs a best-effort detached release
/// (fire-and-forget); call [`Self::release`] when you need the outcome.
#[async_trait]
pub trait LockHandle: Send + Sync {
    /// The lock key.
    fn key(&self) -> &str;

    /// Identity of the current acquisition.
    fn owner(&self) -> &OwnerId;

    /// Fencing token allocated atomically with the grant. Pass this to any
    /// protected resource and have it reject operations whose token does not
    /// supersede the last accepted one (ADR 0005).
    fn fence(&self) -> FencingToken;

    /// Fast check: has the lease been lost (expired or revoked) under us?
    ///
    /// With the watchdog enabled this turns definitive renewal failure into
    /// an observable signal; without it, loss is only detected when a
    /// release/extend attempt fails.
    fn is_lost(&self) -> bool;

    /// Resolves once the lease is lost. Compose with `tokio::select!` to
    /// abort a long critical section the moment safety is in doubt.
    async fn until_lost(&self);

    /// Resets the lease TTL to `ttl`. Fails with [`Error::Lost`] if the
    /// lease already expired — the critical section must then abort.
    async fn extend(&self, ttl: Duration) -> Result<()>;

    /// Releases the lock. Idempotent: later calls succeed without effect.
    /// Returns [`Error::Lost`] if the lease expired before release ran.
    async fn release(&self) -> Result<()>;
}
