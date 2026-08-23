//! Configuration for the Redis backend.

use std::time::Duration;

use palisade_core::{Error, Result};

/// Connection and default-policy settings for [`crate::RedisLockManager`].
#[derive(Clone, Debug)]
pub struct RedisConfig {
    url: String,
    default_ttl: Duration,
    watchdog: bool,
    response_timeout: Duration,
}

impl RedisConfig {
    /// Targets `url`, e.g. `redis://127.0.0.1:6379`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            default_ttl: Duration::from_secs(30),
            watchdog: false,
            response_timeout: Duration::from_secs(5),
        }
    }

    /// Overrides the default lease applied when callers pass
    /// [`palisade_core::LockOptions::default`].
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Bounds how long any single Redis command may hang before the
    /// connection recycles. Default 5s — a blackholed store must never
    /// wedge callers indefinitely.
    pub fn with_response_timeout(mut self, t: Duration) -> Self {
        self.response_timeout = t;
        self
    }

    /// Enables watchdog auto-renewal by default (per-acquisition options
    /// can still override, see [`palisade_core::LockOptions::with_watchdog`]).
    pub fn with_watchdog(mut self, watchdog: bool) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// The configured endpoint URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The configured default lease duration.
    pub fn default_ttl(&self) -> Duration {
        self.default_ttl
    }

    /// The configured watchdog default.
    pub fn watchdog(&self) -> bool {
        self.watchdog
    }

    /// The configured per-command response timeout.
    pub fn response_timeout(&self) -> Duration {
        self.response_timeout
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.url.trim().is_empty() {
            return Err(Error::InvalidConfig("redis url is empty".into()));
        }
        if self.default_ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "default ttl {:?} is below the {:?} floor",
                self.default_ttl,
                palisade_core::MIN_TTL
            )));
        }
        Ok(())
    }
}
