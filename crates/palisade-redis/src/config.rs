//! Configuration for the Redis backend.

use std::time::Duration;

use palisade_core::{Error, Result};

/// Connection and default-policy settings for [`crate::RedisLockManager`].
#[derive(Clone, Debug)]
pub struct RedisConfig {
    url: String,
    default_ttl: Duration,
}

impl RedisConfig {
    /// Targets `url`, e.g. `redis://127.0.0.1:6379`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            default_ttl: Duration::from_secs(30),
        }
    }

    /// Overrides the default lease applied when callers pass
    /// [`palisade_core::LockOptions::default`].
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
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
