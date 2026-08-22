//! CountDownLatch: one-shot coordination barrier (Redisson parity).
//!
//! N parties count down; waiters block until the count reaches zero.
//! The count never goes negative, and initialization is atomic-racy-safe
//! (`NX` semantics): whichever racer initializes wins, others adopt.

use std::time::Duration;

use redis::Script;

use palisade_core::{Error, Result};

use crate::single::POLL_INTERVAL;

/// Scripts: init uses NX so concurrent initializers agree on one count.
const LATCH_INIT: &str = r"
if redis.call('SET', KEYS[1], ARGV[1], 'NX') then
  return 1
end
return 0
";

const LATCH_COUNT_DOWN: &str = r"
local v = tonumber(redis.call('GET', KEYS[1]) or '-1')
if v < 0 then
  return -1
end
if v == 0 then
  return 0
end
return redis.call('DECR', KEYS[1]) + 1
";

const LATCH_GET: &str = r"
local v = tonumber(redis.call('GET', KEYS[1]) or '-1')
if v < 0 then
  return -1
end
return v
";

/// A countdown latch bound to one key.
#[derive(Clone)]
pub struct RedisCountDownLatch {
    manager: crate::single::RedisLockManager,
    key: String,
}

impl std::fmt::Debug for RedisCountDownLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCountLatch")
            .field("key", &self.key)
            .finish()
    }
}

impl RedisCountDownLatch {
    /// Creates the latch if absent (idempotent; first initializer wins).
    pub async fn create(
        manager: &crate::single::RedisLockManager,
        key: &str,
        initial_count: u32,
    ) -> Result<Self> {
        let mut conn = manager.conn.clone();
        let script = Script::new(LATCH_INIT);
        let created: i64 = script
            .key(key)
            .arg(initial_counts(initial_count)?)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("latch init failed: {e}")))?;
        metrics::counter!("palisade_latches_created_total").increment(created as u64);
        Ok(Self {
            manager: manager.clone(),
            key: key.to_owned(),
        })
    }

    /// The latch key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Decrements by one; returns the remaining count. Idempotent at zero.
    pub async fn count_down(&self) -> Result<u64> {
        self.eval_remaining(LATCH_COUNT_DOWN).await
    }

    /// Current remaining count.
    pub async fn count(&self) -> Result<u64> {
        self.eval_remaining(LATCH_GET).await
    }

    /// Polls until the count hits zero or `timeout` elapses.
    pub async fn wait_until_zero(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count().await? == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: self.key.clone(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn eval_remaining(&self, lua: &str) -> Result<u64> {
        let mut conn = self.manager.conn.clone();
        let script = Script::new(lua);
        let v: i64 = script
            .key(&self.key)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("latch op failed: {e}")))?;
        if v < 0 {
            return Err(Error::Lost {
                key: self.key.clone(),
                fence: 0,
            });
        }
        Ok(v as u64)
    }
}

fn initial_counts(n: u32) -> Result<String> {
    if n == 0 {
        return Err(Error::InvalidConfig(
            "latch initial count must be at least 1".into(),
        ));
    }
    Ok(n.to_string())
}
