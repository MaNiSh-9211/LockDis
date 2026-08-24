//! INV-3 · Lock Testament: deathbed state transfer.
//!
//! A holder may store arbitrary payload bytes bound to its lock+token. The
//! will is readable ONLY after a successor acquires the same key, enabling
//! leaders to pass state across death boundaries:
//!
//! ```text
//! leader-A: set_will(key, b"checkpoint=4172,inflight=x")
//! leader-A crashes (no release)
//! leader-B acquires → read_will(key) → Some(b"checkpoint=4172,…")
//! ```
//!
//! The will dies with its owner: explicit release deletes it, and the
//! storage TTL matches the lease so it never outlives the epoch.

use std::time::Duration;

use redis::Script;

use palisade_core::{Error, Result};

use crate::single::RedisLockManager;

/// Sets/overwrites the will for a lock the caller currently owns.
const WILL_SET: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0
";

/// Deletes the will on graceful release (ownership-checked).
const WILL_DEL: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
";

impl RedisLockManager {
    /// Stores `payload` as the dying declaration for `key`. Only succeeds
    /// while `token` still owns the lock. Will TTL mirrors the lease.
    pub async fn set_testament(
        &self,
        key: &str,
        token: &str,
        ttl: Duration,
        payload: &[u8],
    ) -> Result<()> {
        let mut conn = self.conn.clone();
        let script = Script::new(WILL_SET);
        let ok: i64 = script
            .key(key)
            .key(format!("{key}:will"))
            .arg(token)
            .arg(payload)
            .arg(ttl.as_millis() as u64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("testament set failed: {e}")))?;
        if ok != 1 {
            return Err(Error::Lost {
                key: key.to_owned(),
                fence: 0,
            });
        }
        Ok(())
    }

    /// Successor reads the previous holder's testament AFTER acquiring.
    /// Returns `None` when the predecessor released gracefully or no will
    /// was ever written. Payload bytes are opaque to Palisade.
    pub async fn read_testament(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let v: Option<Vec<u8>> = redis::cmd("GET")
            .arg(format!("{key}:will"))
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("testament read failed: {e}")))?;
        Ok(v)
    }

    /// Removes the will (called internally on graceful release paths).
    pub async fn clear_testament(&self, key: &str, token: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let script = Script::new(WILL_DEL);
        let _: i64 = script
            .key(key)
            .key(format!("{key}:will"))
            .arg(token)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("testament clear failed: {e}")))?;
        Ok(())
    }
}
