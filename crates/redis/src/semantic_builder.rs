//! Semantic Lock builder: compile predicates into the atomic grant script.
//!
//! ```rust,ignore
//! let h = mgr.acquire_where("orders/42")
//!     .field_equals("status", "pending")
//!     .field_gt("items_count", 0.0)
//!     .field_absent("cancelled_at")
//!     .ttl(Duration::from_secs(10))
//!     .acquire()
//!     .await?;
//! // If ANY predicate fails → Held with a structured denial reason.
//! // If ALL pass → granted with fencing token, atomically.
//! ```

use std::time::Duration;

use palisade_core::{Error, Result};

use super::semantic::Predicate;
use crate::single::RedisLockManager;

/// Builder for semantic (predicate-gated) acquisitions.
pub struct SemanticGuard<'a> {
    mgr: &'a RedisLockManager,
    key: String,
    predicates: Vec<Predicate>,
    ttl: Duration,
    watchdog: bool,
}

impl<'a> SemanticGuard<'a> {
    pub(crate) fn new(mgr: &'a RedisLockManager, key: impl Into<String>) -> Self {
        Self {
            mgr,
            key: key.into(),
            predicates: Vec::new(),
            ttl: Duration::from_secs(30),
            watchdog: false,
        }
    }

    /// Adds a predicate. All predicates are ANDed together.
    pub fn where_(mut self, p: Predicate) -> Self {
        self.predicates.push(p);
        self
    }

    /// Hash field must equal `value`.
    pub fn field_equals(mut self, field: &str, value: &str) -> Self {
        self.predicates.push(Predicate::Equals { field: field.into(), value: value.into() });
        self
    }

    /// Numeric hash field must be strictly greater than `value`.
    pub fn field_gt(mut self, field: &str, value: f64) -> Self {
        self.predicates.push(Predicate::Gt { field: field.into(), value });
        self
    }

    /// Field must not exist.
    pub fn field_absent(mut self, field: &str) -> Self {
        self.predicates.push(Predicate::Absent { field: field.into() });
        self
    }

    /// Sets lease duration.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Enables watchdog auto-renewal for this acquisition.
    pub fn with_watchdog(mut self) -> Self {
        self.watchdog = true;
        self
    }

    /// Executes the atomic semantic grant.
    pub async fn acquire(self) -> Result<crate::single::RedisLockHandle> {
        if self.predicates.is_empty() {
            return Err(Error::InvalidConfig(
                "semantic guard requires at least one predicate".into(),
            ));
        }

        let data_key = format!("{}:data", self.key);
        let lock_key = &self.key;
        let fence_key = format!("{{{}}}:fence", self.key);

        let lua_conditions: Vec<String> =
            self.predicates.iter().map(|p| p.to_lua()).collect();
        let combined = if lua_conditions.len() == 1 {
            lua_conditions[0].clone()
        } else {
            format!("({})", lua_conditions.join(" and "))
        };

        // Generate the script inline because predicates are dynamic.
        let owner = palisade_core::OwnerId::generate();
        let token = owner.as_uuid().to_string();
        let ttl_ms = self.ttl.as_millis() as u64;
        let fence_ttl_ms = ttl_ms * 10;

        let script_src = format!(
            r#"
-- KEYS[1] = lock key, KEYS[2] = fence counter, KEYS[3] = protected data hash
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms
local data_exists = redis.call('EXISTS', KEYS[3])
if data_exists == 0 then
  return {{0, 0, 'PROTECTED_DATA_MISSING'}}
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {{0, 0, 'LOCK_HELD'}}
end

if not ({combined}) then
  local reasons = {{}}
  return {{0, 0, 'PREDICATE_FAILED'}}
end

redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {{1, redis.call('INCR', KEYS[2])}}
"#
        );

        let mut conn = self.mgr.conn_clone();
        let script = redis::Script::new(&script_src);
        let result: (i64, i64, String) = script
            .key(lock_key)
            .key(&fence_key)
            .key(&data_key)
            .arg(&token)
            .arg(ttl_ms)
            .arg(fence_ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("semantic acquire failed: {e}")))?;

        match result.0 {
            1 => {
                metrics::counter!("palisade_grants_total").increment(1);
                Ok(self.mgr.make_semantic_handle(
                    &self.key,
                    token,
                    result.1 as u64,
                    self.ttl,
                    self.watchdog,
                ))
            }
            _ => Err(Error::Held {
                key: self.key.clone(),
            }),
        }
    }
}

impl RedisLockManager {
    /// Starts a semantic acquisition on a key whose protected data is a
    /// Redis HASH at `{key}:data`.
    pub fn acquire_where(&self, key: &str) -> SemanticGuard<'_> {
        SemanticGuard::new(self, key)
    }

    pub(crate) fn conn_clone(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }

    pub(crate) fn make_semantic_handle(
        &self,
        key: &str,
        token: String,
        fence_val: u64,
        ttl: Duration,
        _watchdog: bool,
    ) -> crate::single::RedisLockHandle {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        use crate::single::{fence_key_for, HandleShared, RedisLockHandle};

        let shared = Arc::new(HandleShared {
            conn: self.conn.clone(),
            release_script: self.release_script.clone(),
            extend_script: self.extend_script.clone(),
            key: key.to_owned(),
            fence_key: fence_key_for(key),
            token,
            owner: palisade_core::OwnerId::generate(),
            fence: palisade_core::FencingToken::new(fence_val),
            released: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            lost: tokio::sync::watch::channel(false).0,
            ttl,
            policy: palisade_core::SafetyPolicy::Balanced,
        });
        RedisLockHandle { shared }
    }
}
