//! INV-6 · Semantic Locks: business predicates evaluated atomically inside
//! the grant script. Zero TOCTOU window between condition check and lock.

use std::time::Duration;

use palisade_core::{Error, Result};

use crate::single::{RedisLockManager, fence_key_for};

/// A single predicate on a protected hash field.
#[derive(Clone, Debug)]
pub enum Predicate {
    /// HGET field == value
    Equals { field: String, value: String },
    /// numeric HGET field > value
    FieldGt { field: String, value: f64 },
    /// field must NOT exist in hash
    Absent { field: String },
}

impl Predicate {
    pub(crate) fn to_lua(&self) -> String {
        match self {
            Self::Equals { field, value } => {
                format!(
                    "(redis.call('HGET', KEYS[3], '{}') == '{}')",
                    esc(field),
                    esc(value)
                )
            }
            Self::FieldGt { field, value } => format!(
                "((tonumber(redis.call('HGET', KEYS[3], '{}')) or -999999) > {})",
                esc(field),
                value
            ),
            Self::Absent { field } => {
                format!("(redis.call('HEXISTS', KEYS[3], '{}') == 0)", esc(field))
            }
        }
    }
}

fn esc(s: &str) -> String {
    s.replace('\'', "\\'")
}

/// Builder for semantic acquisitions.
pub struct SemanticGuard<'a> {
    mgr: &'a RedisLockManager,
    key: String,
    preds: Vec<Predicate>,
    ttl: Duration,
}

impl<'a> SemanticGuard<'a> {
    pub(crate) fn new(mgr: &'a RedisLockManager, key: impl Into<String>) -> Self {
        Self {
            mgr,
            key: key.into(),
            preds: Vec::new(),
            ttl: Duration::from_secs(30),
        }
    }

    /// Adds a raw predicate.
    pub fn where_(mut self, p: Predicate) -> Self {
        self.preds.push(p);
        self
    }

    /// Hash field must equal value.
    pub fn field_equals(mut self, f: &str, v: &str) -> Self {
        self.preds.push(Predicate::Equals {
            field: f.into(),
            value: v.into(),
        });
        self
    }

    /// Numeric hash field > threshold.
    pub fn field_gt(mut self, f: &str, v: f64) -> Self {
        self.preds.push(Predicate::FieldGt {
            field: f.into(),
            value: v,
        });
        self
    }

    /// Field must not exist.
    pub fn field_absent(mut self, f: &str) -> Self {
        self.preds.push(Predicate::Absent { field: f.into() });
        self
    }

    /// Sets lease duration.
    pub fn ttl(mut self, d: Duration) -> Self {
        self.ttl = d;
        self
    }

    /// Executes the atomic grant.
    pub async fn acquire(self) -> Result<crate::single::RedisLockHandle> {
        if self.preds.is_empty() {
            return Err(Error::InvalidConfig(
                "at least one predicate required".into(),
            ));
        }
        let data_key = format!("{}:data", self.key);
        let fence_key = fence_key_for(&self.key);
        let conditions: Vec<String> = self.preds.iter().map(|p| p.to_lua()).collect();
        let combined = conditions.join(" and ");

        let owner = palisade_core::OwnerId::generate();
        let token = owner.as_uuid().to_string();
        let ttl_ms = self.ttl.as_millis() as u64;

        let src = format!(
            r#"
if redis.call('EXISTS', KEYS[3]) == 0 then return {{0, 0}} end
if redis.call('EXISTS', KEYS[1]) == 1 then return {{0, 0}} end
if not ({combined}) then return {{0, 0}} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {{1, redis.call('INCR', KEYS[2])}}
"#
        );

        let mut conn = self.mgr.conn.clone();
        let script = redis::Script::new(&src);
        let (status, fence): (i64, i64) = script
            .key(&self.key)
            .key(&fence_key)
            .key(&data_key)
            .arg(&token)
            .arg(ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("semantic acquire failed: {e}")))?;

        if status != 1 {
            return Err(Error::Held {
                key: self.key.clone(),
            });
        }
        Ok(self
            .mgr
            .build_handle_from_parts(&self.key, token, owner, fence as u64, self.ttl))
    }
}

impl RedisLockManager {
    /// Starts a semantic acquisition on `key`.
    pub fn acquire_where(&self, key: &str) -> SemanticGuard<'_> {
        SemanticGuard::new(self, key)
    }
}
