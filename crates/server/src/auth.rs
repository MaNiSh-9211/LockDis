//! Authorization, quotas, and audit (ADR 0028).
//!
//! Principals are configured statically (JSON file) with bearer tokens,
//! key-prefix grants, and per-principal quotas. The server resolves each
//! request's principal from the `authorization: Bearer <token>` header and
//! enforces:
//! - prefix-scoped permissions (`lock`, `unlock`, `extend`, `watch`, `admin`),
//! - concurrent-lock quota per principal,
//! - watcher quota per principal.
//!
//! Denials and admin actions emit structured audit lines. With no ACL file
//! the server runs in open mode (single implicit `anonymous` principal with
//! everything allowed) — right for localhost, wrong for production.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use palisade_core::Error;
use palisade_core::Result as PResult;

#[derive(serde::Deserialize, Clone, Debug)]
pub struct PrincipalConfig {
    pub name: String,
    pub token: String,
    /// Key prefixes this principal may operate on; "" = all keys.
    pub key_prefixes: Vec<String>,
    #[serde(default = "default_true")]
    pub can_lock: bool,
    #[serde(default = "default_true")]
    pub can_unlock: bool,
    #[serde(default = "default_true")]
    pub can_extend: bool,
    #[serde(default = "default_true")]
    pub can_watch: bool,
    #[serde(default)]
    pub can_admin: bool,
    /// Max concurrently held keys; 0 = unlimited.
    #[serde(default)]
    pub max_keys: u32,
    /// Max simultaneous watchers; 0 = unlimited.
    #[serde(default)]
    pub max_watchers: u32,
}

fn default_true() -> bool {
    true
}

/// Loaded ACL set. `open` mode when no file is provided.
#[derive(Clone)]
pub struct Acl {
    inner: Arc<AclInner>,
}

struct AclInner {
    open: bool,
    by_token: HashMap<String, Principal>,
}

#[derive(Clone)]
pub struct Principal {
    pub name: String,
    cfg: PrincipalConfig,
    held_keys: Arc<AtomicUsize>,
    watchers: Arc<AtomicUsize>,
}

impl Principal {
    pub fn name(&self) -> &str {
        &self.name
    }

    fn may(&self, key: &str, allowed: bool) -> Result<(), tonic::Status> {
        if self
            .cfg
            .key_prefixes
            .iter()
            .any(|p| key.starts_with(p.as_str()))
        {
            if allowed {
                return Ok(());
            }
            metrics::counter!("palisade_authz_denials_total").increment(1);
            return Err(tonic::Status::permission_denied(format!(
                "principal `{}` lacks permission for `{key}`",
                self.name
            )));
        }
        metrics::counter!("palisade_authz_denials_total").increment(1);
        Err(tonic::Status::permission_denied(format!(
            "principal `{}` may not touch keys outside its prefixes (`{key}`)",
            self.name
        )))
    }

    pub fn check_lock(&self, key: &str) -> Result<(), tonic::Status> {
        self.may(key, self.cfg.can_lock)?;
        let max = self.cfg.max_keys;
        if max > 0 && self.held_keys.load(Ordering::Acquire) >= max as usize {
            return Err(tonic::Status::resource_exhausted(format!(
                "principal `{}` hit max_keys={max}",
                self.name
            )));
        }
        Ok(())
    }

    pub fn check_unlock(&self, key: &str) -> Result<(), tonic::Status> {
        self.may(key, self.cfg.can_unlock)
    }

    pub fn check_extend(&self, key: &str) -> Result<(), tonic::Status> {
        self.may(key, self.cfg.can_extend)
    }

    pub fn check_watch(&self, key: &str) -> Result<WatcherGuard, tonic::Status> {
        self.may(key, self.cfg.can_watch)?;
        let max = self.cfg.max_watchers;
        let prev = self.watchers.fetch_add(1, Ordering::AcqRel);
        if max > 0 && prev >= max as usize {
            self.watchers.fetch_sub(1, Ordering::AcqRel);
            return Err(tonic::Status::resource_exhausted(format!(
                "principal `{}` hit max_watchers={max}",
                self.name
            )));
        }
        Ok(WatcherGuard {
            counter: self.watchers.clone(),
        })
    }

    pub fn check_admin(&self, key: &str) -> Result<(), tonic::Status> {
        self.may(key, self.cfg.can_admin)
    }

    /// Concurrent-key ceiling for this principal (0 = unlimited).
    pub fn max_keys(&self) -> u32 {
        self.cfg.max_keys
    }

    pub fn note_watcher_denial(&self) {
        self.watchers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// RAII slot for an active watcher.
pub struct WatcherGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for WatcherGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Acl {
    /// Open mode: one anonymous principal, all permissions, no quotas.
    pub fn open() -> Self {
        Self {
            inner: Arc::new(AclInner {
                open: true,
                by_token: HashMap::new(),
            }),
        }
    }

    /// Parses an ACL JSON document:
    ///
    /// ```json
    /// { "principals": [ { "name": "ci", "token": "s3cret",
    ///     "key_prefixes": ["ci/"], "max_keys": 50 } ] }
    /// ```
    pub fn from_json(json: &str) -> PResult<Self> {
        #[derive(serde::Deserialize)]
        struct Doc {
            principals: Vec<PrincipalConfig>,
        }
        let doc: Doc = serde_json::from_str(json)
            .map_err(|e| Error::InvalidConfig(format!("bad acl json: {e}")))?;
        let mut by_token = HashMap::new();
        for cfg in doc.principals {
            if cfg.token.is_empty() {
                return Err(Error::InvalidConfig(format!(
                    "principal `{}` has empty token",
                    cfg.name
                )));
            }
            by_token.insert(cfg.token.clone(), Self::make_principal(cfg));
        }
        Ok(Self {
            inner: Arc::new(AclInner {
                open: false,
                by_token,
            }),
        })
    }

    /// Loads the ACL from a JSON file.
    pub fn load_file(path: &std::path::Path) -> PResult<Self> {
        let json = std::fs::read_to_string(path)
            .map_err(|e| Error::InvalidConfig(format!("cannot read acl file {path:?}: {e}")))?;
        Self::from_json(&json)
    }

    fn make_principal(cfg: PrincipalConfig) -> Principal {
        Principal {
            name: cfg.name.clone(),
            cfg,
            held_keys: Arc::new(AtomicUsize::new(0)),
            watchers: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Resolves the caller's principal from the `authorization` header.
    /// In open mode every caller is `anonymous`.
    pub fn resolve(&self, bearer: Option<&str>) -> Result<Principal, tonic::Status> {
        if self.inner.open {
            return Ok(Self::make_principal(PrincipalConfig {
                name: "anonymous".into(),
                token: String::new(),
                key_prefixes: vec![String::new()],
                can_lock: true,
                can_unlock: true,
                can_extend: true,
                can_watch: true,
                can_admin: true,
                max_keys: 0,
                max_watchers: 0,
            }));
        }
        let token = bearer
            .and_then(|h| h.strip_prefix("Bearer "))
            .ok_or_else(|| {
                tonic::Status::unauthenticated("missing authorization: Bearer <token>")
            })?;
        self.inner.by_token.get(token).cloned().ok_or_else(|| {
            metrics::counter!("palisade_authz_denials_total").increment(1);
            tonic::Status::unauthenticated("unknown bearer token")
        })
    }

    /// Audit line helper — denials and admin ops must be attributable.
    pub fn audit(principal: &str, action: &str, key: &str, outcome: &str) {
        tracing::info!(principal, action, key, outcome, "audit");
    }
}
