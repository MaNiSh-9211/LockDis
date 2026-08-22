//! etcd backend for Palisade: consensus-correct locking (ADR 0026).
//!
//! - Grant: `txn{ create_revision(key) == 0 â†’ put(key, token, lease) }`.
//!   Fence token = the transaction's MVCC revision â€” globally monotonic
//!   and linearizable, allocated by Raft itself. No allocator node.
//! - Liveness: the lock key carries an etcd lease; the *server* expires
//!   it when keepalives stop. A crashed client's lock dies server-side.
//! - Release: `txn{ value(key) == token â†’ delete }`, then lease revoke.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};

use palisade_core::{Error, FencingToken, LockHandle, LockManager, LockOptions, OwnerId, Result};

/// Poll cadence while waiting for a contended key.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Connection settings for the etcd backend.
#[derive(Clone, Debug)]
pub struct EtcdConfig {
    endpoints: Vec<String>,
    default_ttl: Duration,
}

impl EtcdConfig {
    /// Targets cluster endpoints, e.g. `["http://127.0.0.1:2379"]`.
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            default_ttl: Duration::from_secs(30),
        }
    }

    /// Overrides the default lease applied when callers pass default options.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.endpoints.is_empty() || self.endpoints.iter().any(|e| e.trim().is_empty()) {
            return Err(Error::InvalidConfig("etcd endpoints are empty".into()));
        }
        Ok(())
    }

    /// Lease TTLs are whole seconds in etcd; clamp upward from the floor.
    fn lease_secs(ttl: Duration) -> i64 {
        (ttl.as_secs_f64().ceil() as i64).max(1)
    }
}

/// [`LockManager`] over an etcd cluster.
#[derive(Clone)]
pub struct EtcdLockManager {
    client: Client,
    default_ttl: Duration,
}

impl std::fmt::Debug for EtcdLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdLockManager").finish()
    }
}

impl EtcdLockManager {
    /// Connects to the cluster. All endpoints are used for discovery.
    pub async fn connect(config: EtcdConfig) -> Result<Self> {
        config.validate()?;
        let client = Client::connect(config.endpoints.clone(), None)
            .await
            .map_err(|e| Error::Backend(format!("etcd connect failed: {e}")))?;
        Ok(Self {
            client,
            default_ttl: config.default_ttl,
        })
    }

    /// Attempts immediate acquisition using the backend's default lease.
    pub async fn try_lock(&self, key: &str) -> Result<EtcdLockHandle> {
        self.try_lock_with(key, &LockOptions::default().with_ttl(self.default_ttl))
            .await
    }

    /// Attempts immediate acquisition with explicit options.
    pub async fn try_lock_with(&self, key: &str, options: &LockOptions) -> Result<EtcdLockHandle> {
        options.validate()?;
        let started = Instant::now();

        let lease_secs = EtcdConfig::lease_secs(options.ttl);
        let mut lease_client = self.client.lease_client();
        let grant = lease_client
            .grant(lease_secs, None)
            .await
            .map_err(|e| Error::Backend(format!("lease grant failed: {e}")))?;
        let lease_id = grant.id();

        let owner = OwnerId::generate();
        let token = owner.as_uuid().to_string();

        let cmp = Compare::create_revision(key, CompareOp::Equal, 0);
        let put = TxnOp::put(
            key,
            token.as_str(),
            Some(PutOptions::new().with_lease(lease_id)),
        );
        let txn = Txn::new().when(vec![cmp]).and_then(vec![put]);

        let mut kv = self.client.kv_client();
        let txn_response = match kv.txn(txn).await {
            Ok(r) => r,
            Err(e) => {
                let _ = lease_client.revoke(lease_id).await;
                return Err(Error::Backend(format!("acquire txn failed: {e}")));
            }
        };

        if !txn_response.succeeded() {
            let _ = lease_client.revoke(lease_id).await;
            return Err(Error::Held {
                key: key.to_owned(),
            });
        }

        // The granting transaction's revision IS our fence: globally
        // monotonic under Raft, strictly increasing per key across grants.
        let revision = txn_response.header().map(|h| h.revision()).unwrap_or(0);
        let fence = FencingToken::new(revision as u64);
        metrics::counter!("palisade_grants_total").increment(1);
        tracing::debug!(
            key,
            fence = fence.value(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "acquired via etcd"
        );

        Ok(self.make_handle(
            key.to_owned(),
            token,
            owner,
            fence,
            lease_id,
            options.ttl,
            options.watchdog.unwrap_or(false),
        ))
    }

    /// Polls until acquired or `wait` elapses.
    pub async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<EtcdLockHandle> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match self.try_lock_with(key, options).await {
                Ok(h) => return Ok(h),
                Err(Error::Held { .. }) => {}
                Err(other) => return Err(other),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: key.to_owned(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Remaining seconds for an etcd lease (-1 when already gone).
    /// Ops/debugging helper — also exposes any server-side TTL floor.
    pub async fn lease_time_to_live(&self, lease_id: i64) -> Result<i64> {
        let mut lease = self.client.lease_client();
        let resp = lease
            .time_to_live(lease_id, None)
            .await
            .map_err(|e| Error::Backend(format!("time_to_live failed: {e}")))?;
        Ok(resp.ttl())
    }

    #[allow(clippy::too_many_arguments)]
    fn make_handle(
        &self,
        key: String,
        token: String,
        owner: OwnerId,
        fence: FencingToken,
        lease_id: i64,
        ttl: Duration,
        watchdog: bool,
    ) -> EtcdLockHandle {
        let shared = Arc::new(HandleShared {
            kv: self.client.kv_client(),
            lease: self.client.lease_client(),
            key,
            token,
            owner,
            fence,
            lease_id,
            gone: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            lost: tokio::sync::watch::channel(false).0,
        });
        if watchdog {
            spawn_watchdog(&shared, ttl);
        }
        EtcdLockHandle { shared }
    }
}

#[async_trait]
impl LockManager for EtcdLockManager {
    async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(
            EtcdLockManager::try_lock_with(self, key, options).await?,
        ))
    }

    async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(
            EtcdLockManager::try_lock_for(self, key, options, wait).await?,
        ))
    }
}

struct HandleShared {
    kv: etcd_client::KvClient,
    lease: etcd_client::LeaseClient,
    key: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    lease_id: i64,
    gone: AtomicBool,
    poisoned: AtomicBool,
    lost: tokio::sync::watch::Sender<bool>,
}

impl HandleShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }

    fn mark_lost(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.lost.send_replace(true);
    }

    async fn release_inner(&self) -> Result<bool> {
        let cmp = Compare::value(self.key.as_str(), CompareOp::Equal, self.token.as_str());
        let del = TxnOp::delete(self.key.as_str(), None);
        let txn = Txn::new().when(vec![cmp]).and_then(vec![del]);
        let mut kv = self.kv.clone();
        let response = kv
            .txn(txn)
            .await
            .map_err(|e| Error::Backend(format!("release txn failed: {e}")))?;

        if !response.succeeded() {
            return Ok(false);
        }
        let mut lease = self.lease.clone();
        if let Err(e) = lease.revoke(self.lease_id).await {
            tracing::warn!(lease_id = self.lease_id, error = %e, "lease revoke after release failed");
        }
        Ok(true)
    }
}

impl Drop for HandleShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let shared = HandleShared {
                kv: self.kv.clone(),
                lease: self.lease.clone(),
                key: self.key.clone(),
                token: self.token.clone(),
                owner: self.owner.clone(),
                fence: self.fence,
                lease_id: self.lease_id,
                gone: AtomicBool::new(true),
                poisoned: AtomicBool::new(false),
                lost: tokio::sync::watch::channel(true).0,
            };
            std::mem::drop(runtime.spawn(async move {
                let _ = shared.release_inner().await;
            }));
        }
    }
}

/// Server-side liveness means the watchdog only *accelerates* safety: etcd
/// would expire the lease anyway; we just refresh at ttl/3 so long critical
/// sections survive, and poison the handle the moment the server disagrees.
fn spawn_watchdog(shared: &Arc<HandleShared>, ttl: Duration) {
    let weak = Arc::downgrade(shared);
    let cadence = ttl / 3;
    let lease_id = shared.lease_id;
    tokio::spawn(async move {
        // One long-lived keepalive stream for the whole handle lifetime.
        let Some(s) = weak.upgrade() else { return };
        let mut lease = s.lease.clone();
        drop(s);
        let pair = lease.keep_alive(lease_id).await;
        let (mut keeper, mut stream) = match pair {
            Ok(pair) => pair,
            Err(_) => {
                if let Some(s) = weak.upgrade() {
                    s.mark_lost();
                }
                return;
            }
        };

        loop {
            tokio::time::sleep(cadence).await;
            let Some(s) = weak.upgrade() else { return };
            let gone = s.gone.load(Ordering::Acquire);
            drop(s);
            if gone {
                return;
            }
            if keeper.keep_alive().await.is_err() {
                continue;
            }
            match stream.message().await {
                Ok(Some(resp)) if resp.ttl() > 0 => {}
                _ => {
                    if let Some(s) = weak.upgrade() {
                        s.mark_lost();
                    }
                    return;
                }
            }
        }
    });
}

/// A lock held on an etcd cluster.
#[derive(Clone)]
pub struct EtcdLockHandle {
    shared: Arc<HandleShared>,
}

impl std::fmt::Debug for EtcdLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdLockHandle")
            .field("key", &self.shared.key)
            .field("fence", &self.shared.fence)
            .field("lease_id", &self.shared.lease_id)
            .finish()
    }
}

impl EtcdLockHandle {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Identity of this acquisition.
    pub fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    /// Fence token â€” the granting transaction's MVCC revision.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Underlying etcd lease id (ops/debugging).
    pub fn lease_id(&self) -> i64 {
        self.shared.lease_id
    }

    /// Releases the key (ownership-checked) and revokes the lease.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        if self.shared.release_inner().await? {
            metrics::counter!("palisade_releases_total").increment(1);
            Ok(())
        } else {
            self.shared.mark_lost();
            Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            })
        }
    }

    /// Server-authoritative liveness makes explicit extension unnecessary
    /// (the watchdog keeps the lease alive); retained for API parity.
    pub async fn extend(&self, _ttl: Duration) -> Result<()> {
        if self.shared.gone.load(Ordering::Acquire) || self.shared.poisoned.load(Ordering::Acquire)
        {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        let mut lease = self.shared.lease.clone();
        let pair = lease.keep_alive(self.shared.lease_id).await;
        match pair {
            Ok((mut keeper, mut stream)) => {
                if keeper.keep_alive().await.is_err() {
                    return Err(Error::Backend("keepalive send failed".into()));
                }
                match stream.message().await {
                    Ok(Some(resp)) if resp.ttl() > 0 => Ok(()),
                    _ => {
                        self.shared.mark_lost();
                        Err(Error::Lost {
                            key: self.shared.key.clone(),
                            fence: self.shared.fence.value(),
                        })
                    }
                }
            }
            Err(e) => Err(Error::Backend(format!("keepalive failed: {e}"))),
        }
    }

    /// Fast check whether the server has declared this lease dead.
    pub fn is_lost(&self) -> bool {
        self.shared.poisoned.load(Ordering::Acquire)
    }

    /// Resolves when the server-side lease is known dead.
    pub async fn until_lost(&self) {
        let mut rx = self.shared.lost.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[async_trait]
impl LockHandle for EtcdLockHandle {
    fn key(&self) -> &str {
        EtcdLockHandle::key(self)
    }

    fn owner(&self) -> &OwnerId {
        EtcdLockHandle::owner(self)
    }

    fn fence(&self) -> FencingToken {
        EtcdLockHandle::fence(self)
    }

    fn is_lost(&self) -> bool {
        EtcdLockHandle::is_lost(self)
    }

    async fn until_lost(&self) {
        EtcdLockHandle::until_lost(self).await
    }

    async fn extend(&self, ttl: Duration) -> Result<()> {
        EtcdLockHandle::extend(self, ttl).await
    }

    async fn release(&self) -> Result<()> {
        EtcdLockHandle::release(self).await
    }
}
