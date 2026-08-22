//! Redlock: quorum-based locking across N independent Redis masters.
//!
//! Safety model (ADR 0020): a grant requires `quorum = n/2 + 1` nodes to
//! accept within the lease budget, so two holders cannot both reach quorum
//! for the same key. Fence tokens come from ONE dedicated allocator node â€”
//! per-node counters are not comparable, and a single linearizable counter
//! is what makes stale-quorum writes rejectable downstream.
//!
//! Topology requirement: nodes must be INDEPENDENT masters. Replicated or
//! cluster setups defeat the algorithm (async replication can double-grant).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;
use tokio::sync::watch;

use palisade_core::{Error, FencingToken, LockHandle, LockManager, LockOptions, OwnerId, Result};

use crate::scripts;
use crate::single::POLL_INTERVAL;

/// Renewal cadence divisor and transient-failure tolerance, mirroring the
/// single-instance watchdog (ADR 0013).
const RENEWAL_DIVISOR: u32 = 3;
const MAX_CONSECUTIVE_RENEW_FAILURES: u32 = 2;

/// A grant is abandoned if the acquisition round consumed this fraction of
/// the lease â€” the remaining validity would be too short to be useful.
const MAX_ROUND_FRACTION: f64 = 0.9;

/// Connection settings for a Redlock ring.
#[derive(Clone, Debug)]
pub struct RedlockConfig {
    nodes: Vec<String>,
}

impl RedlockConfig {
    /// Targets independent master endpoints, e.g.
    /// `["redis://h1:6379", "redis://h2:6379", ...]`.
    pub fn new(nodes: Vec<String>) -> Self {
        Self { nodes }
    }

    /// Number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn validate(&self) -> Result<()> {
        if self.nodes.len() < 3 {
            return Err(Error::InvalidConfig(format!(
                "redlock needs at least 3 independent masters, got {}",
                self.nodes.len()
            )));
        }
        if self.nodes.iter().any(|u| u.trim().is_empty()) {
            return Err(Error::InvalidConfig("empty redlock node url".into()));
        }
        Ok(())
    }
}

/// One ring member: its connection plus pre-parsed scripts.
struct RingNode {
    conn: ConnectionManager,
}

/// Quorum lock manager over N independent masters.
#[derive(Clone)]
pub struct RedlockManager {
    nodes: Arc<Vec<RingNode>>,
    allocator: Arc<RingNode>,
    quorum: usize,
    default_ttl: Duration,
    acquire_script: Script,
    release_script: Script,
    extend_script: Script,
}

impl std::fmt::Debug for RedlockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedlockManager")
            .field("nodes", &self.nodes.len())
            .field("quorum", &self.quorum)
            .finish()
    }
}

impl RedlockManager {
    /// Connects to every node; all must be reachable at startup.
    pub async fn connect(config: RedlockConfig) -> Result<Self> {
        config.validate()?;
        let mut nodes = Vec::with_capacity(config.nodes.len());
        for url in &config.nodes {
            let client = redis::Client::open(url.as_str())
                .map_err(|e| Error::InvalidConfig(format!("bad redlock url `{url}`: {e}")))?;
            let conn = client
                .get_connection_manager()
                .await
                .map_err(|e| Error::Backend(format!("redlock connect {url}: {e}")))?;
            nodes.push(RingNode { conn });
        }
        // The fence allocator is the linearizable counter authority; node 0
        // by default. If it dies, grants stop (liveness hit only).
        let allocator = Arc::new(RingNode {
            conn: nodes[0].conn.clone(),
        });
        Ok(Self {
            nodes: Arc::new(nodes),
            allocator,
            quorum: config.nodes.len() / 2 + 1,
            default_ttl: Duration::from_secs(30),
            acquire_script: Script::new(scripts::ACQUIRE),
            release_script: Script::new(scripts::RELEASE),
            extend_script: Script::new(scripts::EXTEND),
        })
    }

    /// Attempts one acquisition round: sequential grants across the ring,
    /// rollback on lost quorum, fence from the allocator on success.
    pub async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<RedlockHandle> {
        options.validate()?;
        let started = Instant::now();
        let ttl_ms = options.ttl.as_millis() as u64;
        let fence_ttl_ms = ttl_ms * 10;
        let owner = OwnerId::generate();
        let token = owner.as_uuid().to_string();

        let mut acquired: Vec<usize> = Vec::with_capacity(self.nodes.len());
        for (i, node) in self.nodes.iter().enumerate() {
            let mut conn = node.conn.clone();
            // Per-node bound: a blackholed member eats at most this slice of
            // the validity budget, never the whole round.
            let slice = Duration::from_millis((ttl_ms / 4).max(100));
            let res: std::result::Result<std::result::Result<(i64, i64), _>, _> =
                tokio::time::timeout(slice, async {
                    self.acquire_script
                        .key(key)
                        .key(crate::single::fence_key_for(key))
                        .arg(&token)
                        .arg(ttl_ms)
                        .arg(fence_ttl_ms)
                        .invoke_async(&mut conn)
                        .await
                })
                .await;
            match res {
                Ok(Ok((1 | 2, _))) => acquired.push(i),
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(Error::Backend(format!("redlock node {i}: {e}"))),
                Err(_) => {} // per-node slice elapsed: treat as failed node
            }
        }

        let elapsed_ok =
            started.elapsed().as_secs_f64() < options.ttl.as_secs_f64() * MAX_ROUND_FRACTION;

        if acquired.len() >= self.quorum && elapsed_ok {
            let fence = self.allocate_fence().await?;
            metrics::counter!("palisade_redlock_grants_total").increment(1);
            return Ok(self.make_handle(key.to_owned(), token, owner, fence, options.ttl));
        }

        // Lost quorum (or took too long): roll back everything we took.
        for i in acquired {
            self.release_node(i, key, &token).await;
        }
        metrics::counter!("palisade_redlock_rollback_total").increment(1);
        Err(Error::Held {
            key: key.to_owned(),
        })
    }

    /// Retries full rounds with jittered backoff until `wait` elapses.
    pub async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<RedlockHandle> {
        let deadline = Instant::now() + wait;
        loop {
            match self.try_lock(key, options).await {
                Ok(h) => return Ok(h),
                Err(Error::Held { .. }) => {}
                Err(other) => return Err(other),
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout {
                    key: key.to_owned(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Attempts acquisition with the manager's default lease.
    pub async fn try_lock_default(&self, key: &str) -> Result<RedlockHandle> {
        self.try_lock(key, &LockOptions::default().with_ttl(self.default_ttl))
            .await
    }

    async fn allocate_fence(&self) -> Result<FencingToken> {
        let mut conn = self.allocator.conn.clone();
        let v: i64 = redis::cmd("INCR")
            .arg("__palisade__:fence-allocator")
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Backend(format!("fence allocation failed: {e}")))?;
        Ok(FencingToken::new(v as u64))
    }

    async fn release_node(&self, i: usize, key: &str, token: &str) -> Option<i64> {
        let mut conn = self.nodes[i].conn.clone();
        let res: std::result::Result<i64, _> = self
            .release_script
            .key(key)
            .key(crate::single::fence_key_for(key))
            .arg(token)
            .invoke_async(&mut conn)
            .await;
        res.ok()
    }

    fn make_handle(
        &self,
        key: String,
        token: String,
        owner: OwnerId,
        fence: FencingToken,
        ttl: Duration,
    ) -> RedlockHandle {
        let shared = Arc::new(RedlockShared {
            manager: self.clone(),
            key,
            token,
            owner,
            fence,
            ttl,
            gone: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            lost: watch::channel(false).0,
        });
        spawn_watchdog(&shared);
        RedlockHandle { shared }
    }
}

#[async_trait]
impl LockManager for RedlockManager {
    async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(
            RedlockManager::try_lock(self, key, options).await?,
        ))
    }

    async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<Box<dyn LockHandle>> {
        Ok(Box::new(
            RedlockManager::try_lock_for(self, key, options, wait).await?,
        ))
    }
}

struct RedlockShared {
    manager: RedlockManager,
    key: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    ttl: Duration,
    gone: AtomicBool,
    poisoned: AtomicBool,
    lost: watch::Sender<bool>,
}

impl RedlockShared {
    fn mark_gone(&self) -> bool {
        !self.gone.swap(true, Ordering::AcqRel)
    }

    fn mark_lost(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.lost.send_replace(true);
    }

    /// Extends on every node; quorum of successes required.
    async fn fire_extend(&self, ttl: Duration) -> Result<bool> {
        let ttl_ms = ttl.as_millis() as u64;
        let mut ok = 0usize;
        let mut saw_not_owner = false;
        for node in self.manager.nodes.iter() {
            let mut conn = node.conn.clone();
            let res: std::result::Result<i64, _> = self
                .manager
                .extend_script
                .key(&self.key)
                .key(crate::single::fence_key_for(&self.key))
                .arg(&self.token)
                .arg(ttl_ms)
                .arg(ttl_ms * 10)
                .invoke_async(&mut conn)
                .await;
            match res {
                Ok(1) => ok += 1,
                Ok(_) => saw_not_owner = true,
                Err(_) => {}
            }
        }
        if ok >= self.manager.quorum {
            return Ok(true);
        }
        if saw_not_owner {
            return Ok(false);
        }
        Err(Error::Backend("extend round failed on all nodes".into()))
    }
}

impl Drop for RedlockShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let shared = RedlockShared {
                manager: self.manager.clone(),
                key: self.key.clone(),
                token: self.token.clone(),
                owner: self.owner.clone(),
                fence: self.fence,
                ttl: self.ttl,
                gone: AtomicBool::new(true),
                poisoned: AtomicBool::new(false),
                lost: watch::channel(true).0,
            };
            std::mem::drop(runtime.spawn(async move {
                let _ = run_release(&shared).await;
            }));
        }
    }
}

async fn run_release(shared: &RedlockShared) -> Result<()> {
    let mut released_any = false;
    let mut saw_not_owner = false;
    for i in 0..shared.manager.nodes.len() {
        match shared
            .manager
            .release_node(i, &shared.key, &shared.token)
            .await
        {
            Some(1) => released_any = true,
            Some(0) => saw_not_owner = true,
            _ => {}
        }
    }
    if released_any {
        metrics::counter!("palisade_redlock_releases_total").increment(1);
        Ok(())
    } else if saw_not_owner {
        Err(Error::Lost {
            key: shared.key.clone(),
            fence: shared.fence.value(),
        })
    } else {
        Err(Error::Backend("release failed on all nodes".into()))
    }
}

fn spawn_watchdog(shared: &Arc<RedlockShared>) {
    let weak = Arc::downgrade(shared);
    let ttl = shared.ttl;
    tokio::spawn(async move {
        let mut transient_failures = 0u32;
        loop {
            tokio::time::sleep(ttl / RENEWAL_DIVISOR).await;
            let Some(s) = weak.upgrade() else {
                return;
            };
            if s.gone.load(Ordering::Acquire) {
                return;
            }
            match s.fire_extend(ttl).await {
                Ok(true) => {
                    transient_failures = 0;
                    metrics::counter!("palisade_renewals_total").increment(1);
                }
                Ok(false) => {
                    s.mark_lost();
                    return;
                }
                Err(_) => {
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

/// A quorum-held lock.
#[derive(Clone)]
pub struct RedlockHandle {
    shared: Arc<RedlockShared>,
}

impl std::fmt::Debug for RedlockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedlockHandle")
            .field("key", &self.shared.key)
            .field("owner", &self.shared.owner)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

impl RedlockHandle {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Identity of this acquisition.
    pub fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    /// Fence token from the dedicated allocator.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Releases on every node that still records our token.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        run_release(&self.shared).await
    }

    /// Quorum-checked lease refresh.
    pub async fn extend(&self, ttl: Duration) -> Result<()> {
        if ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "extend ttl {:?} is below the {:?} floor",
                ttl,
                palisade_core::MIN_TTL
            )));
        }
        if self.shared.gone.load(Ordering::Acquire) {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        match self.shared.fire_extend(ttl).await {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.shared.mark_lost();
                Err(Error::Lost {
                    key: self.shared.key.clone(),
                    fence: self.shared.fence.value(),
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Fast check whether quorum was lost under us.
    pub fn is_lost(&self) -> bool {
        self.shared.poisoned.load(Ordering::Acquire)
    }

    /// Resolves when quorum is definitively lost.
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
impl LockHandle for RedlockHandle {
    fn key(&self) -> &str {
        RedlockHandle::key(self)
    }

    fn owner(&self) -> &OwnerId {
        RedlockHandle::owner(self)
    }

    fn fence(&self) -> FencingToken {
        RedlockHandle::fence(self)
    }

    fn is_lost(&self) -> bool {
        RedlockHandle::is_lost(self)
    }

    async fn until_lost(&self) {
        RedlockHandle::until_lost(self).await
    }

    async fn extend(&self, ttl: Duration) -> Result<()> {
        RedlockHandle::extend(self, ttl).await
    }

    async fn release(&self) -> Result<()> {
        RedlockHandle::release(self).await
    }
}
