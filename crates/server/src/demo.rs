//! Web demo: an HTTP + WebSocket façade over the same locking primitives
//! the gRPC service exposes, plus an embedded single-page frontend served
//! at `/`.
//!
//! There is deliberately **no second locking implementation** here: every
//! mutation goes through [`RedisLockManager`] and its Lua-guarded scripts,
//! preserving the pass-through semantics of ADR 0021 (the server mints
//! tokens, the caller owns renewal/release authority). Watch streams reuse
//! the shared [`WatchHub`] fan-out (ADR 0029) and remain anonymized
//! (ADR 0022): subscribers learn *that* a key changed, never who holds it.
//!
//! REST surface:
//! - `POST /api/lock`      `{key, ttl_ms?}`        → grant | held
//! - `POST /api/unlock`    `{key, token}`          → released | lost
//! - `GET  /api/describe/{key}`                     → held + version + ttl
//! - `GET  /api/locks?prefix=`                      → admin introspection
//! - `GET  /api/pressure`                           → Store Pressure Index
//! - `WS   /api/watch/{key}`                        → versioned lock events

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use palisade_core::{Error, LockHandle, OwnerId, SafetyPolicy};
use palisade_proto::lock_event;
use palisade_redis::{
    FairLockHandle, MultiLockHandle, RedisConfig, RedisCountDownLatch, RedisLockManager,
    RedisSemaphore, ReentrantLockHandle, RwReadHandle, RwWriteHandle, SemaphorePermit,
};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;

use crate::lattice::{StorePressureIndex, Tier};
use crate::sessions::{HbResult, SessionBook};
use crate::watch_hub::WatchHub;

/// Lease ceiling for demo grants (the gRPC server's max_ttl analogue).
const DEMO_MAX_TTL: Duration = Duration::from_secs(60);

/// Embedded single-page frontend (vanilla JS + Tailwind CDN, no build step).
pub const INDEX_HTML: &str = include_str!("../assets/index.html");

/// Server-side handles for primitives whose release path is handle-based
/// (reentrant, RW, semaphore, fair, multi). The demo console *is* the client
/// process, so it keeps the handles exactly like a library user would.
#[derive(Default)]
pub struct LiveHandles {
    /// Reentrant handles keyed by `{key}|{owner_id}`.
    reentrant: Mutex<HashMap<String, ReentrantLockHandle>>,
    /// RW read handles keyed by `{key}|{fence}`.
    rw_read: Mutex<HashMap<String, RwReadHandle>>,
    /// RW write handles keyed by `{key}|{fence}`.
    rw_write: Mutex<HashMap<String, RwWriteHandle>>,
    /// Semaphore permits keyed by `{key}|{fence}`.
    permits: Mutex<HashMap<String, SemaphorePermit>>,
    /// Fair-queue handles keyed by `{key}|{fence}`.
    fair: Mutex<HashMap<String, FairLockHandle>>,
    /// Multi-lock handles keyed by generated `multi_id`.
    multi: Mutex<HashMap<String, MultiLockHandle>>,
}

/// Shared demo state: one backend connection, one watch hub, one SPI, one
/// session book, one protected-resource simulator, one handle registry.
#[derive(Clone)]
pub struct DemoState {
    manager: Arc<RedisLockManager>,
    hub: WatchHub,
    pressure: Arc<Mutex<StorePressureIndex>>,
    sessions: Arc<SessionBook>,
    /// Independent Redis connection simulating the *protected resource*
    /// (hash fields semantic predicates read; the "downstream store" that
    /// fencing tokens would guard).
    data: redis::Client,
    live: Arc<LiveHandles>,
}

impl DemoState {
    /// Builds state around a connected backend.
    pub fn new(manager: RedisLockManager) -> Self {
        let manager = Arc::new(manager);
        let hub = WatchHub::new((*manager).clone());
        let sessions = Arc::new(SessionBook::new(manager.clone()));
        let data_url =
            env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let data = redis::Client::open(data_url).expect("valid redis url for data simulator");
        Self {
            manager,
            hub,
            pressure: Arc::new(Mutex::new(StorePressureIndex::new())),
            sessions,
            data,
            live: Arc::new(LiveHandles::default()),
        }
    }

    /// Records one operation outcome into the Store Pressure Index (INV-5).
    fn observe(&self, started: Instant, ok: bool, denied: bool) {
        self.pressure
            .lock()
            .expect("pressure lock")
            .observe(started.elapsed(), ok, denied);
    }
}

fn parse_safety_policy(s: &str) -> Result<SafetyPolicy, ApiError> {
    match s.to_ascii_lowercase().as_str() {
        "cowardly" => Ok(SafetyPolicy::Cowardly),
        "balanced" => Ok(SafetyPolicy::Balanced),
        "aggressive" => Ok(SafetyPolicy::Aggressive),
        other => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("unknown safety policy `{other}` (cowardly|balanced|aggressive)"),
        )),
    }
}

/// Builds the demo router: frontend at `/`, API under `/api/*`, static
/// assets under `/images/`. Also spawns the session sweeper.
pub fn demo_router(manager: RedisLockManager) -> Router {
    let state = DemoState::new(manager);

    // Server-authoritative sessions (ADR 0027): expired sessions' bound
    // locks are released by the sweeper, same cadence as the gRPC server.
    let book = state.sessions.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            book.sweep_once().await;
        }
    });

    let assets_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    Router::new()
        .route("/", get(index))
        // Mutex + sessions + extend + admin
        .route("/api/lock", post(acquire))
        .route("/api/unlock", post(release))
        .route("/api/extend", post(extend))
        .route("/api/force-unlock", post(force_unlock))
        .route("/api/describe/{key}", get(describe))
        .route("/api/locks", get(list_locks))
        .route("/api/pressure", get(pressure))
        .route("/api/watch/{key}", get(watch_upgrade))
        // Reentrant
        .route("/api/reentrant/acquire", post(reentrant_acquire))
        .route("/api/reentrant/release", post(reentrant_release))
        // Read-write
        .route("/api/rw/read", post(rw_read))
        .route("/api/rw/write", post(rw_write))
        .route("/api/rw/release", post(rw_release))
        // Semaphore
        .route("/api/semaphore/acquire", post(semaphore_acquire))
        .route("/api/semaphore/release", post(semaphore_release))
        // Fair FIFO
        .route("/api/fair/lock", post(fair_lock))
        .route("/api/fair/release", post(fair_release))
        // Multi-lock
        .route("/api/multi/lock", post(multi_lock))
        .route("/api/multi/release", post(multi_release))
        // Count-down latch
        .route("/api/latch/create", post(latch_create))
        .route("/api/latch/count-down", post(latch_count_down))
        .route("/api/latch/count/{key}", get(latch_count))
        .route("/api/latch/wait", post(latch_wait))
        // Semantic locks + protected-resource simulator
        .route("/api/semantic/acquire", post(semantic_acquire))
        .route("/api/semantic/data", post(semantic_set_data))
        .route("/api/semantic/data/{key}", get(semantic_get_data))
        // Testament
        .route("/api/testament/set", post(testament_set))
        .route("/api/testament/{key}", get(testament_read))
        // Sessions
        .route("/api/session/register", post(session_register))
        .route("/api/session/heartbeat", post(session_heartbeat))
        .route("/api/session/close", post(session_close))
        .nest_service("/images", ServeDir::new(assets_dir.join("images")))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

/// Connects to Redis, binds `addr`, serves until Ctrl-C.
///
/// # Errors
/// Propagates Redis connection, bind, and serve failures.
pub async fn run(addr: SocketAddr, redis_url: &str) -> palisade_core::Result<()> {
    let manager = RedisLockManager::connect(RedisConfig::new(redis_url)).await?;
    let app = demo_router(manager);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Backend(format!("bind {addr}: {e}")))?;
    tracing::info!(addr = %addr, "palisade-demo listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await
        .map_err(|e| Error::Backend(format!("http serve: {e}")))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

// ---------------------------------------------------------------------------
// Request/response payloads
// ---------------------------------------------------------------------------

/// `POST /api/lock` body.
#[derive(Debug, Deserialize)]
pub struct AcquireRequest {
    /// Key to compete for.
    pub key: String,
    /// Lease duration in milliseconds; defaults to 4 s, capped at 60 s.
    pub ttl_ms: Option<u64>,
    /// Watchdog auto-renewal (ttl/3 cadence). Defaults to off so expiry is
    /// visible in the demo.
    pub watchdog: Option<bool>,
    /// INV-2 safety policy: `cowardly` | `balanced` | `aggressive`.
    pub safety_policy: Option<String>,
    /// Bind the grant to a registered session; unknown sessions undo the
    /// grant (mirrors the gRPC TryLock session flow).
    pub session_token: Option<String>,
}

/// `POST /api/lock` outcome, tagged by `result` like the gRPC oneof.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum LockOutcome {
    /// Granted: the caller now owns release authority via `token`.
    #[serde(rename = "granted")]
    Granted {
        /// Contended key.
        key: String,
        /// Opaque owner token; required to unlock or extend.
        token: String,
        /// Monotonic fencing token for downstream store checks.
        fence: u64,
        /// Actual lease duration in milliseconds.
        ttl_ms: u64,
    },
    /// Someone else holds the key right now.
    #[serde(rename = "held")]
    Held {
        /// Contended key.
        key: String,
    },
}

/// `POST /api/unlock` body.
#[derive(Debug, Deserialize)]
pub struct ReleaseRequest {
    /// Key to release.
    pub key: String,
    /// Owner token returned by the original grant.
    pub token: String,
}

/// `POST /api/unlock` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum UnlockOutcome {
    /// Ownership-checked release succeeded.
    #[serde(rename = "released")]
    Released,
    /// The token no longer owns this key (expired or superseded).
    #[serde(rename = "lost")]
    Lost,
}

/// `GET /api/describe/{key}` response.
#[derive(Debug, Serialize)]
pub struct DescribeResponse {
    /// Is the key currently held?
    pub held: bool,
    /// Fence-counter version of the most recent grant.
    pub version: u64,
    /// Remaining lease in milliseconds (0 when free).
    pub ttl_ms: u64,
}

/// `GET /api/locks` query parameters.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Enumerate keys under this prefix (`{prefix}*`).
    pub prefix: Option<String>,
}

/// One entry of `GET /api/locks`.
#[derive(Debug, Serialize)]
pub struct KeyEntry {
    /// Held key.
    pub key: String,
    /// Remaining lease in milliseconds.
    pub ttl_ms: u64,
}

/// `GET /api/locks` response.
#[derive(Debug, Serialize)]
pub struct ListLocksResponse {
    /// Prefix used for the scan.
    pub prefix: String,
    /// Currently held keys with their remaining TTLs.
    pub locks: Vec<KeyEntry>,
}

/// `GET /api/pressure` response.
#[derive(Debug, Serialize)]
pub struct PressureResponse {
    /// Composite index in [0, 100].
    pub spi: f64,
    /// Degradation tier derived from the index.
    pub tier: &'static str,
}

/// Minimal error mapping: infrastructure failures become status codes,
/// expected outcomes stay in the payload (mirrors ADR 0022's split).
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.1 });
        (self.0, Json(body)).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        let status = match &e {
            Error::InvalidConfig(_) => StatusCode::BAD_REQUEST,
            Error::Backend(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(status, e.to_string())
    }
}

fn clamp_ttl(ttl_ms: Option<u64>) -> Duration {
    let floor_ms = u64::try_from(palisade_core::MIN_TTL.as_millis()).unwrap_or(u64::MAX);
    let requested = ttl_ms.unwrap_or(4_000).min(DEMO_MAX_TTL.as_millis() as u64);
    Duration::from_millis(requested.max(floor_ms))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn acquire(
    State(state): State<DemoState>,
    Json(req): Json<AcquireRequest>,
) -> Result<Json<LockOutcome>, ApiError> {
    if req.key.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "key must not be empty".into(),
        ));
    }
    let ttl = clamp_ttl(req.ttl_ms);
    let policy = match &req.safety_policy {
        Some(p) => parse_safety_policy(p)?,
        None => SafetyPolicy::default(),
    };
    let opts = palisade_core::LockOptions::default()
        .with_ttl(ttl)
        .with_watchdog(req.watchdog.unwrap_or(false))
        .with_safety_policy(policy);

    let started = Instant::now();
    match state.manager.try_lock_with(&req.key, &opts).await {
        Ok(handle) => {
            let token = handle.owner().as_uuid().to_string();
            // Session binding (ADR 0027): an unknown session undoes the grant.
            if let Some(sess) = &req.session_token {
                if !state.sessions.bind(sess, &req.key, &token) {
                    let _ = handle.release().await;
                    return Err(ApiError(
                        StatusCode::NOT_FOUND,
                        "unknown or expired session".into(),
                    ));
                }
            }
            state.observe(started, true, false);
            let outcome = LockOutcome::Granted {
                key: req.key,
                token,
                fence: handle.fence().value(),
                ttl_ms: opts.ttl.as_millis() as u64,
            };
            // Pass-through (ADR 0021): suppress the Drop-time release so the
            // grant survives this handler returning.
            handle.disarm();
            Ok(Json(outcome))
        }
        Err(Error::Held { .. }) => {
            state.observe(started, true, true);
            Ok(Json(LockOutcome::Held { key: req.key }))
        }
        Err(e) => {
            state.observe(started, false, false);
            Err(ApiError::from(e))
        }
    }
}

async fn release(
    State(state): State<DemoState>,
    Json(req): Json<ReleaseRequest>,
) -> Result<Json<UnlockOutcome>, ApiError> {
    let started = Instant::now();
    let released = state
        .manager
        .unlock_with_token(&req.key, &req.token)
        .await?;
    state.observe(started, true, false);
    if released {
        Ok(Json(UnlockOutcome::Released))
    } else {
        Ok(Json(UnlockOutcome::Lost))
    }
}

async fn describe(
    State(state): State<DemoState>,
    Path(key): Path<String>,
) -> Result<Json<DescribeResponse>, ApiError> {
    let (held, version, ttl_ms) = state.manager.describe_key(&key).await?;
    Ok(Json(DescribeResponse {
        held,
        version,
        ttl_ms,
    }))
}

async fn list_locks(
    State(state): State<DemoState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListLocksResponse>, ApiError> {
    let prefix = q.prefix.unwrap_or_default();
    let entries = state.manager.scan_held(&prefix).await?;
    Ok(Json(ListLocksResponse {
        prefix,
        locks: entries
            .into_iter()
            .map(|(key, ttl_ms)| KeyEntry { key, ttl_ms })
            .collect(),
    }))
}

async fn pressure(State(state): State<DemoState>) -> Json<PressureResponse> {
    let (spi, tier) = {
        let mut idx = state.pressure.lock().expect("pressure lock");
        (idx.tick(), idx.tier())
    };
    Json(PressureResponse {
        spi,
        tier: tier_name(tier),
    })
}

fn tier_name(tier: Tier) -> &'static str {
    match tier {
        Tier::Normal => "NORMAL",
        Tier::Elevated => "ELEVATED",
        Tier::Critical => "CRITICAL",
        Tier::Siege => "SIEGE",
    }
}

// ---------------------------------------------------------------------------
// Extend / force-unlock
// ---------------------------------------------------------------------------

/// `POST /api/extend` body.
#[derive(Debug, Deserialize)]
pub struct ExtendRequest {
    /// Key whose lease is renewed.
    pub key: String,
    /// Owner token from the original grant.
    pub token: String,
    /// New lease duration in milliseconds.
    pub ttl_ms: u64,
}

/// `POST /api/extend` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum ExtendOutcome {
    /// Lease renewed.
    #[serde(rename = "extended")]
    Extended,
    /// The token no longer owns this key.
    #[serde(rename = "lost")]
    Lost,
}

async fn extend(
    State(state): State<DemoState>,
    Json(req): Json<ExtendRequest>,
) -> Result<Json<ExtendOutcome>, ApiError> {
    let ttl = clamp_ttl(Some(req.ttl_ms));
    let ok = state
        .manager
        .extend_with_token(&req.key, &req.token, ttl)
        .await?;
    if ok {
        Ok(Json(ExtendOutcome::Extended))
    } else {
        Ok(Json(ExtendOutcome::Lost))
    }
}

/// `POST /api/force-unlock` body.
#[derive(Debug, Deserialize)]
pub struct ForceUnlockRequest {
    /// Key to delete with NO ownership check (admin break-glass).
    pub key: String,
}

/// `POST /api/force-unlock` response.
#[derive(Debug, Serialize)]
pub struct ForceUnlockResponse {
    /// True when a held key was deleted.
    pub released: bool,
}

async fn force_unlock(
    State(state): State<DemoState>,
    Json(req): Json<ForceUnlockRequest>,
) -> Result<Json<ForceUnlockResponse>, ApiError> {
    let released = state.manager.force_unlock(&req.key).await?;
    Ok(Json(ForceUnlockResponse { released }))
}

// ---------------------------------------------------------------------------
// Reentrant locks (ADR 0015)
// ---------------------------------------------------------------------------

/// `POST /api/reentrant/acquire` body.
#[derive(Debug, Deserialize)]
pub struct ReentrantAcquireRequest {
    /// Key to lock.
    pub key: String,
    /// Lease duration in milliseconds.
    pub ttl_ms: Option<u64>,
    /// Owner identity from a previous acquire — the SAME owner re-enters
    /// freely (hold count incremented); omit to mint a new owner.
    pub owner_id: Option<String>,
}

/// `POST /api/reentrant/acquire` response.
#[derive(Debug, Serialize)]
pub struct ReentrantAcquireResponse {
    /// Owner identity: pass it back to release; same owner re-enters freely.
    pub owner_id: String,
    /// Fencing token of the outermost grant.
    pub fence: u64,
}

async fn reentrant_acquire(
    State(state): State<DemoState>,
    Json(req): Json<ReentrantAcquireRequest>,
) -> Result<Json<ReentrantAcquireResponse>, ApiError> {
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    let owner = match &req.owner_id {
        Some(id) => {
            let uuid = uuid::Uuid::parse_str(id).map_err(|_| {
                ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("owner_id `{id}` is not a valid uuid"),
                )
            })?;
            OwnerId::from_uuid(uuid)
        }
        None => OwnerId::generate(),
    };
    let handle = state
        .manager
        .try_lock_reentrant(&req.key, owner, &opts)
        .await?;
    let owner_id = handle.owner().as_uuid().to_string();
    let fence = handle.fence().value();
    state
        .live
        .reentrant
        .lock()
        .expect("reentrant map")
        .insert(format!("{}|{owner_id}", req.key), handle);
    Ok(Json(ReentrantAcquireResponse { owner_id, fence }))
}

/// `POST /api/reentrant/release` body.
#[derive(Debug, Deserialize)]
pub struct ReentrantReleaseRequest {
    /// Key to release.
    pub key: String,
    /// Owner identity from acquire.
    pub owner_id: String,
    /// Release every held count instead of one.
    pub all: Option<bool>,
}

/// `POST /api/reentrant/release` response.
#[derive(Debug, Serialize)]
pub struct ReleasedResponse {
    /// True when a live handle was found and released.
    pub released: bool,
}

async fn reentrant_release(
    State(state): State<DemoState>,
    Json(req): Json<ReentrantReleaseRequest>,
) -> Result<Json<ReleasedResponse>, ApiError> {
    let handle = state
        .live
        .reentrant
        .lock()
        .expect("reentrant map")
        .remove(&format!("{}|{}", req.key, req.owner_id));
    let Some(h) = handle else {
        return Ok(Json(ReleasedResponse { released: false }));
    };
    if req.all.unwrap_or(false) {
        h.release_all().await?;
    } else {
        h.release_one().await?;
    }
    Ok(Json(ReleasedResponse { released: true }))
}

// ---------------------------------------------------------------------------
// Read-write locks (ADR 0016)
// ---------------------------------------------------------------------------

/// `POST /api/rw/read` and `/api/rw/write` body.
#[derive(Debug, Deserialize)]
pub struct RwAcquireRequest {
    /// Key to lock.
    pub key: String,
    /// Lease duration in milliseconds.
    pub ttl_ms: Option<u64>,
}

/// `POST /api/rw/*` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum RwOutcome {
    /// Grant succeeded.
    #[serde(rename = "acquired")]
    Acquired {
        /// Fencing token; pass it back to release.
        fence: u64,
    },
    /// Incompatible holders present (writer while readers, etc.).
    #[serde(rename = "held")]
    Held,
}

async fn rw_read(
    State(state): State<DemoState>,
    Json(req): Json<RwAcquireRequest>,
) -> Result<Json<RwOutcome>, ApiError> {
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    match state.manager.try_read(&req.key, &opts).await {
        Ok(handle) => {
            let fence = handle.fence().value();
            state
                .live
                .rw_read
                .lock()
                .expect("rw read map")
                .insert(rw_map_key(&req.key, fence), handle);
            Ok(Json(RwOutcome::Acquired { fence }))
        }
        Err(Error::Held { .. }) => Ok(Json(RwOutcome::Held)),
        Err(e) => Err(ApiError::from(e)),
    }
}

async fn rw_write(
    State(state): State<DemoState>,
    Json(req): Json<RwAcquireRequest>,
) -> Result<Json<RwOutcome>, ApiError> {
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    match state.manager.try_write(&req.key, &opts).await {
        Ok(handle) => {
            let fence = handle.fence().value();
            state
                .live
                .rw_write
                .lock()
                .expect("rw write map")
                .insert(rw_map_key(&req.key, fence), handle);
            Ok(Json(RwOutcome::Acquired { fence }))
        }
        Err(Error::Held { .. }) => Ok(Json(RwOutcome::Held)),
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `POST /api/rw/release` body.
#[derive(Debug, Deserialize)]
pub struct RwReleaseRequest {
    /// Key to release.
    pub key: String,
    /// Fence from acquire.
    pub fence: u64,
    /// Which side to release: `read` or `write`.
    pub mode: String,
}

async fn rw_release(
    State(state): State<DemoState>,
    Json(req): Json<RwReleaseRequest>,
) -> Result<Json<ReleasedResponse>, ApiError> {
    let map_key = rw_map_key(&req.key, req.fence);
    match req.mode.as_str() {
        "read" => {
            let h = state
                .live
                .rw_read
                .lock()
                .expect("rw read map")
                .remove(&map_key);
            match h {
                Some(h) => {
                    h.release().await?;
                    Ok(Json(ReleasedResponse { released: true }))
                }
                None => Ok(Json(ReleasedResponse { released: false })),
            }
        }
        "write" => {
            let h = state
                .live
                .rw_write
                .lock()
                .expect("rw write map")
                .remove(&map_key);
            match h {
                Some(h) => {
                    h.release().await?;
                    Ok(Json(ReleasedResponse { released: true }))
                }
                None => Ok(Json(ReleasedResponse { released: false })),
            }
        }
        other => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("mode must be `read` or `write`, got `{other}`"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Semaphore (ADR 0017)
// ---------------------------------------------------------------------------

/// `POST /api/semaphore/acquire` body.
#[derive(Debug, Deserialize)]
pub struct SemaphoreAcquireRequest {
    /// Semaphore key.
    pub key: String,
    /// Total permit capacity (NX-created; first racer wins).
    pub max_permits: u32,
    /// Permit lease in milliseconds.
    pub ttl_ms: Option<u64>,
}

/// `POST /api/semaphore/acquire` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum SemaphoreOutcome {
    /// Permit granted.
    #[serde(rename = "acquired")]
    Acquired {
        /// Fencing token; pass back to release.
        fence: u64,
    },
    /// All permits are in use.
    #[serde(rename = "full")]
    Full,
}

fn rw_map_key(key: &str, fence: u64) -> String {
    format!("{key}|{fence}")
}

async fn semaphore_acquire(
    State(state): State<DemoState>,
    Json(req): Json<SemaphoreAcquireRequest>,
) -> Result<Json<SemaphoreOutcome>, ApiError> {
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    let sem: RedisSemaphore = state.manager.semaphore(&req.key, req.max_permits)?;
    match sem.try_acquire(&opts).await {
        Ok(permit) => {
            let fence = permit.fence().value();
            state
                .live
                .permits
                .lock()
                .expect("permit map")
                .insert(rw_map_key(&req.key, fence), permit);
            Ok(Json(SemaphoreOutcome::Acquired { fence }))
        }
        Err(Error::Held { .. }) => Ok(Json(SemaphoreOutcome::Full)),
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `POST /api/semaphore/release` body.
#[derive(Debug, Deserialize)]
pub struct FenceReleaseRequest {
    /// Semaphore key.
    pub key: String,
    /// Fence from acquire.
    pub fence: u64,
}

async fn semaphore_release(
    State(state): State<DemoState>,
    Json(req): Json<FenceReleaseRequest>,
) -> Result<Json<ReleasedResponse>, ApiError> {
    let permit = state
        .live
        .permits
        .lock()
        .expect("permit map")
        .remove(&rw_map_key(&req.key, req.fence));
    match permit {
        Some(p) => {
            p.release().await?;
            Ok(Json(ReleasedResponse { released: true }))
        }
        None => Ok(Json(ReleasedResponse { released: false })),
    }
}

// ---------------------------------------------------------------------------
// Fair FIFO locks (ADR 0018)
// ---------------------------------------------------------------------------

/// `POST /api/fair/lock` response.
#[derive(Debug, Serialize)]
pub struct FairAcquireResponse {
    /// Queue owner identity.
    pub owner: String,
    /// Fencing token; pass back to release.
    pub fence: u64,
}

async fn fair_lock(
    State(state): State<DemoState>,
    Json(req): Json<RwAcquireRequest>,
) -> Result<Json<FairAcquireResponse>, ApiError> {
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    let handle = state.manager.try_lock_fair(&req.key, &opts).await?;
    let owner = handle.owner().as_uuid().to_string();
    let fence = handle.fence().value();
    state
        .live
        .fair
        .lock()
        .expect("fair map")
        .insert(rw_map_key(&req.key, fence), handle);
    Ok(Json(FairAcquireResponse { owner, fence }))
}

async fn fair_release(
    State(state): State<DemoState>,
    Json(req): Json<FenceReleaseRequest>,
) -> Result<Json<ReleasedResponse>, ApiError> {
    let handle = state
        .live
        .fair
        .lock()
        .expect("fair map")
        .remove(&rw_map_key(&req.key, req.fence));
    match handle {
        Some(h) => {
            h.release().await?;
            Ok(Json(ReleasedResponse { released: true }))
        }
        None => Ok(Json(ReleasedResponse { released: false })),
    }
}

// ---------------------------------------------------------------------------
// Multi-lock (ADR 0019)
// ---------------------------------------------------------------------------

/// `POST /api/multi/lock` body.
#[derive(Debug, Deserialize)]
pub struct MultiAcquireRequest {
    /// Keys to acquire atomically (sorted internally; rollback on failure).
    pub keys: Vec<String>,
    /// Lease per key in milliseconds.
    pub ttl_ms: Option<u64>,
    /// Total wait budget in milliseconds (default 2000).
    pub wait_ms: Option<u64>,
}

/// One acquired key of a multi-lock.
#[derive(Debug, Serialize)]
pub struct MultiKeyFence {
    /// Acquired key.
    pub key: String,
    /// Its fencing token.
    pub fence: u64,
}

/// `POST /api/multi/lock` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum MultiOutcome {
    /// All keys acquired atomically.
    #[serde(rename = "acquired")]
    Acquired {
        /// Handle id for release.
        multi_id: String,
        /// Per-key fences in acquisition order.
        locks: Vec<MultiKeyFence>,
    },
    /// Not every key could be acquired within the budget; all-or-nothing
    /// rollback released any partial grants.
    #[serde(rename = "timeout")]
    Timeout,
}

async fn multi_lock(
    State(state): State<DemoState>,
    Json(req): Json<MultiAcquireRequest>,
) -> Result<Json<MultiOutcome>, ApiError> {
    if req.keys.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "keys must not be empty".into(),
        ));
    }
    let opts = palisade_core::LockOptions::default().with_ttl(clamp_ttl(req.ttl_ms));
    let wait = Duration::from_millis(req.wait_ms.unwrap_or(2_000).min(30_000));
    match state.manager.try_lock_all(&req.keys, &opts, wait).await {
        Ok(handle) => {
            let keys = handle.keys().to_vec();
            let fences = handle.fences();
            let multi_id = OwnerId::generate().as_uuid().to_string();
            let locks: Vec<MultiKeyFence> = keys
                .into_iter()
                .zip(fences.iter().map(|f| f.value()))
                .map(|(key, fence)| MultiKeyFence { key, fence })
                .collect();
            state
                .live
                .multi
                .lock()
                .expect("multi map")
                .insert(multi_id.clone(), handle);
            Ok(Json(MultiOutcome::Acquired { multi_id, locks }))
        }
        Err(Error::Timeout { .. }) => Ok(Json(MultiOutcome::Timeout)),
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `POST /api/multi/release` body.
#[derive(Debug, Deserialize)]
pub struct MultiReleaseRequest {
    /// Handle id from acquire.
    pub multi_id: String,
}

async fn multi_release(
    State(state): State<DemoState>,
    Json(req): Json<MultiReleaseRequest>,
) -> Result<Json<ReleasedResponse>, ApiError> {
    let handle = state
        .live
        .multi
        .lock()
        .expect("multi map")
        .remove(&req.multi_id);
    match handle {
        Some(h) => {
            h.release_all().await?;
            Ok(Json(ReleasedResponse { released: true }))
        }
        None => Ok(Json(ReleasedResponse { released: false })),
    }
}

// ---------------------------------------------------------------------------
// Count-down latch
// ---------------------------------------------------------------------------

/// `POST /api/latch/create` body.
#[derive(Debug, Deserialize)]
pub struct LatchCreateRequest {
    /// Latch key.
    pub key: String,
    /// Initial count (NX semantics: first creator wins).
    pub count: u32,
}

/// `POST /api/latch/count-down` body.
#[derive(Debug, Deserialize)]
pub struct LatchKeyRequest {
    /// Latch key.
    pub key: String,
}

/// Latch count response.
#[derive(Debug, Serialize)]
pub struct LatchCountResponse {
    /// Remaining count.
    pub count: u64,
}

/// `POST /api/latch/wait` body.
#[derive(Debug, Deserialize)]
pub struct LatchWaitRequest {
    /// Latch key.
    pub key: String,
    /// How long to block in milliseconds (default 5000).
    pub timeout_ms: Option<u64>,
}

/// `POST /api/latch/wait` response.
#[derive(Debug, Serialize)]
pub struct LatchWaitResponse {
    /// True when the latch reached zero within the budget.
    pub zero: bool,
}

async fn latch_create(
    State(state): State<DemoState>,
    Json(req): Json<LatchCreateRequest>,
) -> Result<Json<LatchCountResponse>, ApiError> {
    let latch = RedisCountDownLatch::create(&state.manager, &req.key, req.count).await?;
    let count = latch.count().await?;
    Ok(Json(LatchCountResponse { count }))
}

async fn latch_count_down(
    State(state): State<DemoState>,
    Json(req): Json<LatchKeyRequest>,
) -> Result<Json<LatchCountResponse>, ApiError> {
    // NX-init: adopting an existing latch never overwrites its count.
    let latch = RedisCountDownLatch::create(&state.manager, &req.key, 1).await?;
    let count = latch.count_down().await?;
    Ok(Json(LatchCountResponse { count }))
}

async fn latch_count(
    State(state): State<DemoState>,
    Path(key): Path<String>,
) -> Result<Json<LatchCountResponse>, ApiError> {
    let latch = RedisCountDownLatch::create(&state.manager, &key, 1).await?;
    let count = latch.count().await?;
    Ok(Json(LatchCountResponse { count }))
}

async fn latch_wait(
    State(state): State<DemoState>,
    Json(req): Json<LatchWaitRequest>,
) -> Result<Json<LatchWaitResponse>, ApiError> {
    let latch = RedisCountDownLatch::create(&state.manager, &req.key, 1).await?;
    let timeout = Duration::from_millis(req.timeout_ms.unwrap_or(5_000).min(30_000));
    let zero = latch.wait_until_zero(timeout).await.is_ok();
    Ok(Json(LatchWaitResponse { zero }))
}

// ---------------------------------------------------------------------------
// Semantic locks (INV-6) + protected-resource simulator
// ---------------------------------------------------------------------------

/// One predicate of `POST /api/semantic/acquire`.
#[derive(Debug, Deserialize)]
pub struct PredicateSpec {
    /// Hash field to test.
    pub field: String,
    /// `eq` (equals), `gt` (numeric greater-than), or `absent`.
    pub op: String,
    /// Comparison value for `eq`/`gt`.
    pub value: Option<String>,
}

/// `POST /api/semantic/acquire` body.
#[derive(Debug, Deserialize)]
pub struct SemanticAcquireRequest {
    /// Lock key.
    pub key: String,
    /// Lease duration in milliseconds.
    pub ttl_ms: Option<u64>,
    /// Predicates evaluated atomically inside the grant script.
    pub predicates: Vec<PredicateSpec>,
}

/// `POST /api/semantic/acquire` outcome.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum SemanticOutcome {
    /// Granted: predicates held at grant time.
    #[serde(rename = "granted")]
    Granted {
        /// Owner token.
        token: String,
        /// Fencing token.
        fence: u64,
    },
    /// Someone holds the key.
    #[serde(rename = "held")]
    Held,
    /// Key free but at least one predicate failed.
    #[serde(rename = "predicates-failed")]
    PredicatesFailed,
}

async fn semantic_acquire(
    State(state): State<DemoState>,
    Json(req): Json<SemanticAcquireRequest>,
) -> Result<Json<SemanticOutcome>, ApiError> {
    if req.predicates.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "at least one predicate required".into(),
        ));
    }
    let mut guard = state.manager.acquire_where(&req.key);
    guard = guard.ttl(clamp_ttl(req.ttl_ms));
    for p in &req.predicates {
        match p.op.as_str() {
            "eq" => {
                let v = p.value.clone().unwrap_or_default();
                guard = guard.field_equals(&p.field, &v);
            }
            "gt" => {
                let v: f64 = p
                    .value
                    .as_deref()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        ApiError(
                            StatusCode::BAD_REQUEST,
                            format!("predicate `{}` needs numeric value", p.field),
                        )
                    })?;
                guard = guard.field_gt(&p.field, v);
            }
            "absent" => {
                guard = guard.field_absent(&p.field);
            }
            other => {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("unknown predicate op `{other}` (eq|gt|absent)"),
                ));
            }
        }
    }
    let started = Instant::now();
    match guard.acquire().await {
        Ok(handle) => {
            state.observe(started, true, false);
            let outcome = SemanticOutcome::Granted {
                token: handle.owner().as_uuid().to_string(),
                fence: handle.fence().value(),
            };
            handle.disarm();
            Ok(Json(outcome))
        }
        Err(Error::Held { .. }) => {
            state.observe(started, true, true);
            // The library script returns {0,0} for both "lock exists" and
            // "predicates failed" — distinguish via a describe probe.
            let (held, _, _) = state.manager.describe_key(&req.key).await?;
            if held {
                Ok(Json(SemanticOutcome::Held))
            } else {
                Ok(Json(SemanticOutcome::PredicatesFailed))
            }
        }
        Err(e) => {
            state.observe(started, false, false);
            Err(ApiError::from(e))
        }
    }
}

/// `POST /api/semantic/data` body: writes the protected-resource hash the
/// predicates read (simulates the downstream store).
#[derive(Debug, Deserialize)]
pub struct SemanticDataRequest {
    /// Hash key (conventionally `{lock_key}:data`).
    pub key: String,
    /// Field to write.
    pub field: String,
    /// Value to write.
    pub value: String,
}

async fn semantic_set_data(
    State(state): State<DemoState>,
    Json(req): Json<SemanticDataRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut conn = state
        .data
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| Error::Backend(format!("data connect: {e}")))?;
    let _: () = redis::cmd("HSET")
        .arg(&req.key)
        .arg(&req.field)
        .arg(&req.value)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Backend(format!("hset failed: {e}")))?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// `GET /api/semantic/data/{key}` response.
#[derive(Debug, Serialize)]
pub struct SemanticDataResponse {
    /// All hash fields of the protected resource.
    pub fields: HashMap<String, String>,
}

async fn semantic_get_data(
    State(state): State<DemoState>,
    Path(key): Path<String>,
) -> Result<Json<SemanticDataResponse>, ApiError> {
    let mut conn = state
        .data
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| Error::Backend(format!("data connect: {e}")))?;
    let fields: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Backend(format!("hgetall failed: {e}")))?;
    Ok(Json(SemanticDataResponse { fields }))
}

// ---------------------------------------------------------------------------
// Lock Testament (INV-3)
// ---------------------------------------------------------------------------

/// `POST /api/testament/set` body.
#[derive(Debug, Deserialize)]
pub struct TestamentSetRequest {
    /// Key the testament is attached to.
    pub key: String,
    /// Current holder's owner token (ownership-checked).
    pub token: String,
    /// How long the testament outlives the lease, in milliseconds.
    pub ttl_ms: Option<u64>,
    /// Payload the successor will read after acquiring.
    pub payload: String,
}

/// `POST /api/testament/set` response.
#[derive(Debug, Serialize)]
pub struct TestamentSetResponse {
    /// True when the testament was stored.
    pub stored: bool,
}

async fn testament_set(
    State(state): State<DemoState>,
    Json(req): Json<TestamentSetRequest>,
) -> Result<Json<TestamentSetResponse>, ApiError> {
    let ttl = clamp_ttl(req.ttl_ms);
    match state
        .manager
        .set_testament(&req.key, &req.token, ttl, req.payload.as_bytes())
        .await
    {
        Ok(()) => Ok(Json(TestamentSetResponse { stored: true })),
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `GET /api/testament/{key}` response.
#[derive(Debug, Serialize)]
pub struct TestamentReadResponse {
    /// Payload left by the previous holder, if any.
    pub payload: Option<String>,
}

async fn testament_read(
    State(state): State<DemoState>,
    Path(key): Path<String>,
) -> Result<Json<TestamentReadResponse>, ApiError> {
    let payload = state.manager.read_testament(&key).await?;
    Ok(Json(TestamentReadResponse {
        payload: payload.map(|b| String::from_utf8_lossy(&b).into_owned()),
    }))
}

// ---------------------------------------------------------------------------
// Sessions (ADR 0027)
// ---------------------------------------------------------------------------

/// `POST /api/session/register` body.
#[derive(Debug, Deserialize)]
pub struct SessionRegisterRequest {
    /// Free-form client identity.
    pub client_id: String,
    /// Session liveness budget in milliseconds (default 10000).
    pub ttl_ms: Option<u64>,
}

/// `POST /api/session/register` response.
#[derive(Debug, Serialize)]
pub struct SessionRegisterResponse {
    /// Session token; pass to heartbeat/close and as `session_token` on lock.
    pub session_token: String,
    /// Effective session TTL in milliseconds.
    pub ttl_ms: u64,
}

async fn session_register(
    State(state): State<DemoState>,
    Json(req): Json<SessionRegisterRequest>,
) -> Result<Json<SessionRegisterResponse>, ApiError> {
    let ttl = Duration::from_millis(req.ttl_ms.unwrap_or(10_000).min(60_000));
    let token = state.sessions.register(req.client_id, ttl);
    Ok(Json(SessionRegisterResponse {
        session_token: token,
        ttl_ms: ttl.as_millis() as u64,
    }))
}

/// `POST /api/session/heartbeat` response.
#[derive(Debug, Serialize)]
#[serde(tag = "result")]
pub enum SessionHeartbeatOutcome {
    /// Liveness refreshed.
    #[serde(rename = "ok")]
    Ok,
    /// Arrived faster than the ttl/20 floor.
    #[serde(rename = "rate_limited")]
    RateLimited,
    /// Unknown or expired session (its locks were swept).
    #[serde(rename = "unknown")]
    Unknown,
}

async fn session_heartbeat(
    State(state): State<DemoState>,
    Json(req): Json<LatchKeyRequest>, // { key } reused as { session_token }
) -> Json<SessionHeartbeatOutcome> {
    match state.sessions.heartbeat(&req.key) {
        HbResult::Ok => Json(SessionHeartbeatOutcome::Ok),
        HbResult::RateLimited => Json(SessionHeartbeatOutcome::RateLimited),
        HbResult::Unknown => Json(SessionHeartbeatOutcome::Unknown),
    }
}

/// `POST /api/session/close` response.
#[derive(Debug, Serialize)]
pub struct SessionCloseResponse {
    /// Number of bound locks the server released.
    pub released_locks: u32,
}

async fn session_close(
    State(state): State<DemoState>,
    Json(req): Json<LatchKeyRequest>, // { key } reused as { session_token }
) -> Json<SessionCloseResponse> {
    let released = state.sessions.close(&req.key).await;
    Json(SessionCloseResponse {
        released_locks: released,
    })
}

// ---------------------------------------------------------------------------
// WebSocket watch stream
// ---------------------------------------------------------------------------

async fn watch_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<DemoState>,
    Path(key): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| async move { pump_events(socket, state, key).await })
}

/// Forwards hub transitions to one browser socket; anonymized by the hub
/// already (ADR 0022), so nothing leaks here either.
async fn pump_events(mut socket: WebSocket, state: DemoState, key: String) {
    let mut rx = state.hub.subscribe(&key).await;
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(Ok(ev)) => {
                        let text = render_event(&ev);
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    Some(Err(_)) | None => return, // hub retired / channel closed
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(_)) => {} // client chatter ignored; pings are automatic
                    _ => return,      // browser disconnected
                }
            }
        }
    }
}

fn render_event(event: &palisade_proto::LockEvent) -> String {
    let payload = match event.event.as_ref() {
        Some(lock_event::Event::Acquired(a)) => {
            serde_json::json!({ "event": "acquired", "version": a.version })
        }
        Some(lock_event::Event::Freed(f)) => {
            serde_json::json!({ "event": "freed", "version": f.version })
        }
        None => serde_json::json!({ "event": "unknown" }),
    };
    payload.to_string()
}
