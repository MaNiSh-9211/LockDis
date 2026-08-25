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

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::services::ServeDir;
use palisade_core::{Error, LockHandle};
use palisade_proto::lock_event;
use palisade_redis::{RedisConfig, RedisLockManager};
use serde::{Deserialize, Serialize};

use crate::lattice::{StorePressureIndex, Tier};
use crate::watch_hub::WatchHub;

/// Lease ceiling for demo grants (the gRPC server's max_ttl analogue).
const DEMO_MAX_TTL: Duration = Duration::from_secs(60);

/// Embedded single-page frontend (vanilla JS + Tailwind CDN, no build step).
pub const INDEX_HTML: &str = include_str!("../assets/index.html");

/// Shared demo state: one backend connection, one watch hub, one SPI.
#[derive(Clone)]
pub struct DemoState {
    manager: Arc<RedisLockManager>,
    hub: WatchHub,
    pressure: Arc<Mutex<StorePressureIndex>>,
}

impl DemoState {
    /// Builds state around a connected backend.
    pub fn new(manager: RedisLockManager) -> Self {
        let manager = Arc::new(manager);
        let hub = WatchHub::new((*manager).clone());
        Self {
            manager,
            hub,
            pressure: Arc::new(Mutex::new(StorePressureIndex::new())),
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

/// Builds the demo router: frontend at `/`, API under `/api/*`, static assets under `/images/`.
pub fn demo_router(manager: RedisLockManager) -> Router {
    let state = DemoState::new(manager);
    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    Router::new()
        .route("/", get(index))
        .route("/api/lock", post(acquire))
        .route("/api/unlock", post(release))
        .route("/api/describe/{key}", get(describe))
        .route("/api/locks", get(list_locks))
        .route("/api/pressure", get(pressure))
        .route("/api/watch/{key}", get(watch_upgrade))
        .nest_service("/images", ServeDir::new(assets_dir.join("images")))
        .layer(DefaultBodyLimit::max(16 * 1024))
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
    // Watchdog OFF by design: expiry must be visible for the demo to show
    // lost leases and fencing-token succession.
    let opts = palisade_core::LockOptions::default()
        .with_ttl(ttl)
        .with_watchdog(false);

    let started = Instant::now();
    match state.manager.try_lock_with(&req.key, &opts).await {
        Ok(handle) => {
            state.observe(started, true, false);
            let outcome = LockOutcome::Granted {
                key: req.key,
                token: handle.owner().as_uuid().to_string(),
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
