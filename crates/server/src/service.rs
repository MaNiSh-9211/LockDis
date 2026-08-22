//! Palisade gRPC service implementation.
//!
//! Stateless pass-through over a Redis backend (ADR 0021): the server
//! mints tokens and enforces ownership on every mutation, but holds no
//! session state — clients renew via `Extend` (the Rust SDK does this
//! automatically). Watch streams are anonymized (ADR 0022).

use std::sync::Arc;
use std::time::Duration;

use palisade_core::{LockHandle, LockOptions};
use palisade_proto::lock_service_server::LockService;
use palisade_proto::{
    ExtendRequest, ExtendResponse, Extended, Granted, Held, LockEvent,
    LockOptions as LockOptionsPb, LockOutcome, Lost, Released, TimedOut, TryLockForRequest,
    TryLockRequest, UnlockRequest, UnlockResponse, WatchRequest,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use palisade_redis::RedisLockManager;

/// Poll cadence for watch streams.
const WATCH_POLL: Duration = Duration::from_millis(100);

/// Tunable knobs for [`PalisadeService`].
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    /// Default lease when a request omits one.
    pub default_ttl: Duration,
    /// Maximum lease the server will grant or extend to.
    pub max_ttl: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(30),
            max_ttl: Duration::from_secs(600),
        }
    }
}

/// The concrete [`LockService`] implementation.
pub struct PalisadeService {
    manager: Arc<RedisLockManager>,
    config: ServiceConfig,
    ready: Arc<std::sync::atomic::AtomicBool>,
    sessions: Arc<crate::sessions::SessionBook>,
    acl: crate::auth::Acl,
    hub: crate::watch_hub::WatchHub,
    registry: Arc<crate::registry::HeldRegistry>,
}

impl PalisadeService {
    /// Binds the service to a connected backend (open-mode authorization).
    pub fn new(manager: RedisLockManager, config: ServiceConfig) -> Self {
        let manager = Arc::new(manager);
        let sessions = Arc::new(crate::sessions::SessionBook::new(manager.clone()));
        Self {
            hub: crate::watch_hub::WatchHub::new((*manager).clone()),
            registry: Arc::new(crate::registry::HeldRegistry::new()),
            manager,
            config,
            ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            sessions,
            acl: crate::auth::Acl::open(),
        }
    }

    /// Replaces open mode with a real ACL set.
    pub fn with_acl(mut self, acl: crate::auth::Acl) -> Self {
        self.acl = acl;
        self
    }

    fn principal<T>(&self, request: &Request<T>) -> Result<crate::auth::Principal, Status> {
        let bearer = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        self.acl.resolve(bearer)
    }

    /// The shared session book (for the sweeper and introspection).
    pub fn session_book(&self) -> Arc<crate::sessions::SessionBook> {
        self.sessions.clone()
    }

    /// Flips readiness. While draining, new grants are refused with
    /// `Unavailable`; existing leases are untouched (the server holds no
    /// session state — see ADR 0021).
    pub fn set_ready(&self, ready: bool) {
        self.ready
            .store(ready, std::sync::atomic::Ordering::Release);
    }

    /// Clonable readiness handle for embedders that need to drain from
    /// outside the service call path.
    pub fn ready_handle(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.ready.clone()
    }

    fn check_ready(&self) -> Result<(), Status> {
        if self.ready.load(std::sync::atomic::Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::unavailable("server is draining"))
        }
    }

    /// The underlying manager (for tests and embedders).
    pub fn manager(&self) -> &RedisLockManager {
        &self.manager
    }
}

fn options_from_pb(
    pb: Option<&LockOptionsPb>,
    config: &ServiceConfig,
) -> Result<LockOptions, Status> {
    let ttl = pb
        .and_then(|o| {
            if o.ttl_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(o.ttl_ms))
            }
        })
        .unwrap_or(config.default_ttl);
    if ttl > config.max_ttl {
        return Err(Status::invalid_argument(format!(
            "ttl {ttl:?} exceeds server maximum {:?}",
            config.max_ttl
        )));
    }
    let mut opts = LockOptions::default().with_ttl(ttl);
    if let Some(wd) = pb.and_then(|o| o.watchdog) {
        opts = opts.with_watchdog(wd);
    }
    Ok(opts)
}

fn granted_outcome(token: String, fence: u64, ttl_ms: u64) -> LockOutcome {
    LockOutcome {
        result: Some(palisade_proto::lock_outcome::Result::Granted(Granted {
            token,
            fencing_token: fence,
            ttl_ms,
        })),
    }
}

fn held_outcome(key: &str) -> LockOutcome {
    LockOutcome {
        result: Some(palisade_proto::lock_outcome::Result::Held(Held {
            key: key.to_owned(),
        })),
    }
}

#[tonic::async_trait]
impl LockService for PalisadeService {
    async fn try_lock(
        &self,
        request: Request<TryLockRequest>,
    ) -> Result<Response<LockOutcome>, Status> {
        self.check_ready()?;
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        let opts = options_from_pb(req.options.as_ref(), &self.config)?;
        principal.check_lock(&req.key)?;
        match self.manager.try_lock_with(&req.key, &opts).await {
            Ok(h) => {
                if !req.session.is_empty()
                    && !self
                        .sessions
                        .bind(&req.session, &req.key, &h.owner().as_uuid().to_string())
                {
                    // Session died between register and lock: undo the grant.
                    let _ = h.release().await;
                    return Err(Status::not_found("unknown session"));
                }
                if !self
                    .registry
                    .try_acquire(&req.key, principal.name(), principal.max_keys())
                {
                    let _ = h.release().await;
                    return Err(Status::resource_exhausted(format!(
                        "principal `{}` hit max_keys",
                        principal.name()
                    )));
                }
                h.disarm(); // pass-through: ownership now belongs to the client
                Ok(Response::new(granted_outcome(
                    h.owner().as_uuid().to_string(),
                    h.fence().value(),
                    opts.ttl.as_millis() as u64,
                )))
            }
            Err(palisade_core::Error::Held { .. }) => Ok(Response::new(held_outcome(&req.key))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn try_lock_for(
        &self,
        request: Request<TryLockForRequest>,
    ) -> Result<Response<LockOutcome>, Status> {
        self.check_ready()?;
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        let opts = options_from_pb(req.options.as_ref(), &self.config)?;
        principal.check_lock(&req.key)?;
        let wait = Duration::from_millis(req.wait_ms);
        match self.manager.try_lock_for(&req.key, &opts, wait).await {
            Ok(h) => {
                if !req.session.is_empty()
                    && !self
                        .sessions
                        .bind(&req.session, &req.key, &h.owner().as_uuid().to_string())
                {
                    let _ = h.release().await;
                    return Err(Status::not_found("unknown session"));
                }
                if !self
                    .registry
                    .try_acquire(&req.key, principal.name(), principal.max_keys())
                {
                    let _ = h.release().await;
                    return Err(Status::resource_exhausted(format!(
                        "principal `{}` hit max_keys",
                        principal.name()
                    )));
                }
                h.disarm(); // pass-through: ownership now belongs to the client
                Ok(Response::new(granted_outcome(
                    h.owner().as_uuid().to_string(),
                    h.fence().value(),
                    opts.ttl.as_millis() as u64,
                )))
            }
            Err(palisade_core::Error::Held { .. }) => Ok(Response::new(held_outcome(&req.key))),
            Err(palisade_core::Error::Timeout { .. }) => Ok(Response::new(LockOutcome {
                result: Some(palisade_proto::lock_outcome::Result::TimedOut(TimedOut {
                    key: req.key.clone(),
                })),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        principal.check_unlock(&req.key)?;
        let released = self
            .manager
            .unlock_with_token(&req.key, &req.token)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if released {
            self.registry.release(&req.key);
        }
        let response = if released {
            UnlockResponse {
                result: Some(palisade_proto::unlock_response::Result::Released(
                    Released {},
                )),
            }
        } else {
            UnlockResponse {
                result: Some(palisade_proto::unlock_response::Result::Lost(Lost {
                    fencing_token: 0,
                })),
            }
        };
        Ok(Response::new(response))
    }

    async fn extend(
        &self,
        request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        principal.check_extend(&req.key)?;
        let ttl = Duration::from_millis(req.ttl_ms);
        if ttl > self.config.max_ttl {
            return Err(Status::invalid_argument(format!(
                "ttl {ttl:?} exceeds server maximum {:?}",
                self.config.max_ttl
            )));
        }
        let extended = self
            .manager
            .extend_with_token(&req.key, &req.token, ttl)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let response = if extended {
            ExtendResponse {
                result: Some(palisade_proto::extend_response::Result::Extended(
                    Extended {},
                )),
            }
        } else {
            ExtendResponse {
                result: Some(palisade_proto::extend_response::Result::Lost(Lost {
                    fencing_token: 0,
                })),
            }
        };
        Ok(Response::new(response))
    }

    type WatchStream = ReceiverStream<Result<LockEvent, Status>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        let guard = principal.check_watch(&req.key)?;
        let _ = WATCH_POLL;
        let mut hub_rx = self.hub.subscribe(&req.key).await;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<LockEvent, Status>>(16);

        // Thin per-subscriber forwarder: no store polling here — the hub's
        // single per-key poller does all the work.
        tokio::spawn(async move {
            let _guard = guard;
            loop {
                tokio::select! {
                    event = hub_rx.recv() => {
                        match event {
                            Some(ev) => {
                                if tx.send(ev).await.is_err() {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                    // Client disconnected: free the quota slot NOW instead of
                    // waiting for the next transition.
                    _ = tx.closed() => return,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn register_session(
        &self,
        request: Request<palisade_proto::RegisterSessionRequest>,
    ) -> Result<Response<palisade_proto::RegisterSessionResponse>, Status> {
        let req = request.into_inner();
        let ttl = if req.ttl_ms == 0 {
            Duration::from_secs(10)
        } else {
            Duration::from_millis(req.ttl_ms).min(self.config.max_ttl)
        };
        let token = self.sessions.register(req.client_id, ttl);
        tracing::info!(ttl_ms = ttl.as_millis() as u64, "session registered");
        Ok(Response::new(palisade_proto::RegisterSessionResponse {
            session_token: token,
            ttl_ms: ttl.as_millis() as u64,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<palisade_proto::HeartbeatRequest>,
    ) -> Result<Response<palisade_proto::HeartbeatResponse>, Status> {
        let req = request.into_inner();
        match self.sessions.heartbeat(&req.session_token) {
            crate::sessions::HbResult::Ok => {
                Ok(Response::new(palisade_proto::HeartbeatResponse {}))
            }
            crate::sessions::HbResult::RateLimited => Err(Status::resource_exhausted(
                "heartbeat exceeds rate floor (ttl/20); use the documented ttl/3 cadence",
            )),
            crate::sessions::HbResult::Unknown => {
                Err(Status::not_found("unknown or expired session"))
            }
        }
    }

    async fn close_session(
        &self,
        request: Request<palisade_proto::CloseSessionRequest>,
    ) -> Result<Response<palisade_proto::CloseSessionResponse>, Status> {
        let req = request.into_inner();
        let released = self.sessions.close(&req.session_token).await;
        Ok(Response::new(palisade_proto::CloseSessionResponse {
            released_locks: released,
        }))
    }

    async fn describe_key(
        &self,
        request: Request<palisade_proto::DescribeKeyRequest>,
    ) -> Result<Response<palisade_proto::DescribeKeyResponse>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        // Read-only observation: watch-level permission, no quota slot.
        principal.check_watch(&req.key)?;
        let (held, version, ttl_ms) = self
            .manager
            .describe_key(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(palisade_proto::DescribeKeyResponse {
            held,
            version,
            ttl_ms,
        }))
    }

    async fn unlock_force(
        &self,
        request: Request<palisade_proto::UnlockForceRequest>,
    ) -> Result<Response<palisade_proto::UnlockForceResponse>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        principal.check_admin(&req.key)?;
        let released = self
            .manager
            .force_unlock(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        // Free the ORIGINAL holder's quota slot, not the admin's.
        let victim = self.registry.release(&req.key);
        crate::auth::Acl::audit(
            principal.name(),
            "force-unlock",
            &req.key,
            if released { "released" } else { "key-absent" },
        );
        if let Some(v) = &victim {
            tracing::info!(victim_principal = %v, "force-unlock freed victim quota slot");
        }
        Ok(Response::new(palisade_proto::UnlockForceResponse {
            released,
        }))
    }

    type ListLocksStream = ReceiverStream<Result<palisade_proto::KeyState, Status>>;

    async fn list_locks(
        &self,
        request: Request<palisade_proto::ListLocksRequest>,
    ) -> Result<Response<Self::ListLocksStream>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        principal.check_admin(&req.prefix)?;
        crate::auth::Acl::audit(principal.name(), "list-locks", &req.prefix, "ok");

        let manager = self.manager.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            match manager.scan_held(&req.prefix).await {
                Ok(entries) => {
                    for (key, ttl_ms) in entries {
                        let state = palisade_proto::KeyState {
                            key,
                            held: true,
                            ttl_ms,
                        };
                        if tx.send(Ok(state)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
