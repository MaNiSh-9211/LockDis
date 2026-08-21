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
    Acquired, ExtendRequest, ExtendResponse, Extended, Freed, Granted, Held, LockEvent,
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
}

impl PalisadeService {
    /// Binds the service to a connected backend.
    pub fn new(manager: RedisLockManager, config: ServiceConfig) -> Self {
        Self {
            manager: Arc::new(manager),
            config,
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
        let req = request.into_inner();
        let opts = options_from_pb(req.options.as_ref(), &self.config)?;
        match self.manager.try_lock_with(&req.key, &opts).await {
            Ok(h) => Ok(Response::new(granted_outcome(
                h.owner().as_uuid().to_string(),
                h.fence().value(),
                opts.ttl.as_millis() as u64,
            ))),
            Err(palisade_core::Error::Held { .. }) => Ok(Response::new(held_outcome(&req.key))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn try_lock_for(
        &self,
        request: Request<TryLockForRequest>,
    ) -> Result<Response<LockOutcome>, Status> {
        let req = request.into_inner();
        let opts = options_from_pb(req.options.as_ref(), &self.config)?;
        let wait = Duration::from_millis(req.wait_ms);
        match self.manager.try_lock_for(&req.key, &opts, wait).await {
            Ok(h) => Ok(Response::new(granted_outcome(
                h.owner().as_uuid().to_string(),
                h.fence().value(),
                opts.ttl.as_millis() as u64,
            ))),
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
        let req = request.into_inner();
        let released = self
            .manager
            .unlock_with_token(&req.key, &req.token)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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
        let req = request.into_inner();
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
        let req = request.into_inner();
        let manager = self.manager.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<LockEvent, Status>>(16);

        tokio::spawn(async move {
            let mut last_held = false;
            loop {
                tokio::time::sleep(WATCH_POLL).await;
                let held = match manager.probe_held(&req.key).await {
                    Ok(held) => held,
                    Err(_) => continue,
                };
                if held != last_held {
                    let event = if held {
                        LockEvent {
                            event: Some(palisade_proto::lock_event::Event::Acquired(Acquired {})),
                        }
                    } else {
                        LockEvent {
                            event: Some(palisade_proto::lock_event::Event::Freed(Freed {})),
                        }
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                    last_held = held;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
