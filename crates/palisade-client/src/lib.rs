//! Palisade client SDK over gRPC.
//!
//! One ergonomic surface in front of the wire contract: acquire, extend,
//! release, and watch — with an SDK-side watchdog (ADR 0013 semantics)
//! so remote leases stay alive exactly like local ones. The server is
//! stateless pass-through (ADR 0021); this crate owns the renewal loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use palisade_core::{Error, FencingToken, LockHandle, LockOptions, OwnerId, Result};
use palisade_proto::{
    ExtendRequest, LockOptions as OptionsPb, TryLockForRequest, TryLockRequest, UnlockRequest,
    WatchRequest, lock_service_client::LockServiceClient,
};
use palisade_testing::history::OpKind;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

/// Renewal cadence divisor, mirroring the library watchdog.
const RENEWAL_DIVISOR: u32 = 3;
const MAX_CONSECUTIVE_RENEW_FAILURES: u32 = 2;

/// Connection to a `palisade-server` endpoint. Cheap to clone.
#[derive(Clone)]
pub struct PalisadeClient {
    grpc: LockServiceClient<Channel>,
    history: Option<palisade_testing::HistoryRecorder>,
}

impl std::fmt::Debug for PalisadeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PalisadeClient").finish()
    }
}

/// Anonymized state change for a watched key (ADR 0022: no tokens).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchEvent {
    /// Someone holds the key now.
    Acquired,
    /// The key is free now.
    Freed,
}

fn status_to_error(status: tonic::Status) -> Error {
    Error::Backend(format!("rpc failed: {status}"))
}

fn options_pb(options: &LockOptions) -> OptionsPb {
    OptionsPb {
        ttl_ms: options.ttl.as_millis() as u64,
        watchdog: options.watchdog,
    }
}

impl PalisadeClient {
    /// Connects to `endpoint`, e.g. `http://127.0.0.1:50051`.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let channel = Self::channel(endpoint.into(), None).await?;
        Ok(Self {
            grpc: LockServiceClient::new(channel),
            history: None,
        })
    }

    /// Attaches a [`palisade_testing::HistoryRecorder`] so every subsequent
    /// operation lands in a checkable timeline.
    pub fn with_history(mut self, recorder: palisade_testing::HistoryRecorder) -> Self {
        self.history = Some(recorder);
        self
    }

    /// Connects over mutual TLS. `ca_pem` verifies the server; `cert_pem`/
    /// `key_pem` identify this client to the server.
    pub async fn connect_mtls(
        endpoint: impl Into<String>,
        ca_pem: impl AsRef<[u8]>,
        cert_pem: impl AsRef<[u8]>,
        key_pem: impl AsRef<[u8]>,
    ) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
            .identity(tonic::transport::Identity::from_pem(cert_pem, key_pem));
        let channel = Self::channel(endpoint.into(), Some(tls)).await?;
        Ok(Self {
            grpc: LockServiceClient::new(channel),
            history: None,
        })
    }

    async fn channel(
        endpoint: String,
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Result<Channel> {
        let mut ep = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| Error::InvalidConfig(format!("bad endpoint `{endpoint}`: {e}")))?;
        if let Some(tls) = tls {
            ep = ep
                .tls_config(tls)
                .map_err(|e| Error::InvalidConfig(format!("tls config rejected: {e}")))?;
        }
        ep.connect()
            .await
            .map_err(|e| Error::Backend(format!("connect failed: {e}")))
    }

    /// Attempts immediate acquisition over the wire.
    pub async fn try_lock(&self, key: &str, options: &LockOptions) -> Result<RemoteLockHandle> {
        let started = std::time::Instant::now();
        let request = TryLockRequest {
            key: key.to_owned(),
            options: Some(options_pb(options)),
        };
        let outcome = self
            .grpc
            .clone()
            .try_lock(Request::new(request))
            .await
            .map_err(status_to_error)?
            .into_inner();

        match outcome.result {
            Some(palisade_proto::lock_outcome::Result::Granted(g)) => {
                if let Some(rec) = &self.history {
                    rec.record(key, OpKind::TryAcquire, true, g.fencing_token, started);
                }
                Ok(self.make_handle(key, g.token, g.fencing_token, options))
            }
            Some(palisade_proto::lock_outcome::Result::Held(_)) => {
                if let Some(rec) = &self.history {
                    rec.record(key, OpKind::TryAcquire, false, 0, started);
                }
                Err(Error::Held {
                    key: key.to_owned(),
                })
            }
            _ => Err(Error::Backend("malformed LockOutcome".into())),
        }
    }

    /// Polls the server until acquired or `wait` elapses.
    pub async fn try_lock_for(
        &self,
        key: &str,
        options: &LockOptions,
        wait: Duration,
    ) -> Result<RemoteLockHandle> {
        let started = std::time::Instant::now();
        let request = TryLockForRequest {
            key: key.to_owned(),
            options: Some(options_pb(options)),
            wait_ms: wait.as_millis() as u64,
        };
        let outcome = self
            .grpc
            .clone()
            .try_lock_for(Request::new(request))
            .await
            .map_err(status_to_error)?
            .into_inner();

        match outcome.result {
            Some(palisade_proto::lock_outcome::Result::Granted(g)) => {
                if let Some(rec) = &self.history {
                    rec.record(key, OpKind::TryAcquire, true, g.fencing_token, started);
                }
                Ok(self.make_handle(key, g.token, g.fencing_token, options))
            }
            Some(palisade_proto::lock_outcome::Result::Held(_)) => {
                if let Some(rec) = &self.history {
                    rec.record(key, OpKind::TryAcquire, false, 0, started);
                }
                Err(Error::Held {
                    key: key.to_owned(),
                })
            }
            Some(palisade_proto::lock_outcome::Result::TimedOut(_)) => {
                if let Some(rec) = &self.history {
                    rec.record(key, OpKind::TryAcquire, false, 0, started);
                }
                Err(Error::Timeout {
                    key: key.to_owned(),
                })
            }
            None => Err(Error::Backend("malformed LockOutcome".into())),
        }
    }

    /// Subscribes to anonymized state changes for `key`.
    pub async fn watch(&self, key: &str) -> Result<ReceiverStream<WatchEvent>> {
        let mut grpc = self.grpc.clone();
        let mut stream = grpc
            .watch(Request::new(WatchRequest {
                key: key.to_owned(),
            }))
            .await
            .map_err(status_to_error)?
            .into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel::<WatchEvent>(16);
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            while let Some(Ok(event)) = stream.next().await {
                let ev = match event.event {
                    Some(palisade_proto::lock_event::Event::Acquired(_)) => WatchEvent::Acquired,
                    Some(palisade_proto::lock_event::Event::Freed(_)) => WatchEvent::Freed,
                    None => continue,
                };
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
        });
        Ok(ReceiverStream::new(rx))
    }

    fn make_handle(
        &self,
        key: &str,
        token: String,
        fence_value: u64,
        options: &LockOptions,
    ) -> RemoteLockHandle {
        let ttl = options.ttl;
        let shared = Arc::new(RemoteShared {
            grpc: self.grpc.clone(),
            history: self.history.clone(),
            key: key.to_owned(),
            token,
            fence: FencingToken::new(fence_value),
            owner: OwnerId::generate(),
            ttl,
            released: AtomicBool::new(false),
            lost: AtomicBool::new(false),
            lost_tx: tokio::sync::watch::channel(false).0,
        });
        if options.watchdog.unwrap_or(true) {
            spawn_watchdog(&shared);
        }
        RemoteLockHandle { shared }
    }
}

struct RemoteShared {
    grpc: LockServiceClient<Channel>,
    history: Option<palisade_testing::HistoryRecorder>,
    key: String,
    token: String,
    owner: OwnerId,
    fence: FencingToken,
    ttl: Duration,
    released: AtomicBool,
    lost: AtomicBool,
    lost_tx: tokio::sync::watch::Sender<bool>,
}

impl RemoteShared {
    fn mark_gone(&self) -> bool {
        !self.released.swap(true, Ordering::AcqRel)
    }

    fn mark_lost(&self) {
        self.lost.store(true, Ordering::Release);
        self.lost_tx.send_replace(true);
    }

    async fn fire_extend(&self, ttl: Duration) -> Result<bool> {
        let started = std::time::Instant::now();
        let mut grpc = self.grpc.clone();
        let response = grpc
            .extend(Request::new(ExtendRequest {
                key: self.key.clone(),
                token: self.token.clone(),
                ttl_ms: ttl.as_millis() as u64,
            }))
            .await
            .map_err(status_to_error)?
            .into_inner();
        let ok = matches!(
            response.result,
            Some(palisade_proto::extend_response::Result::Extended(_))
        );
        if let Some(rec) = &self.history {
            rec.record(&self.key, OpKind::Extend, ok, self.fence.value(), started);
        }
        Ok(ok)
    }

    async fn run_unlock(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let mut grpc = self.grpc.clone();
        let response = grpc
            .unlock(Request::new(UnlockRequest {
                key: self.key.clone(),
                token: self.token.clone(),
            }))
            .await
            .map_err(status_to_error)?
            .into_inner();
        let released = matches!(
            response.result,
            Some(palisade_proto::unlock_response::Result::Released(_))
        );
        if let Some(rec) = &self.history {
            rec.record(
                &self.key,
                OpKind::Release,
                released,
                self.fence.value(),
                started,
            );
        }
        if released {
            Ok(())
        } else {
            Err(Error::Lost {
                key: self.key.clone(),
                fence: self.fence.value(),
            })
        }
    }
}

impl Drop for RemoteShared {
    fn drop(&mut self) {
        if !self.mark_gone() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let shared = RemoteShared {
                grpc: self.grpc.clone(),
                history: None,
                key: self.key.clone(),
                token: self.token.clone(),
                owner: self.owner.clone(),
                fence: self.fence,
                ttl: self.ttl,
                released: AtomicBool::new(true),
                lost: AtomicBool::new(false),
                lost_tx: tokio::sync::watch::channel(true).0,
            };
            std::mem::drop(runtime.spawn(async move {
                let _ = shared.run_unlock().await;
            }));
        }
    }
}

fn spawn_watchdog(shared: &Arc<RemoteShared>) {
    let weak = Arc::downgrade(shared);
    let ttl = shared.ttl;
    tokio::spawn(async move {
        let mut transient_failures = 0u32;
        loop {
            tokio::time::sleep(ttl / RENEWAL_DIVISOR).await;
            let Some(s) = weak.upgrade() else {
                return;
            };
            if s.released.load(Ordering::Acquire) {
                return;
            }
            match s.fire_extend(ttl).await {
                Ok(true) => transient_failures = 0,
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

/// A remotely-held lock. Clones share one lease.
#[derive(Clone)]
pub struct RemoteLockHandle {
    shared: Arc<RemoteShared>,
}

impl std::fmt::Debug for RemoteLockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLockHandle")
            .field("key", &self.shared.key)
            .field("fence", &self.shared.fence)
            .finish()
    }
}

impl RemoteLockHandle {
    /// The lock key.
    pub fn key(&self) -> &str {
        &self.shared.key
    }

    /// Fence token from the grant.
    pub fn fence(&self) -> FencingToken {
        self.shared.fence
    }

    /// Releases over the wire. Idempotent per handle.
    pub async fn release(&self) -> Result<()> {
        if !self.shared.mark_gone() {
            return Ok(());
        }
        self.shared.run_unlock().await
    }

    /// Refreshes the remote lease.
    pub async fn extend(&self, ttl: Duration) -> Result<()> {
        if ttl < palisade_core::MIN_TTL {
            return Err(Error::InvalidConfig(format!(
                "extend ttl {:?} is below the {:?} floor",
                ttl,
                palisade_core::MIN_TTL
            )));
        }
        if self.shared.released.load(Ordering::Acquire) {
            return Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            });
        }
        if self.shared.fire_extend(ttl).await? {
            Ok(())
        } else {
            self.shared.mark_lost();
            Err(Error::Lost {
                key: self.shared.key.clone(),
                fence: self.shared.fence.value(),
            })
        }
    }
}

#[async_trait]
impl LockHandle for RemoteLockHandle {
    fn key(&self) -> &str {
        RemoteLockHandle::key(self)
    }

    fn owner(&self) -> &OwnerId {
        &self.shared.owner
    }

    fn fence(&self) -> FencingToken {
        RemoteLockHandle::fence(self)
    }

    fn is_lost(&self) -> bool {
        self.shared.lost.load(Ordering::Acquire)
    }

    async fn until_lost(&self) {
        let mut rx = self.shared.lost_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    async fn extend(&self, ttl: Duration) -> Result<()> {
        RemoteLockHandle::extend(self, ttl).await
    }

    async fn release(&self) -> Result<()> {
        RemoteLockHandle::release(self).await
    }
}
