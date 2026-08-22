//! Edge-case proofs (ROADMAP Track A): panic-unwind release, grant-undo on
//! dead sessions, slow-consumer isolation, heartbeat rate limiting.
//! Skips silently without a Redis.

use std::time::Duration;

use palisade_client::{PalisadeClient, RemoteLockHandle};
use palisade_core::LockOptions;
use palisade_proto::lock_service_client::LockServiceClient;
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_proto::{HeartbeatRequest, RegisterSessionRequest};
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::Request;
use tonic::transport::{Channel, Server};

async fn spawn_stack() -> Option<(PalisadeClient, String)> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping edges e2e: no redis: {e}");
            return None;
        }
    };
    let service = PalisadeService::new(manager, ServiceConfig::default());
    let _sweeper = palisade_server::start_session_sweeper(&service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(LockServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Some((
        PalisadeClient::connect(format!("http://{addr}"))
            .await
            .expect("connect"),
        addr,
    ))
}

fn unique(tag: &str) -> String {
    format!(
        "palisade-edge-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

fn opts() -> LockOptions {
    LockOptions::default()
        .with_ttl(Duration::from_secs(60))
        .with_watchdog(false)
}

/// A4: a panic mid-critical-section must still release — Drop runs during
/// unwind, so successors are not blocked for the full TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_inside_critical_section_releases() {
    let Some((client, _)) = spawn_stack().await else {
        return;
    };
    let key = unique("panic");

    // The spawned task owns the ONLY handle: unwind must drop it and fire
    // the detached release.
    let c2 = client.clone();
    let task_key = key.clone();
    let result = tokio::spawn(async move {
        let _held: RemoteLockHandle = c2.try_lock(&task_key, &opts()).await.expect("grant");
        panic!("boom inside critical section");
    })
    .await;

    assert!(result.is_err(), "task should have panicked");
    // Unwind dropped the handle clones -> detached release fired.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if client.try_lock(&key, &opts()).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "panic did not release the lock"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A5: acquiring with a session that died before the attempt must be
/// rejected AND must not leave the granted lock behind (server undo).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grant_with_dead_session_is_undone() {
    let Some((client, addr)) = spawn_stack().await else {
        return;
    };
    let key = unique("deadsession");

    // Register a session with a tiny TTL and let it expire.
    let mut raw = LockServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("raw connect");
    let reg = raw
        .register_session(Request::new(RegisterSessionRequest {
            client_id: "ghost".into(),
            ttl_ms: 100,
        }))
        .await
        .expect("register")
        .into_inner();
    let ghost_token = reg.session_token;
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Raw gRPC lets us present the dead token explicitly.
    let mut raw2 = LockServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("raw connect");
    let outcome = raw2
        .try_lock(Request::new(palisade_proto::TryLockRequest {
            key: key.clone(),
            options: Some(palisade_proto::LockOptions {
                ttl_ms: 5_000,
                watchdog: Some(false),
            }),
            session: ghost_token,
        }))
        .await;

    match outcome {
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::NotFound, "got {status}");
        }
        Ok(resp) => {
            // If the server raced us and accepted, the bind must have failed
            // closed too — either way the key must be free right after.
            let granted = matches!(
                resp.into_inner().result,
                Some(palisade_proto::lock_outcome::Result::Granted(_))
            );
            assert!(!granted, "grant bound to a dead session slipped through");
        }
    }

    // Key must be free: nothing left behind by the undone grant.
    let probe = client.describe_key(&key).await.expect("describe");
    assert!(!probe.0, "undone grant leaked a held key");
}

/// A7/B3: a slow watcher must not block others, and heartbeats faster than
/// the ttl/20 floor get rate-limited while legit cadence passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumer_and_rate_limit() {
    let Some((client, addr)) = spawn_stack().await else {
        return;
    };
    let key = unique("slow");

    // Slow consumer: subscribe and deliberately don't read.
    let slow = client.watch(&key).await.expect("slow watch");

    let fast = client.watch(&key).await.expect("fast watch");
    let mut fast = fast;

    let opts = LockOptions::default()
        .with_ttl(Duration::from_millis(150))
        .with_watchdog(false);

    // Churn 12 short cycles; the slow side reads nothing during this.
    for _ in 0..12 {
        let h = client.try_lock(&key, &opts).await.expect("cycle grant");
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(h);
        tokio::time::sleep(Duration::from_millis(120)).await;
    }

    // Fast watcher still receives live transitions despite the silent one.
    let h = client
        .try_lock(&key, &opts)
        .await
        .expect("post-churn grant");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::time::timeout(Duration::from_millis(200), fast.next()).await {
            Ok(Some(ev)) if ev.kind() == palisade_client::WatchEventKind::Acquired => break,
            Ok(Some(_)) => continue,
            _ => {}
        }
        assert!(std::time::Instant::now() < deadline, "fast watcher starved");
    }
    h.release().await.expect("release");
    drop(slow);

    // Rate limiting: hammer heartbeats on a fresh session.
    let mut raw = LockServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("raw");
    let reg = raw
        .register_session(Request::new(RegisterSessionRequest {
            client_id: "hammer".into(),
            ttl_ms: 2_000,
        }))
        .await
        .expect("register")
        .into_inner();

    let mut limited = false;
    for _ in 0..40 {
        let r = LockServiceClient::clone(&raw)
            .heartbeat(Request::new(HeartbeatRequest {
                session_token: reg.session_token.clone(),
            }))
            .await;
        match r {
            Ok(_) => {}
            Err(s) if s.code() == tonic::Code::ResourceExhausted => {
                limited = true;
                break;
            }
            Err(e) => panic!("unexpected heartbeat error: {e}"),
        }
    }
    assert!(limited, "burst of heartbeats was never rate-limited");
}

// Silence unused import when Redis is absent and tests early-return.
#[allow(unused)]
fn _keep(_: Channel, _: RemoteLockHandle) {}
