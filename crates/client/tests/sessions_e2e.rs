//! Server-authoritative session tests (ADR 0027): locks bound to a session
//! die when the client dies — decided by the server, not lease arithmetic.

use std::time::Duration;

use palisade_client::{PalisadeClient, RemoteLockHandle};
use palisade_core::{Error, LockOptions};
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn spawn_stack_addr() -> Option<(PalisadeClient, String)> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping session e2e: no redis: {e}");
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

fn unique_key(tag: &str) -> String {
    format!(
        "palisade-session-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

/// The headline guarantee: crash the client (drop everything, no release,
/// no close) and the SERVER frees the lock within ttl + sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashed_client_locks_die_server_side() {
    let Some((_client, addr)) = spawn_stack_addr().await else {
        return;
    };
    let key = unique_key("crash");

    {
        // Isolated scope: session client exists only here.
        let session_client = PalisadeClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        session_client
            .attach_session("crasher", Duration::from_secs(3))
            .await
            .expect("register");
        let opts = LockOptions::default().with_ttl(Duration::from_secs(60));
        let _h = session_client.try_lock(&key, &opts).await.expect("grant");
        // Client "crashes": everything dropped without release/close.
    }

    // A fresh observer must get the key within ~ttl + sweep (~4s), even
    // though the original lease TTL was 60s.
    let observer = PalisadeClient::connect(format!("http://{addr}"))
        .await
        .expect("observer connect");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        match observer.try_lock(&key, &opts).await {
            Ok(h) => {
                h.release().await.expect("cleanup");
                break;
            }
            Err(Error::Held { .. }) => {}
            Err(e) => panic!("unexpected: {e}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server never released a dead session's lock"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Heartbeats keep the session alive; close releases immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_hold_and_close_releases() {
    let Some((client, _)) = spawn_stack_addr().await else {
        return;
    };
    let key = unique_key("hb");

    client
        .attach_session("worker", Duration::from_secs(2))
        .await
        .expect("register");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(60))
        .with_watchdog(false);
    let h: RemoteLockHandle = client.try_lock(&key, &opts).await.expect("grant");

    // 5 seconds with heartbeats every ~666ms across a 2s session ttl.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(matches!(
        client.try_lock(&key, &opts).await.unwrap_err(),
        Error::Held { .. },
    ));

    // Explicit close frees instantly.
    drop(h);
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.close_session().await.expect("close");
    let again = client
        .try_lock(&key, &opts)
        .await
        .expect("freed after close");
    again.release().await.expect("cleanup");
}
