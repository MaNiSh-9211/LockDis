//! End-to-end gRPC roundtrip: in-process server + live Redis.
//! Skips silently without a Redis.

use std::time::Duration;

use palisade_client::{PalisadeClient, WatchEvent, WatchEventKind};
use palisade_core::{Error, LockOptions};
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn spawn_stack() -> Option<(PalisadeClient, String)> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping grpc test: no redis at {url}: {e}");
            return None;
        }
    };
    let service = PalisadeService::new(manager, ServiceConfig::default());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(LockServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    let client = PalisadeClient::connect(format!("http://{addr}"))
        .await
        .expect("client connect");
    Some((client, format!("{addr}")))
}

fn unique_key(name: &str) -> String {
    format!(
        "palisade-grpc-test:{name}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

#[tokio::test]
async fn grpc_roundtrip_lock_unlock_and_mutual_exclusion() {
    let Some((client, _)) = spawn_stack().await else {
        return;
    };
    let key = unique_key("roundtrip");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h1 = client.try_lock(&key, &opts).await.expect("grant");
    assert!(h1.fence().value() > 0);

    let err = client.try_lock(&key, &opts).await.unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");

    h1.release().await.expect("release");

    let h2 = client.try_lock(&key, &opts).await.expect("regrant");
    assert!(h2.fence().supersedes(h1.fence()));
    h2.release().await.expect("release");
}

#[tokio::test]
async fn grpc_extend_and_lost_detection() {
    let Some((client, _)) = spawn_stack().await else {
        return;
    };
    let key = unique_key("extend");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h = client.try_lock(&key, &opts).await.expect("grant");
    h.extend(Duration::from_secs(5)).await.expect("extend");

    // Kill our own lease from "another process" using raw redis.
    let redis_url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let rc = redis::Client::open(redis_url.as_str()).unwrap();
    let mut conn = rc.get_multiplexed_async_connection().await.unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .unwrap();

    let err = h.extend(Duration::from_secs(5)).await.unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");
}

#[tokio::test]
async fn grpc_watch_reports_state_changes() {
    let Some((client, _)) = spawn_stack().await else {
        return;
    };
    let key = unique_key("watch");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let mut events = client.watch(&key).await.expect("watch");
    tokio::time::sleep(Duration::from_millis(250)).await;

    let h = client.try_lock(&key, &opts).await.expect("grant");
    let acquired = wait_for(&mut events, WatchEventKind::Acquired).await;
    assert!(acquired, "never saw Acquired");

    h.release().await.expect("release");
    let freed = wait_for(&mut events, WatchEventKind::Freed).await;
    assert!(freed, "never saw Freed");
}

async fn wait_for<S>(stream: &mut S, want: WatchEventKind) -> bool
where
    S: tokio_stream::Stream<Item = WatchEvent> + Unpin,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(300), stream.next()).await {
            Ok(Some(ev)) if ev.kind() == want => return true,
            Ok(Some(_)) => continue,
            _ => {}
        }
    }
    false
}
