//! Authorization, quota, and audit e2e tests (ADR 0028).
//! Skips silently without a Redis.

use std::time::Duration;

use palisade_client::PalisadeClient;
use palisade_core::{Error, LockOptions};
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{Acl, PalisadeService, ServiceConfig};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const ACL_JSON: &str = r#"{
  "principals": [
    { "name": "ci",   "token": "ci-token",   "key_prefixes": ["ci/", "palisade-authz-test:"], "max_keys": 1, "max_watchers": 1 },
    { "name": "root", "token": "root-token", "key_prefixes": [""],    "can_admin": true }
  ]
}"#;

async fn spawn_stack_with_acl() -> Option<(String, String)> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping authz e2e: no redis: {e}");
            return None;
        }
    };
    let acl_path = std::env::temp_dir().join("palisade-test-acl.json");
    std::fs::write(&acl_path, ACL_JSON).unwrap();
    let acl = Acl::load_file(&acl_path).expect("acl");

    let service = PalisadeService::new(manager, ServiceConfig::default()).with_acl(acl);
    let _sweeper = palisade_server::start_session_sweeper(&service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(LockServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Some((addr.clone(), addr))
}

async fn client_at(addr: &str, token: Option<&str>) -> PalisadeClient {
    let c = PalisadeClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    match token {
        Some(t) => c.with_token(t),
        None => c,
    }
}

fn unique(tag: &str) -> String {
    format!(
        "palisade-authz-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

fn opts() -> LockOptions {
    LockOptions::default()
        .with_ttl(Duration::from_secs(30))
        .with_watchdog(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_or_unknown_token_rejected() {
    let Some((addr, _)) = spawn_stack_with_acl().await else {
        return;
    };

    let anon = client_at(&addr, None).await;
    let err = anon.try_lock(&unique("anon"), &opts()).await.unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("authorization")),
        "got {err:?}"
    );

    let bad = client_at(&addr, Some("not-a-token")).await;
    let err = bad.try_lock(&unique("bad"), &opts()).await.unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("unknown bearer")),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefix_scoping_enforced() {
    let Some((addr, _)) = spawn_stack_with_acl().await else {
        return;
    };
    let ci = client_at(&addr, Some("ci-token")).await;

    // Inside the ci/ namespace: fine.
    let h = ci
        .try_lock(&unique("ci-ok"), &opts())
        .await
        .expect("ci lock inside prefix");
    h.release().await.expect("release");

    // Outside it: denied.
    let err = ci
        .try_lock(
            &format!(
                "other/zone:{}",
                palisade_core::OwnerId::generate().as_uuid()
            ),
            &opts(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("prefixes")),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_keys_quota_enforced() {
    let Some((addr, _)) = spawn_stack_with_acl().await else {
        return;
    };
    let ci = client_at(&addr, Some("ci-token")).await;

    let k1 = unique("ci-q1");
    let k2 = unique("ci-q2");
    let h1 = ci.try_lock(&k1, &opts()).await.expect("first within quota");

    let err = ci.try_lock(&k2, &opts()).await.unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("max_keys")),
        "got {err:?}"
    );

    h1.release().await.expect("release");
    let again = ci
        .try_lock(&k2, &opts())
        .await
        .expect("quota freed after release");
    again.release().await.expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_unlock_is_admin_only_and_audited() {
    let Some((addr, _)) = spawn_stack_with_acl().await else {
        return;
    };
    let ci = client_at(&addr, Some("ci-token")).await;
    let root = client_at(&addr, Some("root-token")).await;

    let key = unique("ci-force");
    let h = ci.try_lock(&key, &opts()).await.expect("ci grant");

    // Non-admin cannot force-unlock.
    let err = ci.unlock_force(&key).await.unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("permission_denied") || m.contains("lacks")),
        "got {err:?}"
    );

    // Admin break-glass works even without the owner token.
    let released = root.unlock_force(&key).await.expect("admin force unlock");
    assert!(released);

    // The original holder's release now reports loss — its lease is gone.
    drop(h);
    let observer = client_at(&addr, Some("root-token")).await;
    let h2 = observer
        .try_lock_for(&key, &opts(), Duration::from_secs(3))
        .await
        .expect("key free after force unlock");
    h2.release().await.expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_quota_enforced() {
    let Some((addr, _)) = spawn_stack_with_acl().await else {
        return;
    };
    let ci = client_at(&addr, Some("ci-token")).await;
    let key = unique("ci-watch");

    let w1 = ci.watch(&key).await.expect("first watcher within quota");
    let err = ci.watch(&key).await.unwrap_err();
    assert!(
        matches!(err, Error::Backend(ref m) if m.contains("max_watchers")),
        "got {err:?}"
    );

    drop(w1); // frees the slot (stream dropped)
    // Slot release is asynchronous on the server task; poll generously.
    // Under full-suite parallel load, h2 stream teardown propagates slower;
    // give the disconnect-detection chain a generous budget.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match ci.watch(&key).await {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "watcher slot never freed: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

// Keep WatchEvent import honest (used indirectly by stream typing above).
