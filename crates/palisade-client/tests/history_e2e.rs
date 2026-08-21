//! Layer-3 over real traffic: concurrent gRPC clients record histories,
//! and the invariant checker validates mutual exclusion end-to-end.
//! Skips silently without a Redis.

use std::sync::Arc;
use std::time::Duration;

use palisade_client::{PalisadeClient, RemoteLockHandle};
use palisade_core::{Error, LockOptions, OwnerId};
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};
use palisade_testing::{HistoryRecorder, check_client_history};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn spawn_stack() -> Option<(PalisadeClient, String)> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping history e2e: no redis at {url}: {e}");
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

    Some((
        PalisadeClient::connect(format!("http://{addr}"))
            .await
            .expect("client connect"),
        addr.to_string(),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_grpc_traffic_passes_invariant_checker() {
    let Some((base_client, _)) = spawn_stack().await else {
        return;
    };
    let recorder = HistoryRecorder::new();
    let client = Arc::new(base_client.clone().with_history(recorder.clone()));

    let key = format!("palisade-history-test:{}", OwnerId::generate().as_uuid());
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(30))
        .with_watchdog(false);

    const WORKERS: usize = 4;
    const CYCLES: usize = 5;

    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let client = client.clone();
        let key = key.clone();
        let opts = opts.clone();
        handles.push(tokio::spawn(async move {
            let mut last_fence = 0u64;
            for _ in 0..CYCLES {
                let h: RemoteLockHandle = match client
                    .try_lock_for(&key, &opts, Duration::from_secs(10))
                    .await
                {
                    Ok(h) => h,
                    Err(Error::Held { .. }) | Err(Error::Timeout { .. }) => continue,
                    Err(e) => panic!("unexpected acquire failure: {e}"),
                };
                assert!(h.fence().value() > 0);
                if last_fence > 0 && w == 0 {
                    // Fences only ever grow; per-worker spot check.
                    assert!(h.fence().value() >= last_fence);
                }
                last_fence = h.fence().value();
                h.release().await.expect("release");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // The recorded traffic must satisfy the same invariants the simulator
    // enforces: no double grants, no releases of unheld tokens.
    let entries = recorder.snapshot();
    assert!(
        entries.len() >= WORKERS * CYCLES * 2,
        "expected at least {} entries, got {}",
        WORKERS * CYCLES * 2,
        entries.len()
    );
    check_client_history(&key, &entries, opts.ttl.as_millis() as u64)
        .unwrap_or_else(|v| panic!("real-traffic history violated invariants: {v}"));
}
