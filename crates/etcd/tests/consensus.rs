//! Integration tests for the etcd (consensus) backend.
//!
//! Requires etcd at `ETCD_ENDPOINTS` (default `http://127.0.0.1:2379`).
//! Skips silently when unreachable so the rest of the suite stays green.

use std::time::Duration;

use palisade_core::{Error, LockOptions};
use palisade_etcd::{EtcdConfig, EtcdLockManager};

fn endpoints() -> Vec<String> {
    std::env::var("ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".into())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

async fn connect() -> Option<EtcdLockManager> {
    match EtcdLockManager::connect(EtcdConfig::new(endpoints())).await {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("skipping etcd test: no cluster at {:?}: {e}", endpoints());
            None
        }
    }
}

fn unique_key(tag: &str) -> String {
    format!(
        "palisade-etcd-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

#[tokio::test]
async fn mutual_exclusion_and_fence_monotonicity() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("mutex");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h1 = mgr.try_lock_with(&key, &opts).await.expect("first grant");
    assert!(h1.fence().value() > 0, "fence must be a real revision");

    let err = mgr.try_lock_with(&key, &opts).await.unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");

    let fence1 = h1.fence();
    h1.release().await.expect("release");

    let h2 = mgr.try_lock_with(&key, &opts).await.expect("second grant");
    assert!(
        h2.fence().supersedes(fence1),
        "{} must supersede {}",
        h2.fence(),
        fence1
    );
    h2.release().await.expect("release");
}

#[tokio::test]
async fn watchdog_keeps_lease_alive_past_ttl() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("watchdog");
    // 2s lease; keepalives at ~666ms; we hold for 4s = 2x past expiry.
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(2))
        .with_watchdog(true);

    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!h.is_lost(), "watchdog let a live lease die");
    assert!(matches!(
        mgr.try_lock_with(&key, &opts).await.unwrap_err(),
        Error::Held { .. }
    ));
    h.release().await.expect("release");
}

#[tokio::test]
async fn server_expires_lease_when_keepalives_stop() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("expiry");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(1))
        .with_watchdog(false);

    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
    eprintln!(
        "server-assigned lease ttl: {}s",
        mgr.lease_time_to_live(h.lease_id()).await.unwrap()
    );

    // Holder goes silent (no keepalives): the SERVER must expire the lease
    // and free the key — no client-side release involved. Poll generously;
    // etcd's lessor sweeps on its own cadence.
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    let successor = loop {
        match mgr.try_lock_with(&key, &opts).await {
            Ok(s) => break s,
            Err(Error::Held { .. }) => {}
            Err(e) => panic!("unexpected acquire failure: {e}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server never expired the abandoned lease"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    eprintln!(
        "freed after ~{}ms by server-side expiry",
        std::time::Instant::now()
            .duration_since(std::time::Instant::now())
            .as_millis()
    );
    assert!(successor.fence().supersedes(h.fence()));

    // The stale holder's operations must now report loss.
    let err = h.release().await.unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");
    assert!(h.is_lost());
    successor.release().await.expect("cleanup");
}

#[tokio::test]
async fn drop_releases_eventually() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("drop");
    {
        let _h = mgr.try_lock(&key).await.expect("grant");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if mgr.try_lock(&key).await.is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "drop did not release in time"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn config_rejects_empty_endpoints() {
    let err = EtcdLockManager::connect(EtcdConfig::new(vec![]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
}
