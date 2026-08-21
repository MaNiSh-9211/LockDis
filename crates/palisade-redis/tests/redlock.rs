//! Integration tests for Redlock quorum semantics.
//!
//! Uses five logical DBs on one Redis instance as stand-ins for five
//! independent masters — enough to exercise quorum math, rollback, and
//! loss detection at the algorithm level. True failure-domain independence
//! is validated by the Phase 6 chaos suite.

use std::time::Duration;

use palisade_core::{Error, LockOptions};
use palisade_redis::{RedlockConfig, RedlockManager};

fn urls() -> Vec<String> {
    let base =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    (0..5).map(|db| format!("{base}/{db}")).collect()
}

async fn connect() -> Option<RedlockManager> {
    match RedlockManager::connect(RedlockConfig::new(urls())).await {
        Ok(mgr) => Some(mgr),
        Err(e) => {
            eprintln!("skipping redlock test: no redis: {e}");
            None
        }
    }
}

fn unique_key(name: &str) -> String {
    format!(
        "palisade-test:{name}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

#[tokio::test]
async fn redlock_grants_quorum_and_fences_increase() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("grant");
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let h1 = mgr.try_lock(&key, &opts).await.expect("first grant");
    let f1 = h1.fence();

    let err = mgr.try_lock(&key, &opts).await.unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");

    h1.release().await.expect("release");
    let h2 = mgr.try_lock(&key, &opts).await.expect("second grant");
    assert!(h2.fence().supersedes(f1));
    h2.release().await.expect("release");
}

#[tokio::test]
async fn redlock_rollback_when_quorum_blocked() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("rollback");
    let opts = LockOptions::default().with_ttl(Duration::from_millis(500));

    // Block DBs 0,1,2 (a quorum) via direct single-instance-style holds:
    // acquire with the redlock manager, then delete only the minority
    // copies so a fresh acquire cannot reach quorum... simpler inverse:
    // hold the key ourselves, then remove minority copies, so another
    // acquirer sees majority-held.
    let holder = mgr.try_lock(&key, &opts).await.expect("holder");
    for db in 3..5 {
        raw_del(db, &key).await;
    }

    let err = mgr.try_lock(&key, &opts).await.unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");

    drop(holder);
    tokio::time::sleep(Duration::from_millis(600)).await;

    // After expiry everywhere, acquisition works again.
    let again = mgr
        .try_lock(
            &key,
            &LockOptions::default().with_ttl(Duration::from_secs(5)),
        )
        .await
        .expect("acquire after expiry");
    again.release().await.expect("cleanup");
}

#[tokio::test]
async fn redlock_release_clears_every_node() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("clear");

    let h = mgr.try_lock_default(&key).await.expect("grant");
    h.release().await.expect("release");

    for db in 0..5 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!raw_exists(db, &key).await, "key still present on db {db}");
    }
}

#[tokio::test]
async fn redlock_extend_reports_lost_when_quorum_dies() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("lost");
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let h = mgr.try_lock(&key, &opts).await.expect("grant");
    assert!(!h.is_lost());

    // Kill a quorum: wipe the key on DBs 0..=2.
    for db in 0..3 {
        raw_del(db, &key).await;
    }

    let err = h.extend(Duration::from_secs(5)).await.unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !h.is_lost() {
        assert!(std::time::Instant::now() < deadline, "loss never signaled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn redlock_config_rejects_small_rings() {
    let err = RedlockManager::connect(RedlockConfig::new(vec![
        "redis://127.0.0.1:6379/0".into(),
        "redis://127.0.0.1:6379/1".into(),
    ]))
    .await
    .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
}

async fn conn_for(db: u8) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("{}/{}", base_url(), db).as_str()).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

fn base_url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn raw_del(db: u8, key: &str) {
    let mut conn = conn_for(db).await;
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap();
}

async fn raw_exists(db: u8, key: &str) -> bool {
    let mut conn = conn_for(db).await;
    let n: i64 = redis::cmd("EXISTS")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap();
    n == 1
}
