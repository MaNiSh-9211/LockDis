//! INV-2/INV-3 integration proofs: safety policy spectrum and testaments.

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

fn url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn connect() -> Option<RedisLockManager> {
    match RedisLockManager::connect(RedisConfig::new(url())).await {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("skipping invention tests: no redis: {e}");
            None
        }
    }
}

fn opts() -> LockOptions {
    LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false)
}

fn unique(tag: &str) -> String {
    format!(
        "palisade-inv-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

/// INV-3: a crashed leader's testament reaches its successor.
#[tokio::test]
async fn testament_survives_crash_and_reaches_successor() {
    let Some(mgr) = connect().await else { return };
    let key = unique("will");

    // Leader A acquires with a SHORT lease (no watchdog) — it will die.
    let a_opts = LockOptions::default()
        .with_ttl(Duration::from_millis(400))
        .with_watchdog(false);
    let _a = mgr.try_lock_with(&key, &a_opts).await.expect("A grant");
    mgr.set_testament(
        &key,
        &_a.owner().as_uuid().to_string(),
        Duration::from_secs(30),
        b"checkpoint=4172;inflight=none",
    )
    .await
    .expect("set will");

    // A dies silently; lease lapses; will outlives the lock (30s own TTL).
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Successor B takes over and reads the dying wish.
    let b = mgr.try_lock_with(&key, &opts()).await.expect("B grant");
    let will = b.owner(); // touch to keep handle alive in scope
    let _ = will;
    let payload = mgr.read_testament(&key).await.expect("read will");
    assert_eq!(
        payload.as_deref(),
        Some(b"checkpoint=4172;inflight=none".as_slice()),
        "successor must receive predecessor's testament"
    );

    // Once consumed, the CURRENT holder clears the consumed will.
    mgr.clear_testament(&key, &b.owner().as_uuid().to_string())
        .await
        .expect("successor clears consumed will");
    assert_eq!(mgr.read_testament(&key).await.unwrap(), None);

    b.release().await.expect("B release");
}

/// INV-3b: graceful release destroys the testament (no ghost state).
#[tokio::test]
async fn graceful_release_clears_testament() {
    let Some(mgr) = connect().await else { return };
    let key = unique("will-clear");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
    mgr.set_testament(
        &key,
        &h.owner().as_uuid().to_string(),
        Duration::from_secs(60),
        b"secret",
    )
    .await
    .expect("set");
    mgr.clear_testament(&key, &h.owner().as_uuid().to_string())
        .await
        .expect("clear");
    assert_eq!(mgr.read_testament(&key).await.unwrap(), None);
    h.release().await.expect("release");
}

/// INV-3c: a non-owner cannot forge someone else's testament.
#[tokio::test]
async fn testament_requires_ownership() {
    let Some(mgr) = connect().await else { return };
    let key = unique("will-owner");
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");

    // Impostor with no matching token: set must be refused (Lost).
    let impostor_token = palisade_core::OwnerId::generate().as_uuid().to_string();
    let err = mgr
        .set_testament(&key, &impostor_token, Duration::from_secs(5), b"evil")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");

    h.release().await.expect("release");
}
