//! Integration tests for the single-instance mutex.
//!
//! These run against a real Redis. If none is reachable they skip
//! silently so `cargo test` stays green on machines without Docker.
//!
//! Override the endpoint with `PALISADE_REDIS_URL` (default
//! `redis://127.0.0.1:6379`).

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

const KEY_PREFIX: &str = "palisade-test";

fn url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn connect() -> Option<RedisLockManager> {
    match RedisLockManager::connect(RedisConfig::new(url())).await {
        Ok(mgr) => Some(mgr),
        Err(e) => {
            eprintln!("skipping integration test: no redis at {}: {e}", url());
            None
        }
    }
}

fn unique_key(name: &str) -> String {
    format!("{KEY_PREFIX}:{name}:{}", uuid_v7())
}

fn uuid_v7() -> String {
    palisade_core::OwnerId::generate().as_uuid().to_string()
}

async fn raw_del(key: &str) {
    let client = redis::Client::open(url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap();
}

#[tokio::test]
async fn mutual_exclusion_second_try_lock_fails() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("mutex");

    let h1 = mgr.try_lock(&key).await.expect("first grant");
    let err = mgr.try_lock(&key).await.unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");

    h1.release().await.expect("release");
    let _h2 = mgr.try_lock(&key).await.expect("grant after release");
}

#[tokio::test]
async fn fence_tokens_increase_across_grants() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("fence");

    let mut last = palisade_core::FencingToken::ZERO;
    for _ in 0..3 {
        let h = mgr.try_lock(&key).await.expect("grant");
        let fence = h.fence();
        assert!(fence.supersedes(last), "{fence} must supersede {last}");
        last = fence;
        h.release().await.expect("release");
    }
}

#[tokio::test]
async fn release_of_expired_lease_reports_lost() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("lost");

    let h = mgr
        .try_lock_with(
            &key,
            &LockOptions {
                ttl: Duration::from_millis(50),
            },
        )
        .await
        .expect("grant");
    raw_del(&key).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let err = h.release().await.unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");
}

#[tokio::test]
async fn extend_keeps_ownership_and_blocks_others() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("extend");

    let h = mgr
        .try_lock_with(
            &key,
            &LockOptions {
                ttl: Duration::from_millis(120),
            },
        )
        .await
        .expect("grant");
    h.extend(Duration::from_secs(5)).await.expect("extend");

    assert!(matches!(
        mgr.try_lock(&key).await.unwrap_err(),
        Error::Held { .. }
    ));

    raw_del(&key).await;
    assert!(matches!(
        h.extend(Duration::from_secs(5)).await.unwrap_err(),
        Error::Lost { .. }
    ));
}

#[tokio::test]
async fn try_lock_for_times_out_while_contended() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("timeout");

    let _h = mgr.try_lock(&key).await.expect("grant");
    let started = std::time::Instant::now();
    let err = mgr
        .try_lock_for(&key, &LockOptions::new(), Duration::from_millis(150))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout { .. }), "got {err:?}");
    assert!(started.elapsed() >= Duration::from_millis(140));
}

#[tokio::test]
async fn drop_releases_eventually() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("drop");

    {
        let _h = mgr.try_lock(&key).await.expect("grant");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if mgr.try_lock(&key).await.is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "drop did not release in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn invalid_options_rejected() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("invalid");

    let err = mgr
        .try_lock_with(
            &key,
            &LockOptions {
                ttl: Duration::from_millis(1),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
}
