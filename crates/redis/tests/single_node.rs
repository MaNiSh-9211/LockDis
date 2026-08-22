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
            &LockOptions::default().with_ttl(Duration::from_millis(50)),
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
            &LockOptions::default().with_ttl(Duration::from_millis(120)),
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
            &LockOptions::default().with_ttl(Duration::from_millis(1)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "got {err:?}");
}

async fn raw_exists(key: &str) -> bool {
    let client = redis::Client::open(url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let n: i64 = redis::cmd("EXISTS")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap();
    n == 1
}

#[tokio::test]
async fn watchdog_renews_lease_past_ttl() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("wd-renew");

    let opts = LockOptions::default()
        .with_ttl(Duration::from_millis(300))
        .with_watchdog(true);
    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");

    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        matches!(mgr.try_lock(&key).await, Err(Error::Held { .. })),
        "watchdog failed to keep the lease alive past its ttl"
    );

    h.release().await.expect("release");
    mgr.try_lock(&key).await.expect("grant after release");
}

#[tokio::test]
async fn watchdog_stops_renewing_after_release() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("wd-stop");

    let opts = LockOptions::default()
        .with_ttl(Duration::from_millis(200))
        .with_watchdog(true);
    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
    h.release().await.expect("release");

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !raw_exists(&key).await,
        "watchdog resurrected the lease after release"
    );
}

#[tokio::test]
async fn watchdog_poisons_handle_when_lease_revoked() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("wd-poison");

    let opts = LockOptions::default()
        .with_ttl(Duration::from_millis(240))
        .with_watchdog(true);
    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
    raw_del(&key).await;

    let waiter = {
        let h = h.clone();
        tokio::spawn(async move { h.until_lost().await })
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !h.is_lost() && !waiter.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "loss was never signaled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("until_lost did not resolve")
        .unwrap();
    assert!(h.is_lost());
}

#[tokio::test]
async fn stale_holder_is_detected_and_new_fence_supersedes() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("stale");

    // Holder A "pauses" right after acquiring.
    let a = mgr
        .try_lock_with(
            &key,
            &LockOptions::default().with_ttl(Duration::from_millis(120)),
        )
        .await
        .expect("grant A");
    let fence_a = a.fence();

    // The lease dies while A is paused.
    raw_del(&key).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // B takes over and gets a strictly newer fence token.
    let b = mgr.try_lock(&key).await.expect("grant B");
    let fence_b = b.fence();
    assert!(
        fence_b.supersedes(fence_a),
        "{fence_b} must supersede {fence_a}"
    );

    // A resumes believing it still holds the lock; the lock layer must
    // refuse, and downstream resources would reject A's writes by fence.
    let err = a.extend(Duration::from_secs(5)).await.unwrap_err();
    assert!(matches!(err, Error::Lost { .. }), "got {err:?}");
    assert!(a.is_lost());

    b.release().await.expect("release B");
}
