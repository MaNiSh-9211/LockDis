//! INV-6 · Semantic Locks: business predicates evaluated atomically
//! inside the grant script — zero TOCTOU window.
//! Skips silently without a Redis.

use std::time::Duration;

use palisade_core::{Error, LockHandle};
use palisade_redis::RedisConfig;

fn url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn connect() -> Option<palisade_redis::RedisLockManager> {
    match palisade_redis::RedisLockManager::connect(RedisConfig::new(url())).await {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("skipping semantic test: no redis: {e}");
            None
        }
    }
}

fn unique(tag: &str) -> String {
    format!(
        "palisade-sem-test:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

async fn seed_data(key: &str, fields: &[(&str, &str)]) {
    let client = redis::Client::open(url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for (f, v) in fields {
        let _: i64 = redis::cmd("HSET")
            .arg(format!("{key}:data"))
            .arg(f)
            .arg(v)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn predicate_pass_grants_and_fail_denies() {
    let Some(mgr) = connect().await else { return };
    let key = unique("sem");

    // Seed: status=pending, items=5, no cancelled_at.
    seed_data(&key, &[("status", "pending"), ("items", "5")]).await;

    // All predicates pass → grant.
    let h = mgr
        .acquire_where(&key)
        .field_equals("status", "pending")
        .field_gt("items", 0.0)
        .field_absent("cancelled_at")
        .ttl(Duration::from_secs(10))
        .acquire()
        .await
        .expect("grant when all predicates pass");
    assert!(h.fence().value() > 0);

    // Second acquire on same key → Held (lock is exclusive).
    let err = mgr
        .acquire_where(&key)
        .field_equals("status", "pending")
        .ttl(Duration::from_secs(10))
        .acquire()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Held { .. }));

    h.release().await.expect("release");

    // Now flip a field to violate the predicate → Held even though lock is free.
    seed_data(&key, &[("cancelled_at", "now")]).await;
    let err = mgr
        .acquire_where(&key)
        .field_absent("cancelled_at")
        .ttl(Duration::from_secs(10))
        .acquire()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Held { .. }), "got {err:?}");
}

#[tokio::test]
async fn zero_predicates_rejected() {
    let Some(mgr) = connect().await else { return };
    let key = unique("no-preds");
    let err = mgr.acquire_where(&key).acquire().await.unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}
