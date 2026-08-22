//! Integration tests for the Phase 3 primitives: reentrant, read-write,
//! semaphore, fair queue, multi-lock. Skips silently without a Redis.

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions, OwnerId};
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
    format!("{KEY_PREFIX}:{name}:{}", OwnerId::generate().as_uuid())
}

#[tokio::test]
async fn reentrant_same_owner_nests_and_counts() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("reent");
    let owner = OwnerId::generate();
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let h1 = mgr
        .try_lock_reentrant(&key, owner.clone(), &opts)
        .await
        .expect("first hold");
    let f1 = h1.fence();
    let h2 = mgr
        .try_lock_reentrant(&key, owner.clone(), &opts)
        .await
        .expect("reentry");
    assert!(h2.fence().supersedes(f1));

    let other = mgr
        .try_lock_reentrant(&key, OwnerId::generate(), &opts)
        .await
        .unwrap_err();
    assert!(matches!(other, Error::Held { .. }), "got {other:?}");

    h2.release_one().await.expect("inner release");
    assert!(matches!(
        mgr.try_lock_reentrant(&key, OwnerId::generate(), &opts)
            .await
            .unwrap_err(),
        Error::Held { .. }
    ));

    h1.release_one().await.expect("outer release");
    mgr.try_lock_reentrant(&key, OwnerId::generate(), &opts)
        .await
        .expect("fully freed after count hits zero");
}

#[tokio::test]
async fn reentrant_release_all_drops_every_hold() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("reent-all");
    let owner = OwnerId::generate();
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let _h1 = mgr
        .try_lock_reentrant(&key, owner.clone(), &opts)
        .await
        .expect("hold 1");
    let h2 = mgr
        .try_lock_reentrant(&key, owner.clone(), &opts)
        .await
        .expect("hold 2");

    h2.release_all().await.expect("release all");
    mgr.try_lock_reentrant(&key, OwnerId::generate(), &opts)
        .await
        .expect("freed despite one handle still alive");
}

#[tokio::test]
async fn rw_multiple_readers_then_exclusive_writer() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("rwl");
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let r1 = mgr.try_read(&key, &opts).await.expect("reader 1");
    let r2 = mgr.try_read(&key, &opts).await.expect("reader 2");
    assert!(matches!(
        mgr.try_write(&key, &opts).await.unwrap_err(),
        Error::Held { .. }
    ));

    r1.release().await.expect("r1 release");
    r2.release().await.expect("r2 release");

    let w = mgr
        .try_write(&key, &opts)
        .await
        .expect("writer after readers");
    assert!(matches!(
        mgr.try_read(&key, &opts).await.unwrap_err(),
        Error::Held { .. }
    ));
    w.release().await.expect("writer release");
    mgr.try_read(&key, &opts)
        .await
        .expect("reader after writer");
}

#[tokio::test]
async fn semaphore_capacity_and_expired_permit_recovery() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("sem");
    let sem = mgr.semaphore(&key, 2).expect("semaphore");
    let short = LockOptions::default().with_ttl(Duration::from_millis(200));

    let p1 = sem.try_acquire(&short).await.expect("permit 1");
    let _p2 = sem.try_acquire(&short).await.expect("permit 2");
    assert!(matches!(
        sem.try_acquire(&short).await.unwrap_err(),
        Error::Held { .. }
    ));

    // A crashed holder's permit expires server-side; capacity recovers.
    std::mem::drop(p1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let p3 = sem
        .try_acquire_for(&short, Duration::from_secs(2))
        .await
        .expect("permit after expiry recovery");
    p3.release().await.expect("p3 release");
}

#[tokio::test]
async fn fair_queue_serves_oldest_waiter_on_release() {
    let Some(mgr) = connect().await else { return };
    let key = unique_key("fair");
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let holder = mgr.try_lock_fair(&key, &opts).await.expect("holder");
    let holder_fence = holder.fence();

    let waiter = {
        let mgr = mgr.clone();
        let key = key.clone();
        let opts = opts.clone();
        tokio::spawn(async move {
            mgr.try_lock_fair_for(&key, &opts, Duration::from_secs(5))
                .await
        })
    };

    // Give the waiter time to enqueue.
    tokio::time::sleep(Duration::from_millis(150)).await;
    holder.release().await.expect("holder release");

    let acquired = tokio::time::timeout(Duration::from_secs(4), waiter)
        .await
        .expect("waiter never got the handoff")
        .unwrap()
        .expect("waiter acquire");
    assert!(
        acquired.fence().supersedes(holder_fence),
        "handoff must allocate a newer fence"
    );
    acquired.release().await.expect("waiter release");
}

#[tokio::test]
async fn multi_lock_all_or_nothing_with_rollback() {
    let Some(mgr) = connect().await else { return };
    let ka = unique_key("multi-a");
    let kb = unique_key("multi-b");
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    let keys = vec![kb.clone(), ka.clone()];
    let multi = mgr
        .try_lock_all(&keys, &opts, Duration::from_secs(1))
        .await
        .expect("both");
    assert_eq!(
        multi.keys(),
        vec![ka.clone(), kb.clone()],
        "sorted acquisition order"
    );
    multi.release_all().await.expect("release all");

    // Contention on the LAST sorted key must roll back the FIRST.
    let blocker = mgr.try_lock(&kb).await.expect("blocker holds b");
    let err = mgr
        .try_lock_all(&keys, &opts, Duration::from_millis(250))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout { .. }), "got {err:?}");

    // `a` must be free again â€” the rollback worked.
    let a_again = mgr.try_lock(&ka).await.expect("a rolled back and free");
    a_again.release().await.expect("cleanup a");
    blocker.release().await.expect("cleanup b");
}
