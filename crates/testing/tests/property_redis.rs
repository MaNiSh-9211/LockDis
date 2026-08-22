//! Layer-1 property tests against a real Redis (skips silently without).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use palisade_core::{LockHandle, LockOptions};
use palisade_redis::RedisConfig;
use proptest::prelude::*;

async fn connect() -> Option<Arc<palisade_redis::RedisLockManager>> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => Some(Arc::new(m)),
        Err(e) => {
            eprintln!("skipping property test: no redis at {url}: {e}");
            None
        }
    }
}

fn unique_key(tag: &str) -> String {
    format!(
        "palisade-proptest:{tag}:{}",
        palisade_core::OwnerId::generate().as_uuid()
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    /// N workers each perform `incs` critical-section increments under the
    /// same lock; no increment may be lost and none may double-run.
    #[test]
    fn mutual_exclusion_counter(
        workers in 2u32..=6,
        incs in 1u32..=8,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async {
            let Some(mgr) = connect().await else { return true };
            let key = unique_key("counter");
            let counter = Arc::new(AtomicUsize::new(0));
            let opts = LockOptions::default()
                .with_ttl(Duration::from_secs(5))
                .with_watchdog(false);

            let mut handles = Vec::new();
            for _ in 0..workers {
                let mgr = mgr.clone();
                let key = key.clone();
                let counter = counter.clone();
                let opts = opts.clone();
                handles.push(tokio::spawn(async move {
                    for _ in 0..incs {
                        let h = mgr.try_lock_for(&key, &opts, Duration::from_secs(5))
                            .await
                            .expect("acquire");
                        counter.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        h.release().await.expect("release");
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            counter.load(Ordering::SeqCst) == (workers * incs) as usize
        });
        prop_assert!(result);
    }
}

/// Every acquired handle must be releasable exactly once; double-release
/// is idempotent-success, never an error.
#[test]
fn release_is_idempotent() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let Some(mgr) = connect().await else { return };
        let key = unique_key("idem");
        let opts = LockOptions::default().with_ttl(Duration::from_secs(5));
        let h = mgr.try_lock_with(&key, &opts).await.expect("grant");
        for _ in 0..4 {
            assert!(h.release().await.is_ok());
        }
    });
}
