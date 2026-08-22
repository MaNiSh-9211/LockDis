//! Soak harness: sustained mixed workload with invariant checking.
//!
//! Runs N workers doing acquire/hold/release cycles against one key while a
//! shared counter proves mutual exclusion; prints periodic progress. Designed
//! for long runs (`SOAK_SECS=3600 cargo run -p palisade-testing --example soak`)
//! to surface rare races the short suites cannot reach.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::RedisConfig;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let workers: usize = std::env::var("SOAK_WORKERS")
        .unwrap_or_else(|_| "4".into())
        .parse()?;
    let secs: u64 = std::env::var("SOAK_SECS")
        .unwrap_or_else(|_| "60".into())
        .parse()?;

    let mgr = Arc::new(palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await?);
    let key = format!(
        "palisade-soak:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let counter = Arc::new(AtomicUsize::new(0));
    let opts = LockOptions::default().with_ttl(Duration::from_secs(15));

    println!("soak: {workers} workers x {secs}s on {key}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);

    let mut handles = Vec::new();
    for w in 0..workers {
        let mgr = mgr.clone();
        let opts = opts.clone();
        let key = key.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            let mut mine = 0usize;
            while tokio::time::Instant::now() < deadline {
                match mgr.try_lock_for(&key, &opts, Duration::from_secs(5)).await {
                    Ok(h) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        mine += 1;
                        if h.release().await.is_err() {
                            eprintln!("worker {w}: release reported Lost (lease lapsed)");
                        }
                    }
                    Err(Error::Timeout { .. }) => {}
                    Err(e) => eprintln!("worker {w}: {e}"),
                }
            }
            mine
        }));
    }

    let mut total = 0usize;
    for h in handles {
        total += h.await.unwrap_or(0);
    }
    let final_count = counter.load(Ordering::SeqCst);

    println!("cycles completed: {total}");
    println!("counter: {final_count}");
    assert_eq!(
        final_count, total,
        "mutual exclusion violated: counter diverged from successful grants"
    );
    println!("soak PASSED — no invariant violations");
    Ok(())
}
