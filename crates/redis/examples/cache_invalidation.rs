//! Read-write lock for cache invalidation: many readers, one writer that
//! rebuilds the cache exclusively.
//!
//! Run: cargo run -p palisade-redis --example cache_invalidation

use std::sync::Arc;
use std::time::Duration;

use palisade_core::{Error, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

const CACHE_KEY: &str = "cache/products/v3";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let mgr = Arc::new(RedisLockManager::connect(RedisConfig::new(&url)).await?);
    let opts = LockOptions::default().with_ttl(Duration::from_secs(10));

    // 4 concurrent readers…
    let mut readers = Vec::new();
    for i in 0..4 {
        let mgr = mgr.clone();
        let key = CACHE_KEY.to_owned();
        let opts = opts.clone();
        readers.push(tokio::spawn(async move {
            match mgr.try_read(&key, &opts).await {
                Ok(r) => {
                    println!("reader {i} reading stale copy while rebuild runs elsewhere");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    r.release().await.expect("reader release");
                }
                Err(Error::Held { .. }) => println!("reader {i}: rebuild in progress"),
                Err(e) => eprintln!("reader {i}: {e}"),
            }
        }));
    }

    // …and one writer that takes the exclusive side afterwards.
    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let w = mgr.try_write(CACHE_KEY, &opts).await.expect("writer");
        println!("writer rebuilding cache exclusively (fence #{})", w.fence());
        tokio::time::sleep(Duration::from_millis(100)).await;
        w.release().await.expect("writer release");
    }

    for r in readers {
        r.await.unwrap();
    }
    println!("done");
    Ok(())
}
