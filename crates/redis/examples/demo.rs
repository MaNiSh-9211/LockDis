//! Interactive demo: multiple workers compete for the same lock while the
//! terminal shows real-time contention, fencing tokens, and safety events.
//!
//! Run: cargo run -p palisade-redis --example demo -- --workers 5

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

fn url() -> String {
    std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mgr = Arc::new(RedisLockManager::connect(RedisConfig::new(url())).await?);
    let key = format!(
        "palisade-demo:{}",
        palisade_core::OwnerId::generate().as_uuid()
    );
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(true);

    println!("\n╔══════════════════════════════════════════════════╗");
    println!("║  Palisade Distributed Lock — Live Demo           ║");
    println!("║  Watch 6 workers fight over one lock             ║");
    println!("║  Press Ctrl+C to stop                            ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    let cycle = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for w in 0..6u32 {
        let mgr = mgr.clone();
        let key = key.clone();
        let opts = opts.clone();
        let cycle = cycle.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match mgr.try_lock_for(&key, &opts, Duration::from_secs(15)).await {
                    Ok(h) => {
                        let c = cycle.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "\x1b[32m[Worker {w}] ✓ GRANTED fence#{f} (cycle {c})\x1b[0m",
                            f = h.fence().value(),
                        );
                        // Simulate critical section work.
                        tokio::time::sleep(Duration::from_millis(300 + rand_delay(w))).await;
                        match h.release().await {
                            Ok(()) => println!("\x1b[90m[Worker {w}]   released\x1b[0m"),
                            Err(e) => println!("\x1b[31m[Worker {w}] ✗ release failed: {e}\x1b[0m"),
                        }
                    }
                    Err(Error::Held { .. }) => {
                        println!("\x1b[33m[Worker {w}] ⏳ held by another worker\x1b[0m");
                        tokio::time::sleep(Duration::from_millis(500 + rand_delay(w))).await;
                    }
                    Err(e) => {
                        println!("\x1b[31m[Worker {w}] ✗ error: {e}\x1b[0m");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(rand_delay(w))).await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

fn rand_delay(worker: u32) -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    (t ^ (worker as u64 * 7919)) % 400
}
