//! Exactly-once job processing: the fencing token gates every write so a
//! zombie worker (paused past its lease) can never double-apply a job.
//!
//! Run: cargo run -p palisade-redis --example job_dedup

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let mgr = RedisLockManager::connect(RedisConfig::new(&url)).await?;

    let job_id = "job-4711";
    let lock_key = format!("jobs/{job_id}/lock");

    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(15))
        .with_watchdog(true);

    match mgr.try_lock_with(&lock_key, &opts).await {
        Ok(handle) => {
            let fence = handle.fence();
            println!("processing {job_id} under fence #{fence}");

            // EVERY durable write carries the fence; the store rejects
            // anything whose token is not newer than the last accepted one.
            // See palisade_core::pg_fenced_update for the SQL shape:
            //   UPDATE jobs SET status=$2, last_fence=$3
            //   WHERE id=$1 AND $3 > last_fence;
            let rows_applied = apply_job(job_id, fence.value());
            if rows_applied == 0 {
                println!("🛑 stale worker detected — job already advanced past us");
            } else {
                println!("✅ applied under fence #{fence}");
            }

            handle.release().await?;
        }
        Err(Error::Held { .. }) => println!("another worker owns {job_id} — skipping"),
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn apply_job(_id: &str, _fence: u64) -> usize {
    // Stand-in for: client.execute(&fenced_sql, &[...]).unwrap();
    1
}
