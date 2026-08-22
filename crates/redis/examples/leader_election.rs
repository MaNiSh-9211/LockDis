//! Leader election with Palisade.
//!
//! The leader holds `services/billing/leader` with watchdog renewal; if it
//! dies or loses the lease, anyone waiting becomes the next leader.
//!
//! Run: cargo run -p palisade-redis --example leader_election

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::{RedisConfig, RedisLockManager};

const LEADER_KEY: &str = "services/billing/leader";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let mgr = RedisLockManager::connect(RedisConfig::new(&url)).await?;

    println!(
        "[{pid}] campaigning for {LEADER_KEY}",
        pid = std::process::id()
    );

    // Short lease + watchdog: fast failover if we die, no expiry while sane.
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(true);

    loop {
        match mgr
            .try_lock_for(LEADER_KEY, &opts, Duration::from_secs(5))
            .await
        {
            Ok(handle) => {
                println!("🎉 elected leader (fence #{})", handle.fence());
                while !handle.is_lost() {
                    // Do leader work here. is_lost()/until_lost() flip when
                    // the watchdog can no longer renew — act accordingly.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                println!("⚠ lost leadership mid-term — standing down");
                let _ = handle.release().await;
            }
            Err(Error::Held { .. }) | Err(Error::Timeout { .. }) => {
                // Someone else leads; stay warm as follower.
                println!("following…");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
