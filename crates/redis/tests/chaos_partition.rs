//! A1 chaos: store blackout mid-hold (CLIENT PAUSE simulates a partition).
//! Requires `PALISADE_CHAOS=1` and Redis at PALISADE_REDIS_URL.

use std::time::Duration;

use palisade_core::{Error, LockHandle, LockOptions};
use palisade_redis::RedisConfig;

fn url() -> String {
    // Chaos MUST hit an isolated instance: CLIENT PAUSE ALL freezes every
    // client on the target server.
    std::env::var("PALISADE_CHAOS_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
}

async fn raw_pause(ms: u64) {
    let client = redis::Client::open(url()).unwrap();
    // Dedicated connection: the PAUSE would block this very command's reply,
    // so fire it on its own socket with a generous read timeout.
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(ms)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blackout_mid_hold_poisons_and_heals() {
    if std::env::var("PALISADE_CHAOS").as_deref() != Ok("1") {
        eprintln!("skipping chaos: set PALISADE_CHAOS=1");
        return;
    }
    let mgr = palisade_redis::RedisLockManager::connect(RedisConfig::new(url()))
        .await
        .expect("redis");

    let key = format!("palisade-chaos:{}:{}", std::process::id(), chrono_id());

    // Short lease + watchdog: the pause outlives both.
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(1))
        .with_watchdog(true);
    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");

    // Blackout for 3s: lease dies server-side at ~1s; the watchdog's
    // blocked extend completes after the pause and must report loss.
    let pause_started = std::time::Instant::now();
    raw_pause(3000).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !h.is_lost() {
        assert!(
            std::time::Instant::now() < deadline,
            "watchdog never poisoned the handle during blackout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait out the remainder of the blackout: commands issued mid-pause
    // queue server-side and would time out our client.
    let elapsed = pause_started.elapsed();
    if elapsed < Duration::from_millis(3200) {
        tokio::time::sleep(Duration::from_millis(3200) - elapsed).await;
    }

    // Blackout over: the successor path works immediately.
    let successor = mgr
        .try_lock_with(
            &key,
            &LockOptions::default().with_ttl(Duration::from_secs(5)),
        )
        .await
        .expect("successor acquires after blackout");
    assert!(matches!(
        h.extend(Duration::from_secs(1)).await.unwrap_err(),
        Error::Lost { .. }
    ));
    successor.release().await.expect("cleanup");
}

fn chrono_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
