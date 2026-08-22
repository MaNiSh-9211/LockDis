//! A2 chaos: blackhole a Redlock majority mid-hold.
//! Requires `PALISADE_CHAOS=1` and three independent masters
//! (default `localhost:6380..6382`, provided by deploy/compose/chaos.yaml).

use std::time::Duration;

use palisade_core::{Error, LockOptions};

fn ring() -> Vec<String> {
    std::env::var("REDLOCK_CHAOS_ENDPOINTS")
        .unwrap_or_else(|_| {
            "redis://127.0.0.1:6380,redis://127.0.0.1:6381,redis://127.0.0.1:6382".into()
        })
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

async fn raw_pause(endpoint: &str, ms: u64) {
    let client = redis::Client::open(endpoint).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(ms)
        .arg("ALL")
        .query_async::<String>(&mut conn)
        .await
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_quorum_poisons_the_holder() {
    if std::env::var("PALISADE_CHAOS").as_deref() != Ok("1") {
        eprintln!("skipping redlock chaos: set PALISADE_CHAOS=1");
        return;
    }
    let mgr = palisade_redis::RedlockManager::connect(palisade_redis::RedlockConfig::new(ring()))
        .await
        .expect("ring up");

    let key = format!(
        "palisade-rl-chaos:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(2))
        .with_watchdog(true);
    let h = mgr.try_lock(&key, &opts).await.expect("grant");

    // Blackhole two of three nodes (the whole quorum margin).
    let eps = ring();
    raw_pause(&eps[0], 6000).await;
    raw_pause(&eps[1], 6000).await;

    // Quorum renewal is now impossible: the handle must go Lost.
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while !h.is_lost() {
        assert!(
            std::time::Instant::now() < deadline,
            "holder never learned it lost quorum"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Wait out the blackout; every per-node lease has expired server-side.
    tokio::time::sleep(Duration::from_secs(7)).await;
    match mgr.try_lock(&key, &opts).await {
        Ok(s) => s.release().await.expect("cleanup"),
        Err(Error::Held { .. }) => panic!("key still held after full expiry"),
        Err(e) => panic!("unexpected: {e}"),
    }
}
