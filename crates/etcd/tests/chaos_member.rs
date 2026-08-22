//! A3 chaos: stop the etcd member mid-hold; the server-side lease clock
//! keeps running and the holder must learn it lost the lock.
//! Requires `PALISADE_CHAOS_ETCD=1`, a running `palisade-etcd` container,
//! and etcd at ETCD_ENDPOINTS (default http://127.0.0.1:2379).

use std::time::Duration;

use palisade_core::{Error, LockOptions};
use palisade_etcd::{EtcdConfig, EtcdLockManager};

fn endpoints() -> Vec<String> {
    std::env::var("ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".into())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

fn docker(args: &[&str]) {
    let mut cmd = std::process::Command::new("docker");
    cmd.arg("exec").arg("palisade-etcd").arg("etcdctl");
    for a in args {
        cmd.arg(a);
    }
    let out = cmd.output().expect("docker");
    assert!(out.status.success(), "etcdctl failed: {:?}", out.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn member_outage_is_survivable_and_detected() {
    if std::env::var("PALISADE_CHAOS_ETCD").as_deref() != Ok("1") {
        eprintln!("skipping etcd chaos: set PALISADE_CHAOS_ETCD=1");
        return;
    }
    let mgr = EtcdLockManager::connect(EtcdConfig::new(endpoints()))
        .await
        .expect("etcd");

    let key = format!(
        "palisade-etcd-chaos:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Watchdog ON: keepalives stream to the member.
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(2))
        .with_watchdog(true);
    let h = mgr.try_lock_with(&key, &opts).await.expect("grant");

    // Suspend the member: keepalives fail while the wall-clock lease
    // deadline passes server-side storage (resumes on start).
    docker(&["put", "/chaos/probe", "down"]);
    let _ = std::process::Command::new("docker")
        .args(["stop", "--time=2", "palisade-etcd"])
        .output()
        .expect("docker stop");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = std::process::Command::new("docker")
        .args(["start", "palisade-etcd"])
        .output()
        .expect("docker start");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Holder must have learned of loss through failed/closed keepalives,
    // and the key must be acquirable again once the cluster serves.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if h.is_lost() {
            break;
        }
        match mgr.try_lock_with(&key, &opts).await {
            Ok(s) => {
                tracing_or_print("successor took over before poison surfaced");
                s.release().await.expect("cleanup");
                return;
            }
            Err(Error::Held { .. }) => {}
            Err(_) => {}
        }
        assert!(
            std::time::Instant::now() < deadline,
            "neither loss detection nor recovery within window"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // After recovery the key is free (lease expired during outage).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match mgr.try_lock_with(&key, &opts).await {
            Ok(s) => {
                s.release().await.expect("cleanup");
                return;
            }
            Err(Error::Held { .. }) => {}
            Err(e) => panic!("unexpected: {e}"),
        }
        assert!(std::time::Instant::now() < deadline, "key never freed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn tracing_or_print(msg: &str) {
    println!("{msg}");
}
