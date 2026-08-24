//! Demo HTTP+WS surface: in-process axum server + live Redis.
//! Skips silently without a Redis.

use std::time::Duration;

use futures_util::StreamExt;
use palisade_core::OwnerId;
use palisade_redis::RedisConfig;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

async fn spawn_demo() -> Option<String> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping demo test: no redis at {url}: {e}");
            return None;
        }
    };
    let app = palisade_server::demo::demo_router(manager);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Some(format!("http://{addr}"))
}

fn unique_key(name: &str) -> String {
    format!(
        "palisade-demo-test:{name}:{}",
        OwnerId::generate().as_uuid()
    )
}

async fn post_json(base: &str, path: &str, body: Value) -> Value {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .expect("http post")
        .json()
        .await
        .expect("json body")
}

#[tokio::test]
async fn demo_rest_roundtrip_lock_describe_unlock() {
    let Some(base) = spawn_demo().await else {
        return;
    };
    let key = unique_key("roundtrip");

    // Grant carries token + monotonic fence.
    let granted = post_json(&base, "/api/lock", json!({ "key": key, "ttl_ms": 8000 })).await;
    assert_eq!(granted["result"], "granted", "got {granted}");
    assert!(granted["fence"].as_u64().unwrap_or(0) > 0);
    let token = granted["token"].as_str().expect("token").to_owned();

    // Contender is told the key is held, never who holds it (ADR 0022).
    let held = post_json(&base, "/api/lock", json!({ "key": key })).await;
    assert_eq!(held["result"], "held", "got {held}");

    // Describe reflects live state.
    let described: Value = reqwest::get(format!("{base}/api/describe/{key}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(described["held"], true, "got {described}");
    assert_eq!(
        described["version"], granted["fence"],
        "describe version must equal last fence"
    );
    assert!(described["ttl_ms"].as_u64().unwrap_or(0) > 0);

    // Ownership-checked release, then idempotence surfaces as Lost.
    let released = post_json(&base, "/api/unlock", json!({ "key": key, "token": token })).await;
    assert_eq!(released["result"], "released", "got {released}");

    let lost = post_json(&base, "/api/unlock", json!({ "key": key, "token": token })).await;
    assert_eq!(lost["result"], "lost", "got {lost}");

    let described: Value = reqwest::get(format!("{base}/api/describe/{key}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(described["held"], false, "got {described}");
}

#[tokio::test]
async fn demo_list_locks_scans_held_keys_under_prefix() {
    let Some(base) = spawn_demo().await else {
        return;
    };
    let prefix = format!("palisade-demo-scan:{}", OwnerId::generate().as_uuid());
    let key = format!("{prefix}:hot");

    let granted = post_json(&base, "/api/lock", json!({ "key": key, "ttl_ms": 15_000 })).await;
    assert_eq!(granted["result"], "granted");

    let listed: Value = reqwest::get(format!("{base}/api/locks?prefix={prefix}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["prefix"], prefix);
    let keys: Vec<&str> = listed["locks"]
        .as_array()
        .expect("locks array")
        .iter()
        .filter_map(|e| e["key"].as_str())
        .collect();
    assert!(keys.contains(&key.as_str()), "got {listed:?}");

    // Empty prefix lists everything; just check shape.
    let all: Value = reqwest::get(format!("{base}/api/locks?prefix="))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(all["locks"].is_array());
}

#[tokio::test]
async fn demo_pressure_reports_index_and_tier() {
    let Some(base) = spawn_demo().await else {
        return;
    };
    let p: Value = reqwest::get(format!("{base}/api/pressure"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let spi = p["spi"].as_f64().expect("spi numeric");
    assert!((0.0..=100.0).contains(&spi), "spi {spi} out of range");
    assert!(matches!(
        p["tier"].as_str(),
        Some("NORMAL" | "ELEVATED" | "CRITICAL" | "SIEGE")
    ));
}

#[tokio::test]
async fn demo_websocket_streams_versioned_events() {
    let Some(base) = spawn_demo().await else {
        return;
    };
    let key = unique_key("watch");
    let ws_url = format!(
        "ws://{}/api/watch/{}",
        base.trim_start_matches("http://"),
        key
    );

    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("ws connect");
    let deadline = std::time::Instant::now() + Duration::from_secs(15);

    // Trigger a grant + release while subscribed. The hub is level-triggered
    // (one 100 ms poller per key, ADR 0029), so hold long enough for the
    // poller to observe the held state between transitions.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let granted = post_json(&base, "/api/lock", json!({ "key": key, "ttl_ms": 8000 })).await;
    assert_eq!(granted["result"], "granted");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let token = granted["token"].as_str().expect("token").to_owned();
    post_json(&base, "/api/unlock", json!({ "key": key, "token": token })).await;

    let mut saw_acquired = false;
    let mut saw_freed = false;
    while (std::time::Instant::now() < deadline) && !(saw_acquired && saw_freed) {
        let msg = match tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        let Message::Text(text) = msg else { continue };
        let ev: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        match ev["event"].as_str() {
            Some("acquired") => {
                assert!(ev["version"].as_u64().unwrap_or(0) > 0, "got {ev}");
                saw_acquired = true;
            }
            Some("freed") => saw_freed = true,
            _ => {}
        }
    }
    let _ = ws.close(None).await;
    assert!(saw_acquired, "never observed Acquired over websocket");
    assert!(saw_freed, "never observed Freed over websocket");
}

#[tokio::test]
async fn demo_frontend_is_served_at_root() {
    let Some(base) = spawn_demo().await else {
        return;
    };
    let html = reqwest::get(format!("{base}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("Palisade"), "frontend missing title");
    assert!(html.contains("/api/watch"), "frontend missing watch wiring");
}
