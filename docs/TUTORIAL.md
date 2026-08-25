# Tutorial: Your First Distributed Lock in 5 Minutes

## Prerequisites
- Rust 1.85+
- Redis running locally (`docker run -d -p 6379:6379 redis:7-alpine`)

## Step 1 — Add dependency

```toml
[dependencies]
palisade-core = "*"
palisade-redis = "*"
tokio = { version = "1", features = ["full"] }
```

## Step 2 — Acquire and release

```rust
use palisade_core::LockOptions;
use palisade_redis::{RedisConfig, RedisLockManager};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mgr = RedisLockManager::connect(
        RedisConfig::new("redis://127.0.0.1:6379")
    ).await?;

    let opts = LockOptions::default()
        .with_ttl(std::time::Duration::from_secs(30))
        .with_watchdog(true); // auto-renew at ttl/3

    let handle = mgr.try_lock_with("my-resource", &opts).await?;
    println!("Acquired! Fencing token: {}", handle.fence());

    // ... do exclusive work here ...

    handle.release().await?;
    println!("Released.");
    Ok(())
}
```

Every grant returns a **fencing token** — a monotonically increasing number.
Pass it to any protected resource so stale holders' writes are rejected.

## Step 3 — Handle loss gracefully

```rust
let h = mgr.try_lock_with(&key, &opts).await?;

tokio::select! {
    result = do_work() => result?,
    _ = h.until_lost() => {
        // Lease expired or was revoked mid-flight. Abort everything.
        eprintln!("Lost the lock — work aborted");
    }
}
```

## Step 4 — Semantic locking (no TOCTOU)

Instead of checking a condition then locking (race-prone), embed the check:

```rust
let h = mgr.acquire_where("orders/42")
    .field_equals("status", "pending")
    .field_gt("total", 0.0)
    .ttl(Duration::from_secs(10))
    .acquire()
    .await?;
// The predicate ran INSIDE the same Lua script as the CAS.
// Zero window between condition and grant.
```

## Next Steps

- Read [docs/fencing-guide.md](fencing-guide.md) to protect downstream writes
- Run `cargo run -p palisade-redis --example leader_election` for a real demo
- See [docs/EDGE_CASES.md](EDGE_CASES.md) for what can go wrong and how Palisade handles it
