# Palisade

A distributed locking system in Rust with **mandatory fencing tokens**, **server-authoritative sessions**, **semantic predicates inside the atomic grant**, and **chaos-proven correctness** — backed by Redis or etcd consensus.

## Why Palisade exists

Every distributed lock system asks one question: *"is this key free?"* That's not enough. It's why developers write TOCTOU-riddled check-then-lock patterns, why stale holders corrupt state after GC pauses, and why failover double-grants go unnoticed until production data is corrupted.

Palisade fixes the root causes:

- **Fencing tokens on every grant** — downstream stores reject stale holders' writes
- **Semantic Locks** — business predicates evaluated atomically *inside* the grant script (zero TOCTOU window)
- **Safety Policy Spectrum** — explicitly choose Cowardly / Balanced / Aggressive staleness tolerance
- **Lock Testament** — dying leaders pass state to successors across crash boundaries
- **Black Box Recorder** — hash-chained flight recorder for post-mortem analysis
- **Server-authoritative sessions** — crashed clients' locks die in seconds, not TTL-minutes
- **Store Pressure Index** — renewals-as-sensors predictive backpressure

## Quick Start

```rust
use palisade_redis::{RedisConfig, RedisLockManager};
use palisade_core::LockOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mgr = RedisLockManager::connect(
        RedisConfig::new("redis://127.0.0.1:6379")
    ).await?;

    // Simple mutex with fencing token and watchdog renewal.
    let opts = LockOptions::default()
        .with_ttl(std::time::Duration::from_secs(30))
        .with_watchdog(true);
    let handle = mgr.try_lock_with("my-resource", &opts).await?;
    println!("fence: {}", handle.fence());
    handle.release().await?;
    Ok(())
}
```

## Crates

| Crate | Purpose |
|---|---|
| `palisade-core` | Backend-neutral types, traits, fencing, SafetyPolicy, BlackBox |
| `palisade-redis` | Redis + Redlock backends: all primitives, Lua-guarded |
| `palisade-etcd` | etcd consensus backend: MVCC transactions, server-side leases |
| `palisade-proto` | Protobuf contract + tonic codegen |
| `palisade-server` | gRPC service: mTLS, authz, sessions, health/drain, Prometheus |
| `palisade-client` | SDK: gRPC client with watchdog, watch streams, bearer/proxy auth |
| `palisade-testing` | Deterministic simulation, invariant checker, property tests |

## Primitives

Mutex · Reentrant · Read-Write · Semaphore · Fair FIFO · Multi-Lock · CountDownLatch · **Semantic Locks**

## Backends

| | Redis | etcd |
|---|---|---|
| Consensus | No (Redlock quorum available) | **Raft** |
| Fencing source | Per-key INCR counter | MVCC revision (linearizable) |
| Session death | TTL expiry or gRPC sweeper | Server-side lease expiry |
| Durability | AOF/RDB (config-dependent) | Raft log fsync |

## Development

```sh
cargo build --workspace
cargo test --workspace          # requires local Redis (and optionally etcd)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

CI enforces all four gates against live Redis + etcd containers.

## Documentation

- [PLAN.md](PLAN.md) — architecture and roadmap
- [ROADMAP.md](ROADMAP.md) — V2 completion status
- [docs/decisions/](docs/decisions/README.md) — 31 Architecture Decision Records
- [docs/EDGE_CASES.md](docs/EDGE_CASES.md) — failure mode catalog with dispositions
- [docs/fencing-guide.md](docs/fencing-guide.md) — how to actually use fencing tokens
- [docs/durability.md](docs/durability.md) — per-backend restart behavior
- [docs/performance.md](docs/performance.md) — benchmark numbers
- [docs/integrations.md](docs/integrations.md) — gateway/UAM/Grafana wiring

## License

MIT OR Apache-2.0
