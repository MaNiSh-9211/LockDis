# Palisade

![Palisade logo](crates/server/assets/images/Palisade.png)

[![CI](https://github.com/MaNiSh-9211/Palisade/actions/workflows/ci.yml/badge.svg)](https://github.com/MaNiSh-9211/Palisade/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue.svg)](LICENSE-MIT)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![Safety: no unsafe](https://img.shields.io/badge/unsafe-forbidden-red.svg)](https://github.com/MaNiSh-9211/Palisade)

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

## See it live in 60 seconds

```sh
docker run -d --name palisade-redis -p 6379:6379 redis:7-alpine
cargo run -p palisade-server --bin palisade-demo -- --listen 127.0.0.1:8080
```

Open **http://localhost:8080** — a grid of workers competing for one lock,
live fencing-token timeline, contention indicator, and a Store Pressure
Index gauge. Hit *Chaos* and watch leases expire, fences climb, and stale
holders get marked LOST in real time. The gRPC server (`palisade-server`)
and the demo binary share the same Lua-guarded backend; the demo adds no
locking semantics of its own (see [ARCHITECTURE.md](ARCHITECTURE.md)).

## Crates

| Crate | Purpose |
|---|---|
| `palisade-core` | Backend-neutral types, traits, fencing, SafetyPolicy, BlackBox |
| `palisade-redis` | Redis + Redlock backends: all primitives, Lua-guarded |
| `palisade-etcd` | etcd consensus backend: MVCC transactions, server-side leases |
| `palisade-proto` | Protobuf contract + tonic codegen |
| `palisade-server` | gRPC service (mTLS, authz, sessions, health/drain, Prometheus) + web demo binary |
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

| Guide | Audience |
|---|---|
| [Tutorial](docs/TUTORIAL.md) | Your first lock in 5 minutes |
| [Architecture](ARCHITECTURE.md) | System design with diagrams |
| [PLAN.md](PLAN.md) | Original architecture and roadmap |
| [ROADMAP.md](ROADMAP.md) | V2 completion status |
| [Comparison](docs/COMPARISON.md) | vs Redisson, ZooKeeper, etcd, Consul |
| [Fencing Guide](docs/fencing-guide.md) | How to actually use fence tokens |
| [Edge Cases](docs/EDGE_CASES.md) | 40+ failure modes catalogued |
| [Operations Runbook](docs/OPERATIONS.md) | Deploy, monitor, troubleshoot |
| [Integrations](docs/integrations.md) | Gateway/UAM/Grafana wiring |
| [Durability](docs/durability.md) | Restart behavior per backend |
| [Performance](docs/performance.md) | Benchmark numbers |
| [Security Policy](SECURITY.md) | Reporting and model |
| [ADRs](docs/decisions/README.md) | Every decision + rejected alternatives |

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option — the standard Rust ecosystem arrangement.
