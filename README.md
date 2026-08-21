# Palisade

A modern distributed locking system in Rust: a client library + gRPC service over Redis,
with **mandatory fencing tokens**, watchdog lease renewal, fair queuing, and
**Jepsen-grade correctness evidence** (deterministic simulation + linearizability checking).

> Status: **Phase 0 — scaffolding.** See [PLAN.md](PLAN.md) for the full roadmap and
> [docs/decisions](docs/decisions/README.md) for every architectural decision made so far,
> including rejected alternatives.

## Crates

| Crate | Purpose |
|---|---|
| `palisade-core` | Backend-neutral types, traits, fencing primitives (no I/O) |
| `palisade-redis` | Redis backend: Lua-guarded leases, Redlock, watchdog |
| `palisade-proto` | Protobuf contract + tonic codegen (Phase 5) |
| `palisade-server` | Standalone gRPC lock service (Phase 5) |
| `palisade-client` | Ergonomic SDK: direct backend or gRPC transport |
| `palisade-testing` | Property tests, madsim simulation, linearizability checker, chaos |

## Development

```sh
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

CI enforces all four gates on every PR (`#![forbid(unsafe_code)]` workspace-wide).

## License

MIT OR Apache-2.0
