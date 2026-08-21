# 0009. Redis client crate: `redis` (tokio-comp)

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
The Redis backend needs an async client with pipelining, Lua EVAL support, connection pooling/reconnection, and Sentinel/cluster awareness.

## Options considered
1. `redis` crate (with `tokio-comp`, `connection-manager`)
2. `fred` (async-first Redis client)
3. Hand-rolled RESP client

## Decision
`redis` crate with Tokio features and connection-manager; revisit if benchmarks show contention hotspots.

## Why this is the best option
- **Reference maturity**: de-facto standard, widest protocol coverage (EVAL, functions, Sentinel, cluster routing), battle-tested edge-case handling (MOVED/ASK redirects, reconnect storms).
- **Everything we need today**: script invocation, pipelines, pub/sub, connection-manager auto-reconnect — all first-class.
- **Hiring/contribution surface**: most Rust devs already know it; lowest friction for contributors reviewing our scripts' invocation layer.
- **Sufficient performance**: our p99 target (≤5 ms grants LAN) is dominated by network RTT and Redis itself, not client overhead; criterion benchmarks will verify rather than assume.

## Why not the alternatives
- **fred**: genuinely strong (async-native design, higher throughput in some microbenchmarks, better-pooled pub/sub), but smaller community and its API shapes our public types more invasively. Documented as the swap-in candidate if perf demands it — the backend isolates it behind `palisade-redis`, so switching cost is contained.
- **Hand-rolled RESP**: full control, zero deps — but months of work reimplementing reconnect/Sentinel/cluster logic that has nothing to do with our thesis (locking correctness).

## Consequences
- All Redis interaction confined to `palisade-redis` (no `redis::` types leak into `palisade-core` public API) — keeps ADR 0003's future-backend promise credible.
- Connection-manager settings (pool size, retry policy) exposed through builder config.
- Benchmark gate in Phase 7: if fred outperforms materially at 50k ops/s, swap decision gets its own ADR.
