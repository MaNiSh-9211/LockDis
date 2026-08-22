# PALISADE V2 ROADMAP — Production Completeness

> Status legend: ✅ done · 🔶 in progress · ⬜ queued
> Every item lands with tests + an ADR when it introduces a design decision.

## Track A — Chaos & edge-case completion (Layer 4 of correctness strategy)

| # | Item | Proves | Status |
|---|---|---|---|
| A1 | Store blackout mid-hold (CLIENT PAUSE partition sim) | Watchdog poisons; fencing holds; heal recovers - tested | ✅ |
| A2 | Redlock majority blackhole mid-hold (3 live masters) | Quorum loss = Lost; expiry frees key - tested | ✅ |
| A3 | etcd member stop/start mid-hold (live container) | Loss detected; key re-acquirable - tested | ✅ |
| A4 | Panic-unwind through critical section | Drop releases without explicit unlock - tested | ✅ |
| A5 | Lock attempt on session dead pre-grant | Grant-undo returns NotFound; no leak - tested | ✅ |
| A6 | Fence-counter reset window | Strict-greater comparison keeps safety (core ordering tests) | ✅ |
| A7 | Slow watch consumer isolation | Silent subscriber never starves fast one - tested | ✅ |

Chaos runs are gated behind `PALISADE_CHAOS=1` + docker compose lab
(`deploy/compose/chaos.yaml`) so the standard suite stays fast and green.

## Track B — Wire & SDK maturity

| # | Item | Value | Status |
|---|---|---|---|
| B1 | `DescribeKey(key)` RPC -> {held, version, ttl_ms} | Introspection for authorized callers | ✅ |
| B2 | Versioned watch events (fence as per-key order token) | Ordered, dedupable event streams - tested | ✅ |
| B3 | Heartbeat rate-limiting per session (token bucket) | DoS containment for #2's control plane | 🔶 |

## Track C — Ecosystem & quality

| # | Item | Value | Status |
|---|---|---|---|
| C1 | `RedisCountDownLatch` primitive (Redisson parity) | NX-init, floored countdown, wait/timeout | ✅ |
| C2 | Postgres fence-SQL builders in core (injection-guarded, unit-tested) | Fencing made copy-paste for the #1 downstream store | ✅ |
| C3 | Examples: leader election, job dedupe, RW cache invalidation | crates/redis/examples/ - build-gated | ✅ |
| C4 | CI: redis+etcd service containers + protoc, full workspace suite | Gates enforce what we claim | ✅ |
| C5 | Publish prep: core metadata polished; path-dep crates marked publish=false until first crates.io release | crates.io readiness staged | ✅ |
| C6 | Soak harness (testing/examples/soak.rs; SOAK_SECS/SOAK_WORKERS) | Smoke: 22.6k cycles clean | ✅ |

## Track D — Performance validation

| # | Item | Value | Status |
|---|---|---|---|
| D1 | Criterion vs local Redis; numbers in docs/performance.md | Roundtrip p99 0.86 ms (>5x headroom vs target) | ✅ |

## Explicitly out of scope for v2 (with rationale)

- **Geo/multi-region quorum** — needs WAN-consensus design; etcd tier covers cross-AZ today.
- **Deadlock waits-for graph** — timeouts+sessions cover practical cases; complexity disproportionate.
- **TLA+/formal specs** — simulation+checker give strong empirical guarantees; formal proofs tracked as stretch.
- **Embedded Raft (openraft)** — ADR 0026 defers to etcd until requirements mature.

---

## Execution order

A (chaos) → B (wire) → C (ecosystem) → D (perf) → final gates → tag `v0.2.0`.
