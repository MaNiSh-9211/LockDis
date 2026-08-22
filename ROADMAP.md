# PALISADE V2 ROADMAP — Production Completeness

> Status legend: ✅ done · 🔶 in progress · ⬜ queued
> Every item lands with tests + an ADR when it introduces a design decision.

## Track A — Chaos & edge-case completion (Layer 4 of correctness strategy)

| # | Item | Proves | Status |
|---|---|---|---|
| A1 | toxiproxy partition vs Redis mid-hold | Watchdog poisons on blackout; fencing holds; heal recovers | ⬜ |
| A2 | Redlock minority node-loss mid-hold | Quorum survives −1; Lost at −2; heal re-enables | ⬜ |
| A3 | etcd member stop/start mid-hold | Server-side expiry wins; recovery re-arms | ⬜ |
| A4 | Panic-unwind through critical section | Drop releases without explicit unlock | ⬜ |
| A5 | Lock attempt on session dead pre-grant | Grant-undo path returns NotFound | ⬜ |
| A6 | Fence-counter reset window in sim | Strict-greater comparison stays safe | ⬜ |
| A7 | Slow watch consumer isolation | Lagged subscriber never stalls hub poller/others | ⬜ |

Chaos runs are gated behind `PALISADE_CHAOS=1` + docker compose lab
(`deploy/compose/chaos.yaml`) so the standard suite stays fast and green.

## Track B — Wire & SDK maturity

| # | Item | Value | Status |
|---|---|---|---|
| B1 | `DescribeKey(key)` RPC → {held, fence/version, ttl_ms} | Introspection for authorized callers; feeds versioned watch | 🔶 |
| B2 | Versioned watch events (per-key fence as ordering token) | Ordered, dedupable event streams | ⬜ |
| B3 | Heartbeat rate-limiting per session (token bucket) | DoS containment for #2's control plane | 🔶 |

## Track C — Ecosystem & quality

| # | Item | Value | Status |
|---|---|---|---|
| C1 | `CountDownLatch` primitive (Redisson parity) | Coordination pattern completeness | ⬜ |
| C2 | Fence adapters: Postgres conditional-update helper module | Fencing made copy-paste for the #1 downstream store | ⬜ |
| C3 | Examples: leader election, job dedupe, RW cache invalidation | First-hour productivity | ⬜ |
| C4 | CI: redis+etcd service containers, full matrix incl. chaos smoke | Gates enforce what we claim | ⬜ |
| C5 | Publish prep: crate metadata, workspace lints pass on docs.rs build | crates.io readiness | 🔶 |
| C6 | Soak harness script (1h mixed workload, invariant-checked) | Long-tail race discovery | ⬜ |

## Track D — Performance validation

| # | Item | Value | Status |
|---|---|---|---|
| D1 | Run criterion vs local Redis; publish p50/p99 numbers | Claims become measurements | ⬜ |

## Explicitly out of scope for v2 (with rationale)

- **Geo/multi-region quorum** — needs WAN-consensus design; etcd tier covers cross-AZ today.
- **Deadlock waits-for graph** — timeouts+sessions cover practical cases; complexity disproportionate.
- **TLA+/formal specs** — simulation+checker give strong empirical guarantees; formal proofs tracked as stretch.
- **Embedded Raft (openraft)** — ADR 0026 defers to etcd until requirements mature.

---

## Execution order

A (chaos) → B (wire) → C (ecosystem) → D (perf) → final gates → tag `v0.2.0`.
