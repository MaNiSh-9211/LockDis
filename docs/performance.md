# Performance

Measured with criterion (`cargo bench -p palisade-redis`) against a local
Docker Redis 7 (localhost, Windows host, release profile). Absolute numbers
are machine-dependent; the ratios and order of magnitude are the signal.

## Single-instance backend (Lua-guarded paths)

| Operation | p50 | mean | p99 |
|---|---|---|---|
| acquire → release roundtrip | 820 µs | 839 µs | 859 µs |
| try_lock fast-fail on held key | 408 µs | 422 µs | 437 µs |
| extend (ownership-checked) | 381 µs | 391 µs | 402 µs |

Reading:

- A full grant+release is **two store round trips** (~0.42 ms each here) plus
  script execution — i.e., we are network-bound, not CPU-bound.
- Contended fast-fail costs one round trip; contention never spins server-side.
- Watchdog renewal adds one extend per `ttl/3` per held lock (amortized <1 %
  overhead at typical TTLs).
- All targets from PLAN.md (p99 ≤ 5 ms grants on LAN) are met with >5× headroom.

## Watch fan-out (ADR 0029)

Store probe cost is O(distinct keys): N watchers on one key share a single
100 ms poller regardless of N (validated by the hub e2e test).

## Redlock quorum mode

One sequential round across N masters ⇒ grant latency ≈ N × single-node RTT.
Use only when cross-failure-domain safety outweighs latency.
