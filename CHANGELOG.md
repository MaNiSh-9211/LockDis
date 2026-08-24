# Changelog

All notable changes to Palisade are documented here.

## [0.3.0] — 2026-08-22

### Added

#### Inventions (novel capabilities not found in any other lock system)
- **Semantic Locks**: business predicates evaluated atomically inside the grant Lua script. Zero TOCTOU window between condition check and lock acquisition. Builder API: `acquire_where(key).field_equals(...).field_gt(...).acquire()`.
- **Black Box Recorder**: hash-chained flight recorder for lock operations. Tamper-evident; `verify_chain()` detects retroactive edits. Auto-dumpable on anomalies.
- **Safety Policy Spectrum**: `Cowardly` / `Balanced` / `Aggressive` explicit knob for the staleness-vs-liveness trade-off. Wired into Redis watchdog and etcd keepalive.
- **Lock Testament**: deathbed state transfer. Dying leaders store payload that successors read after acquiring.
- **The Lock Lattice**: Store Pressure Index computed from renewal-as-sensor telemetry. Drives graduated backpressure (NORMAL→ELEVATED→CRITICAL→SIEGE) with recommended watchdog cadence multipliers.
- **Contention Predictor**: EWMA + variance forecasting of contended key availability windows.
- **FenceSeal**: HMAC attestation per grant; downstream stores verify offline.
- **CountDownLatch**: NX-init, floored countdown, wait/timeout.

#### Infrastructure
- etcd consensus backend (Raft): MVCC transactional locks, server-side leases, revision-based fencing.
- gRPC service with mTLS, trusted-header auth mode, health/drain, Prometheus metrics endpoint.
- Session management: RegisterSession/Heartbeat/CloseSession with server-authoritative sweeper.
- Authorization: JSON ACLs, bearer + trusted-header modes, prefix grants, max_keys/max_watchers quotas, audited ForceUnlock.
- Watch fan-out hub: O(distinct keys) polling, versioned events, dead-head discard.
- ListLocks admin introspection RPC.
- DescribeKey introspection RPC.
- Grafana dashboard JSON.
- Docker Compose chaos lab.
- CI with redis+etcd service containers.
- Examples: leader election, job dedup, cache invalidation.
- Soak harness.

#### Correctness
- Deterministic simulation with fault injection (200-seed gate).
- Mutation-validated invariant checker (proves the validator catches bugs).
- Real-traffic history recording → invariant checking e2e.
- Property tests against live Redis.
- Edge-case proofs: panic-unwind release, grant-undo on dead session, slow-consumer isolation.

#### Hardening fixes (found by testing)
- Server-side handles self-released wire grants on Drop → fixed with `disarm()`.
- Partial-release scripts refreshed with TTL 0 → Redis 7 deletes immediately → fixed by passing real lease TTLs.
- Fair-mode polling minted a new identity per attempt → orphaned queue entries → fixed by hoisting identity out of the loop.
- Stale-reader releases corrupted RWL accounting → per-reader token membership.
- Redlock rounds unbounded per node → per-node deadline at ttl/4.
- Redis connections lacked response timeout → bounded at 5 s default.
- Publish flags were inverted → corrected.
- rustls crypto provider ambiguity under parallel test load → pinned to ring.

## [0.2.0] — 2026-08-21

### Added
- Redlock quorum backend across N independent masters with dedicated fence allocator.
- Watchdog auto-renewal (`ttl/3` cadence, weak-ref task, definitive-vs-transient failure policy).
- Lost-lease signaling via watch channel broadcast (`is_lost()` / `until_lost()`).
- Reentrant mutex (hash hold-count), read-write lock, semaphore, fair FIFO queue, multi-lock.
- gRPC service + Rust SDK with bearer/proxy auth.
- Watch fan-out hub with versioned events.
- mTLS support (server + client).
- Graceful drain with readiness flip.
- Prometheus metrics endpoint.
- CountDownLatch primitive.

## [0.1.0] — 2026-08-21

### Added
- Single-instance Redis mutex with mandatory fencing tokens.
- Backend-neutral `LockManager` / `LockHandle` traits.
- Lua-guarded acquire/release/extend scripts.
- Black Box Recorder (hash-chained operation history).
- Safety Policy spectrum (Cowardly/Balanced/Aggressive).
- Lock Testament (deathbed state transfer).
- Contention Predictor (hold-duration forecasting).
- FenceSeal HMAC attestations.
- Postgres fence-SQL builders.
- Deterministic simulation + mutation-validated invariant checker.
- Property tests vs live Redis.
- CI workflow.
