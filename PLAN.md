# Palisade — A Modern Distributed Locking System in Rust

> Working name **Palisade** (a defensive fence — a nod to *fencing tokens*, the feature that separates toy locks from correct ones).
> Alternatives: `wicket`, `latchkey`, `kestrel`.

---

## 1. Vision

Build the most modern open-source distributed locking system: a **Rust workspace** that ships

1. a **client library** (`palisade-core` + `palisade-redis`) with pluggable backends,
2. a **standalone gRPC lock service** (`palisade-server`) so non-Rust clients can use it,
3. a **multi-language client SDK story** starting with Rust, driven by protobuf,
4. **Jepsen-grade correctness evidence**: deterministic simulation + linearizability checking + real-cluster chaos tests.

### Why most existing solutions fall short (our differentiators)

| Existing | Weakness we fix |
|---|---|
| Redlock libs (redsync, redlock-rs) | No fencing tokens, no auto-renewal, no fairness |
| Redisson (Java) | Great features, JVM-only, opaque internals |
| etcd/ZooKeeper | Correct but heavy operational footprint |
| Naive `SET NX PX` tutorials | Broken under GC pauses, clock skew, failover |

### Design principles

1. **Safety over liveness.** A lock must never be held by two owners, even across partitions, pauses, and failovers.
2. **Fencing tokens everywhere.** Every grant returns a monotonically increasing token; we ship helpers to enforce it downstream.
3. **No trust in wall clocks** for correctness — TTLs use Redis-side expiry; local timing uses `Instant` (monotonic).
4. **Async-native** (Tokio), cancellation-safe APIs, zero blocking on the hot path.
5. **Observable by default**: OpenTelemetry traces, Prometheus metrics, structured logs.
6. **Prove, don't promise**: every algorithm ships with a property/simulation test that would catch its known failure modes.

---

## 2. Feature Set

### Core primitives
- [x] **Mutex lease** — exclusive lock with TTL (`SET key token NX PX ttl` + Lua-guarded release/extend)
- [x] **Fencing tokens** — 64-bit monotonic token per grant (Redis `INCR` on a fence counter, atomic with the grant via Lua)
- [x] **Watchdog auto-renewal** — background task extends the lease at `ttl/3` while the critical section runs; stops on drop/cancel
- [x] **Reentrant locks** — owner ID + hold count stored in a hash; same owner re-acquires instantly
- [x] **Read-write lock** — shared readers counted, writer exclusive; reader-preferring or writer-preferring modes
- [x] **Semaphore** — N concurrent holders, fair FIFO queue
- [x] **Fair (queued) mode** — FIFO waiter queue via Redis list + `BLPOP`-style signaling with polling fallback (keyspace notifications are fire-and-forget, never trusted alone)
- [x] **Multi-lock** — acquire K locks in a globally sorted order (deadlock-free), all-or-nothing with rollback
- [x] **Try-lock with deadline** — `try_lock_for(duration)` built on cancellation-safe Tokio timers
- [x] **Lost-lease signaling** — `is_lost()` / `until_lost()` so critical sections can abort when the watchdog detects loss

### Service layer
- [ ] **gRPC server** (tonic): Lock/Unlock/Extend/Watch RPCs, streaming watch for lock-state changes
- [ ] **mTLS + token auth** between clients and server
- [ ] **Graceful drain**: server refuses new grants, lets leases expire naturally
- [ ] **Health probes** (gRPC health protocol) for k8s

### Ops / quality
- [ ] Prometheus metrics: grant latency histograms, contention counters, renewal failures, fence token gaps
- [ ] OpenTelemetry spans linking `lock → critical section → unlock`
- [ ] Chaos harness: toxiproxy partitions, Redis failover (Sentinel), process pausing
- [ ] Deterministic simulation (madsim) with seeded schedules → reproducible bugs
- [ ] Linearizability checker over recorded histories
- [ ] Benchmarks (criterion) + load generator
- [ ] cargo-fuzz targets for script/response parsing

---

## 3. Repository Layout (Cargo workspace)

```
palisade/
├── Cargo.toml                  # workspace
├── crates/
│   ├── palisade-core/          # traits, types, fencing, errors — no I/O
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── lock.rs         # trait DistributedLock, LockHandle, LockOptions
│   │       ├── fencing.rs      # FencingToken(u64), FenceStore trait
│   │       ├── owner.rs        # OwnerId (UUIDv7), generation logic
│   │       └── error.rs
│   ├── palisade-redis/         # Redis backend: scripts, Redlock, watchdog
│   │   └── src/
│   │       ├── scripts/        # .lua files, embedded via include_str!
│   │       ├── single.rs       # single-instance algorithm
│   │       ├── redlock.rs      # quorum across N independent masters
│   │       ├── watchdog.rs     # renewal task
│   │       ├── rwlock.rs
│   │       ├── semaphore.rs
│   │       └── fair.rs         # FIFO queue mode
│   ├── palisade-proto/         # .proto + tonic build
│   ├── palisade-server/        # gRPC service binary
│   ├── palisade-client/        # ergonomic SDK over gRPC or direct backend
│   └── palisade-testing/       # sim, chaos, linearizability, workloads
├── examples/
├── benches/
├── fuzz/
└── docs/
    ├── decisions/              # ADRs — one per feature/architectural decision
    │   └── README.md           # index + template (see instructions.txt.txt)
    └── algorithm-specs.md      # safety arguments per algorithm
```

> **Process rule** (from `instructions.txt.txt`): every feature and architectural
> decision must get an ADR in `docs/decisions/` recording what we picked, why
> it's the best option, and which alternatives were rejected and why.
> Nine ADRs already exist covering the foundational choices.

**Key dependency choices**

| Concern | Crate |
|---|---|
| Async runtime | `tokio` (full) |
| Redis client | `redis` with `tokio-comp` (+ `connection-manager`) |
| gRPC | `tonic` + `prost` |
| Errors | `thiserror` (libs), `anyhow` (bins) |
| IDs | `uuid` v7 features |
| Observability | `tracing`, `opentelemetry`, `metrics` + `metrics-exporter-prometheus` |
| CLI/config | `clap`, `serde`, `figment` or `config` |
| Simulation | `madsim` (deterministic Tokio simulator w/ fault injection) |
| Property tests | `proptest` |
| Integration | `testcontainers` (real Redis in Docker) |
| Chaos | `toxiproxy-rust` driving a toxiproxy container |
| Model checking (stretch) | `stateright` |

---

## 4. Algorithms & Safety Design

### 4.1 Single-instance mutex (the foundation)

Grant (atomic via Lua):

```lua
-- KEYS[1]=lock key, KEYS[2]=fence counter
-- ARGV: token, ttl_ms
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])   -- fencing token
end
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[3] then                   -- reentrant path (owner match)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return redis.call('INCR', KEYS[2])
end
return nil
```

Release / extend (only with matching token — prevents releasing someone else's lock after your lease expired):

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])            -- or PEXPIRE for extend
  return 1
end
return 0
```

**Why this is safe:** mutual exclusion holds while (a) Redis is a single failure domain and (b) holders make progress within the TTL or use the watchdog. The fencing token covers case (b) failing: a paused holder whose lease expired gets a stale token, and any fence-checking resource rejects its writes even if it still "thinks" it holds the lock.

### 4.2 Fencing tokens (first-class, not optional)

- Token = `INCR` on `<key>:fence`, returned with every successful grant.
- `FencingToken` type flows through `LockHandle::fence()`.
- Ship `palisade-core::fence` helpers: a `FenceCheck` trait plus ready-made adapters (e.g., Postgres `UPDATE ... WHERE fence > $last` pattern) and docs showing how to gate writes through it.
- Tokens are compared, never generated, by clients — ordering authority is always the store.

### 4.3 Watchdog (lease auto-renewal)

- On grant, spawn a renewal task: sleep `ttl/3`, call extend-Lua, repeat.
- Renewal failure twice in a row ⇒ mark handle `Poisoned`, wake the critical section via a `CancellationToken`, surface `LockLostError`.
- `Drop` cancels the task and best-effort releases. Panics/unwind still drop ⇒ no orphaned leases in-process.

### 4.4 Redlock (quorum mode)

- N=5 **independent** masters (no replicas, no cluster — async replication breaks safety).
- Acquire: attempt all N with small per-node timeouts; success iff ≥ ⌊N/2⌋+1 grants within `validity < ttl`. Retry with jitter on failure; always release acquired nodes on partial failure.
- Fence token = max of per-node fence counters is NOT safe ⇒ instead use a dedicated global fence allocator node, or derive tokens from a vector of per-node counters compared component-wise. Document the trade-off; default to the allocator node.
- Honest docs section: Redlock's known controversy (Kleppmann vs antirez) and why fencing is mandatory in our implementation.

### 4.5 Fairness (queued mode)

- Waiters `LPUSH` a request record onto `<key>:queue`; holder, on release, pops the next live waiter and grants explicitly (record carries waiter id + fencing token).
- Liveness probe: each queued entry has a TTL heartbeat; dead waiters are skipped.
- Non-queued (barge-in) mode remains available for max throughput.

### 4.6 What we explicitly do NOT do

- No reliance on client wall clocks for expiry decisions.
- No keyspace-notification-only wakeups (always poll fallback).
- No lock promotion (read→write) without release+reacquire (deadlock-prone); documented.

---

## 5. Public API Sketch

```rust
use palisade_redis::{RedisLockManager, RedlockConfig};

let mgr = RedisLockManager::builder()
    .endpoint("redis://127.0.0.1:6379")
    .default_ttl(Duration::from_secs(30))
    .watchdog(true)                 // auto-renewal
    .fair(true)                     // FIFO waiters
    .build()
    .await?;

let lock = mgr.lock("orders:42")
    .ttl(Duration::from_secs(10))
    .reentrant(owner_id)
    .acquire()
    .await?;                        // cancellation-safe

let fence: FencingToken = lock.fence();
do_protected_write(fence).await?;   // resource checks fence > last_seen

drop(lock);                         // watchdog stops, best-effort release
```

```rust
// Multi-lock, deadlock-free (sorted acquisition)
let multi = mgr.lock_all(["a", "b", "c"]).acquire_all().await?;
```

### gRPC surface (v1)

```proto
service LockService {
  rpc Lock(LockRequest) returns (LockResponse);          // blocks/fails per mode
  rpc TryLock(TryLockRequest) returns (LockResponse);
  rpc Unlock(UnlockRequest) returns (UnlockResponse);
  rpc Extend(ExtendRequest) returns (ExtendResponse);
  rpc Watch(WatchRequest) returns (stream LockEvent);    // acquired/released/expired
}
message LockResponse {
  string token = 1;        // owner token
  uint64 fencing_token = 2;
  google.protobuf.Duration lease_ttl = 3;
}
```

---

## 6. Correctness Strategy (the headline feature)

Three escalating layers:

### Layer 1 — Property tests (`proptest`)
- Mutual exclusion invariant: N tasks increment a shared counter under the lock; final count == number of increments.
- No-lost-release: random acquire/drop interleavings leave no residual keys.
- Reentrancy counts, RWL reader/writer overlap rules, semaphore capacity bounds.

### Layer 2 — Deterministic simulation (`madsim`)
- Run the whole system (client tasks + fake Redis) under a seeded scheduler.
- Inject: dropped/delayed/reordered messages, process pauses longer than TTL, clock jumps.
- Every bug reproduces from its seed — CI reruns seeds on failure.
- Record operation histories `(op, start, end, value)` during simulation.

### Layer 3 — Linearizability checking
- Feed histories into a checker (port of Porcupine/Knossos approach; or `stateright` models) proving every history is linearizable w.r.t. Lock/Unlock semantics.
- Gate releases on: zero violations across ≥ 10⁶ simulated ops × 100 seeds.

### Layer 4 — Real-world chaos (nightly, Docker Compose)
- Topology: app container ↔ toxiproxy ↔ {1× Redis | 5× Redis masters | Sentinel failover set}.
- Scenarios: partition during critical section, kill Redis mid-hold, failover mid-renewal, 500 ms process pause (SIGSTOP) exceeding TTL, network delay spikes.
- Assertions: mutual exclusion never violated (audited via a shared append-only log with fence checks), liveness recovers within SLO.

---

## 7. Observability & Performance

- **Metrics** (Prometheus): `grant_duration_seconds` histogram, `wait_queue_depth`, `renewal_failures_total`, `fence_rejections_total{side}`, `leases_active`, `redlock_quorum_size`.
- **Tracing**: span per lifecycle `lock.acquire → cs.execute → lock.release`, linked by lock key + token; OTLP exporter.
- **Benchmarks** (criterion): grant p50/p99 at 1k–50k ops/s, watchdog overhead, fair-vs-barge contention curves.
- **Targets**: p99 grant ≤ 5 ms single-node LAN; watchdog adds < 1 % CPU per 10 k active leases.

---

## 8. Milestones

| Phase | Scope | Exit criteria |
|---|---|---|
| **0. Scaffold** (wk 1) | Workspace, CI (fmt, clippy `-D warnings`, test, coverage), README skeleton | `cargo ci` green on empty crates |
| **1. Single-node mutex** ✅ | Lua scripts, `DistributedLock` trait, OwnerId, basic metrics | Layer-1 property tests pass against real Redis (testcontainers) |
| **2. Fencing + watchdog** ✅ | Fence counters, `FencingToken`, renewal task, `LockLostError`, poison semantics | Pause-holder simulation proves stale holder detected; docs chapter on fencing |
| **3. Rich primitives** ✅ | Reentrant, RWL, semaphore, fair queue, multi-lock | Each primitive has property tests + example |
| **4. Redlock** ✅ | Quorum acquire/release, partial-failure rollback, config | Chaos test: kill minority of masters mid-hold ⇒ no double-hold |
| **5. gRPC service + SDK** (wk 9–10) | tonic server, auth (mTLS), Watch stream, Rust client, Python/TS stubs gen | Interop tests; k8s manifests + Helm chart |
| **6. Simulation & lin-check** (wk 11–12) | madsim harness, history recorder, linearizability checker, seed corpus in CI | 100 seeds × 10⁶ ops, zero violations |
| **7. Hardening & release** (wk 13+) | Fuzzing, benchmarks, tuning, docs site, `cargo-dist` releases, changelog | v0.1.0 tagged; benchmark report published |

---

## 9. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Redis async replication silently breaks safety on failover | Docs mandate Redlock topology or accept RPO; Sentinel mode flagged "availability-optimized"; fencing tokens make stale-leader writes rejectable |
| Keyspace notifications unreliable | Poll-with-jitter fallback; notifications only optimize latency |
| madsim fidelity gap vs real Redis | Layer-4 chaos on real Redis closes the loop |
| Watchdog leaks on abnormal exit | Lease TTL bounds damage; server-side max-TTL cap option |
| Fair mode head-of-line blocking | Waiter heartbeats + configurable queue timeout |
| Scope creep toward "build etcd" | Non-goals enforced: no consensus implementation, no persistence engine |

## 10. Non-goals (v1)

- Implementing our own consensus/Raft (Redis-only per scope; etcd adapter is a future crate behind the same trait).
- Cross-region geo-locking.
- Lock-free data structures beyond the listed primitives.

## 11. References

- Kleppmann, *How to do distributed locking* (fencing-token argument)
- antirez, *Is Redlock safe?*
- Redis docs: `SET NX PX`, Lua scripting, Sentinel
- Redisson watchdog design (feature inspiration)
- Jepsen analyses of Redis/Redlock; Porcupine & Knossos linearizability checkers
- madsim: deterministic simulation for Tokio ecosystems
