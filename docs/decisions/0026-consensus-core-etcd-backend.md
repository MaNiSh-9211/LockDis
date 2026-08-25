# 0026. Consensus core: first-class etcd backend

- **Status:** Accepted
- **Date:** 2026-08-22

## Context

ADR 0003 standardized on Redis as the storage backend, but Redis alone offers
no consensus: a single master can lose acknowledged writes on failover, and
Redlock (ADR 0020) trades that weakness for liveness assumptions that remain
debated. Users locking around *money*, *leases*, or *leadership elections*
asked for a backend whose safety does not depend on clocks or quorum-guessing.
etcd's Raft log and MVCC revisions provide exactly those primitives — but
bolting etcd on as an afterthought would fork the API and split the test
suite, the two failure modes ADR 0002 was written to prevent.

Constraints:

- One `LockManager` trait (core) must serve both backends unchanged.
- Fencing tokens (ADR 0005) must be **globally monotonic** on etcd, not just
  per-key, since Raft gives us that for free.
- Crash detection must stay server-authoritative (ADR 0027 semantics): the
  store, not the client, decides when a dead holder's lock dies.

## Options considered

1. First-class etcd backend implementing the core trait (chosen)
2. Redis-style shim: emulate per-key INCR counters on top of etcd
3. Wrap etcd's concurrency-package lock RPC (`etcdctl lock` equivalent)
4. Keep Redis-only; document Redlock as the consensus story

## Decision

`palisade-etcd` implements `LockManager` directly over etcdv3 transactions:

- **Grant** — one atomic txn:
  `if create_revision(key) == 0 then put(key, token, lease_id)`.
  No check-then-put window exists; the comparison and the write commit or
  fail together inside Raft.
- **Fencing tokens** — the grant transaction's **MVCC revision** is the
  fence. Revisions are allocated by Raft itself: globally monotonic,
  linearizable, gapless per cluster. There is no allocator node to elect,
  no counter key to retain (ADR 0012's rolling-TTL problem simply vanishes).
- **Liveness** — every lock key carries an etcd **lease** kept alive by the
  holder. Stop keeping alive and the *server* deletes the key: crash death
  is decided by the store, matching ADR 0027's philosophy.
- **Release** — one atomic txn: `if value(key) == token then delete(key)`,
  then lease revoke. Stale tokens cannot delete a successor's grant,
  mirroring the Redis release script (ADR 0011).

Contended waiting stays poll-based with a bounded interval (ADR 0010), so
both backends share identical liveness behavior and one test vocabulary.

## Why this is the best option

- **Safety upgrades without API churn**: callers switch backends by changing
  a constructor call; handles, options, watchdogs, and fencing comparisons
  (`fence.supersedes()`) behave identically.
- **Strongest possible fence source**: Raft-allocated revisions remove the
  entire class of counter-expiry bugs ADR 0012 has to manage on Redis.
- **Server-side death**: a partitioned holder loses its lock exactly when
  the lease expires *at the store* — provable by stopping the etcd member
  mid-hold (`chaos_member.rs`) and watching the holder learn it lost.
- **Honest trade-off surface**: users choose Redis (throughput, rich
  semantic predicates) vs etcd (consensus, linearizable fences) as an
  explicit deployment decision instead of a hidden default.

## Why not the alternatives

- **Counter emulation on etcd**: pays etcd's latency while keeping Redis's
  weaker per-key counters; strictly dominated by using revisions.
- **Concurrency-package lock RPC**: opaque session keys, no access to the
  fence value we require, no room for Testament payloads or future
  predicate work; wraps someone else's black box instead of ours.
- **Redis-only + Redlock marketing**: ADR 0020 already documents why
  Redlock's safety is timing-dependent. Refusing a consensus backend would
  leave our strongest guarantee claim unfounded.

## Consequences

- Two backends now carry the invariant suite: property tests run against
  live Redis; consensus behavior is proven against a live single-member
  cluster plus a kill-the-member chaos scenario.
- etcd leases tick in whole seconds, so sub-second TTL floors clamp upward;
  documented in `EtcdConfig::lease_secs`.
- Semantic Locks (INV-6) remain Redis-only: hash-field predicates need Lua.
  etcd grants are plain mutexes until an STM design proves itself.
- The gRPC server still binds one backend per process (ADR 0031's BackendOps
  proposal tracks multi-backend routing); the demo binary is Redis-only.
