# Durability Guide

What happens to locks when the store restarts — per backend and configuration.
Fencing tokens (ADR 0005) make *correctness* survive any of these scenarios;
this page is about *availability* after data loss.

## The universal rule

> After a store loses lock state, every previously held lease is gone.
> Holders still think they hold; fencing rejects their writes. New acquirers
> proceed immediately. Availability recovers instantly; stale holders are
> neutralized by token comparison at the resource.

## Redis topologies

| Configuration | Restart behavior | Risk |
|---|---|---|
| Standalone, no persistence (default) | All locks vanish on restart | Stale holders fenced; instant re-acquirable. **Dev/lab only.** |
| RDB snapshots | Locks resurrect from last snapshot with TTLs already partially elapsed | Resurrected locks may expire quickly (good) but block re-acquire briefly (bad). Acceptable with short TTLs. |
| AOF `appendfsync everysec` | ≤1s of writes lost: a recently *released* lock may resurrect | Released-then-resurrected keys self-heal at TTL. Fencing covers the overlap window. **Recommended minimum for prod.** |
| AOF `appendfsync always` | No acknowledged writes lost | Correct but ~5-10× write latency cost; only for lock-critical workloads. |

## Redis Sentinel / Replica failover

Async replication means a promoted replica can lack the latest releases:
a released lock may **reappear** after failover. Consequences:

- Correctness: preserved by fencing tokens (the released holder's successor
  fence is newer).
- Availability: the phantom key blocks re-acquisition until its TTL lapses.

**Mitigation**: keep TTLs short relative to recovery time; prefer Redlock
across independent masters when phantom-blocking is unacceptable.

## Redlock (N independent masters)

Losing < quorum nodes changes nothing (grants require majority). Losing a
full site requires N spread across failure domains — treat that as the
deployment requirement, not an afterthought.

## etcd backend (consensus)

Lock state lives in the Raft log, fsync'd before acknowledgement:

| Event | Behavior |
|---|---|
| Minority member loss | Zero impact |
| Majority loss | Service pauses until quorum restores; committed grants survive |
| Full cluster restart | State recovered from log/snapshot; leases continue their countdown from persisted remaining-TTL |

This is the "consensus-correct" tier: no phantom keys, no lost releases,
no snapshot windows. Cost: run ≥3 members.

## Recommendations by use case

| Use case | Backend | Config |
|---|---|---|
| Dev / tests | Redis standalone | defaults |
| App-level coordination (leader election, dedup) | Redis + AOF everysec | TTLs 10–30 s |
| Money-adjacent serialization gates | etcd (3/5 nodes) + fencing downstream | default durable |
| Cross-region | not yet supported (roadmap) | — |
