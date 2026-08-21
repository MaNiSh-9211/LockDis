# 0020. Redlock: independent masters + dedicated fence allocator

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Single-instance locks have one failure domain; a Redis crash blocks everyone until restart. Redlock trades availability across failures for quorum semantics — but its fence-token story is where most implementations quietly break.

## Options considered
1. N independent masters, sequential rounds, dedicated allocator node for fences (chosen)
2. N masters with per-node fence counters compared component-wise
3. N masters with max-of-counters fencing
4. Replicated/cluster Redis with "just retry on failover"

## Decision
- Ring of ≥ 3 **independent** masters (config-validated); replicas and cluster mode are rejected by documentation and topology checks.
- Acquisition: one sequential round over the ring running the standard grant script per node; success requires `quorum = n/2+1` grants AND the round finishing within 90% of the lease. Otherwise: rollback everything taken, report `Held`.
- **Fence tokens come from a single dedicated allocator node** (`INCR` on a well-known key), never from the ring's per-node counters.
- Extend requires quorum; release fans out to every node (token-checked), tolerating minority failures.
- Watchdog renews at `ttl/3` with the same definitive-vs-transient policy as single-instance (ADR 0013).

## Why this is the best option
- **Quorum math is the safety argument**: two holders cannot both collect `n/2+1` grants for the same key while fewer than `n−quorum+1 = quorum` nodes sit between them — the classic majority-overlap proof.
- **A single allocator is the only honest fence source**: per-node counters are incomparable across nodes (node A's #7 ≠ node B's #7), and max-of-counters breaks when a stale holder's node counter races ahead during a partition. One linearizable counter gives a total order — exactly what downstream rejection needs. Cost: if the allocator dies, *grants* stop but no held lock is lost (liveness-only failure, fail-stop by design).
- **Sequential rounds with a validity budget** prevent a slow/down node from eating the lease: if the round takes too long, we roll back rather than hand out a sliver-validity lock.
- **Rollback on lost quorum** releases partial acquisitions immediately instead of waiting out TTLs.

## Why not the alternatives
- **Vector/component-wise counters**: correct in theory, unusable in practice — every downstream resource would need to compare vectors, and nothing downstream speaks vector clocks.
- **Max-of-counters**: ratchet-winds forward under partitions and desynchronizes from actual grant order; unsafe as an ordering authority.
- **Replicated Redis "HA"**: async replication means a failover can resurrect a released/expired key and double-grant — precisely the scenario Redlock-with-fencing exists to survive. Rejected at the topology level, not patched at the code level.

## Consequences
- Allocator is a SPOF for *new* grants only; documented, monitored via `palisade_redlock_grants_total` stall alerts.
- Algorithm-level tests run over logical DBs; true failure-domain chaos (killing real nodes mid-hold) lands with the Phase 6 toxiproxy suite.
- Sequential rounds cap throughput vs parallel fan-out; acceptable at v1 — parallelism is a measured optimization, not a design change.
