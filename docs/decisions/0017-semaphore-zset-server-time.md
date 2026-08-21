# 0017. Semaphore: ZSET scored by Redis-side expiry instants

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
A distributed semaphore must reclaim permits from crashed holders without leaking capacity, and must not trust client clocks for expiry decisions.

## Options considered
1. ZSET scored by Redis-computed expiry instant; prune before admit (chosen)
2. SET of tokens with whole-set TTL
3. Counter string + per-holder lease hashes
4. Sorted list with explicit heartbeats

## Decision
Permits are ZSET members scored by `Redis TIME + ttl`. Every acquire first prunes expired members (`ZREMRANGEBYSCORE -inf now`), then admits if under capacity. Extend re-scores the caller's member; release removes it, deleting the structure when empty.

## Why this is the best option
- **Crash-safe capacity recovery**: a holder that dies mid-critical-section stops renewing; its score passes `now` and the next acquire prunes it. No heartbeats, no GC task, no coordination.
- **Server-clock purity**: Redis computes both the score and the comparison basis, honoring our no-client-wall-clock principle (PLAN §2). Client clock skew cannot steal or extend permits.
- **O(log n) operations** with tiny scripts; fence allocation rides along via the same `INCR` pattern as every other primitive.

## Why not the alternatives
- **SET + set-level TTL**: one dead holder pins its slot until the entire structure expires under zero activity — capacity leaks precisely under sustained load, the opposite of what we need.
- **Counter + lease hashes**: N+1 keys per semaphore and multi-key scripts for what ZSET does in one; more surface, no benefit.
- **Heartbeat lists**: pushes liveness responsibility onto clients; the watchdog already exists, but scoring by expiry makes even watchdog-less users safe.

## Consequences
- Capacity is eventually consistent after crashes: recovery latency = remaining TTL of the dead permit (mitigated by short TTLs + watchdog).
- `TIME` inside scripts requires effect-based replication (Redis ≥ 5); fine for our supported versions, noted in docs.
