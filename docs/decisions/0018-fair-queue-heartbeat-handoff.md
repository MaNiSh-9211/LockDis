# 0018. Fair queue: FIFO list, heartbeats, handoff-on-release

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Barge-in polling (ADR 0010) is throughput-optimal but unordered. Some workloads need FIFO fairness: no waiter may acquire while an earlier waiter still waits.

## Options considered
1. FIFO list + per-waiter heartbeat keys + handoff-on-release (chosen)
2. Redis Streams consumer groups
3. Sorted-set priority queue by arrival timestamp
4. Single global lock-service queue (server-side fairness)

## Decision
Waiters `LPUSH` their token onto `{key}:q` and refresh a heartbeat key (`{key}:hb:{token}`, TTL = max(2 s, ttl)) on every poll. Acquisition grants directly only when the lock is free AND the waiter is queue-head (or queue empty). Release deletes the lock, then pops from the tail until it finds a live heartbeat — that waiter's token is written straight into the lock, discovered by its next poll.

## Why this is the best option
- **Strict no-barging invariant** falls out of one rule: direct grants require an empty-or-head-of-queue position; everything else enqueues.
- **Dead waiters can't block the line**: heartbeats make liveness explicit at handoff; a crashed waiter is skipped in O(1) per dead entry, not resurrected by timeout.
- **Handoff preserves fence monotonicity**: the winner's grant allocates a fresh `INCR` fence exactly like any other path — fairness never bypasses token ordering.
- **Self-grant covers expiry**: if a holder dies without releasing, the head waiter's own poll finds the lock free with itself at head and grants directly.

## Why not the alternatives
- **Streams**: powerful (acks, retries) but heavy for "who's next"; consumer-group semantics don't map cleanly to lease handoff and complicate simulation.
- **ZSET by timestamp**: equivalent ordering to a list but with score bookkeeping we don't need; lists give O(1) push/pop-at-ends.
- **Server-side queues**: right answer inside the Phase 5 service eventually; the library must stay Redis-only (ADR 0003).

## Consequences
- Handoff scripts touch dynamically-named heartbeat keys, which cluster deployments reject outside declared KEYS — fair mode is documented standalone/single-slot only.
- Waiter latency includes one poll interval (ADR 0010 cadence); fairness costs ~one RTT over barge-in.
- Queue TTL (10× lease) garbage-collects abandoned queues.
