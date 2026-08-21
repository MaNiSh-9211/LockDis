# 0010. Contended waiting: polling with bounded interval

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
`try_lock_for` must wait when a lock is contended. Redis offers several wakeup mechanisms: keyspace notifications (pub/sub), `BLPOP` on waiter lists, Redis Streams, or plain polling.

## Options considered
1. Fixed-interval polling (20 ms) with deadline checks (chosen)
2. Keyspace-notification-driven wakeup
3. BLPOP-based blocking wait
4. Streams-based fair queue (planned separately in Phase 3)

## Decision
Poll the acquire script at a fixed 20 ms interval until grant or deadline. Notifications/queues layer on top later as latency optimizations, never as correctness dependencies.

## Why this is the best option
- **Correctness is independent of delivery guarantees**: keyspace notifications are fire-and-forget (missed under partitions/reconnects) and pub/sub subscriptions silently drop during failover. Polling has no failure mode that can deadlock a waiter.
- **Trivially testable**: deterministic simulation (ADR 0006) can model timers exactly; modeling notification loss paths would multiply the state space for zero safety benefit.
- **Good enough latency**: 20 ms worst-case pickup is far below typical lock hold times; the Lua acquire attempt itself stays the only ordering authority.
- **One code path**: barge-in mode and future fair mode share the same acquisition primitive.

## Why not the alternatives
- **Notifications-only**: missed message = stuck waiter until some timeout; violates our "never trust fire-and-forget" principle (PLAN.md §4.6).
- **BLPOP**: blocks a connection per waiter (connection-pool pressure at scale), and its fairness semantics don't compose with fencing-token grants without extra bookkeeping — that's the Phase 3 fair queue's job.
- **Streams now**: right design for FIFO fairness, premature for the plain mutex; arrives in Phase 3 with its own ADR.

## Consequences
- p99 grant-under-contention includes up to one poll interval; documented in performance targets.
- Polling load scales with waiters × 50 req/s ceiling per waiter — acceptable; fair mode will replace spin for queued waiters.
- Interval constant is private today; revisit if benchmarks show hot-loop waste.
