# 0029. Watch fan-out: hub with one poller per key

- **Status:** Accepted
- **Date:** 2026-08-22

## Context
The original watch implementation spawned a store-poller per subscriber: 10k watchers on a hot key meant 100k `EXISTS` probes/sec against Redis — our own gap-analysis item #4. Watch cost must scale with *distinct keys*, not *watchers*.

## Options considered
1. Hub: one poller per key, broadcast to N subscribers (chosen)
2. Redis keyspace notifications as primary delivery
3. Per-watcher polling with longer intervals
4. Push subscribers to poll themselves client-side

## Decision
`WatchHub` in the server keeps `map<key, broadcast-state>`; the first subscriber spawns that key's single probe loop (`EXISTS` every 100 ms), transitions broadcast over `tokio::sync::watch`, and per-subscriber forwarder tasks translate into bounded mpsc streams. Pollers retire when their last subscriber departs; a gauge tracks active keys. Slow consumers lag on their own channel — level-triggered semantics mean they simply see the next transition, never a permanent stall of the shared path.

## Why this is the best option
- **O(distinct keys) instead of O(watchers)**: the exact scaling wall from the audit is gone; 10k watchers on one key now cost 10 probes/sec.
- **Level-triggered by construction**: new/lagging subscribers get current state on next transition without replay logic — matches how lock watchers are actually used ("tell me when it frees").
- **Retirement keeps memory honest**: keys vanish from the map when unwatched; no graveyard growth.
- **Backpressure isolation**: a stalled consumer only fills its own 16-slot channel; the poller and other subscribers never block.

## Why not the alternatives
- **Keyspace notifications**: fire-and-forget (our standing principle: never trusted alone, PLAN §4.6); would still need the poll fallback we already have. Revisit as a latency optimization layered under the same hub.
- **Longer per-watcher intervals**: multiplies the problem rather than fixing it.
- **Client-side polling**: moves load to every SDK and loses server-side quota/audit visibility.

## Consequences
- Transition latency is bounded by the shared 100 ms cadence (unchanged).
- Events remain anonymized (ADR 0022); hub adds no new data exposure.
- Quota accounting stays per-subscriber via forward-task guards — N watchers = N quota slots even though they share one poller.
- etcd-native watch (server-push, ordered revisions) remains the consensus-backend upgrade path.
