# 0012. Fence counter retention: rolling TTL at 10× lease

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Fence counters (`{key}:fence`) must never reset while a stale holder could still compare tokens — but immortal counters leak memory across millions of distinct lock keys.

## Options considered
1. Rolling TTL refreshed to 10× lease on every grant/extend (chosen)
2. No expiry (immortal counters)
3. Store fence inside the lock key's value/hash
4. Separate fence registry hash with LRU eviction

## Decision
Each grant/extend sets the fence counter's TTL to `10 × lease TTL`. Since the lock key itself never outlives its lease, the counter provably outlives its lock; counters die shortly after their lock lifecycle ends.

## Why this is the best option
- **No unbounded growth**: memory is bounded by live locks × 11 keys' worth of overhead, not by historical key cardinality — critical for a library used with per-entity keys (per-user, per-order).
- **Safety margin is quantifiable**: a counter reset requires a pause exceeding ten consecutive lease durations *while the lock lifecycle fully dies and restarts*; even then, strict-greater comparison keeps safety (a replayed equal token is rejected — worst case is a brief liveness hiccup for the new holder, resolved by the next INCR).
- **Zero extra infrastructure**: no registry, no eviction policy to reason about.

## Why not the alternatives
- **Immortal counters**: turns the library into a slow memory leak; unacceptable default for a public library even though it's the simplest-to-reason-about option.
- **Fence inside lock value**: resets to zero whenever the lock expires+restarts — destroys monotonicity exactly across the failover boundary where fencing matters most.
- **LRU registry**: evicts precisely the cold-but-alive counters fencing needs; complexity without a safety win.

## Consequences
- Token reuse across distant lifecycles of the same key is possible; safe by strict-greater comparison, documented in `FencingToken`.
- Multiplier 10 is a constant today; exposed as config if real-world pause profiles demand it.
- Redlock mode (Phase 4) needs its own fence-allocator design — this ADR covers single-instance only.
