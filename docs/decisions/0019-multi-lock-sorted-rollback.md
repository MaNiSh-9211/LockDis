# 0019. Multi-lock: sorted acquisition + rollback retry

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Workflows need several locks at once (transfer between two accounts, migrate a set of shards). Naive concurrent acquisition deadlocks; naive sequential acquisition leaks partial holdings on failure.

## Options considered
1. Sort keys, acquire in order, rollback + retry on contention (chosen)
2. Redlock-style atomic multi-key Lua script
3. Lock ordering without retry (fail fast, no rollback)
4. Two-phase handshake protocol

## Decision
Keys are deduplicated and sorted client-side (global total order ⇒ no cycle possible). Acquisition walks the sorted list; any `Held`/backend failure releases everything acquired so far (reverse order) and the whole attempt retries until the deadline. Duplicates are an `InvalidConfig` error.

## Why this is the best option
- **Deadlock-freedom is structural**: with all clients using one total order, the wait-for graph is acyclic by construction — no detection, no timeouts-as-safety.
- **Rollback bounds damage**: partial holdings live for milliseconds, not lease durations; other users see transient unavailability, not stuck resources.
- **Composes with existing machinery**: each key uses the standard single-lock scripts, so fencing tokens, watchdogs, and metrics work per-key unchanged. No new correctness surface to prove.

## Why not the alternatives
- **One multi-key Lua script**: atomic across keys only when all keys share a hash slot — useless for real multi-resource workloads on cluster, and it would silently change semantics between standalone/cluster.
- **Fail-fast without rollback**: callers must remember to clean up; the exact footgun this API exists to remove.
- **Two-phase handshake**: distributed-transaction territory; enormous complexity for marginal availability gains over retry-with-rollback.

## Consequences
- No cross-key atomicity: observers can briefly see half-acquired sets during retries; documented as the price of cluster compatibility and simplicity.
- Timeout error reports the full sorted key set for debuggability (`multi:[...]`).
- `release_all` is reverse-order and idempotent; individual `Lost` errors surface after all releases run.
