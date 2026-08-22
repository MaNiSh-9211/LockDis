# 0030. Admin introspection: ListLocks + durability guidance

- **Status:** Accepted
- **Date:** 2026-08-22

## Context
Gap-analysis items #6 and #7: operators had no way to answer "who holds what right now?" without raw store access, and durability behavior across backends was undocumented folklore.

## Decision
Two additions:

1. **`ListLocks(prefix)` RPC** — admin-gated (`can_admin`), audited, streams `KeyState { key, held, ttl_ms }` for held keys under a prefix. Backed by a new `scan_held` on the Redis backend (SCAN + PTTL, non-blocking cursor loop). etcd backend parity lands with its native-range implementation.
2. **Durability matrix** (docs/durability.md) making the trade-offs explicit per backend and topology.

## Why this is the best option
- **Introspection closes the ops loop started by ForceUnlock**: break-glass needs a *before* view ("what is stuck?") as much as an action. Both are admin-gated and audited identically.
- **SCAN over KEYS**: non-blocking incremental enumeration keeps the introspection path from becoming the DoS vector it would be with `KEYS prefix*`.
- **Streaming response**: a namespace with 100k locks streams instead of materializing.
- **Documented durability > implicit assumptions**: fencing already makes stale-state writes rejectable; the doc now states plainly which configurations risk availability vs correctness after restart.

## Why not the alternatives
- **Unauthenticated listing**: leaks tenancy structure; sits behind the same ACL system everything else uses.
- **Shadow index of all grants** (write-through registry): perfectly queryable but adds a second source of truth to keep consistent — classic complexity trap. SCAN is honest and cheap at current scale; revisit past ~1M live keys.

## Consequences
- Listing reflects Redis lazily-expired keys accurately via PTTL>0 filtering.
- Prefix comes from the request; operators should grant `can_admin` narrowly since prefixes are caller-chosen within admin scope.
- Durability recommendations are advisory — enforcement is configuration-level (Redis AOF settings, etcd defaults), not library code.
