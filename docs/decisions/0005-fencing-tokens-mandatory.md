# 0005. Fencing tokens are mandatory on every grant

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Any lease-based lock can be held by two owners transiently: holder A pauses (GC, SIGSTOP, VM migration) past the TTL, the lease expires, B acquires, A resumes believing it still holds the lock. No TTL-based scheme can prevent this at the lock layer alone. The known mitigation is Kleppmann's fencing token: a monotonically increasing number returned with each grant, checked by the resource being protected, which rejects writes carrying stale tokens.

## Options considered
1. Fencing tokens mandatory, first-class API
2. Fencing tokens optional opt-in feature
3. No fencing tokens (like most Redlock libraries)

## Decision
Every successful grant returns a `FencingToken(u64)` allocated atomically by the store (`INCR` in the same Lua script as the grant). The library ships fence-check helpers/adapters, and documentation treats unprotected usage as explicitly unsafe.

## Why this is the best option
- **Closes the only unfixable hole** in lease-based locking: even when the lock layer double-grants after a pause/failover, stale holders' downstream writes are rejected by fence comparison (`fence > last_seen`).
- **Zero-cost when unused**: the token rides along in the response; clients that ignore it pay nothing.
- **Differentiator**: virtually no mainstream Redis lock library does this properly; it's the single clearest "modern & correct" signal.
- **Covers Redis's async-replication failover**: a promoted replica may grant the same lock to B while A still holds it; fence ordering makes A's subsequent writes rejectable.

## Why not the alternatives
- **Opt-in**: safety features that are optional get skipped under deadline pressure; defaults must be safe.
- **None**: we'd be shipping exactly the failure mode our testing layer exists to expose — indefensible for this project.

## Consequences
- Token allocation must be crash-safe and ordered: store-side counter, never client-generated.
- Redlock mode needs a dedicated design (per-node counters aren't trivially comparable): use a dedicated global fence allocator node; vector-compare variant documented as alternative.
- Downstream resources must cooperate (check the token); we provide adapters (e.g., Postgres conditional-update pattern) and docs, since the lock library alone cannot enforce it.
