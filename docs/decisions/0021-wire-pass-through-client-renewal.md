# 0021. Wire semantics: stateless pass-through, client-side renewal

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Over gRPC, someone must own the lease lifecycle. The server could hold sessions (renewing on behalf of connected clients), or the wire can pass tokens through and let clients renew.

## Options considered
1. Stateless pass-through: server mints tokens, clients call Extend (chosen)
2. Server-side sessions with connection-bound watchdogs
3. Hybrid: session watchdogs only for clients that request them

## Decision
The server is a stateless pass-through: `TryLock` returns `(token, fencing_token, ttl)`; every mutation (`Unlock`, `Extend`) carries the token and is ownership-checked by the same Lua scripts as local use. Renewal is the client's job — the Rust SDK embeds the identical watchdog (ttl/3 cadence, definitive-vs-transient policy) so remote handles behave like library handles.

## Why this is the best option
- **One safety argument covers both transports**: the wire adds no new locking semantics, so the Phase 1–4 proofs and tests apply unchanged; there is no second implementation to keep correct.
- **Horizontal scaling for free**: any server instance can serve any request against any Redis; no session affinity, no draining complexity beyond refusing new grants.
- **Crash semantics are already defined**: a dead client stops renewing; the TTL bounds the leak — the exact model the whole system is built around. Session tracking would add false-positive releases on network blips (a dropped TCP connection is not a dead client).
- **Language parity**: pass-through means Python/TS clients get identical behavior without each SDK needing magic.

## Why not the alternatives
- **Server-side sessions**: nicer crash response in theory, but requires connection liveness heuristics that misfire under partitions — releasing a lock because a socket blinked reintroduces split-brain at the service layer.
- **Hybrid**: two modes to test, document, and simulate; the availability gain over TTL-bounded leaks did not justify it for v1.

## Consequences
- A crashed gRPC client blocks its key until TTL expiry (mitigation: short TTLs + SDK watchdog); documented prominently.
- Server enforces a `max_ttl` ceiling regardless of what clients request.
- Watchdog-over-wire costs one RPC per ttl/3 per held lock — acceptable; batched renewal is a future optimization.
