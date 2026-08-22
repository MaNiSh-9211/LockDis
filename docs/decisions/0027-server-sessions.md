# 0027. Server-authoritative sessions: heartbeat-bound lock lifetimes

- **Status:** Accepted
- **Date:** 2026-08-22

## Context
Gap-analysis item #2: with client-side watchdogs, a crashed gRPC client's locks live out their **entire lease TTL** because the server cannot distinguish dead from slow. ZooKeeper solves this by binding ephemeral nodes to sessions the *server* declares expired. Our Redis leases could not be server-declared — until we put the service layer to work.

## Options considered
1. Session table + heartbeat RPC + sweeper releasing bound locks (chosen)
2. Connection-bound leases keyed on gRPC connection state
3. etcd-style: require the consensus backend for session semantics
4. Status quo: TTL-only, document the crash window

## Decision
Three new RPCs: `RegisterSession(client_id, ttl) → token`, `Heartbeat(token)` (client cadence `ttl/3`), `CloseSession(token)`. Locks acquired while a session is active are recorded in a server-side book `(session → [(key, owner_token)])`. A sweeper task runs every 500 ms: any session whose silence exceeds its TTL has every bound lock released via the standard ownership-checked scripts, then is dropped.

## Why this is the best option
- **Server decides death**: detection moved from "lease arithmetic on a dead process's behalf" to "the process stopped talking to us" — exactly ZooKeeper's model, implemented at the layer that can act.
- **Bounded, tunable crash window**: worst case = session ttl + sweep interval (e.g., 3s + 0.5s), versus full lease TTLs of 30–60s. The e2e test demonstrates a 60-second lock dying in ~3 seconds after client death.
- **Opt-in, additive**: sessions are optional per client; un-sessioned locks keep pure-TTL semantics. Zero breakage for existing users; wire changes are additive fields/messages only (ADR 0008 discipline).
- **Reuses the ownership machinery**: the sweeper releases through the same Lua token-checked path as normal unlocks — no second release implementation to keep correct.

## Why not the alternatives
- **Connection-state binding**: TCP resets under partition fire before we learn whether the client died or the network did — false-positive releases reintroduce split-brain at the service layer. Heartbeats are application-level truth.
- **etcd-required**: right answer for the etcd backend natively (its leases ARE server-authoritative), but Redis-tier users deserve the same semantics without changing stores.
- **TTL-only status quo**: the audit called this out as a top-3 production gap; TTL windows are simply too coarse for fast failover of long-lived leadership locks.

## Consequences
- Heartbeat traffic: one small RPC per `ttl/3` per client (not per lock) — negligible; batched renewal remains available via the SDK watchdog for lease extension itself.
- Sweeper releases are audited (`expired session` lines) and counted (`palisade_sessions_expired_total`).
- Sessions live in-process: multi-replica deployments must route a client's calls to one replica (sticky LB) OR accept that heartbeats landing anywhere still work (table is per-replica → use single active session registrar). Documented deployment note; shared session store is future work if needed.
