# 0031. Backend abstraction for the gRPC tier (F-02) — designed, migration pending

- **Status:** Proposed
- **Date:** 2026-08-22

## Context
The gRPC server hardcodes `RedisLockManager` (10 call sites), so the consensus tier cannot be reached over the wire. Pass-through needs token/key-level ops only: grant, unlock-token, extend-token, probe, describe, force-delete, prefix-scan.

## Decision (design)
A `BackendOps` async trait owned by the server crate:

```
try_grant(key, ttl) -> Grant { token, fence }
unlock_token / extend_token / probe_state / describe_key
force_unlock / scan_held(prefix)
```

Redis impl = direct delegation to existing manager methods. etcd impl uses **composite tokens `"{uuid}:{lease_id}"`** so a stateless server can ownership-check-delete and revoke without local handles; fence = transaction revision; describe reports ttl as unknown until per-key lease introspection lands.

## Status: foundation laid, service migration is the follow-up

Done:
- etcd composite-token scheme specified here (unlocks stateless release).
- Server-side `disarm()` pattern already proven — implementations must suppress Drop releases on grants.

Remaining (single focused PR):
1. Land `BackendOps` trait + Redis/Etcd impls in the server crate.
2. Switch `PalisadeService`, `SessionBook`, `WatchHub` to `Arc<dyn BackendOps>`.
3. `--backend redis|etcd` flag constructing the matching manager.
4. gRPC smoke test against live etcd.

## Why staged
The service refactor touches every handler plus three helper structs; doing it at the tail of a long session risks a broken build. The design above is complete enough that the migration is mechanical.
