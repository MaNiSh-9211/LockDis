# 0013. Watchdog auto-renewal: ttl/3 cadence, weak-ref task

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Long critical sections outlive their lease unless someone renews it. Manual `extend` calls push correctness onto every user. Redisson's "watchdog" popularized automatic renewal; we need our own with explicit failure semantics.

## Options considered
1. Background renewal task at `ttl/3` cadence, weak reference to handle state (chosen)
2. Renewal piggybacked on user API calls ("lazy renewal")
3. No watchdog; users compose `extend` manually
4. Server-side renewal (lock service owns leases)

## Decision
When enabled (`RedisConfig::with_watchdog` default or `LockOptions::with_watchdog` override), acquisition spawns a Tokio task that sleeps `ttl/3`, runs the ownership-checked extend script, and repeats. The task holds only a `Weak` to the shared handle state.

Failure policy:
- extend says **not-owner** → poison immediately (ownership loss is definitive);
- backend error → tolerate up to 2 consecutive failures, then poison;
- released flag observed → exit silently.

## Why this is the best option
- **ttl/3 cadence** tolerates two consecutive missed renewals before the lease is even at risk — the same margin Redisson validated in production.
- **Weak reference breaks the ownership cycle**: a strong ref would keep `HandleShared` alive forever, so the Drop-detached release (ADR 0011) could never fire and leases would leak until TTL. With `Weak`, the task self-terminates when the last handle drops.
- **Definitive-vs-transient distinction**: retrying after "you are not the owner" would be safety theater — the answer cannot change. Retrying network errors is correct because they carry no information.
- **Poisoning over silent death**: a holder that keeps working after losing its lease is exactly the bug fencing exists to catch; surfacing it via `is_lost`/`until_lost` lets critical sections abort early instead of discovering it at write time.

## Why not the alternatives
- **Lazy renewal**: only helps APIs that happen to get called mid-section; silent failure modes and unpredictable timing.
- **Manual-only**: correct but hostile; every user reimplements the loop and gets the failure policy wrong.
- **Server-side**: right answer for the gRPC service long-term (Phase 5 server renews on behalf of connected clients), but the library must stand alone without a server.

## Consequences
- Watchdog cadence uses the grant-time TTL; a manual `extend` with a different TTL does not rescale the cadence (documented; revisit if needed).
- After the last handle drop, the task may linger up to one sleep interval before observing the dead `Weak` — harmless.
- New metrics: `palisade_renewals_total`, `palisade_renewal_failures_total`.
