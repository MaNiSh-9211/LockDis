# 0023. Readiness & drain: readiness flag + health protocol, leases never die at shutdown

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Rolling deploys must not break lock holders. The server is stateless pass-through (ADR 0021), so "draining" means only: stop issuing *new* grants, let in-flight RPCs finish, exit.

## Options considered
1. Shared readiness flag gating grant RPCs + gRPC health reflection + signal-driven grace window (chosen)
2. Immediate hard shutdown on SIGTERM
3. Server-side session tracking so shutdown can release all held locks

## Decision
`PalisadeService` carries an `Arc<AtomicBool>` readiness flag (exposed via `set_ready`/`ready_handle`). On SIGTERM/SIGINT: flag flips false → `TryLock`/`TryLockFor` return `Unavailable`; the tonic-health reporter flips to `NOT_SERVING` so k8s stops routing; the process waits out a configurable grace (`--drain-grace-secs`, default 10) before closing the listener. Unlock/Extend/Watch stay live throughout — holders are never penalized for our deploy.

## Why this is the best option
- **k8s-native sequencing**: flipping health to NOT_SERVING before the grace window means endpoints-controller removal races ahead of actual shutdown, so most traffic has already moved on when grants stop.
- **Matches the ownership model**: since clients renew their own leases, a server restart is invisible to holders as long as Redis stays up. Releasing locks on shutdown would be actively wrong — other processes may legitimately depend on those leases.
- **One flag, two consumers**: both the RPC guard and the health reporter read/write through the same lifecycle step, so they cannot disagree mid-rollout.

## Why not the alternatives
- **Hard shutdown**: in-flight `TryLockFor` waiters get connection-reset noise and retry storms; a 10-second courtesy costs nothing.
- **Release-on-shutdown**: violates least-surprise for every client holding a lease across our rolling update; also unimplementable honestly once servers scale horizontally.
- **Full leader-election / proxy-based draining**: heavy machinery for a stateless service; unnecessary until multi-region concerns exist.

## Consequences
- Drain grace bounds how long a pod lingers; load-balancer propagation is usually faster but not guaranteed — operators tune `--drain-grace-secs` per environment.
- Health service covers the whole process, not per-backend Redis connectivity; a Redis-down server still reports SERVING (it fails requests fast with internal errors). A dedicated redis-readiness check is future work if operators ask.
