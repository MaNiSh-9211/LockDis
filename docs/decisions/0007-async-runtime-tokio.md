# 0007. Async runtime: Tokio

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
A lock client spends its life waiting: on grants, queues, renewals, deadlines. The runtime shapes API ergonomics (cancellation), ecosystem compatibility, and simulation support.

## Options considered
1. Tokio
2. async-std
3. smol
4. Synchronous threads (std only)

## Decision
Tokio (full features), with cancellation-safety as an explicit API-design rule.

## Why this is the best option
- **Ecosystem gravity**: redis (tokio-comp), tonic, madsim, testcontainers-tokio, tracing — every dependency in our stack is Tokio-first. Anything else fights upstream.
- **Cancellation primitives**: `CancellationToken` (tokio-util) is exactly what watchdog tasks and queued waiters need to shut down cleanly — a correctness requirement here, not a nicety.
- **madsim compatibility**: deterministic simulation (ADR 0006) works by intercepting Tokio's timers/network; choosing another runtime forfeits that.
- **Maturity**: multi-threaded work-stealing scheduler, production-hardened at extreme scale.

## Why not the alternatives
- **async-std**: effectively in maintenance decline; ecosystem moved to Tokio.
- **smol**: elegant and light, but our dependencies pull Tokio anyway; two runtimes = bloat and interop friction.
- **Threads-only**: blocking waits in a lock service waste resources under contention and complicate fair-queue signaling; also incompatible with madsim's deterministic scheduling.

## Consequences
- All public APIs take/return Tokio futures; blocking wrappers (`block_on`) provided only in examples for scripts.
- Cancellation-safety review checklist for every public method (a dropped future must never corrupt lock state — e.g., release must complete or retry via a detached task).
