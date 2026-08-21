# 0011. Release semantics: idempotent release + detached drop-release

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
What happens when a holder releases twice, or drops the handle without releasing (early return, panic, `?` propagation)? The answers define both API ergonomics and leak behavior.

## Options considered
1. Idempotent `release(&self)` + Drop spawns a detached best-effort release (chosen)
2. Consuming `release(self)` with compile-time double-release prevention
3. Drop does nothing; leases simply expire server-side
4. RAII-only (no explicit release method)

## Decision
`release()` takes `&self`, runs the ownership-checked script exactly once (guarded by an atomic flag), and is safe to call multiple times. Dropping the last clone of a handle marks it released and, if inside a Tokio runtime, spawns a detached task firing the same release script; outside a runtime, the lease is left to expire.

## Why this is the best option
- **Panic/`?` safety by default**: Rust code paths commonly unwind or early-return mid-critical-section; RAII cleanup means a crashed critical section doesn't block others for a full TTL.
- **Idempotence matches distributed reality**: the network can already deliver your release twice; a library API that punishes double-release just moves the problem into user code.
- **Detached (not awaited) drop-release**: `Drop` cannot be async; awaiting inside drop is impossible and blocking is forbidden. Fire-and-forget with the ownership-checked script is best-effort by construction — and the TTL bounds any miss.
- **`&self` keeps handles clone-friendly**: all clones share one lease and one release flag (`Arc<Shared>`), so clone-drop patterns can't double-free.

## Why not the alternatives
- **Consuming release**: fights common control flow (try-operator chains), forces `mem::forget`-style workarounds, and still can't cover panics.
- **Drop does nothing**: turns every forgotten release into a full-TTL stall — hostile default for users.
- **RAII-only**: explicit release is needed anyway to surface `Error::Lost` (expired-before-release) to callers who care; keeping both costs nothing.

## Consequences
- Detached release outcome is unobservable by design; users needing the result call `release()` explicitly before dropping.
- Dropping a handle outside any Tokio runtime leaves the lease to expire — documented in `LockHandle` docs.
- The atomic flag makes release-vs-drop races single-shot; the Lua token check remains the real safety authority.
