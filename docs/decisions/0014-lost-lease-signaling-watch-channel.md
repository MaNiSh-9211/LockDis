# 0014. Lost-lease signaling: watch channel broadcast

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Once the watchdog can detect lease loss (ADR 0013), holders need a way to observe it: a fast boolean check and an async wait for critical sections that should abort mid-flight.

## Options considered
1. `tokio::sync::watch<bool>` broadcast + atomic fast path (chosen)
2. `tokio::sync::Notify`
3. `tokio_util::sync::CancellationToken`
4. Polling `is_lost()` only

## Decision
`HandleShared` carries an `AtomicBool` (`poisoned`) for lock-free `is_lost()` and a `watch::Sender<bool>` whose value flips to `true` on loss. `until_lost()` subscribes, checks `borrow_and_update()`, then awaits `changed()` — resolving immediately if loss already happened.

## Why this is the best option
- **No lost-wakeup race**: `Notify` wakes only callers already waiting unless a permit was stored, and a stored permit collapses multiple wakeups into one — subtle to get right for N concurrent critical sections sharing clones of a handle. `watch` is level-triggered: subscribe after the fact and still see `true`.
- **Multi-waker by construction**: any number of tasks can hold receivers; all resolve.
- **Zero extra dependencies**: `CancellationToken` means adding tokio-util for one type; `watch` ships with Tokio, which we already require (ADR 0007).
- **Composability**: `until_lost()` returning a plain future slots into `tokio::select!` against the critical section — the exact usage the poisoning concept exists to enable.

## Why not the alternatives
- **Notify**: permit semantics make it edge-triggered; late subscribers miss the event entirely.
- **CancellationToken**: ergonomically nice but pulls in tokio-util and its child-token machinery we don't need; can migrate later behind the same trait method without breaking users.
- **Polling-only**: forces every user to write sleep-check loops; fine as an *optimization* inside `is_lost`, wrong as the only interface.

## Consequences
- `until_lost()` resolves silently if the sender is dropped (last handle gone) — there is no lease left to lose; documented on the trait method.
- Voluntary `release()` does NOT flip the signal: `is_lost` means involuntary loss (expired/revoked), keeping the two concepts distinct for metrics and alerting.
- The gRPC `Watch` stream (Phase 5) will forward this signal server-side; the client-side shape is already correct.
