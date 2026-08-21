# 0015. Reentrant lock: hash-based hold count, caller-supplied owner

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Recursive call stacks and layered APIs need the same logical owner to reacquire a lock it already holds, with nesting tracked and the lock freed only at zero.

## Options considered
1. HASH `{owner, count}` keyed by caller-supplied `OwnerId` (chosen)
2. Per-process implicit owner id auto-derived from machine/process identity
3. String value + side-car counter key
4. Session tokens issued by a central service

## Decision
The lock is a HASH holding the owner token and a hold count. The caller supplies the `OwnerId` (share it across the stack that needs reentrancy); same-token acquisition increments the count and issues a fresh fence token; release decrements, deleting at zero. Each handle = one hold; clones share a hold; `release_all` drops every hold at once.

## Why this is the best option
- **Explicit ownership beats magic ownership**: deriving identity from host/pid breaks the moment two tasks in one process contend (they'd wrongly share reentrancy) or a container restarts (leases orphaned). Caller-supplied ids make scope visible in code review.
- **Count-in-the-hash is atomic with ownership checks** — one script decides grant/reentry/deny, so no interleaving can double-count or free early.
- **Fresh fence per reentry** keeps the token stream monotonic even across nested acquisitions, so downstream fence checks stay meaningful.
- **Handle=hold maps to scoped cleanup**: RAII drop decrements exactly what that scope acquired; `release_all` covers "abort everything" paths.

## Why not the alternatives
- **Implicit process owner**: convenient until it isn't — silent cross-task sharing is a correctness bug disguised as ergonomics.
- **Value + side-car counter**: two keys to keep consistent under expiry; the hash makes count+owner expire as one unit.
- **Central session service**: reintroduces the server dependency the library deliberately avoids (ADR 0003).

## Consequences
- Reentrancy is per-token, not per-thread/task: passing the OwnerId across tasks intentionally shares holds — documented.
- TTL refreshes on every reentry and partial release; watchdog integration inherits the single-instance design unchanged.
