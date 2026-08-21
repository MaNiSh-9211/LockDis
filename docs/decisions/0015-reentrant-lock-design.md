# 0015. Reentrant locks: hash-based hold count, caller-supplied owner

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Recursive call stacks and layered APIs need the same owner to re-acquire a lock it already holds without deadlocking. The owner identity must be stable across acquisitions.

## Options considered
1. Redis HASH (`owner`, `count`) with caller-supplied `OwnerId` (chosen)
2. Thread-local/async-context owner auto-detection
3. Plain mutex + client-side reentrancy map

## Decision
`try_lock_reentrant(key, owner)` stores `{owner, count}` in a hash; same-token acquisition increments the count and issues a fresh fence token. Each handle is one hold; `release_one` decrements (delete at zero), `release_all` drops everything. Clones of a handle share one hold.

## Why this is the best option
- **Explicit ownership beats magic**: auto-detecting "same task" across an async runtime is unreliable (tasks migrate workers) and hides the distributed reality that ownership is a claim you present, not ambient state.
- **Fence token per reentry**: nested critical sections each get a strictly newer token, so the innermost scope's writes are the ones downstream accepts — consistent with ADR 0005.
- **Count-in-hash is atomic**: Lua guards every transition, so partial-release crashes degrade to TTL expiry, never to a stuck positive count with zero holders... unless a holder dies mid-nesting, which the TTL bounds exactly like the plain mutex.

## Why not the alternatives
- **Auto-detected owners**: impossible to do correctly across processes; within one process it papers over the fact that "who holds this" must be answerable by the store alone.
- **Client-side maps**: a crash loses the map but not the lock — leaked until TTL with no way for another instance to distinguish leak from hold.

## Consequences
- Callers must thread one `OwnerId` through their stack to get reentrancy — documented in the type docs.
- `release_one` on an expired lease returns `Lost`; `release_all` is the escape hatch for unwinding complex paths.
