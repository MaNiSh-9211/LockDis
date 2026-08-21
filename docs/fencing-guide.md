# Fencing Tokens: How to Actually Use Them

The lock guarantees mutual exclusion **only while the lease is alive and Redis
is a single failure domain**. Three situations can still produce two holders
who both *believe* they hold the lock:

1. **Pause past TTL** — GC stop, SIGSTOP, VM migration, laptop sleep.
   The lease expires; another owner acquires; the paused holder resumes.
2. **Failover with async replication** — the promoted replica never saw the
   original grant (or release), so it grants again.
3. **Network partition + retry** — a timed-out acquire that actually succeeded
   resurfaces later.

No TTL-based lock can prevent these at the lock layer. What prevents *damage*
is the fencing token every Palisade grant returns.

## The rule

> The protected resource accepts an operation only if its fencing token is
> strictly newer than the last token it accepted. Everything else is rejected.

```
holder A: fence #7 ── pauses ──────────────► resumes, writes with #7 ──► REJECTED
holder B:                    acquires, #8 ── writes with #8 ───────────► ACCEPTED
```

## Pattern: conditional write (Postgres example)

```sql
-- last_fence stored per resource row
UPDATE orders
SET status = 'shipped', last_fence = $fence
WHERE id = $order_id AND $fence > last_fence;
-- 0 rows updated ⇒ your lease was stale; abort the critical section.
```

## Pattern: with the watchdog

```rust
let h = mgr.try_lock_with(&key, &LockOptions::default()
        .with_ttl(Duration::from_secs(5))
        .with_watchdog(true))
    .await?;

tokio::select! {
    r = do_protected_work(h.fence()) => r,
    _ = h.until_lost() => {
        // Lease expired or was revoked mid-flight. Abort everything the
        // critical section hasn't committed; anything already committed
        // will be rejected downstream by the fence check.
        return Err(Error::Lost { key: key.into(), fence: h.fence().value() });
    }
}
```

## What Palisade guarantees — and what it cannot

| Guarantee | Mechanism |
|---|---|
| Only one live holder per key per Redis instance | Lua-guarded grant/release |
| Stale holders get `Lost`, not silence | ownership-checked extend/release + watchdog poisoning |
| Fence tokens strictly increase per grant | atomic `INCR` in the grant script |
| Counter outlives its lock | fence TTL = 10× lease (ADR 0012) |
| **Downstream rejection of stale writes** | **your resource checks the token — we ship the patterns, you apply them** |

The last row is the honest boundary: a lock library that claims to fix the
paused-holder problem without cooperation from the storage being protected is
lying. Palisade makes the token impossible to lose track of (`handle.fence()`)
and cheap to check; the check itself lives where the data lives.
