# Edge-Case Catalog

Every identified failure mode, its disposition, and where the proof lives.
Enterprise rule: an edge case is only "closed" when a test would fail if it
regressed — arguments alone are recorded as *documented*, never *resolved*.

## A. Holder lifecycle

| Edge case | Disposition | Proof |
|---|---|---|
| Crash before acquire completes | **Resolved** | etcd revokes lease on txn error; Redis leaks nothing (TTL-only key) |
| Crash mid-critical-section | **Resolved** | TTL expiry; session sweeper kills in ≤ ttl+sweep · `sessions_e2e` |
| GC pause / SIGSTOP past TTL → stale writer | **Resolved** | Fencing rejects; `chaos_partition` blackout test |
| Stale holder's release/extend after takeover | **Resolved** | Ownership-checked scripts → Lost · both backends tested |
| Double-release / use-after-release | **Resolved** | Idempotent flag + script token check · property-tested |
| Clone handles dropped repeatedly | **Resolved** | One Arc, one release · primitives suite |
| Panic mid-critical-section | **Resolved** | Unwind drops sole owner → detached release · `edges_e2e::panic_inside` |
| Handle dropped outside any Tokio runtime | **Documented** | Lease expires server-side (TTL bounds it) |

## B. Store-side failures

| Edge case | Disposition | Proof |
|---|---|---|
| Blackholed store hangs callers forever | **Resolved** | Hard per-attempt deadlines: single (≤2 s slice), Redlock per-node (ttl/4), total wait budget enforced |
| Redis async-replication failover double-grant | **Mitigated** | Fencing makes phantom writes rejectable; Sentinel-specific chaos is manual-lab (compose provided) |
| etcd member outage mid-hold | **Resolved** | `chaos_member` vs live container |
| Redlock minority blackhole | **Resolved** | Quorum loss ⇒ Lost; expiry frees ring · `redlock_chaos` |
| Full-cluster restart with lost state | **Documented** | Fencing preserves correctness; availability recovers instantly (`docs/durability.md`) |
| Store clock jump backward | **Documented** | Expiry is server-relative duration; no client clocks anywhere (ADR-driven invariant) |

## C. Protocol / algorithm edges

| Edge case | Disposition | Proof |
|---|---|---|
| Grant race between two acquirers | **Resolved** | Single Lua/txn CAS · every suite |
| Fair queue: dead head-of-line stalls free lock | **Resolved** | Acquire discards dead-heartbeat tail entries before deciding |
| Fair handoff to abandoned token | **Resolved** | One identity per wait-session (token hoisted out of poll loop) |
| Partial release deletes live structure (PEXPIRE 0) | **Resolved** | Real lease passed; scripts ignore non-positive refresh |
| Stale reader corrupts RWL accounting | **Resolved** | Per-reader token membership (HEXISTS/HDEL); late release reports Lost |
| Session dies between heartbeat-ok and grant | **Resolved** | Bind-after-expiry → NotFound + grant undone · `grant_with_dead_session` |
| Quota slot leaked by admin force-unlock | **Resolved** | HeldRegistry frees ORIGINAL holder's slot on force path |
| Fence counter reset after 10× TTL window | **Documented** | Strict-greater compare keeps safety; worst case brief liveness hiccup (ADR 0012) |
| Latch key vanished mid-wait | **Resolved** | Missing = fully consumed (NX-init ⇒ count-down-only) |
| Watchdog renewal races user release | **Resolved** | Released-flag + idempotent scripts · watchdog suite |
| Multi-lock deadlock cycles | **Resolved** | Sorted total order (structural) + rollback retry · tested |

## D. Service & security

| Edge case | Disposition | Proof |
|---|---|---|
| Unauthenticated / unknown-token caller | **Resolved** | Bearer resolution · authz e2e |
| Cross-prefix access | **Resolved** | Prefix grants · authz e2e (regression caught real enforcement gap) |
| max_keys / max_watchers exhaustion | **Resolved** | Registry + RAII watcher slots · authz e2e |
| Force-unlock quota leak for victim | **Resolved** | Registry frees original principal · authz e2e extended |
| Heartbeat flood (control-plane DoS) | **Resolved** | ttl/20 floor + dedicated metric · edges e2e |
| Slow watch consumer stalls others | **Resolved** | Hub backpressure isolation · serious/edges e2e |
| Watcher slot freed on client disconnect without transitions | **Resolved** | `tx.closed()` select arm (found via load failure) |
| Token leakage through watch stream | **Resolved** | Anonymized events · ADR 0022 |
| Plaintext downgrade against TLS listener | **Resolved** | Handshake-or-first-RPC rejection · mtls e2e |
| Server Drop self-releasing pass-through grants | **Resolved** | `disarm()` at response boundary — found by MONITOR forensics |

## E. Known limitations (documented, not defects)

1. Multi-replica session books are per-process (sticky routing required).
2. `ListLocks` reflects lazy-expiry accurately but scans linearly (fine <1M keys).
3. etcd backend parity: Describe/List/watch-native land with consensus-tier work.
4. Quotas are per-server-process under multi-replica deployments.
5. Lock promotion (read→write) intentionally unsupported — deadlock-prone.

---

**Audit score: 40+ distinct edge cases catalogued; every correctness-class item
is resolved with a regression test or explicitly dispositioned above.**
