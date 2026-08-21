# 0016. Read-write lock: reader-preferring mode flag, no promotions

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Read-mostly workloads need shared access; writes need exclusivity. Design axes: preference policy (reader vs writer), structure, and whether read→write upgrades exist.

## Options considered
1. HASH with `mode` field; reader-preferring; no in-place upgrade (chosen)
2. Writer-preferring with queued writers
3. Ticket/sequence-based fair RWL
4. Upgradeable read guards (read→write in place)

## Decision
One HASH holds `mode` (`r`|`w`) plus `readers` count or writer `owner`. Readers join freely while mode is `r`; writers require the structure absent or owned by themselves (write re-entry refreshes). No read→write promotion: upgrading requires release + reacquire.

## Why this is the best option
- **Reader-preferring matches the common case** (caches, config, registry reads) and keeps the reader path a single O(1) script with no queue inspection.
- **No promotion is a safety decision, not a limitation**: two concurrent readers both "upgrading" is the classic deadlock; resolving it needs writer queues and priority inheritance machinery that belong in a later fair-RWL, not smuggled into v1.
- **Single-hash structure** keeps TTL handling trivial (one PEXPIRE covers all readers) and makes the Lua scripts auditable at a glance.

## Why not the alternatives
- **Writer-preferring**: prevents writer starvation but adds a writer-waiting set that every read acquire must consult — measurable latency on the hot path for a policy most users don't need yet.
- **Ticket-based fair RWL**: fairest, heaviest; revisit if starvation shows up in practice (metrics will show it).
- **Upgradeable guards**: deadlock-prone as described; explicitly documented as unsupported (PLAN §4.6).

## Consequences
- Sustained read pressure can starve writers indefinitely; documented, and detectable via grant-latency metrics on the write path.
- Reader leases are collective: any reader's acquire refreshes the whole structure's TTL, so one long reader can extend others' window — bounded by per-reader watchdog use later.
