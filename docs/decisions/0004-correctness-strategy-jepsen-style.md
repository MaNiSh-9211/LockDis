# 0004. Correctness strategy: Jepsen-style layered testing

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
A lock's entire value proposition is its safety invariant ("never two holders"). Most open-source lock libraries ship unit tests that cannot detect the failures that matter (pauses, partitions, failover). We must decide how far to go on correctness evidence.

## Options considered
1. Layered Jepsen-style strategy (property tests → deterministic simulation + linearizability checking → real-cluster chaos)
2. Standard tests (unit + integration against real Redis via Docker)
3. Minimal smoke tests

## Decision
Layered Jepsen-style strategy, gating releases on zero linearizability violations across a seed corpus.

## Why this is the best option
- **Each layer catches what the others can't**:
  - property tests → logic bugs fast, in seconds;
  - madsim deterministic simulation → rare interleavings (pause > TTL, message loss) *reproducibly*, from a seed, in CI;
  - linearizability checker → converts "we ran chaos and nothing broke" into a *proof* over recorded histories;
  - real-cluster chaos (toxiproxy, Sentinel failover, SIGSTOP) → validates assumptions the simulator makes about real Redis/network behavior.
- **CI-friendly**: Jepsen-class rigor without a Jepsen-class lab — simulation runs on a laptop; chaos runs nightly in Docker Compose.
- **Reproducibility**: seeded schedules turn heisenbugs into ticket-reproducible failures — the single biggest practical gap in distributed-systems debugging.

## Why not the alternatives
- **Standard tests only**: integration tests pass precisely because they don't pause processes or partition networks — they validate the happy path that was never in doubt.
- **Minimal**: shipping an unverified mutual-exclusion claim would be malpractice for this specific product category.
- **Full external Jepsen audit**: gold standard but costs a specialist + weeks per run; our layered approach approximates it continuously instead of episodically. (A future external audit remains a stretch goal.)

## Consequences
- `palisade-testing` crate is a first-class deliverable (~30% of total effort budgeted).
- Need history recording built into the client from Phase 1 (retrofitting is painful).
- Release gate defined: ≥100 seeds × 10⁶ simulated ops, zero violations, plus nightly chaos suite green.
