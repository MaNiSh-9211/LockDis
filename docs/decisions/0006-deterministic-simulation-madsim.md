# 0006. Deterministic simulation via madsim

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Layer 2 of our correctness strategy (ADR 0004) needs to explore rare interleavings — process pauses longer than TTLs, dropped/delayed/reordered messages, clock jumps — reproducibly, on commodity CI hardware.

## Options considered
1. madsim (deterministic simulator for Tokio ecosystems)
2. Real-cluster chaos only (toxiproxy + Docker)
3. External Jepsen
4. Random-stress tests without determinism

## Decision
madsim as the backbone of Layer 2: run client tasks against a simulated Redis under seeded schedulers with fault injection; record histories for the linearizability checker.

## Why this is the best option
- **Determinism = reproducibility**: a failing seed replays bit-for-bit in CI and locally. This converts the worst class of bug (heisenbug) into a normal unit-test failure — no other approach in the Rust ecosystem offers this.
- **Explores what chaos can't reach**: real-network chaos samples the interleaving space randomly and slowly; simulation can systematically push pause durations past TTL boundaries exactly where double-grant bugs live.
- **Zero infra**: thousands of simulated nodes/failures on one core, seconds per run — fits the ≥100 seeds × 10⁶ ops release gate in every PR.
- **Tokio-native**: intercepts Tokio timers/network/spawn, so production code runs unmodified (ADR 0007 synergy).

## Why not the alternatives
- **Chaos-only**: can't reproduce failures deterministically, can't guarantee coverage of boundary conditions, too slow for per-PR gating (nightly at best).
- **External Jepsen**: gold-standard brand, but requires specialist setup, VM fleets, and episodic runs; madsim approximates the methodology continuously. (External audit stays a stretch goal.)
- **Random stress without seeds**: finds bugs once, then loses them — "couldn't reproduce" is how lock bugs ship to production.

## Consequences
- Simulation fidelity ≠ reality: madsim models the network/timers, not Redis internals — hence Layer 4 real-cluster chaos remains mandatory to validate assumptions.
- Client code must avoid wall-clock (`SystemTime`) decisions and use injectable time, or simulation results lie — codified as a lint/review rule.
- History recording (op, start, end, result) becomes part of the public test API from Phase 1 onward.
