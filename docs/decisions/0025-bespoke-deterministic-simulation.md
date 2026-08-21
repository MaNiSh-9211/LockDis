# 0025. Deterministic simulation: bespoke seeded scheduler over a virtual-time store

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Layer 2 of the correctness strategy (ADR 0004) needs rare interleavings — pause past TTL, release-after-expiry, grant-while-held — reproducibly from a seed. The original plan named madsim; implementation forced an honest re-evaluation.

## Options considered
1. Bespoke single-threaded event loop over virtual time + in-memory mirror of the store semantics (chosen)
2. madsim (deterministic Tokio simulator)
3. Random stress tests with sleep-based timing
4. Full model checking (stateright) over the algorithm

## Decision
`palisade-testing::sim` implements the lock-store semantics (CAS grant + TTL, ownership-checked release, monotonic fence counter, fence-checking resource) as pure state advanced by a seeded xorshift-driven scheduler. Workers are explicit state machines stepped by earliest-wake-time order; faults are first-class scenario flags (`pause_probability_pct`, plus `broken_cas`/`broken_fencing` bug injectors). Histories feed the invariant checker.

## Why this is the best option
- **Right layer of fidelity**: our dangerous interleavings live in *algorithm-time* (who acts before the lease dies), not socket-bytes-time. A virtual clock advances directly to the interesting instant instead of simulating TCP to get there.
- **Total determinism, trivially**: one thread, one RNG, stable tie-breaking ⇒ same seed replays bit-for-bit on any platform, forever. CI runs hundreds of seeds in milliseconds.
- **The checker is validated by mutation**: `broken_cas` and `broken_fencing` deliberately violate safety; tests assert the checker catches ≥90% of injected bugs across seeds. A validator that has never failed proves nothing.
- **No runtime capture**: madsim intercepts Tokio internals; our Redis client uses real sockets and cannot run under it without a simulated RESP server — which would simulate the wire, the layer we explicitly defer to chaos testing.

## Why not the alternatives
- **madsim**: excellent for Tokio-service topologies (request routing, node crashes); wrong fidelity target for store-semantics validation, and incompatible with the production Redis client as-is. Documented as the tool of choice if we ever simulate the gRPC service topology itself.
- **Random stress + sleeps**: nondeterministic heisenbugs; the exact anti-goal (ADR 0004).
- **Model checking**: exhaustive but state-explosion-prone beyond toy scenarios; the seeded fuzzer explores far more realistic schedules per CPU-minute. Stateright remains a stretch goal for protocol-level proofs.

## Consequences
- Simulation validates semantics, not the actual Lua or the actual network — Layers 1 (property vs real Redis), 3 (checker on recorded histories), and 4 (chaos) close those gaps by design.
- The store mirror must track the Lua scripts when they change; enforced socially via code-review checklist pointing at this ADR.
- Release gate (Phase 6): 200 seeds × zero violations + mutation-catch thresholds ≥ 45/50 per bug class — currently green.
