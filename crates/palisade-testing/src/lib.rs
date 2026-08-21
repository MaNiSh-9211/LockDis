//! Palisade correctness harness.
//!
//! Layered strategy (see ADR 0004):
//! 1. property tests (proptest),
//! 2. deterministic simulation with fault injection (madsim),
//! 3. linearizability checking over recorded histories,
//! 4. real-cluster chaos via toxiproxy + testcontainers.
//!
//! Status: Phase 6 — history recording lands in Phase 1.
