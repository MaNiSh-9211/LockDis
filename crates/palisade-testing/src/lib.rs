//! Palisade correctness harness.
//!
//! Layered strategy (see ADR 0004):
//! 1. property tests (proptest) against a real backend,
//! 2. deterministic simulation with fault injection ([`sim`]),
//! 3. invariant checking over recorded histories ([`checker`]),
//! 4. real-cluster chaos via toxiproxy (planned).
//!
//! Layers 2–3 run here as pure computation: every scenario replays
//! bit-for-bit from its seed, so failures are reproducible forever.

pub mod checker;
pub mod history;
pub mod sim;

pub use checker::{Violation, check};
pub use history::HistoryRecorder;
pub use sim::{Event, EventKind, Scenario, simulate};
