//! History recording: capture `(start, end, result)` for lock operations so
//! wire/library traffic can be checked for linearizability after the fact.
//!
//! The simulator records its own event log; this type is the equivalent
//! capture point for *real* backends — wrap any `LockManager` and every
//! acquire/release/extend lands here with wall-agnostic monotonic timing.

use std::sync::{Arc, Mutex};
use std::time::Instant;

/// What happened, in checker vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    /// Acquisition attempt; success = granted, failure = denied/timed out.
    TryAcquire,
    Release,
    Extend,
}

/// One completed operation.
#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub key: String,
    pub op: OpKind,
    pub ok: bool,
    /// Monotonic start, relative to recorder creation (microseconds).
    pub start_us: u64,
    /// Monotonic end, relative to recorder creation (microseconds).
    pub end_us: u64,
}

/// Thread-safe append-only history. Clone-cheap via `Arc` internals.
#[derive(Clone)]
pub struct HistoryRecorder {
    entries: Arc<Mutex<Vec<HistoryEntry>>>,
    origin: Arc<Instant>,
}

impl Default for HistoryRecorder {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            origin: Arc::new(Instant::now()),
        }
    }
}

impl HistoryRecorder {
    /// Starts a new recording timeline at now.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one completed operation using the current instant as end
    /// and `started_at` as its beginning.
    pub fn record(&self, key: &str, op: OpKind, ok: bool, started_at: Instant) {
        let end_us = self.origin.elapsed().as_micros() as u64;
        let start_us = started_at
            .saturating_duration_since(*self.origin)
            .as_micros() as u64;
        self.entries
            .lock()
            .expect("history mutex")
            .push(HistoryEntry {
                key: key.to_owned(),
                op,
                ok,
                start_us,
                end_us,
            });
    }

    /// Snapshot of everything recorded so far, in completion order.
    pub fn snapshot(&self) -> Vec<HistoryEntry> {
        self.entries.lock().expect("history mutex").clone()
    }
}
