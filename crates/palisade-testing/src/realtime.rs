//! Bridge from *real* recorded traffic to the invariant checker.
//!
//! `HistoryRecorder` captures wall-clock operations; this module converts
//! them per-key into checker events and validates the same invariants the
//! simulator enforces. With sane TTLs (holds far shorter than leases) the
//! strong signal is mutual exclusion of grants — exactly what a lock must
//! never get wrong.

use crate::checker::{Violation, check};
use crate::history::HistoryEntry;
use crate::sim::{Event, EventKind};

/// Validates all recorded operations for one key.
///
/// `ttl_ms` is the lease the clients operated with. Events are ordered by
/// completion time; releases are placed at their completion, which makes
/// hold windows slightly generous — conservative in the safe direction.
pub fn check_client_history(
    key: &str,
    entries: &[HistoryEntry],
    ttl_ms: u64,
) -> Result<(), Violation> {
    let mut events: Vec<Event> = Vec::with_capacity(entries.len());
    for e in entries.iter().filter(|e| e.key == key) {
        let kind = match e.op {
            crate::history::OpKind::TryAcquire if e.ok => EventKind::Grant,
            crate::history::OpKind::TryAcquire => EventKind::Deny,
            crate::history::OpKind::Release if e.ok => EventKind::ReleaseOk,
            crate::history::OpKind::Release => EventKind::ReleaseLost,
            // Extends don't change holder state; skip them.
            crate::history::OpKind::Extend => continue,
        };
        events.push(Event {
            t_ms: e.end_us / 1_000,
            worker: e.fence as usize, // fence doubles as the holder identity
            kind,
            fence: e.fence,
        });
    }
    events.sort_by_key(|ev| ev.t_ms);
    check(&events, ttl_ms)
}
