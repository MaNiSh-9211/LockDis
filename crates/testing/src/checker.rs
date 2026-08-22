//! Invariant checking over simulated histories.
//!
//! Tailored to leased-mutex semantics (full general linearizability is
//! overkill for a CAS+TTL store): a token *validly holds* from its grant
//! until the earlier of its release-invocation or lease expiry. Violations:
//! two valid holders at one instant, releases of unheld tokens, lost-while-
//! active confusion, and any accepted stale write.

use crate::sim::{Event, EventKind};

/// A safety violation with enough context to debug it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub at_ms: u64,
    pub description: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "violation @{}ms: {}", self.at_ms, self.description)
    }
}

struct ActiveHold {
    expires_ms: u64,
}

/// Checks one single-key history. Returns the first violation found.
pub fn check(history: &[Event], ttl_ms: u64) -> Result<(), Violation> {
    let mut active: Vec<(usize, ActiveHold)> = Vec::new();
    let mut last_accepted_fence: u64 = 0;
    let mut released_seen: Vec<(usize, u64)> = Vec::new();

    for e in history {
        // Expire anything whose lease has passed by this event's time.
        active.retain(|(w, h)| {
            if h.expires_ms <= e.t_ms {
                released_seen.push((*w, e.t_ms));
                false
            } else {
                true
            }
        });

        match e.kind {
            EventKind::Grant => {
                if let Some((other, _)) = active.first() {
                    return Err(Violation {
                        at_ms: e.t_ms,
                        description: format!(
                            "worker {} granted while worker {} still validly holds",
                            e.worker, other
                        ),
                    });
                }
                active.push((
                    e.worker,
                    ActiveHold {
                        expires_ms: e.t_ms + ttl_ms,
                    },
                ));
                if e.fence == 0 {
                    return Err(Violation {
                        at_ms: e.t_ms,
                        description: "grant without fencing token".into(),
                    });
                }
            }
            EventKind::ReleaseOk => match active.iter().position(|(w, _)| *w == e.worker) {
                Some(i) => {
                    active.remove(i);
                    released_seen.push((e.worker, e.t_ms));
                }
                None => {
                    return Err(Violation {
                        at_ms: e.t_ms,
                        description: format!(
                            "worker {} released a lock it does not validly hold",
                            e.worker
                        ),
                    });
                }
            },
            EventKind::ReleaseLost => {
                if let Some((_, h)) = active.iter().find(|(w, _)| *w == e.worker) {
                    if e.t_ms < h.expires_ms {
                        return Err(Violation {
                            at_ms: e.t_ms,
                            description: format!(
                                "worker {} told lease lost while still inside validity window",
                                e.worker
                            ),
                        });
                    }
                }
            }
            EventKind::WriteAccepted => {
                if !active.iter().any(|(w, _)| *w == e.worker) {
                    return Err(Violation {
                        at_ms: e.t_ms,
                        description: format!(
                            "stale write ACCEPTED from worker {} who holds nothing",
                            e.worker
                        ),
                    });
                }
                if e.fence <= last_accepted_fence {
                    return Err(Violation {
                        at_ms: e.t_ms,
                        description: format!(
                            "fence {} accepted but not newer than last accepted {}",
                            e.fence, last_accepted_fence
                        ),
                    });
                }
                last_accepted_fence = e.fence;
            }
            EventKind::WriteRejected | EventKind::Deny => {}
        }
    }
    Ok(())
}
