//! Ownership registry for quota integrity (EDGE_CASES.md #3).
//!
//! Tracks which principal holds which key so that:
//! - `max_keys` counts reflect reality across explicit unlock AND admin
//!   force-unlock (the original holder's slot is freed, not leaked),
//! - quota checks are a single authoritative lookup.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct HeldRegistry {
    /// key -> principal name
    holders: Mutex<HashMap<String, String>>,
    /// principal name -> live held count
    counts: Mutex<HashMap<String, usize>>,
}

impl HeldRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically registers a successful grant. Returns false when the
    /// principal is at its `max_keys` ceiling (caller must reject + undo).
    pub fn try_acquire(&self, key: &str, principal: &str, max_keys: u32) -> bool {
        let mut holders = self.holders.lock().expect("registry");
        if holders.contains_key(key) {
            return true; // already tracked; be permissive
        }
        let max = max_keys as usize;
        if max > 0 {
            let cur = self
                .counts
                .lock()
                .expect("counts")
                .get(principal)
                .copied()
                .unwrap_or(0);
            if cur >= max {
                return false;
            }
        }
        holders.insert(key.to_owned(), principal.to_owned());
        *self
            .counts
            .lock()
            .expect("counts")
            .entry(principal.to_owned())
            .or_insert(0) += 1;
        true
    }

    /// Frees the slot under `key`; returns the owning principal if known.
    pub fn release(&self, key: &str) -> Option<String> {
        let owner = self.holders.lock().expect("registry").remove(key)?;
        let mut counts = self.counts.lock().expect("counts");
        let n = counts.get_mut(&owner)?;
        *n = n.saturating_sub(1);
        Some(owner)
    }
}
