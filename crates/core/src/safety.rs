//! INV-2 · The Safety Policy Spectrum.
//!
//! Distributed locks trade staleness-risk against availability when renewal
//! becomes unreliable. Most libraries hardcode one point on that frontier.
//! Palisade makes it an explicit, documented choice:
//!
//! - [`SafetyPolicy::Cowardly`] — the instant a renewal *errors*, the holder
//!   surrenders: releases what it can and poisons itself. Zero stale-hold
//!   window; leadership may flap during store instability.
//! - [`SafetyPolicy::Balanced`] — tolerate a bounded number of transient
//!   failures before surrendering (default; matches Redisson's spirit).
//! - [`SafetyPolicy::Aggressive`] — keep retrying until the lease actually
//!   dies server-side; maximum liveness, longest stale-detection window.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SafetyPolicy {
    /// Surrender on first renewal error. Safest.
    Cowardly,
    /// Tolerate a bounded number of transient errors (default).
    #[default]
    Balanced,
    /// Only definitive not-owner answers poison; transient errors retry
    /// until the server-side lease expires.
    Aggressive,
}

impl SafetyPolicy {
    /// Max consecutive transient renewal failures tolerated before poison.
    pub fn max_transient_failures(self) -> u32 {
        match self {
            Self::Cowardly => 0,
            Self::Balanced => 2,
            Self::Aggressive => u32::MAX,
        }
    }
}
