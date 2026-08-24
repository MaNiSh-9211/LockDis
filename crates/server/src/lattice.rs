//! INV-5 · The Lock Lattice: Renewals-as-Sensors Predictive Backpressure.
//!
//! Every lease renewal is a free latency+error probe from a live dependent
//! workload. This module aggregates those probes into a single Store
//! Pressure Index (SPI ∈ [0,100]) using exponential moving averages over
//! three signals:
//!
//! - **renewal latency** (p99-ish via max-of-window EWMA)
//! - **renewal failure ratio** (errors / attempts in window)
//! - **acquire contention** (denied / attempted)
//!
//! SPI thresholds drive graduated degradation:
//!   0–39 NORMAL · 40–69 ELEVATED · 70–89 CRITICAL · 90–100 SIEGE
//!
//! Consumers (gateway, SDK watchdogs) poll the exported gauge or call the
//! `/healthz` endpoint on the metrics port. No push infrastructure needed.

use std::time::{Duration, Instant};

/// Exponential smoothing factor for latency/failure EWMAs (higher = faster).
const ALPHA: f64 = 0.3;
/// Decay toward zero each tick so pressure fades after incidents resolve.

#[derive(Clone, Debug)]
pub struct StorePressureIndex {
    /// EWMA of renewal/acquire latency in microseconds.
    latency_us: f64,
    /// EWMA of failure ratio [0,1].
    failure_ratio: f64,
    /// EWMA of contention ratio (denied/attempts) [0,1].
    contention_ratio: f64,
    /// Composite index [0,100].
    spi: f64,
    last_tick: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    Normal,
    Elevated,
    Critical,
    Siege,
}

impl Tier {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::Elevated => 1,
            Self::Critical => 2,
            Self::Siege => 3,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Elevated => "ELEVATED",
            Self::Critical => "CRITICAL",
            Self::Siege => "SIEGE",
        }
    }
}

impl Default for StorePressureIndex {
    fn default() -> Self {
        Self {
            latency_us: 500.0,
            failure_ratio: 0.0,
            contention_ratio: 0.0,
            spi: 0.0,
            last_tick: Instant::now(),
        }
    }
}

impl StorePressureIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one observation. Called by the server on every acquire/renewal.
    pub fn observe(&mut self, latency: Duration, ok: bool, denied: bool) {
        let us = latency.as_micros() as f64;
        self.latency_us += ALPHA * (us - self.latency_us);
        let f = if ok { 0.0 } else { 1.0 };
        self.failure_ratio += ALPHA * (f - self.failure_ratio);
        let d = if denied { 1.0 } else { 0.0 };
        self.contention_ratio += ALPHA * (d - self.contention_ratio);
    }

    /// Recomputes the composite SPI. Call periodically (e.g., per scrape).
    pub fn tick(&mut self) -> f64 {
        // Normalize latency against a 10ms "healthy" baseline (log scale).
        let lat_norm = ((self.latency_us / 10_000.0).ln().max(0.0) / 5.0).min(1.0);

        let mut score = lat_norm * 35.0 + self.failure_ratio * 55.0 + self.contention_ratio * 10.0;
        // A sustained near-100% failure rate IS a siege regardless of latency.
        if self.failure_ratio > 0.8 {
            score = score.max(95.0);
        }
        self.spi = score.clamp(0.0, 100.0);
        self.last_tick = Instant::now();
        self.spi
    }

    /// Current SPI value [0,100].
    pub fn value(&self) -> f64 {
        self.spi
    }

    /// Current degradation tier.
    pub fn tier(&self) -> Tier {
        if self.spi >= 90.0 {
            Tier::Siege
        } else if self.spi >= 70.0 {
            Tier::Critical
        } else if self.spi >= 40.0 {
            Tier::Elevated
        } else {
            Tier::Normal
        }
    }

    /// Recommended watchdog multiplier (widens under pressure to shed load).
    pub fn recommended_cadence_multiplier(&self) -> f64 {
        match self.tier() {
            Tier::Normal => 1.0,
            Tier::Elevated => 1.5,
            Tier::Critical => 2.0,
            Tier::Siege => 3.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_operations_stay_normal() {
        let mut spi = StorePressureIndex::new();
        for _ in 0..50 {
            spi.observe(Duration::from_millis(2), true, false);
            spi.tick();
        }
        assert_eq!(spi.tier(), Tier::Normal);
        assert!(spi.value() < 20.0);
    }

    #[test]
    fn sustained_failures_escalate_to_siege() {
        let mut spi = StorePressureIndex::new();
        // Simulate store going down: all ops fail fast.
        for _ in 0..200 {
            spi.observe(Duration::from_millis(1), false, false);
            spi.tick();
        }
        assert_eq!(spi.tier(), Tier::Siege);
        assert!(spi.value() > 90.0);
    }

    #[test]
    fn contention_alone_elevates_but_doesnt_siege() {
        let mut spi = StorePressureIndex::new();
        for _ in 0..200 {
            spi.observe(Duration::from_millis(2), true, true); // denied = contended
            spi.tick();
        }
        // Contention contributes to pressure but doesn't escalate alone;
        // denied acquires mean the lock is WORKING as intended.
        assert!(
            spi.value() > 5.0,
            "contention should register some pressure"
        );
        assert!(
            spi.tier() < Tier::Critical,
            "pure contention shouldn't reach siege"
        );
    }

    #[test]
    fn recovery_decays_back_to_normal() {
        let mut spi = StorePressureIndex::new();
        for _ in 0..100 {
            spi.observe(Duration::from_millis(1), false, false);
            spi.tick();
        }
        assert_eq!(spi.tier(), Tier::Siege);
        // Store recovers: all ops succeed quickly.
        for _ in 0..300 {
            spi.observe(Duration::from_millis(1), true, false);
            spi.tick();
        }
        assert_eq!(spi.tier(), Tier::Normal, "should decay back to normal");
    }

    #[test]
    fn cadence_multiplier_increases_under_pressure() {
        let mut spi = StorePressureIndex::new();
        assert!((spi.recommended_cadence_multiplier() - 1.0).abs() < f64::EPSILON);
        for _ in 0..200 {
            spi.observe(Duration::from_millis(1), false, false);
            spi.tick();
        }
        assert!(spi.recommended_cadence_multiplier() > 1.0);
    }
}
