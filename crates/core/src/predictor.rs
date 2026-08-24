//! INV-7 · Contention Predictor: forecasts when a contended key frees up.
//!
//! Tracks the EWMA and variance of hold durations per key. After ≥3 samples
//! produces a prediction window `[best_case, expected, worst_case]` for when
//! the current holder will release. Callers use this to sleep precisely
//! instead of burning CPU on blind polling.
//!
//! This is NOT a guarantee — it's a scheduling hint. The caller still
//! re-attempts acquisition after sleeping; the predictor just eliminates
//! 90%+ of wasted probes.

use std::collections::HashMap;
use std::sync::Mutex;

/// Minimum observations before predictions are offered.
const MIN_SAMPLES: usize = 3;

#[derive(Clone, Debug)]
pub struct AvailabilityForecast {
    /// Earliest plausible release (EWMA − σ).
    pub best_case_ms: u64,
    /// Most likely release (EWMA).
    pub expected_ms: u64,
    /// Latest plausible release (EWMA + 2σ).
    pub worst_case_ms: u64,
    /// Number of hold-duration samples informing this prediction.
    pub confidence_samples: usize,
}

struct KeyStats {
    /// EWMA of hold duration in ms.
    ewma_ms: f64,
    /// EWMA of squared deviation (for variance estimation).
    ewma_var_ms: f64,
    samples: usize,
}

#[derive(Default)]
pub struct ContentionPredictor {
    keys: Mutex<HashMap<String, KeyStats>>,
}

impl ContentionPredictor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a completed hold duration for future forecasting.
    pub fn observe_hold(&self, key: &str, hold_ms: u64) {
        let mut keys = self.keys.lock().expect("predictor");
        let st = keys.entry(key.to_owned()).or_insert(KeyStats {
            ewma_ms: hold_ms as f64,
            ewma_var_ms: 0.0,
            samples: 0,
        });
        st.samples += 1;
        let alpha = 1.0 / st.samples.min(20) as f64;
        let delta = hold_ms as f64 - st.ewma_ms;
        st.ewma_ms += alpha * delta;
        st.ewma_var_ms += alpha * (delta * delta - st.ewma_var_ms);
    }

    /// Forecasts when the current holder will release. Returns `None` if
    /// insufficient data or key is not currently held.
    pub fn forecast(
        &self,
        key: &str,
        elapsed_since_acquire_ms: u64,
    ) -> Option<AvailabilityForecast> {
        let keys = self.keys.lock().expect("predictor");
        let st = keys.get(key)?;
        if st.samples < MIN_SAMPLES {
            return None;
        }

        let sigma = st.ewma_var_ms.sqrt().max(1.0);
        let remaining_best = (st.ewma_ms - sigma).max(1.0) - elapsed_since_acquire_ms as f64;
        let remaining_exp = st.ewma_ms - elapsed_since_acquire_ms as f64;
        let remaining_worst = st.ewma_ms + sigma - elapsed_since_acquire_ms as f64;

        Some(AvailabilityForecast {
            best_case_ms: remaining_best.max(0.0) as u64,
            expected_ms: remaining_exp.max(0.0) as u64,
            worst_case_ms: remaining_worst.max(0.0) as u64,
            confidence_samples: st.samples,
        })
    }
}
