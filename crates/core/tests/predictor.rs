//! INV-7 · Contention Predictor tests.

use palisade_core::ContentionPredictor;

#[test]
fn insufficient_samples_returns_none() {
    let p = ContentionPredictor::new();
    p.observe_hold("k", 100);
    p.observe_hold("k", 200);
    assert!(p.forecast("k", 0).is_none());
}

#[test]
fn forecast_improves_with_samples() {
    let p = ContentionPredictor::new();
    // Simulate stable 500ms hold times with some variance.
    for ms in [480u64, 510, 495, 520, 490, 505, 515] {
        p.observe_hold("k", ms);
    }
    let f = p.forecast("k", 0).expect("forecast after 7 samples");
    assert!(f.confidence_samples >= 7);
    assert!(f.expected_ms > 400 && f.expected_ms < 600);
    assert!(f.best_case_ms <= f.expected_ms);
    assert!(f.worst_case_ms >= f.expected_ms);
}

#[test]
fn different_keys_are_independent() {
    let p = ContentionPredictor::new();
    for _ in 0..10 {
        p.observe_hold("fast-key", 50);
        p.observe_hold("slow-key", 2000);
    }
    let fast = p.forecast("fast-key", 0).unwrap();
    let slow = p.forecast("slow-key", 0).unwrap();
    assert!(fast.expected_ms < slow.expected_ms);
}
