//! Deterministic simulation + invariant checking.
//!
//! The release gate: 200 clean seeds must produce zero violations, and the
//! checker MUST catch injected bugs — a validator that never fails is not
//! a validator.

use palisade_testing::{Scenario, check, simulate};

#[test]
fn two_hundred_clean_seeds_pass() {
    for seed in 0..200 {
        let sc = Scenario::clean(seed);
        let history = simulate(&sc);
        check(&history, sc.ttl_ms).unwrap_or_else(|v| panic!("seed {seed}: {v}"));
    }
}

#[test]
fn determinism_same_seed_same_history() {
    let sc = Scenario::clean(42);
    let a = simulate(&sc);
    let b = simulate(&sc);
    assert_eq!(a, b, "same seed must replay bit-for-bit");
}

#[test]
fn different_seeds_diverge() {
    let a = simulate(&Scenario::clean(1));
    let b = simulate(&Scenario::clean(2));
    assert_ne!(a, b, "distinct seeds should explore distinct schedules");
}

#[test]
fn broken_cas_is_caught() {
    let mut caught = 0;
    for seed in 0..50 {
        let mut sc = Scenario::clean(seed);
        sc.broken_cas = true;
        if check(&simulate(&sc), sc.ttl_ms).is_err() {
            caught += 1;
        }
    }
    assert!(
        caught >= 45,
        "checker caught only {caught}/50 double-grant bugs; it is blind"
    );
}

#[test]
fn broken_fencing_is_caught() {
    let mut caught = 0;
    for seed in 0..50 {
        let mut sc = Scenario::clean(seed);
        sc.broken_fencing = true;
        if check(&simulate(&sc), sc.ttl_ms).is_err() {
            caught += 1;
        }
    }
    assert!(
        caught >= 45,
        "checker caught only {caught}/50 stale-write bugs; it is blind"
    );
}
