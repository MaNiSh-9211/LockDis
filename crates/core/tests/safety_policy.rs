//! INV-2 unit proof: the safety policy frontier actually shifts behavior.

use palisade_core::SafetyPolicy;

#[test]
fn cowardly_surrenders_on_first_error() {
    assert_eq!(SafetyPolicy::Cowardly.max_transient_failures(), 0);
}

#[test]
fn balanced_tolerates_two() {
    assert_eq!(SafetyPolicy::Balanced.max_transient_failures(), 2);
}

#[test]
fn aggressive_never_gives_up_on_transients() {
    assert_eq!(SafetyPolicy::Aggressive.max_transient_failures(), u32::MAX);
}

#[test]
fn default_is_balanced() {
    assert_eq!(SafetyPolicy::default(), SafetyPolicy::Balanced);
}
