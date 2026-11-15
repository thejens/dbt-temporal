//! Retry classification as the node activity applies it to the errors its
//! phases raise.

use std::collections::BTreeMap;
use std::sync::Arc;

use temporalio_sdk::activities::ActivityError;

use crate::activities::retry::{
    RetryDecision, Unclassified, decide_retry, downcast_or_default,
    registry_non_retryable_patterns, to_activity_error,
};
use crate::error::{DbtTemporalError, compile_error_patterns};
use crate::project_registry::ProjectRegistry;

fn empty_patterns() -> Vec<regex::Regex> {
    Vec::new()
}

#[test]
fn decide_retry_no_retry_for_compilation() {
    let err = DbtTemporalError::Compilation("bad ref".into());
    assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
}

#[test]
fn decide_retry_no_retry_for_configuration() {
    let err = DbtTemporalError::Configuration("missing profile".into());
    assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
}

#[test]
fn decide_retry_no_retry_for_project_not_found() {
    let err = DbtTemporalError::ProjectNotFound("nope".into());
    assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
}

#[test]
fn decide_retry_no_retry_for_test_failure() {
    let err = DbtTemporalError::TestFailure {
        unique_id: "test.foo".into(),
        failures: 5,
    };
    assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::NoRetry);
}

#[test]
fn decide_retry_retries_adapter_with_no_patterns() {
    let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection timeout"));
    assert_eq!(decide_retry(&err, &empty_patterns()), RetryDecision::Retry);
}

#[test]
fn decide_retry_promotes_adapter_to_no_retry_when_pattern_matches() {
    let err = DbtTemporalError::Adapter(anyhow::anyhow!("permission denied for table foo"));
    let patterns = compile_error_patterns(&["permission denied".to_string()]);
    assert_eq!(decide_retry(&err, &patterns), RetryDecision::NoRetry);
}

#[test]
fn decide_retry_keeps_adapter_retryable_when_pattern_does_not_match() {
    let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection refused"));
    let patterns = compile_error_patterns(&["permission denied".to_string()]);
    assert_eq!(decide_retry(&err, &patterns), RetryDecision::Retry);
}

// --- downcast_or_wrap_as_adapter ---

#[test]
fn downcast_or_wrap_recovers_dbt_temporal_error_variant() {
    let original = DbtTemporalError::Compilation("bad ref".into());
    let any: anyhow::Error = anyhow::anyhow!(original);
    let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
    // Compilation must survive the round-trip — without this, retry
    // classification would silently demote Compilation to Adapter (retryable).
    assert!(matches!(recovered, DbtTemporalError::Compilation(_)));
    assert!(!recovered.is_retryable());
}

#[test]
fn downcast_or_wrap_recovers_test_failure_variant() {
    let original = DbtTemporalError::TestFailure {
        unique_id: "test.x".into(),
        failures: 1,
    };
    let any: anyhow::Error = anyhow::anyhow!(original);
    let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
    assert!(matches!(recovered, DbtTemporalError::TestFailure { .. }));
}

#[test]
fn downcast_or_wrap_falls_back_to_adapter_for_plain_anyhow() {
    let any: anyhow::Error = anyhow::anyhow!("a plain error not from us");
    let recovered = downcast_or_default(any, Unclassified::RetryAsAdapter);
    // Adapter is the retryable default — keeps us out of false positives
    // for transient warehouse issues that don't carry our typed variant.
    assert!(matches!(recovered, DbtTemporalError::Adapter(_)));
    assert!(recovered.is_retryable());
}

// --- classify_for_temporal ---

#[test]
fn classify_for_temporal_marks_retryable_adapter_error() {
    let err = DbtTemporalError::Adapter(anyhow::anyhow!("connection timeout"));
    let activity_err = to_activity_error(&err, &empty_patterns());
    assert!(matches!(activity_err, ActivityError::Application(ref af) if !af.is_non_retryable()));
}

#[test]
fn classify_for_temporal_marks_compilation_as_non_retryable() {
    let err = DbtTemporalError::Compilation("bad ref".into());
    let activity_err = to_activity_error(&err, &empty_patterns());
    assert!(matches!(activity_err, ActivityError::Application(ref af) if af.is_non_retryable()));
}

#[test]
fn classify_for_temporal_promotes_pattern_match_to_non_retryable() {
    let err = DbtTemporalError::Adapter(anyhow::anyhow!("permission denied for table foo"));
    let patterns = compile_error_patterns(&["permission denied".to_string()]);
    let activity_err = to_activity_error(&err, &patterns);
    assert!(matches!(activity_err, ActivityError::Application(ref af) if af.is_non_retryable()));
}

// --- registry_non_retryable_patterns ---

#[test]
fn registry_non_retryable_patterns_returns_none_for_unknown_project() {
    // Empty registry → unknown project lookup returns None, signalling
    // the caller to fall back to "all adapter errors retry".
    let registry = Arc::new(ProjectRegistry::new(BTreeMap::new()));
    assert!(registry_non_retryable_patterns(&registry, "missing").is_none());
}
