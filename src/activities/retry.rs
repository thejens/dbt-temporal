//! Shared retry classification at the activity boundary.
//!
//! Activities carry errors internally as `anyhow::Error` so call sites can use
//! `?` and `.context()` freely. The typed `DbtTemporalError` rides along in the
//! chain; this module recovers it and turns it into the retryable /
//! non-retryable `ActivityError` Temporal acts on.
//!
//! Two activities need different treatment for an error that carries *no*
//! typed variant. `execute_node` is dominated by warehouse I/O, so an
//! unrecognised failure is most likely transient and retrying is the safe
//! default. `plan_project` reads local project state, so an unrecognised
//! failure there is a bug or a bad input and retrying only wastes attempts —
//! its remote reads are tagged `ArtifactStore` explicitly at the call site.

use std::sync::Arc;

use temporalio_sdk::activities::ActivityError;
use temporalio_sdk::error::ApplicationFailure;

use crate::error::DbtTemporalError;
use crate::project_registry::ProjectRegistry;

/// Whether to surface an error to Temporal as retryable or not.
#[derive(Debug, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry within the activity's retry policy.
    Retry,
    /// Skip the policy — the error is permanent.
    NoRetry,
}

/// How to treat an error that carries no typed `DbtTemporalError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unclassified {
    /// Treat as a transient adapter failure. For activities whose dominant
    /// failure mode is warehouse I/O.
    RetryAsAdapter,
    /// Treat as permanent. For activities where an untyped failure means a bug
    /// or bad input rather than a blip.
    Permanent,
}

/// Decide whether a `DbtTemporalError` should be surfaced to Temporal as retryable.
///
/// The retryable variants (`Adapter`, `ArtifactStore`) retry by default unless
/// the user-supplied pattern list matches the error's display string. Every
/// other variant is permanent.
pub fn decide_retry(
    err: &DbtTemporalError,
    non_retryable_patterns: &[regex::Regex],
) -> RetryDecision {
    if !err.is_retryable() {
        return RetryDecision::NoRetry;
    }
    if non_retryable_patterns.is_empty() {
        return RetryDecision::Retry;
    }
    let msg = err.to_string();
    if crate::error::matches_error_patterns(&msg, non_retryable_patterns) {
        tracing::info!(
            error = %msg,
            "error matched non-retryable pattern, suppressing retry"
        );
        RetryDecision::NoRetry
    } else {
        RetryDecision::Retry
    }
}

/// Recover the typed error from an `anyhow::Error`, applying `unclassified`
/// when no `DbtTemporalError` is present in the chain.
pub fn downcast_or_default(err: anyhow::Error, unclassified: Unclassified) -> DbtTemporalError {
    match err.downcast::<DbtTemporalError>() {
        Ok(typed) => typed,
        Err(other) => match unclassified {
            Unclassified::RetryAsAdapter => DbtTemporalError::Adapter(other),
            Unclassified::Permanent => DbtTemporalError::Configuration(format!("{other:#}")),
        },
    }
}

/// Turn a typed error into an `ActivityError` with the right retry flag.
pub fn to_activity_error(
    dbt_err: &DbtTemporalError,
    non_retryable_patterns: &[regex::Regex],
) -> ActivityError {
    let source = anyhow::anyhow!("{dbt_err}");
    match decide_retry(dbt_err, non_retryable_patterns) {
        RetryDecision::Retry => ActivityError::application(ApplicationFailure::new(source)),
        RetryDecision::NoRetry => {
            ActivityError::application(ApplicationFailure::non_retryable(source))
        }
    }
}

/// One-shot boundary conversion: `anyhow::Error` → classified `ActivityError`.
pub fn classify(
    err: anyhow::Error,
    non_retryable_patterns: &[regex::Regex],
    unclassified: Unclassified,
) -> ActivityError {
    let dbt_err = downcast_or_default(err, unclassified);
    to_activity_error(&dbt_err, non_retryable_patterns)
}

/// Look up the user-configured non-retryable patterns for a project.
///
/// Returns `None` when the project isn't registered (during tests or shutdown)
/// so the caller can fall back to "all retryable errors retry".
pub fn registry_non_retryable_patterns(
    registry: &Arc<ProjectRegistry>,
    project: &str,
) -> Option<Vec<regex::Regex>> {
    let state = registry.get(Some(project)).ok()?;
    Some(state.non_retryable_error_patterns.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untyped_error_respects_the_unclassified_default() {
        let retryable =
            downcast_or_default(anyhow::anyhow!("connection reset"), Unclassified::RetryAsAdapter);
        assert!(retryable.is_retryable());

        let permanent =
            downcast_or_default(anyhow::anyhow!("connection reset"), Unclassified::Permanent);
        assert!(!permanent.is_retryable());
    }

    /// The typed variant must survive the `anyhow` round-trip that every
    /// activity's `?` chain puts it through.
    #[test]
    fn typed_error_survives_anyhow_round_trip() {
        let original = DbtTemporalError::ArtifactStore(anyhow::anyhow!("503 slow down"));
        let wrapped: anyhow::Error = original.into();
        let recovered = downcast_or_default(wrapped, Unclassified::Permanent);
        assert!(
            recovered.is_retryable(),
            "ArtifactStore must stay retryable even under a Permanent default"
        );
    }

    /// `classify` is what every activity boundary actually calls; the pieces
    /// below it are tested individually, this covers them wired together.
    #[test]
    fn classify_maps_retryability_onto_the_activity_error() {
        let retryable =
            classify(anyhow::anyhow!("connection reset"), &[], Unclassified::RetryAsAdapter);
        let rendered = format!("{retryable:?}");
        assert!(
            rendered.contains("non_retryable: false"),
            "transient failure should stay retryable: {rendered}"
        );

        let permanent = classify(
            DbtTemporalError::Compilation("bad ref".to_string()).into(),
            &[],
            Unclassified::RetryAsAdapter,
        );
        let rendered = format!("{permanent:?}");
        assert!(
            rendered.contains("non_retryable: true"),
            "compilation failure must not retry: {rendered}"
        );
    }

    #[test]
    fn user_patterns_demote_retryable_errors_only() {
        let patterns = crate::error::compile_error_patterns(&["quota exceeded".to_string()]);

        let demoted = DbtTemporalError::ArtifactStore(anyhow::anyhow!("quota exceeded"));
        assert_eq!(decide_retry(&demoted, &patterns), RetryDecision::NoRetry);

        let kept = DbtTemporalError::ArtifactStore(anyhow::anyhow!("503 slow down"));
        assert_eq!(decide_retry(&kept, &patterns), RetryDecision::Retry);

        // Patterns can only demote — a permanent variant never becomes retryable.
        let permanent = DbtTemporalError::Compilation("bad sql".to_string());
        assert_eq!(decide_retry(&permanent, &[]), RetryDecision::NoRetry);
    }
}
