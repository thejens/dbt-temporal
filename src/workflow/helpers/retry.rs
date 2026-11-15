use std::time::Duration;

use temporalio_client::RetryPolicy;

use crate::error::DbtTemporalError;
use crate::types::RetryConfig;

/// Build a Temporal RetryPolicy from our RetryConfig.
pub fn build_retry_policy(config: &RetryConfig) -> RetryPolicy {
    RetryPolicy::builder()
        .initial_interval(Duration::from_secs(config.initial_interval_secs))
        .backoff_coefficient(config.backoff_coefficient)
        .maximum_interval(Duration::from_secs(config.max_interval_secs))
        .maximum_attempts(config.max_attempts)
        // Matched against the failure type `DbtTemporalError::type_name` stamps
        // on every activity failure.
        .non_retryable_error_types(DbtTemporalError::NON_RETRYABLE_TYPE_NAMES)
        .build()
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;

    #[test]
    fn build_retry_policy_from_defaults() {
        let config = RetryConfig::default();
        let policy = build_retry_policy(&config);
        assert_eq!(policy.initial_interval(), Duration::from_secs(config.initial_interval_secs));
        assert_eq!(policy.backoff_coefficient(), config.backoff_coefficient);
        assert_eq!(policy.maximum_interval(), Some(Duration::from_secs(config.max_interval_secs)));
        assert_eq!(policy.maximum_attempts(), config.max_attempts);
        assert_eq!(policy.non_retryable_error_types(), DbtTemporalError::NON_RETRYABLE_TYPE_NAMES);
    }

    #[test]
    fn build_retry_policy_custom_values() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_interval_secs: 5,
            max_interval_secs: 120,
            backoff_coefficient: 3.0,
            non_retryable_errors: vec!["permission denied".into()],
            project_hooks: crate::types::ProjectHookRetry::default(),
        };
        let policy = build_retry_policy(&config);
        assert_eq!(policy.maximum_attempts(), 10);
        assert_eq!(policy.initial_interval(), Duration::from_secs(5));
        assert_eq!(policy.maximum_interval(), Some(Duration::from_mins(2)));
        assert_eq!(policy.backoff_coefficient(), 3.0);
    }
}
