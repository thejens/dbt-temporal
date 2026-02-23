use std::fmt;

/// Errors from dbt-temporal activities.
///
/// The variant determines retry behavior:
/// - `Compilation` / `Configuration` / `ProjectNotFound` → non-retryable
/// - `Adapter` → retryable (transient warehouse errors)
#[derive(Debug)]
pub enum DbtTemporalError {
    /// Bad SQL, missing ref, Jinja render failure.
    Compilation(String),
    /// Missing profile, bad config, invalid selector.
    Configuration(String),
    /// Transient warehouse error (connection timeout, rate limit).
    Adapter(anyhow::Error),
    /// Worker doesn't have the requested project loaded.
    ProjectNotFound(String),
    /// A dbt test query returned failing rows — non-retryable (data won't change on retry).
    TestFailure { unique_id: String, failures: i64 },
}

impl DbtTemporalError {
    /// Whether this error is retryable by Temporal.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Adapter(_))
    }
}

impl fmt::Display for DbtTemporalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compilation(msg) => write!(f, "compilation error: {msg}"),
            Self::Configuration(msg) => write!(f, "configuration error: {msg}"),
            Self::Adapter(err) => write!(f, "adapter error: {err}"),
            Self::ProjectNotFound(msg) => write!(f, "project not found: {msg}"),
            Self::TestFailure {
                unique_id,
                failures,
                ..
            } => write!(f, "test failed: {unique_id} ({failures} failing row(s))"),
        }
    }
}

impl std::error::Error for DbtTemporalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Adapter(err) => Some(err.as_ref()),
            Self::Compilation(_)
            | Self::Configuration(_)
            | Self::ProjectNotFound(_)
            | Self::TestFailure { .. } => None,
        }
    }
}

impl From<anyhow::Error> for DbtTemporalError {
    fn from(err: anyhow::Error) -> Self {
        Self::Adapter(err)
    }
}

/// Check if an error message matches any of the given regex patterns.
///
/// Used by `execute_node` to promote adapter errors to non-retryable when the
/// message matches a user-configured pattern from `dbt_temporal.yml`.
pub fn matches_error_patterns(error_message: &str, patterns: &[String]) -> bool {
    for pattern in patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            if re.is_match(error_message) {
                return true;
            }
        } else {
            // Invalid regex — skip silently.
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_is_retryable() {
        let err = DbtTemporalError::Adapter(anyhow::anyhow!("timeout"));
        assert!(err.is_retryable());
    }

    #[test]
    fn non_adapter_variants_are_not_retryable() {
        assert!(!DbtTemporalError::Compilation("bad sql".into()).is_retryable());
        assert!(!DbtTemporalError::Configuration("bad config".into()).is_retryable());
        assert!(!DbtTemporalError::ProjectNotFound("nope".into()).is_retryable());
    }

    #[test]
    fn display_formats_each_variant() {
        assert_eq!(
            DbtTemporalError::Compilation("bad ref".into()).to_string(),
            "compilation error: bad ref"
        );
        assert_eq!(
            DbtTemporalError::Configuration("missing profile".into()).to_string(),
            "configuration error: missing profile"
        );
        assert_eq!(
            DbtTemporalError::ProjectNotFound("foo".into()).to_string(),
            "project not found: foo"
        );
        let adapter = DbtTemporalError::Adapter(anyhow::anyhow!("connection refused"));
        assert!(adapter.to_string().contains("adapter error"));
        assert!(adapter.to_string().contains("connection refused"));
    }

    #[test]
    fn from_anyhow_produces_adapter() {
        let err: DbtTemporalError = anyhow::anyhow!("something broke").into();
        assert!(err.is_retryable());
        assert!(err.to_string().contains("adapter error"));
    }

    #[test]
    fn source_returns_some_for_adapter_none_for_others() {
        use std::error::Error;
        let adapter = DbtTemporalError::Adapter(anyhow::anyhow!("inner"));
        assert!(adapter.source().is_some());

        let compilation = DbtTemporalError::Compilation("x".into());
        assert!(compilation.source().is_none());
    }

    #[test]
    fn matches_error_patterns_exact_substring() {
        let patterns = vec!["permission denied".to_string()];
        assert!(matches_error_patterns(
            "adapter error: permission denied for table foo",
            &patterns
        ));
        assert!(!matches_error_patterns("adapter error: connection timeout", &patterns));
    }

    #[test]
    fn matches_error_patterns_regex() {
        let patterns = vec![r"relation .* does not exist".to_string()];
        assert!(matches_error_patterns(
            "adapter error: relation \"public.foo\" does not exist",
            &patterns
        ));
        assert!(!matches_error_patterns("adapter error: connection refused", &patterns));
    }

    #[test]
    fn matches_error_patterns_multiple() {
        let patterns = vec!["permission denied".to_string(), "access denied".to_string()];
        assert!(matches_error_patterns("access denied for user", &patterns));
        assert!(matches_error_patterns("permission denied on schema", &patterns));
        assert!(!matches_error_patterns("connection timeout", &patterns));
    }

    #[test]
    fn matches_error_patterns_empty_patterns() {
        let patterns: Vec<String> = vec![];
        assert!(!matches_error_patterns("any error", &patterns));
    }

    #[test]
    fn matches_error_patterns_invalid_regex_skipped() {
        let patterns = vec!["[invalid".to_string(), "valid_pattern".to_string()];
        assert!(matches_error_patterns("this has valid_pattern in it", &patterns));
        assert!(!matches_error_patterns("nothing matches", &patterns));
    }
}
