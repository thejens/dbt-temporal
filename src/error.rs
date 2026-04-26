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

/// Compile user-configured error patterns from `dbt_temporal.yml` into regexes.
///
/// Invalid patterns are logged at `warn!` and skipped. Done once at config-load
/// time so each adapter error doesn't re-validate (and re-warn about) the patterns.
pub fn compile_error_patterns(patterns: &[String]) -> Vec<regex::Regex> {
    patterns
        .iter()
        .filter_map(|pattern| match regex::Regex::new(pattern) {
            Ok(re) => Some(re),
            Err(e) => {
                tracing::warn!(
                    pattern = %pattern,
                    error = %e,
                    "invalid regex in non_retryable_errors — pattern will be ignored"
                );
                None
            }
        })
        .collect()
}

/// Check if an error message matches any of the given pre-compiled patterns.
///
/// Used by `execute_node` to promote adapter errors to non-retryable when the
/// message matches a user-configured pattern from `dbt_temporal.yml`.
pub fn matches_error_patterns(error_message: &str, patterns: &[regex::Regex]) -> bool {
    patterns.iter().any(|re| re.is_match(error_message))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
        assert!(
            !DbtTemporalError::TestFailure {
                unique_id: "test.foo".into(),
                failures: 3,
            }
            .is_retryable()
        );
    }

    #[test]
    fn test_failure_display_includes_id_and_count() {
        let err = DbtTemporalError::TestFailure {
            unique_id: "test.unique_customers".into(),
            failures: 5,
        };
        let s = err.to_string();
        assert!(s.contains("test.unique_customers"));
        assert!(s.contains('5'));
    }

    #[test]
    fn source_is_none_for_test_failure() {
        use std::error::Error;
        let err = DbtTemporalError::TestFailure {
            unique_id: "t".into(),
            failures: 1,
        };
        assert!(err.source().is_none());
    }

    #[test]
    fn source_is_none_for_configuration_and_project_not_found() {
        use std::error::Error;
        assert!(
            DbtTemporalError::Configuration("x".into())
                .source()
                .is_none()
        );
        assert!(
            DbtTemporalError::ProjectNotFound("p".into())
                .source()
                .is_none()
        );
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
        let patterns = compile_error_patterns(&["permission denied".to_string()]);
        assert!(matches_error_patterns(
            "adapter error: permission denied for table foo",
            &patterns
        ));
        assert!(!matches_error_patterns("adapter error: connection timeout", &patterns));
    }

    #[test]
    fn matches_error_patterns_regex() {
        let patterns = compile_error_patterns(&[r"relation .* does not exist".to_string()]);
        assert!(matches_error_patterns(
            "adapter error: relation \"public.foo\" does not exist",
            &patterns
        ));
        assert!(!matches_error_patterns("adapter error: connection refused", &patterns));
    }

    #[test]
    fn matches_error_patterns_multiple() {
        let patterns =
            compile_error_patterns(&["permission denied".to_string(), "access denied".to_string()]);
        assert!(matches_error_patterns("access denied for user", &patterns));
        assert!(matches_error_patterns("permission denied on schema", &patterns));
        assert!(!matches_error_patterns("connection timeout", &patterns));
    }

    #[test]
    fn matches_error_patterns_empty_patterns() {
        let patterns: Vec<regex::Regex> = vec![];
        assert!(!matches_error_patterns("any error", &patterns));
    }

    #[test]
    fn compile_error_patterns_skips_invalid() {
        let patterns =
            compile_error_patterns(&["[invalid".to_string(), "valid_pattern".to_string()]);
        // Only the valid pattern compiles — the invalid one is logged and dropped.
        assert_eq!(patterns.len(), 1);
        assert!(matches_error_patterns("this has valid_pattern in it", &patterns));
        assert!(!matches_error_patterns("nothing matches", &patterns));
    }

    #[test]
    fn anyhow_alternate_format_preserves_chain_for_dbt_fusion_errors() {
        // Regression: the worker used to wrap dbt-fusion errors with `: {e}`
        // (Display), which prints only the outermost `anyhow::Error` message
        // and discards the chained context — turning real failures like
        // `dbt1001: Failed to read file <path>` into bare
        // `Failed to read file: No such file or directory (os error 2)`.
        // We now use `: {e:#}` (alternate Display) at every wrapping site so
        // chains print as `outer: inner: deeper`. This test pins that
        // contract: switching back to `{e}` would make it fail.
        use anyhow::Context;
        let inner: Result<(), std::io::Error> =
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "missing /path/to/file.sql"));
        let Err(chained) = inner
            .context("dbt1001: Failed to read file")
            .context("dbt resolve failed")
        else {
            panic!("expected the chained Result to be Err");
        };

        let alt = format!("wrap: {chained:#}");
        assert!(alt.contains("dbt resolve failed"), "got: {alt}");
        assert!(alt.contains("dbt1001: Failed to read file"), "got: {alt}");
        assert!(alt.contains("/path/to/file.sql"), "got: {alt}");

        let basic = format!("wrap: {chained}");
        assert!(basic.contains("dbt resolve failed"));
        assert!(!basic.contains("/path/to/file.sql"), "got: {basic}");
    }
}
