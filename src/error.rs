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
    /// A dbt unit test's actual output differed from the expected fixture —
    /// non-retryable (fixtures and model SQL won't change on retry).
    UnitTestFailure {
        unique_id: String,
        failures: i64,
        diff: String,
    },
    /// A source freshness check exceeded its error_after threshold —
    /// non-retryable (the source won't get fresher by retrying).
    StaleSource {
        unique_id: String,
        age_secs: f64,
        max_allowed_secs: i64,
    },
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
            // Alternate format prints the full anyhow context chain — the top
            // context alone ("parsing X") routinely hides the actionable cause.
            Self::Adapter(err) => write!(f, "adapter error: {err:#}"),
            Self::ProjectNotFound(msg) => write!(f, "project not found: {msg}"),
            Self::TestFailure {
                unique_id,
                failures,
                ..
            } => write!(f, "test failed: {unique_id} ({failures} failing row(s))"),
            Self::UnitTestFailure {
                unique_id,
                failures,
                diff,
            } => write!(f, "unit test failed: {unique_id} ({failures} differing row(s))\n{diff}"),
            Self::StaleSource {
                unique_id,
                age_secs,
                max_allowed_secs,
            } => write!(
                f,
                "source freshness error: {unique_id} is stale \
                 (age {age_secs:.0}s exceeds error_after {max_allowed_secs}s)"
            ),
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
            | Self::TestFailure { .. }
            | Self::UnitTestFailure { .. }
            | Self::StaleSource { .. } => None,
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

/// SQLSTATE classes (first two chars) that mark a transient/retryable warehouse
/// failure. Populated by warehouses that speak SQLSTATE — Redshift/Postgres,
/// SQL Server/Synapse, newer Spark. See <https://en.wikipedia.org/wiki/SQLSTATE>.
const RETRYABLE_SQLSTATE_CLASSES: &[&str] = &[
    "08", // connection exception
    "40", // transaction rollback — deadlock (40P01), serialization failure (40001)
    "53", // insufficient resources
    "57", // operator intervention — admin shutdown, cannot connect now, query canceled
    "58", // system error (I/O)
];

/// Azure SQL Database / SQL Server transient error numbers, surfaced as ADBC
/// `vendor_code`. From Microsoft's transient-fault list (deadlock 1205 included).
const AZURE_SQL_TRANSIENT_VENDOR_CODES: &[i32] = &[
    64, 233, 1205, 4060, 4221, 10053, 10054, 10060, 10928, 10929, 40143, 40197, 40501, 40613,
    49918, 49919, 49920,
];

/// Lowercased substrings that mark a transient failure on managed warehouses
/// that carry HTTP status + reason strings rather than SQLSTATE (BigQuery,
/// Snowflake, Databricks, Athena), plus universal connectivity phrases. ADBC
/// reports network problems as "IO Error". Curated to exclude permanent errors
/// (bad SQL, missing objects, permission denials) so obvious failures don't spin.
const TRANSIENT_MESSAGE_SIGNATURES: &[&str] = &[
    // connectivity / IO
    "io error",
    "connection reset",
    "connection refused",
    "connection closed",
    "connection aborted",
    "connection is closed",
    "connection lost",
    "could not connect",
    "broken pipe",
    "reset by peer",
    "server closed the connection",
    "network error",
    "network is unreachable",
    "no route to host",
    // availability / throttling
    "service unavailable",
    "temporarily unavailable",
    "temporarily_unavailable",
    "try again",
    "please retry",
    "rate limit",
    "ratelimit",
    "too many requests",
    "throttl",
    "slow down",
    "slowdown",
    "backend error",
    "backenderror",
    "internal error",
    "internalerror",
    "request timeout",
    "operation timed out",
    "timed out",
    "timeout",
    // HTTP status hints — precise phrases, not bare numbers
    "429 too many",
    "500 internal server",
    "502 bad gateway",
    "503 service",
    "504 gateway",
    "http 429",
    "http 500",
    "http 502",
    "http 503",
    "http 504",
];

/// Decide whether a warehouse execution failure is transient (worth a Temporal
/// retry). Pure over the fields recovered from an `AdapterError`, so it can be
/// unit-tested with synthesized per-warehouse errors.
///
/// Priority: structured kind → vendor code (Azure/SQL Server) → SQLSTATE class
/// (a real, non-`00` class is authoritative — trusted over the message text) →
/// message signatures (managed warehouses that leave SQLSTATE unset).
fn adapter_failure_is_transient(
    kind_is_transient: bool,
    sqlstate: &str,
    vendor_code: Option<i32>,
    message: &str,
) -> bool {
    if kind_is_transient {
        return true;
    }
    if vendor_code.is_some_and(|code| AZURE_SQL_TRANSIENT_VENDOR_CODES.contains(&code)) {
        return true;
    }
    // A real SQLSTATE class (not the "00000" placeholder DuckDB and some drivers
    // leave unset) is authoritative — so `42P01` (undefined table) stays
    // permanent even if the message text happens to contain a transient word.
    if let Some(class) = sqlstate.get(..2)
        && class != "00"
    {
        return RETRYABLE_SQLSTATE_CLASSES.contains(&class);
    }
    let lower = message.to_ascii_lowercase();
    TRANSIENT_MESSAGE_SIGNATURES
        .iter()
        .any(|sig| lower.contains(sig))
}

/// `dbt_error::ErrorCode` variants that represent an infrastructure-level,
/// worth-a-retry failure. Derived from dbt's own `AdapterErrorKind` → code and
/// SQLSTATE-class → code mappings (`dbt-error/src/adapter_errors.rs`):
/// `Io`/`Cancelled` kinds and SQLSTATE classes 08/40/53/57/58 land here.
/// Deliberately excludes `DbDriverFailed` (kind `Driver` is ambiguous — the
/// embedded-DuckDB harness reports both connection IO errors and catalog
/// errors under it) and `DbAuthFailed`/`DbSyntaxInvalid`/`DbNotFound` (permanent).
const RETRYABLE_ERROR_CODES: &[dbt_error::ErrorCode] = &[
    dbt_error::ErrorCode::DbConnectionFailed,
    dbt_error::ErrorCode::DbUnavailable,
    dbt_error::ErrorCode::DbResourceExceeded,
    dbt_error::ErrorCode::DbTxnConflict,
    dbt_error::ErrorCode::IoError,
    dbt_error::ErrorCode::TaskCancelled,
];

/// Classify an error raised while executing adapter DDL/DML (a materialization
/// macro call, a hook's `run_query`/direct SQL) into the retry contract.
///
/// dbt runs warehouse statements through Jinja, so a transient failure arrives
/// wrapped in a `minijinja::Error` (or, for some Jinja-exposed functions like
/// `run_query`, a `dbt_error::FsError`). The underlying `dbt_common::AdapterError`
/// usually survives in the source chain — walk it and, if found, classify on
/// its kind/SQLSTATE/vendor-code/message. Some conversions (`run_query`)
/// discard the `AdapterError` but keep its derived `ErrorCode` on an `FsError`
/// in the chain; check that too, since it's the only signal left. If neither
/// is found, default to a permanent `Compilation` error carrying `context`.
pub fn classify_adapter_execution_error(
    err: &(dyn std::error::Error + 'static),
    context: &str,
) -> DbtTemporalError {
    let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = cursor {
        if let Some(adapter_err) = current.downcast_ref::<dbt_common::AdapterError>() {
            let kind_is_transient = matches!(
                adapter_err.kind(),
                dbt_common::AdapterErrorKind::Io | dbt_common::AdapterErrorKind::Cancelled
            );
            if adapter_failure_is_transient(
                kind_is_transient,
                adapter_err.sqlstate(),
                adapter_err.vendor_code(),
                &adapter_err.to_string(),
            ) {
                return DbtTemporalError::Adapter(anyhow::anyhow!("{context}: {adapter_err}"));
            }
            // Found the adapter error, but it's a permanent SQL failure.
            break;
        }
        if let Some(fs_err) = current.downcast_ref::<dbt_error::FsError>() {
            if RETRYABLE_ERROR_CODES.contains(&fs_err.code) {
                return DbtTemporalError::Adapter(anyhow::anyhow!("{context}: {fs_err}"));
            }
            break;
        }
        cursor = current.source();
    }
    DbtTemporalError::Compilation(format!("{context}: {err:#}"))
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

    // --- transient-vs-permanent adapter-error classification ---

    use dbt_common::{AdapterError, AdapterErrorKind};

    fn adapter(
        kind: AdapterErrorKind,
        msg: &str,
        sqlstate: [u8; 5],
        vendor: Option<i32>,
    ) -> AdapterError {
        AdapterError::new_with_sqlstate_and_vendor_code(kind, msg.to_string(), sqlstate, vendor)
    }

    #[test]
    fn transient_sqlstate_classes_retry() {
        // Postgres/Redshift connection (08006/08001), deadlock (40P01),
        // serialization (40001), insufficient resources (53300), admin
        // shutdown (57P01), system I/O (58030) — retryable by SQLSTATE class.
        for ss in [
            b"08006", b"08001", b"40P01", b"40001", b"53300", b"57P01", b"58030",
        ] {
            let ae = adapter(AdapterErrorKind::SqlExecution, "boom", *ss, None);
            assert!(
                classify_adapter_execution_error(&ae, "ctx").is_retryable(),
                "sqlstate {} should retry",
                std::str::from_utf8(ss).unwrap()
            );
        }
    }

    #[test]
    fn permanent_sqlstate_classes_do_not_retry() {
        // Undefined table (42P01), syntax (42601), insufficient privilege
        // (42501), data exception (22P02), unique violation (23505).
        for ss in [b"42P01", b"42601", b"42501", b"22P02", b"23505"] {
            let ae = adapter(AdapterErrorKind::SqlExecution, "bad sql", *ss, None);
            assert!(
                !classify_adapter_execution_error(&ae, "ctx").is_retryable(),
                "sqlstate {} should not retry",
                std::str::from_utf8(ss).unwrap()
            );
        }
    }

    #[test]
    fn real_sqlstate_class_overrides_transient_message() {
        // 42P01 whose message mentions "connection" must stay permanent — the
        // authoritative class wins over the text.
        let ae = adapter(
            AdapterErrorKind::SqlExecution,
            "connection to relation lost: relation does not exist",
            *b"42P01",
            None,
        );
        assert!(!classify_adapter_execution_error(&ae, "ctx").is_retryable());
    }

    #[test]
    fn azure_sql_transient_vendor_codes_retry() {
        // SQL Server / Azure SQL leave SQLSTATE generic; the error number is the
        // signal (throttling 40501, gateway 40613, deadlock 1205, …).
        for code in [40197, 40501, 40613, 49918, 10928, 1205] {
            let ae = adapter(AdapterErrorKind::Driver, "SQL Server error", *b"00000", Some(code));
            assert!(
                classify_adapter_execution_error(&ae, "ctx").is_retryable(),
                "vendor code {code} should retry"
            );
        }
    }

    #[test]
    fn managed_warehouse_message_signatures_retry() {
        // BigQuery reasons, Snowflake/Databricks/Athena HTTP-ish messages carry
        // no SQLSTATE — match on the reason string.
        for msg in [
            "rateLimitExceeded: Exceeded rate limits for this project",
            "backendError: An internal error occurred",
            "503 Service Unavailable",
            "The service is temporarily unavailable, please retry",
            "TEMPORARILY_UNAVAILABLE: cluster is starting",
            "ThrottlingException: Rate exceeded",
            "connection reset by peer",
        ] {
            let ae = adapter(AdapterErrorKind::Driver, msg, *b"00000", None);
            assert!(
                classify_adapter_execution_error(&ae, "ctx").is_retryable(),
                "{msg:?} should retry"
            );
        }
    }

    #[test]
    fn duckdb_io_error_retries_but_catalog_error_does_not() {
        // DuckDB reports both as kind=Driver, sqlstate=00000 — the message
        // discriminates. Mirrors the embedded-DuckDB integration tests.
        let io = adapter(AdapterErrorKind::Driver, "IO Error: Cannot open file", *b"00000", None);
        assert!(classify_adapter_execution_error(&io, "ctx").is_retryable());
        let catalog = adapter(
            AdapterErrorKind::Driver,
            "Catalog Error: Table with name x does not exist",
            *b"00000",
            None,
        );
        assert!(!classify_adapter_execution_error(&catalog, "ctx").is_retryable());
    }

    #[test]
    fn io_kind_retries_regardless_of_message() {
        let ae = adapter(AdapterErrorKind::Io, "socket hang up", *b"00000", None);
        assert!(classify_adapter_execution_error(&ae, "ctx").is_retryable());
    }

    #[test]
    fn non_adapter_error_defaults_to_compilation() {
        // No AdapterError in the chain → a genuine compile/template error.
        let plain = std::io::Error::other("template not found");
        let classified = classify_adapter_execution_error(&plain, "rendering");
        assert!(!classified.is_retryable());
        assert!(matches!(classified, DbtTemporalError::Compilation(_)));
    }

    #[test]
    fn classifier_walks_the_source_chain() {
        // In production the AdapterError is nested under a minijinja::Error.
        #[derive(Debug)]
        struct Wrapper(AdapterError);
        impl fmt::Display for Wrapper {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "jinja macro call failed")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let inner =
            adapter(AdapterErrorKind::Driver, "IO Error: connection reset", *b"00000", None);
        assert!(classify_adapter_execution_error(&Wrapper(inner), "ctx").is_retryable());
    }
}
