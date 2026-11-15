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

fn permanent_store_failure() -> anyhow::Error {
    anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound))
        .context("reading artifact")
}

#[test]
fn a_permanent_store_failure_is_not_retryable_and_names_its_own_type() {
    let permanent = DbtTemporalError::ArtifactStore(permanent_store_failure());
    assert!(!permanent.is_retryable());
    assert!(DbtTemporalError::NON_RETRYABLE_TYPE_NAMES.contains(&permanent.type_name()));

    let transient = DbtTemporalError::ArtifactStore(
        anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
            .context("writing artifact"),
    );
    assert!(transient.is_retryable());
    assert_eq!(transient.type_name(), "DbtTemporalError::ArtifactStore");
}

/// One value of every variant. The exhaustive match makes adding a variant
/// without extending this list a compile error.
fn one_of_each_variant() -> Vec<DbtTemporalError> {
    let probe = DbtTemporalError::Compilation(String::new());
    match probe {
        DbtTemporalError::Compilation(_)
        | DbtTemporalError::Configuration(_)
        | DbtTemporalError::Adapter(_)
        | DbtTemporalError::ArtifactStore(_)
        | DbtTemporalError::ProjectNotFound(_)
        | DbtTemporalError::MissingAdapterResponse(_)
        | DbtTemporalError::TestFailure { .. }
        | DbtTemporalError::UnitTestFailure { .. }
        | DbtTemporalError::StaleSource { .. } => {}
    }
    vec![
        DbtTemporalError::Compilation("c".into()),
        DbtTemporalError::Configuration("c".into()),
        DbtTemporalError::Adapter(anyhow::anyhow!("a")),
        DbtTemporalError::ArtifactStore(anyhow::anyhow!("a")),
        DbtTemporalError::ArtifactStore(permanent_store_failure()),
        DbtTemporalError::ProjectNotFound("p".into()),
        DbtTemporalError::MissingAdapterResponse("m".into()),
        DbtTemporalError::TestFailure {
            unique_id: "t".into(),
            failures: 1,
        },
        DbtTemporalError::UnitTestFailure {
            unique_id: "u".into(),
            failures: 1,
            diff: String::new(),
        },
        DbtTemporalError::StaleSource {
            unique_id: "s".into(),
            node_kind: "source",
            max_loaded_at: "x".into(),
            age_secs: 1.0,
            max_allowed_secs: 1,
        },
    ]
}

#[test]
fn non_retryable_type_names_match_is_retryable_for_every_variant() {
    let variants = one_of_each_variant();
    for err in &variants {
        let listed = DbtTemporalError::NON_RETRYABLE_TYPE_NAMES.contains(&err.type_name());
        assert_eq!(
            listed,
            !err.is_retryable(),
            "{} must be in NON_RETRYABLE_TYPE_NAMES exactly when it is not retryable",
            err.type_name()
        );
    }
    let non_retryable = variants.iter().filter(|e| !e.is_retryable()).count();
    assert_eq!(non_retryable, DbtTemporalError::NON_RETRYABLE_TYPE_NAMES.len());
}

#[test]
fn type_names_are_unique() {
    let names: std::collections::BTreeSet<_> = one_of_each_variant()
        .iter()
        .map(DbtTemporalError::type_name)
        .collect();
    assert_eq!(names.len(), one_of_each_variant().len());
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
    let patterns = compile_error_patterns(&["[invalid".to_string(), "valid_pattern".to_string()]);
    // Only the valid pattern compiles — the invalid one is logged and dropped.
    assert_eq!(patterns.len(), 1);
    assert!(matches_error_patterns("this has valid_pattern in it", &patterns));
    assert!(!matches_error_patterns("nothing matches", &patterns));
}

#[test]
fn anyhow_alternate_format_preserves_chain_for_dbt_fusion_errors() {
    // Wrapping sites format dbt-fusion errors with `: {e:#}` (alternate
    // Display) so chains print as `outer: inner: deeper`. Plain `{e}`
    // prints only the outermost `anyhow::Error` message and discards the
    // chained context, which would turn real failures like
    // `dbt1001: Failed to read file <path>` into bare
    // `Failed to read file: No such file or directory (os error 2)`.
    // This test pins that contract: formatting with `{e}` would fail it.
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
fn class_57_codes_that_a_retry_cannot_fix_do_not_retry() {
    // 57014 is what a statement timeout reports: the retry hits the same
    // limit. 57P04 means the database was dropped.
    for ss in [b"57014", b"57P04"] {
        let ae = adapter(
            AdapterErrorKind::SqlExecution,
            "canceling statement due to statement timeout",
            *ss,
            None,
        );
        assert!(
            !classify_adapter_execution_error(&ae, "ctx").is_retryable(),
            "sqlstate {} should not retry",
            std::str::from_utf8(ss).unwrap()
        );
    }
    // The rest of the class is a server going away or not yet up.
    for ss in [b"57000", b"57P01", b"57P02", b"57P03", b"57P05"] {
        let ae = adapter(AdapterErrorKind::SqlExecution, "boom", *ss, None);
        assert!(
            classify_adapter_execution_error(&ae, "ctx").is_retryable(),
            "sqlstate {} should retry",
            std::str::from_utf8(ss).unwrap()
        );
    }
}

#[test]
fn timeout_messages_from_drivers_retry() {
    for msg in [
        "connection timed out",
        "Connection timeout expired",
        "dial tcp 10.0.0.1:5432: i/o timeout",
        "java.net.SocketTimeoutException: Read timed out",
        "Read timed out",
        "Operation timed out after 30000 milliseconds",
        "504 Gateway Timeout",
        "Request Timeout",
        "Timeout expired.  The timeout period elapsed prior to completion",
        "timed out waiting for a connection from the pool",
        "error: connect timeout (os error 110)",
    ] {
        let ae = adapter(AdapterErrorKind::Driver, msg, *b"00000", None);
        assert!(
            classify_adapter_execution_error(&ae, "ctx").is_retryable(),
            "{msg:?} should retry"
        );
    }
}

#[test]
fn identifiers_containing_timeout_do_not_make_an_error_transient() {
    for msg in [
        "syntax error at or near \"timeout_ms\"",
        "Binder Error: Referenced column \"timeout\" not found in FROM clause",
        "column session_timed_out does not exist",
        "Catalog Error: Table with name connection_timeout_log does not exist",
        "relation \"read_timeouts\" does not exist",
        "Parser Error: unexpected token near timed out_at",
    ] {
        let ae = adapter(AdapterErrorKind::Driver, msg, *b"00000", None);
        assert!(
            !classify_adapter_execution_error(&ae, "ctx").is_retryable(),
            "{msg:?} should not retry"
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
    let inner = adapter(AdapterErrorKind::Driver, "IO Error: connection reset", *b"00000", None);
    assert!(classify_adapter_execution_error(&Wrapper(inner), "ctx").is_retryable());
}
