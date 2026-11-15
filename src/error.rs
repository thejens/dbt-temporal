use std::fmt;

/// Errors from dbt-temporal activities.
///
/// The variant determines retry behavior:
/// - `Compilation` / `Configuration` / `ProjectNotFound` / `MissingAdapterResponse` →
///   non-retryable
/// - `Adapter` / `ArtifactStore` → retryable (transient I/O), except an
///   `ArtifactStore` failure that retrying cannot fix (see [`Self::is_retryable`])
#[derive(Debug)]
pub enum DbtTemporalError {
    /// Bad SQL, missing ref, Jinja render failure.
    Compilation(String),
    /// Missing profile, bad config, invalid selector.
    Configuration(String),
    /// Transient warehouse error (connection timeout, rate limit).
    Adapter(anyhow::Error),
    /// Transient artifact-store I/O failure — an object-store 5xx, a throttle,
    /// a dropped connection while reading or writing run artifacts. Distinct
    /// from `Adapter` because it says nothing about the warehouse, but shares
    /// its retryability: the same call usually succeeds on a later attempt.
    /// A failure that is permanent (missing bucket, access denied, invalid
    /// path) is still carried here and classified by its cause.
    ArtifactStore(anyhow::Error),
    /// Worker doesn't have the requested project loaded.
    ProjectNotFound(String),
    /// A dbt-provided materialization finished without running the statement
    /// that produces its result — non-retryable: the same node renders the same
    /// template and ends the same way on every attempt.
    MissingAdapterResponse(String),
    /// A dbt test query returned failing rows — non-retryable (data won't change on retry).
    TestFailure { unique_id: String, failures: i64 },
    /// A dbt unit test's actual output differed from the expected fixture —
    /// non-retryable (fixtures and model SQL won't change on retry).
    UnitTestFailure {
        unique_id: String,
        failures: i64,
        diff: String,
    },
    /// A freshness check exceeded its error_after threshold — non-retryable
    /// (the relation won't get fresher by retrying).
    ///
    /// Named after dbt's own `ErrorCode::StaleSource`, which dbt reuses for
    /// stale models; `node_kind` is what tells the two apart in the message.
    /// A stale node produces no `FreshnessOutcome`, so the measured timestamp
    /// travels in this message — it is the only place it reaches
    /// `run_results.json` and the run log.
    StaleSource {
        unique_id: String,
        node_kind: &'static str,
        max_loaded_at: String,
        age_secs: f64,
        max_allowed_secs: i64,
    },
}

/// Truncate to at most `max_bytes`, at a UTF-8 char boundary.
///
/// Backing up to a boundary keeps the slice from panicking on multi-byte
/// content — warehouse error messages, unit test diffs and user data all
/// routinely carry smart quotes and worse.
///
/// Lives with the error types because both places that bound a rendered
/// message use it: the node result an activity returns, and the run-log line
/// the workflow writes for an activity failure.
pub fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> &str {
    let mut end = s.len().min(max_bytes);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl DbtTemporalError {
    /// `type_name` of every variant for which `is_retryable` is false.
    ///
    /// Single source of truth for the activity retry policy's
    /// `non_retryable_error_types`: Temporal matches that list against the
    /// failure type that `type_name` stamps onto every activity failure. A test
    /// asserts the list agrees with `is_retryable` for every variant.
    pub const NON_RETRYABLE_TYPE_NAMES: [&'static str; 8] = [
        "DbtTemporalError::Compilation",
        "DbtTemporalError::Configuration",
        "DbtTemporalError::ProjectNotFound",
        "DbtTemporalError::MissingAdapterResponse",
        "DbtTemporalError::TestFailure",
        "DbtTemporalError::UnitTestFailure",
        "DbtTemporalError::StaleSource",
        Self::ARTIFACT_STORE_PERMANENT_TYPE_NAME,
    ];

    /// Failure type of an artifact-store error whose cause no retry can fix.
    /// Its own name, rather than `ArtifactStore`'s, so a retry policy can list
    /// it as non-retryable while transient store errors keep retrying.
    const ARTIFACT_STORE_PERMANENT_TYPE_NAME: &'static str =
        "DbtTemporalError::ArtifactStorePermanent";

    /// Whether this error is retryable by Temporal.
    ///
    /// An artifact-store error is retryable unless its cause is permanent, which
    /// is decided from the typed errors in its chain.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Adapter(_) => true,
            Self::ArtifactStore(err) => !crate::artifact_store::is_permanent(err),
            Self::Compilation(_)
            | Self::Configuration(_)
            | Self::ProjectNotFound(_)
            | Self::MissingAdapterResponse(_)
            | Self::TestFailure { .. }
            | Self::UnitTestFailure { .. }
            | Self::StaleSource { .. } => false,
        }
    }

    /// Stable failure type reported to Temporal for this variant, so operators
    /// can filter failures by type and the retry policy can match on it.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Compilation(_) => "DbtTemporalError::Compilation",
            Self::Configuration(_) => "DbtTemporalError::Configuration",
            Self::Adapter(_) => "DbtTemporalError::Adapter",
            Self::ArtifactStore(_) if !self.is_retryable() => {
                Self::ARTIFACT_STORE_PERMANENT_TYPE_NAME
            }
            Self::ArtifactStore(_) => "DbtTemporalError::ArtifactStore",
            Self::ProjectNotFound(_) => "DbtTemporalError::ProjectNotFound",
            Self::MissingAdapterResponse(_) => "DbtTemporalError::MissingAdapterResponse",
            Self::TestFailure { .. } => "DbtTemporalError::TestFailure",
            Self::UnitTestFailure { .. } => "DbtTemporalError::UnitTestFailure",
            Self::StaleSource { .. } => "DbtTemporalError::StaleSource",
        }
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
            Self::ArtifactStore(err) => write!(f, "artifact store error: {err:#}"),
            Self::ProjectNotFound(msg) => write!(f, "project not found: {msg}"),
            Self::MissingAdapterResponse(msg) => write!(f, "missing adapter response: {msg}"),
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
                node_kind,
                max_loaded_at,
                age_secs,
                max_allowed_secs,
            } => write!(
                f,
                "{node_kind} freshness error: {unique_id} is stale \
                 (max_loaded_at {max_loaded_at}, age {age_secs:.0}s \
                 exceeds error_after {max_allowed_secs}s)"
            ),
        }
    }
}

impl std::error::Error for DbtTemporalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Adapter(err) | Self::ArtifactStore(err) => Some(err.as_ref()),
            Self::Compilation(_)
            | Self::Configuration(_)
            | Self::ProjectNotFound(_)
            | Self::MissingAdapterResponse(_)
            | Self::TestFailure { .. }
            | Self::UnitTestFailure { .. }
            | Self::StaleSource { .. } => None,
        }
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
    "57", // operator intervention — admin/crash shutdown, cannot connect now; see NON_RETRYABLE_SQLSTATES
    "58", // system error (I/O)
];

/// SQLSTATEs inside a retryable class that are nonetheless permanent.
///
/// Checked before [`RETRYABLE_SQLSTATE_CLASSES`]. Class 57 (operator
/// intervention) mixes server restarts, which a retry outlasts, with these:
const NON_RETRYABLE_SQLSTATES: &[&str] = &[
    // query_canceled — also what a statement timeout reports. The retry runs
    // the same statement against the same limit and is cancelled at the same
    // point, so it burns every attempt; a user-initiated cancel should not be
    // re-issued either.
    "57014", // database_dropped — the database is gone for good, not restarting.
    "57P04",
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

/// Lowercased phrases for a timeout while reaching or waiting on the service,
/// as drivers and warehouses word it. Matched on word boundaries by
/// [`contains_phrase`]: a bare `timeout` or `timed out` also appears inside
/// identifiers (`timeout_ms`, `session_timed_out`) in otherwise permanent
/// errors such as a syntax error or a missing column. Statement and query
/// timeouts are left out on purpose: the retry would hit the same limit.
const TRANSIENT_TIMEOUT_PHRASES: &[&str] = &[
    "connection timed out",
    "connection timeout",
    "connect timed out",
    "connect timeout",
    "read timed out",
    "read timeout",
    "write timed out",
    "write timeout",
    "handshake timed out",
    "handshake timeout",
    "request timed out",
    "request timeout",
    "operation timed out",
    "gateway timeout",
    "i/o timeout",
    "timeout expired",
    "timed out waiting for",
];

/// Whether `phrase` occurs in `haystack` not embedded in a longer word or
/// identifier (letters, digits and `_` count as word characters).
fn contains_phrase(haystack: &str, phrase: &str) -> bool {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    haystack.match_indices(phrase).any(|(start, matched)| {
        let before = haystack[..start].chars().next_back();
        let after = haystack[start + matched.len()..].chars().next();
        !before.is_some_and(is_word) && !after.is_some_and(is_word)
    })
}

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
        return !NON_RETRYABLE_SQLSTATES.contains(&sqlstate)
            && RETRYABLE_SQLSTATE_CLASSES.contains(&class);
    }
    let lower = message.to_ascii_lowercase();
    TRANSIENT_MESSAGE_SIGNATURES
        .iter()
        .any(|sig| lower.contains(sig))
        || TRANSIENT_TIMEOUT_PHRASES
            .iter()
            .any(|phrase| contains_phrase(&lower, phrase))
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
mod tests;
