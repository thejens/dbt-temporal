//! Process-wide tracing initialization.
//!
//! Two mutually exclusive stacks, chosen at startup:
//!
//! - **Default**: `tracing_subscriber` fmt + `EnvFilter` + the
//!   [`crate::telemetry_compat::DbtTelemetryCompatLayer`] shim. The shim keeps
//!   dbt-fusion adapter code from panicking when it reads `TelemetryAttributes`
//!   from span extensions.
//! - **OTLP** (`DBT_EXPORT_TO_OTLP=1`): dbt-fusion's own telemetry pipeline
//!   (`TelemetryDataLayer` + console output + OTLP trace/log export). The data
//!   layer stores real `TelemetryAttributes`, so the compat shim is not needed
//!   on this path, and dbt's structured events (`QueryExecuted`,
//!   `ConnectionLimitWait`, …) export as OTEL spans. The export endpoint comes
//!   from the standard `OTEL_EXPORTER_OTLP_ENDPOINT` /
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`
//!   env vars (read by the upstream layer; OTLP over HTTP, collector port 4318).
//!
//! Either stack registers a global subscriber, so `init` must be called exactly
//! once, from `main`, before any project loading. Tests never call it.

use std::str::FromStr as _;

use anyhow::{Context, Result};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use dbt_common::io_args::{FsCommand, LogFormat};
use dbt_common::tracing::{FsTraceConfig, TelemetryHandle, dbt_init::init_tracing};

/// Initialize the global tracing subscriber.
///
/// Returns a `TelemetryHandle` on the OTLP path; the caller must invoke
/// [`TelemetryHandle::shutdown_once`] after the worker exits so buffered OTLP
/// batches flush before the process ends.
pub fn init() -> Result<Option<TelemetryHandle>> {
    if env_truthy("DBT_EXPORT_TO_OTLP") {
        init_dbt_pipeline().map(Some)
    } else {
        init_default();
        Ok(None)
    }
}

/// `1`/`true` (case-insensitive) → true.
fn env_truthy(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// The fmt-based stack used when OTLP export is off.
fn init_default() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                // The workflow future logs an error for every unhandled query
                // (e.g. __stack_trace from the Temporal UI) and drops it.
                // Suppress the module entirely; its other warnings (WFT
                // failures, panics) are already surfaced in the Temporal UI.
                .add_directive(
                    "temporalio_sdk::workflow_future=off"
                        .parse()
                        .unwrap_or_else(|_| unreachable!("static directive always parses")),
                ),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(crate::telemetry_compat::DbtTelemetryCompatLayer)
        .init();
}

/// dbt-fusion's telemetry pipeline with OTLP export enabled.
fn init_dbt_pipeline() -> Result<TelemetryHandle> {
    let max_log_verbosity = parse_log_level("DBT_LOG_LEVEL")?.unwrap_or(LevelFilter::INFO);
    let log_format = parse_log_format("DBT_LOG_FORMAT")?.unwrap_or(LogFormat::Default);

    // The worker is long-lived and serves many runs, so per-run trace
    // correlation happens via Invocation root spans opened inside activities —
    // this process-level invocation id only labels worker-lifecycle telemetry.
    let config = FsTraceConfig::new(
        "dbt-temporal",
        FsCommand::Unset,
        None, // project_dir — multi-project worker; no single project root
        None, // target_path
        None, // log_path
        max_log_verbosity,
        LevelFilter::OFF, // no dbt.log file — worker logs go to the console/collector
        None,             // no jsonl telemetry file
        None,             // no parquet telemetry file
        uuid::Uuid::new_v4(),
        None, // parent_span_id
        true, // export_to_otlp
        log_format,
        false, // enable_query_log
        std::collections::HashSet::default(),
        false, // show_all_deprecations
        dbt_common::warn_error_options::WarnErrorOptions::default(),
        None,  // log_file_name
        0,     // log_file_max_bytes
        false, // disable_console_output
    )
    .with_command_name("dbt-temporal");

    let (handle, _config_provider) =
        init_tracing(config).context("initializing dbt telemetry pipeline")?;
    Ok(handle)
}

/// Parse a `LevelFilter` from an env var (`error|warn|info|debug|trace|off`).
fn parse_log_level(name: &str) -> Result<Option<LevelFilter>> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => LevelFilter::from_str(&v)
            .map(Some)
            .with_context(|| format!("invalid {name} '{v}'")),
        _ => Ok(None),
    }
}

/// Parse a `LogFormat` from an env var (`text|json|default|otel`).
fn parse_log_format(name: &str) -> Result<Option<LogFormat>> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => match v.to_lowercase().as_str() {
            "text" => Ok(Some(LogFormat::Text)),
            "json" => Ok(Some(LogFormat::Json)),
            "default" => Ok(Some(LogFormat::Default)),
            "otel" => Ok(Some(LogFormat::Otel)),
            other => anyhow::bail!("invalid {name} '{other}' (expected text|json|default|otel)"),
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn env_truthy_accepts_1_and_true() {
        unsafe { std::env::set_var("TEST_TRACING_TRUTHY", "1") };
        assert!(env_truthy("TEST_TRACING_TRUTHY"));
        unsafe { std::env::set_var("TEST_TRACING_TRUTHY", "TRUE") };
        assert!(env_truthy("TEST_TRACING_TRUTHY"));
        unsafe { std::env::set_var("TEST_TRACING_TRUTHY", "0") };
        assert!(!env_truthy("TEST_TRACING_TRUTHY"));
        unsafe { std::env::remove_var("TEST_TRACING_TRUTHY") };
        assert!(!env_truthy("TEST_TRACING_TRUTHY"));
    }

    #[test]
    fn parse_log_level_valid_invalid_and_missing() {
        unsafe { std::env::set_var("TEST_TRACING_LEVEL", "debug") };
        assert_eq!(parse_log_level("TEST_TRACING_LEVEL").unwrap(), Some(LevelFilter::DEBUG));
        unsafe { std::env::set_var("TEST_TRACING_LEVEL", "verbose") };
        assert!(parse_log_level("TEST_TRACING_LEVEL").is_err());
        unsafe { std::env::remove_var("TEST_TRACING_LEVEL") };
        assert_eq!(parse_log_level("TEST_TRACING_LEVEL").unwrap(), None);
    }

    #[test]
    fn parse_log_format_valid_invalid_and_missing() {
        unsafe { std::env::set_var("TEST_TRACING_FORMAT", "json") };
        assert_eq!(parse_log_format("TEST_TRACING_FORMAT").unwrap(), Some(LogFormat::Json));
        unsafe { std::env::set_var("TEST_TRACING_FORMAT", "yaml") };
        assert!(parse_log_format("TEST_TRACING_FORMAT").is_err());
        unsafe { std::env::remove_var("TEST_TRACING_FORMAT") };
        assert_eq!(parse_log_format("TEST_TRACING_FORMAT").unwrap(), None);
    }
}
