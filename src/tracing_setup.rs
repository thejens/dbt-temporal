//! Process-wide tracing initialization.
//!
//! Every stack goes through dbt's own `init_tracing`, because dbt's data layer
//! only works when that function installed it: the layer resolves a span's root
//! through a process span whose id lives in a private `OnceLock` that nothing
//! else can set, and it reads span start info and `TelemetryAttributes` back out
//! of span extensions in types private to `dbt-tracing`. Rendering dbt Jinja
//! under any other subscriber panics.
//!
//! Two mutually exclusive stacks, chosen at startup:
//!
//! - **Default**: dbt's [`TelemetryDataLayer`] with no middlewares and no
//!   consumers — it records the state dbt reads back and exports nothing —
//!   composed with our own `fmt` layer for console output. The console layer
//!   carries its own `EnvFilter`, so `RUST_LOG` still selects what is printed.
//! - **OTLP** (`DBT_EXPORT_TO_OTLP=1`): dbt-fusion's own telemetry pipeline
//!   (the same data layer + console output + OTLP trace/log export), assembled
//!   by `FsTraceConfig::init`. dbt's structured events (`QueryExecuted`,
//!   `ConnectionLimitWait`, …) export as OTEL spans. The export endpoint comes
//!   from the standard `OTEL_EXPORTER_OTLP_ENDPOINT` /
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`
//!   env vars (read by the upstream layer; OTLP over HTTP, collector port 4318).
//!
//! Either stack registers a global subscriber, so `init` must be called exactly
//! once, from `main`, before any project loading. Integration tests call
//! [`init_for_tests`] instead, which installs the same data layer against a
//! test writer.

use std::str::FromStr as _;

use anyhow::{Context, Result};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Layer as _};

use dbt_common::io_args::{FsCommand, LogFormat};
use dbt_common::tracing::{
    FsTraceConfig, FsTraceConfigBuilder, TelemetryHandle, dbt_data_layer_config,
    dbt_process_span_attributes, init_tracing_with_data_layer,
};
use dbt_tracing::init::BaseSubscriber;
use dbt_tracing::layer::{ConsumerLayer, MiddlewareLayer};
use dbt_tracing::layers::data_layer::TelemetryDataLayer;

/// The process span dbt's `init_tracing` opens, parked for the process
/// lifetime. Dropping it closes the span, and the data layer resolves every
/// parentless event through it — so it has to outlive every log call.
static PROCESS_SPAN: std::sync::OnceLock<tracing::Span> = std::sync::OnceLock::new();

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

/// The console stack used when OTLP export is off.
fn init_default() {
    install_data_layer(tracing_subscriber::fmt::layer().with_filter(console_filter()));
}

/// Initialize tracing for integration tests.
///
/// The same stack as [`init`]'s default path, but printing through the test
/// writer so `cargo test` captures it. Idempotent: every test can call it, and
/// only the first call installs anything.
pub fn init_for_tests() {
    install_data_layer(
        tracing_subscriber::fmt::layer()
            .with_test_writer()
            .with_filter(console_filter()),
    );
}

/// What the console prints. Applied to the console layer alone, not to the
/// subscriber, so narrowing it never starves the data layer of the spans dbt
/// reads back.
fn console_filter() -> EnvFilter {
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
        )
}

/// Install dbt's data layer, with `console` composed onto it, as the global
/// subscriber, and park the process span it opens.
///
/// The verbosity passed here caps the whole pipeline; dbt widens anything below
/// TRACE to DEBUG. `RUST_LOG` selects within that cap through `console`, so it
/// can quiet the console but cannot raise the pipeline past DEBUG.
///
/// A second call is a no-op — dbt reports the already-installed subscriber
/// rather than panicking, which is what lets every integration test call
/// [`init_for_tests`] unconditionally.
fn install_data_layer<L>(console: L)
where
    L: tracing_subscriber::Layer<BaseSubscriber> + Send + Sync + 'static,
{
    let data_layer = TelemetryDataLayer::new(
        dbt_data_layer_config(uuid::Uuid::new_v4().as_u128(), None),
        // Matches how dbt builds it: keep code location in debug builds only.
        !cfg!(debug_assertions),
        std::iter::empty::<MiddlewareLayer>(),
        std::iter::empty::<ConsumerLayer>(),
    )
    .with_filter(tracing_subscriber::filter::filter_fn(is_dbt_instrumentation));

    if let Ok(process_span) = init_tracing_with_data_layer(
        LevelFilter::INFO,
        dbt_process_span_attributes("dbt-temporal"),
        data_layer.and_then(console),
    ) {
        let _ = PROCESS_SPAN.set(process_span);
    }
}

/// Whether a span or event belongs to dbt's own instrumentation, and so belongs
/// to the data layer.
///
/// The layer is written for a process whose entire span tree is dbt's: it
/// asserts that every span it sees descends from a `create_root_info_span` root.
/// A worker's tree is not — the Temporal SDK opens `polling_task`, and this
/// crate logs outside any span at startup — so without this filter the layer
/// trips on the first foreign span. Narrowing it to dbt's crates leaves exactly
/// the spans dbt itself reads back, which is the whole reason the layer is here.
///
/// `dbt_temporal` is deliberately outside the set: our own events are console
/// output, not dbt telemetry, and many are emitted with no span open at all.
fn is_dbt_instrumentation(metadata: &tracing::Metadata<'_>) -> bool {
    let target = metadata.target();
    target.starts_with("dbt_") && !target.starts_with("dbt_temporal")
}

/// dbt-fusion's telemetry pipeline with OTLP export enabled.
fn init_dbt_pipeline() -> Result<TelemetryHandle> {
    let max_log_verbosity = parse_log_level("DBT_LOG_LEVEL")?.unwrap_or(LevelFilter::INFO);
    let log_format = parse_log_format("DBT_LOG_FORMAT")?.unwrap_or(LogFormat::Default);
    let config = fs_trace_config(max_log_verbosity, log_format);

    // The provider is derived from the config, so it has to be taken before
    // `init` consumes it.
    let config_provider = config.create_config_provider();
    config
        .init(config_provider)
        .context("initializing dbt telemetry pipeline")
}

/// The worker's `FsTraceConfig`: OTLP export on, no file sinks. The worker is
/// long-lived and serves many runs, so per-run trace correlation happens via
/// Invocation root spans opened inside activities — this process-level
/// invocation id only labels worker-lifecycle telemetry.
fn fs_trace_config(max_log_verbosity: LevelFilter, log_format: LogFormat) -> FsTraceConfig {
    // Everything left unset keeps the builder's default: no project dir (this
    // worker serves many projects, so there is no single root), no file sinks,
    // no query log, and no parent span.
    FsTraceConfigBuilder::new("dbt-temporal", "dbt-temporal")
        .with_command(FsCommand::Unset)
        .with_max_log_verbosity(max_log_verbosity)
        // No dbt.log file — worker logs go to the console or the collector.
        .with_max_file_log_verbosity(LevelFilter::OFF)
        .with_invocation_id(uuid::Uuid::new_v4())
        .with_export_to_otlp(true)
        .with_log_format(log_format)
        .build()
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
    fn init_registers_default_stack_when_otlp_is_off() {
        // The lib test binary registers no other global subscriber, so the
        // one-time global init is safe to exercise here. Must stay the only
        // test calling init() — a second registration would panic.
        unsafe { std::env::remove_var("DBT_EXPORT_TO_OTLP") };
        let handle = init().unwrap();
        assert!(handle.is_none(), "default stack returns no telemetry handle");
    }

    #[test]
    fn fs_trace_config_builds_for_all_formats() {
        // Constructing the config exercises the long positional-argument
        // call against upstream's signature; a compile-time drift in argument
        // meaning surfaces here rather than at worker startup.
        for format in [
            LogFormat::Default,
            LogFormat::Text,
            LogFormat::Json,
            LogFormat::Otel,
        ] {
            let _config = fs_trace_config(LevelFilter::DEBUG, format);
        }
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
