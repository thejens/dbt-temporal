use std::time::Duration;

use anyhow::{Context, Result};

mod activities;
mod artifact_store;
mod config;
mod error;
mod health;
mod hooks;
mod model_store;
mod project_registry;
mod telemetry_compat;
mod tracing_setup;
mod types;
mod worker;
mod worker_state;
mod workflow;

#[tokio::main]
#[allow(clippy::large_futures)]
async fn main() -> Result<()> {
    // `dbt-temporal healthcheck` — exit 0 if HEALTH_FILE is fresh, 1 otherwise.
    // Designed for use as a Kubernetes exec liveness probe (no shell needed).
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let path = std::env::var("HEALTH_FILE").unwrap_or_else(|_| "/tmp/health".to_string());
        let meta = std::fs::metadata(&path)?;
        let age = meta.modified()?.elapsed().unwrap_or(Duration::MAX);
        if age < Duration::from_mins(1) {
            std::process::exit(0);
        }
        std::process::exit(1);
    }

    // Default stack, or dbt-fusion's telemetry pipeline with OTLP export when
    // DBT_EXPORT_TO_OTLP=1 (see tracing_setup for the trade-offs).
    let telemetry_handle = tracing_setup::init().context("initializing tracing")?;

    let config = config::DbtTemporalConfig::from_env().context("configuration error")?;

    let result = worker::run_worker(config).await;

    // Flush buffered OTLP batches before the process exits; telemetry loss on
    // shutdown is silent otherwise. eprintln because the telemetry pipeline
    // itself is what failed here — tracing output may no longer reach anything.
    #[allow(clippy::print_stderr)]
    if let Some(handle) = telemetry_handle
        && let Err(errors) = handle.shutdown_once()
    {
        for e in errors {
            eprintln!("telemetry shutdown error: {e}");
        }
    }

    result
}
