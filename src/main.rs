use std::time::Duration;

use anyhow::Result;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod activities;
mod artifact_store;
mod config;
mod error;
mod health;
mod hooks;
mod model_store;
mod project_registry;
mod telemetry_compat;
mod types;
mod worker;
mod worker_state;
mod workflow;

#[tokio::main]
#[allow(clippy::large_futures, clippy::unwrap_used)]
async fn main() -> Result<()> {
    // `dbt-temporal healthcheck` — exit 0 if HEALTH_FILE is fresh, 1 otherwise.
    // Designed for use as a Kubernetes exec liveness probe (no shell needed).
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let path = std::env::var("HEALTH_FILE").unwrap_or_else(|_| "/tmp/health".to_string());
        let meta = std::fs::metadata(&path)?;
        let age = meta.modified()?.elapsed().unwrap_or(Duration::MAX);
        if age < Duration::from_secs(60) {
            std::process::exit(0);
        }
        std::process::exit(1);
    }

    // Initialize tracing with a compatibility layer for dbt-fusion's telemetry.
    // dbt-fusion expects TelemetryAttributes in span extensions; without this layer,
    // adapter calls panic with "Missing span event attributes".
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                // The Rust SDK doesn't implement query handlers — it logs an error for
                // every query (e.g. __stack_trace from the Temporal UI) and drops it.
                // Suppress the module entirely; its other warnings (WFT failures, panics)
                // are already surfaced in the Temporal UI.
                .add_directive("temporalio_sdk::workflow_future=off".parse().unwrap()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_compat::DbtTelemetryCompatLayer)
        .init();

    let config = config::DbtTemporalConfig::from_env()
        .map_err(|e| anyhow::anyhow!("configuration error: {e}"))?;

    worker::run_worker(config).await
}
