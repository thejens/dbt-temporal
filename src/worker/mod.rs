pub mod adapter;
pub mod defer_cache;
pub mod engines;
pub mod profile;
pub mod project_checks;
mod project_init;
mod projects;
pub mod relation_cache;
pub mod schemas;
pub mod shutdown;
pub mod single_flight;
mod sql_caches;
pub mod temporal;

use std::sync::Arc;

use anyhow::{Context, Result};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_sdk::runtime::RuntimeOptions;
use temporalio_sdk::{Runtime, Worker};
use tracing::info;

use crate::activities::DbtActivities;
use crate::config::{
    DbtTemporalConfig, PriorityScheduling, RegisteredSearchAttributes, SearchAttributeConfig,
    WriteArtifacts, WriteCatalog, WriteRunLog,
};
use crate::project_registry::ProjectRegistry;
use crate::workflow::DbtRunWorkflow;

// `worker` is `pub mod`, so `pub use` here would leak these out of the
// crate. The clippy lint mistakenly assumes the parent is private.
pub(crate) use self::adapter::build_artifact_store;
pub(crate) use self::profile::rebuild_adapter_engines_shared;
pub use self::project_init::initialize_project;
use self::projects::{build_project_registry, resolve_project_sources};

/// Build a fully configured Temporal worker (without starting it).
///
/// Returns the worker ready to call `.run()`. Separated from `run_worker`
/// so integration tests can spawn the worker in a background task.
///
#[allow(clippy::large_futures)]
// Large futures from SDK initialization; drop suggestion is a false positive on core_worker.
pub async fn build_worker(config: &DbtTemporalConfig) -> Result<Worker> {
    build_worker_with_auth(config, None).await
}

/// Build a worker with an optional auth override for the adapter engine.
///
/// When `auth_override` is `Some`, the adapter engine uses the provided `Auth`
/// implementation instead of the default one derived from the profile type.
/// This allows tests to redirect SQL execution to a different backend.
#[allow(clippy::large_futures)]
// Large futures from SDK initialization; drop suggestion is a false positive on core_worker.
pub async fn build_worker_with_auth(
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<Worker> {
    // Resolve project sources: local paths pass through, remote URLs are fetched.
    let sources = resolve_project_sources(&config.dbt_project_dirs).await?;
    let project_dirs: Vec<std::path::PathBuf> = sources
        .iter()
        .flat_map(|source| source.projects.iter().cloned())
        .collect();

    // The registry takes the sources with it: a fetched directory has to
    // outlive every project loaded from it, and nothing else lives that long.
    let registry = build_project_registry(&project_dirs, config, auth_override.clone())
        .await?
        .with_sources(sources);
    let artifact_store = if config.write_artifacts {
        Some(build_artifact_store(config)?)
    } else {
        None
    };

    let worker = connect_and_register(config, Arc::new(registry), artifact_store).await?;

    Ok(worker)
}

/// Connect to Temporal and register workflows/activities on a new worker.
pub async fn connect_and_register(
    config: &DbtTemporalConfig,
    registry: Arc<ProjectRegistry>,
    artifact_store: Option<Arc<dyn crate::artifact_store::ArtifactStore>>,
) -> Result<Worker> {
    // Set up Temporal runtime (with metrics export when configured).
    let telemetry_options = temporal::build_telemetry_options(config)?;
    // RuntimeOptionsBuilder::build returns Result<_, String>; String doesn't
    // impl std::error::Error, so .context() can't wrap it.
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(telemetry_options)
        .build()
        .map_err(|e| anyhow::anyhow!("building Temporal runtime options: {e}"))?;
    let runtime = Runtime::from_current_tokio(runtime_options)?;

    // Connect to Temporal (new Connection + Client API)
    let tls_options = temporal::build_tls_options(config)?;
    // Auto-prefix `http://` for scheme-less addresses to match the Temporal CLI's
    // `--address localhost:7233` ergonomics. Without this, Url::parse rejects the
    // bare host:port form with the misleading "target URL has no host" error.
    let temporal_url = temporal::normalize_address(&config.temporal_address)
        .context("parsing TEMPORAL_ADDRESS as URL")?;
    let mut conn_opts = ConnectionOptions::new(temporal_url)
        .identity(temporal::worker_identity())
        .build();

    if let Some(tls) = tls_options {
        conn_opts.tls_options = Some(tls);
    }
    if let Some(ref api_key) = config.temporal_api_key {
        conn_opts.api_key = Some(api_key.expose().to_string());
    }

    let connection = Connection::connect(conn_opts)
        .await
        .context("connecting to Temporal server")?;

    let client = Client::new(connection, ClientOptions::new(&config.temporal_namespace).build())
        .context("creating Temporal client")?;

    // List registered search attributes so we can skip unregistered ones at runtime.
    let registered_attrs =
        list_registered_search_attributes(&client, &config.temporal_namespace).await;

    // Build activity struct with all shared state.
    let activities = DbtActivities {
        registry,
        artifact_store,
        search_attr_config: SearchAttributeConfig(config.search_attributes.clone()),
        registered_attrs: RegisteredSearchAttributes(registered_attrs),
        write_run_log: WriteRunLog(config.write_run_log),
        write_artifacts: WriteArtifacts(config.write_artifacts),
        write_catalog: WriteCatalog(config.write_catalog),
        priority_scheduling: PriorityScheduling(config.priority_scheduling),
    };

    // Registrations belong to the options, not the built worker: the worker
    // derives which task types to poll from what is registered, so it has to
    // know before it is constructed.
    let mut worker_options = temporal::build_worker_options(config);
    worker_options.register_activities(activities);
    worker_options
        .register_workflow::<DbtRunWorkflow>()
        .map_err(|e| anyhow::anyhow!("registering DbtRunWorkflow: {e}"))?;

    // Worker::new returns Box<dyn Error>; not Send+Sync, so can't .context().
    let worker = Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("creating Temporal worker: {e}"))?;

    Ok(worker)
}

/// Query the Temporal namespace for registered search attribute names.
///
/// Returns the set of registered custom + system attribute names. If the call
/// fails (e.g. operator service unavailable), logs a warning and returns an
/// empty set so the worker still starts — search attributes will be skipped.
async fn list_registered_search_attributes(
    client: &Client,
    namespace: &str,
) -> std::collections::BTreeSet<String> {
    use temporalio_client::tonic::IntoRequest;

    let connection = client.connection();
    let mut op_svc = connection.operator_service();
    let req =
        temporalio_common::protos::temporal::api::operatorservice::v1::ListSearchAttributesRequest {
            namespace: namespace.to_owned(),
        };
    match op_svc.list_search_attributes(req.into_request()).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            let mut names: std::collections::BTreeSet<String> =
                inner.custom_attributes.into_keys().collect();
            names.extend(inner.system_attributes.into_keys());
            info!(registered = ?names, "discovered search attributes on namespace");
            names
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "failed to list search attributes — attribute upserts will be skipped \
                 (register attributes on the namespace to enable them)"
            );
            std::collections::BTreeSet::new()
        }
    }
}

/// Startup warning to emit when `NEXUS_ENABLED` is set, or `None`. Extracted so
/// the gating is unit-testable without starting a worker. The Temporal Rust SDK
/// can't serve Nexus operation handlers yet, so the flag is currently a no-op —
/// see [`crate::config::DbtTemporalConfig::nexus_enabled`].
fn nexus_unsupported_warning(nexus_enabled: bool) -> Option<&'static str> {
    nexus_enabled.then_some(
        "NEXUS_ENABLED is set, but the Temporal Rust SDK does not yet support serving Nexus \
         operation handlers — the flag is reserved and currently has no effect. Until the SDK \
         ships handler support, expose dbt_run cross-namespace via a Nexus endpoint backed by a \
         handler in an SDK that supports it (Go/Java/TS/Python).",
    )
}

/// Resolve the health-file path when health checks are enabled.
///
/// An explicit `HEALTH_FILE` wins; otherwise a configured `HEALTH_PORT` implies
/// the default `/tmp/health` (the readiness probe needs a file to watch);
/// otherwise health tracking is off.
fn resolve_health_path(
    health_file: Option<&str>,
    health_port: Option<u16>,
) -> Option<std::path::PathBuf> {
    health_file
        .map(std::path::PathBuf::from)
        .or_else(|| health_port.map(|_| std::path::PathBuf::from("/tmp/health")))
}

/// Build and run the Temporal worker.
#[allow(clippy::future_not_send, clippy::large_futures)]
// future_not_send: Temporal SDK Worker is !Send by design.
// large_futures: build_worker returns a large future from SDK initialization.
pub async fn run_worker(config: DbtTemporalConfig) -> Result<()> {
    // Sized before the worker polls, so the first activity already sees the cap.
    crate::dbt_pool::set_capacity(
        std::num::NonZeroUsize::new(config.worker_tuning.max_activity_slots())
            .unwrap_or(std::num::NonZeroUsize::MIN),
    );
    let worker = build_worker(&config).await?;

    // Start health file tracker if configured.
    let health_path = resolve_health_path(config.health_file.as_deref(), config.health_port);

    let _health_touch = if let Some(ref path) = health_path {
        // Touch once synchronously so readiness probes pass immediately.
        crate::health::touch(path)
            .await
            .context("initial health file touch")?;
        Some(crate::health::spawn_health_touch(path.clone()))
    } else {
        None
    };

    let _health_server = if let Some(port) = config.health_port {
        let path = health_path
            .clone()
            .context("health_path must be set when health_port is configured")?;
        Some(crate::health::spawn_health_server(port, path))
    } else {
        None
    };

    if let Some(msg) = nexus_unsupported_warning(config.nexus_enabled) {
        tracing::warn!("{msg}");
    }

    // Installed only now, not before the (possibly long) project load above:
    // a handler replaces SIGINT's default action, and Ctrl-C during startup
    // should still just exit.
    let signals = shutdown::ShutdownSignals::install()?;

    info!(
        task_queue = %config.temporal_task_queue,
        namespace = %config.temporal_namespace,
        "dbt-temporal worker started"
    );

    shutdown::run(worker, signals).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn nexus_unsupported_warning_gated_on_flag() {
        assert!(nexus_unsupported_warning(false).is_none());
        assert!(nexus_unsupported_warning(true).is_some_and(|m| m.contains("NEXUS_ENABLED")));
    }

    #[test]
    fn resolve_health_path_precedence() {
        use std::path::PathBuf;
        // No health config → off.
        assert_eq!(resolve_health_path(None, None), None);
        // Explicit file wins.
        assert_eq!(
            resolve_health_path(Some("/var/run/health"), None),
            Some(PathBuf::from("/var/run/health"))
        );
        // A port with no explicit file implies the default path.
        assert_eq!(resolve_health_path(None, Some(8080)), Some(PathBuf::from("/tmp/health")));
        // Explicit file wins even when a port is also set.
        assert_eq!(
            resolve_health_path(Some("/custom"), Some(8080)),
            Some(PathBuf::from("/custom"))
        );
    }
}
