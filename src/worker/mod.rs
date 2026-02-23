pub mod adapter;
pub mod profile;
pub mod temporal;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk::Worker;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use crate::activities::DbtActivities;
use crate::config::{
    DbtTemporalConfig, RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts,
    WriteRunLog,
};
use crate::project_registry::ProjectRegistry;
use crate::worker_state::WorkerState;
use crate::workflow::DbtRunWorkflow;

// Re-export commonly used items for external callers.
pub use self::adapter::build_artifact_store;
pub use self::profile::rebuild_adapter_engine_with_env;

/// Build a fully configured Temporal worker (without starting it).
///
/// Returns the worker ready to call `.run()`. Separated from `run_worker`
/// so integration tests can spawn the worker in a background task.
///
#[allow(clippy::large_futures, clippy::significant_drop_tightening)]
// Large futures from SDK initialization; drop suggestion is a false positive on core_worker.
pub async fn build_worker(config: &DbtTemporalConfig) -> Result<Worker> {
    build_worker_with_auth(config, None).await
}

/// Build a worker with an optional auth override for the adapter engine.
///
/// When `auth_override` is `Some`, the adapter engine uses the provided `Auth`
/// implementation instead of the default one derived from the profile type.
/// This allows tests to redirect SQL execution to a different backend.
#[allow(clippy::large_futures, clippy::significant_drop_tightening)]
// Large futures from SDK initialization; drop suggestion is a false positive on core_worker.
pub async fn build_worker_with_auth(
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<Worker> {
    // Resolve project sources: local paths pass through, remote URLs are fetched.
    let project_dirs = resolve_project_sources(&config.dbt_project_dirs).await?;

    let registry = build_project_registry(&project_dirs, config, auth_override.clone()).await?;
    let artifact_store = if config.write_artifacts {
        Some(build_artifact_store(config)?)
    } else {
        None
    };

    let worker = connect_and_register(config, Arc::new(registry), artifact_store).await?;

    Ok(worker)
}

/// Partition project source entries into local paths and remote URLs.
/// Fetches remote sources and merges all resolved directories.
async fn resolve_project_sources(entries: &[String]) -> Result<Vec<std::path::PathBuf>> {
    let mut project_dirs = Vec::new();

    for entry in entries {
        if crate::config::is_remote_source(entry) {
            info!(url = %entry, "fetching dbt projects from remote source");
            let dirs = crate::model_store::fetch_models(entry).await?;
            project_dirs.extend(dirs);
        } else {
            project_dirs.push(std::path::PathBuf::from(entry));
        }
    }

    Ok(project_dirs)
}

#[allow(clippy::large_futures)] // Each project initialization loads dbt manifests, producing large futures.
async fn build_project_registry(
    project_dirs: &[std::path::PathBuf],
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<ProjectRegistry> {
    let mut projects = BTreeMap::new();

    for project_dir in project_dirs {
        let state = initialize_project(project_dir, config, auth_override.clone())
            .await
            .with_context(|| format!("initializing dbt project at {}", project_dir.display()))?;
        let name = state.project_name.clone();
        if projects.contains_key(&name) {
            anyhow::bail!("duplicate project name '{}' (from {})", name, project_dir.display());
        }
        info!(project = %name, dir = %project_dir.display(), "loaded project");
        projects.insert(name, Arc::new(state));
    }

    let registry = ProjectRegistry::new(projects);
    let names = registry.project_names();
    info!(projects = ?names, "project registry built ({} project(s))", names.len());

    Ok(registry)
}

/// Connect to Temporal and register workflows/activities on a new worker.
#[allow(clippy::significant_drop_tightening)] // False positive: core_worker is moved into Arc, not dropped.
pub async fn connect_and_register(
    config: &DbtTemporalConfig,
    registry: Arc<ProjectRegistry>,
    artifact_store: Option<Arc<dyn crate::artifact_store::ArtifactStore>>,
) -> Result<Worker> {
    // Set up Temporal runtime
    let telemetry_options = TelemetryOptions::builder().build();
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(telemetry_options)
        .build()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let runtime = CoreRuntime::new_assume_tokio(runtime_options)?;

    // Connect to Temporal (new Connection + Client API)
    let tls_options = temporal::build_tls_options(config)?;
    let mut conn_opts = ConnectionOptions::new(
        Url::parse(&config.temporal_address).context("parsing TEMPORAL_ADDRESS as URL")?,
    )
    .identity(format!("dbt-temporal-worker@{}", std::process::id()))
    .build();

    if let Some(tls) = tls_options {
        conn_opts.tls_options = Some(tls);
    }
    if let Some(ref api_key) = config.temporal_api_key {
        conn_opts.api_key = Some(api_key.clone());
    }

    let connection = Connection::connect(conn_opts)
        .await
        .context("connecting to Temporal server")?;

    let client = Client::new(connection, ClientOptions::new(&config.temporal_namespace).build())
        .map_err(|e| anyhow::anyhow!("creating Temporal client: {e}"))?;

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
    };

    // Build worker options and create worker
    let worker_options = temporal::build_worker_options(config);
    let mut worker = Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("creating Temporal worker: {e}"))?;

    // Register activities and workflow on the worker
    worker.register_activities(activities);
    worker.register_workflow::<DbtRunWorkflow>();

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

/// Build and run the Temporal worker.
#[allow(clippy::future_not_send, clippy::large_futures)]
// future_not_send: Temporal SDK Worker is !Send by design.
// large_futures: build_worker returns a large future from SDK initialization.
pub async fn run_worker(config: DbtTemporalConfig) -> Result<()> {
    let mut worker = build_worker(&config).await?;

    // Start health file tracker if configured.
    let health_path = config
        .health_file
        .as_deref()
        .or_else(|| config.health_port.map(|_| "/tmp/health"))
        .map(std::path::PathBuf::from);

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

    info!(
        task_queue = %config.temporal_task_queue,
        namespace = %config.temporal_namespace,
        "dbt-temporal worker started"
    );

    worker.run().await.context("worker exited with error")
}

/// Load and parse a single dbt project, returning initialized WorkerState.
#[allow(clippy::too_many_lines, clippy::large_futures)]
// Loads dbt config, profile, manifest, and resolver state — sequential initialization steps.
pub async fn initialize_project(
    project_dir: &std::path::Path,
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<WorkerState> {
    use std::path::PathBuf;

    use dbt_common::cancellation::CancellationTokenSource;
    use dbt_common::io_args::IoArgs;
    use dbt_jinja_utils::invocation_args::InvocationArgs;
    use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
    use dbt_loader::args::LoadArgs;
    use dbt_parser::args::ResolveArgs;
    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::state::{
        GetColumnsInRelationCalls, GetRelationCalls, Macros, PatternedDanglingSources,
    };

    let project_dir = PathBuf::from(project_dir);
    let profiles_dir = config
        .dbt_profiles_dir
        .as_ref()
        .map_or_else(|| project_dir.clone(), PathBuf::from);
    // Use a temp directory for dbt-fusion's resolve output so the project directory
    // stays read-only (important for baked-in Docker images with read-only filesystems).
    let out_dir = std::env::temp_dir().join(format!("dbtt-target-{}", uuid::Uuid::new_v4()));

    info!(project_dir = %project_dir.display(), "loading dbt project");

    let io = IoArgs {
        invocation_id: uuid::Uuid::new_v4(),
        in_dir: project_dir.clone(),
        out_dir: out_dir.clone(),
        ..Default::default()
    };

    let profiles_path = profiles_dir.join("profiles.yml");

    let load_args = LoadArgs {
        io: io.clone(),
        profiles_dir: Some(profiles_dir),
        target: config.dbt_target.clone(),
        install_deps: false,
        version_check: false,
        ..Default::default()
    };

    let invocation_args = InvocationArgs::default();
    let cts = CancellationTokenSource::new();
    let token = cts.token();

    // Load the project: reads dbt_project.yml, profiles.yml, packages, etc.
    let (dbt_state, _cloud_config) = dbt_loader::load(&load_args, &invocation_args, &token)
        .await
        .map_err(|e| anyhow::anyhow!("dbt load failed: {e}"))?;
    let dbt_state = Arc::new(dbt_state);

    let project_name = dbt_state
        .packages
        .first()
        .map_or_else(|| "unknown".to_string(), |p| p.dbt_project.name.clone());

    info!(project = %project_name, "project loaded, resolving nodes");

    // Resolve: parses all nodes, macros, refs, sources — builds the full DAG.
    let resolve_args = ResolveArgs {
        io: io.clone(),
        ..Default::default()
    };

    let listener_factory = Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default());

    let (resolver_state, jinja_env) = dbt_parser::resolver::resolve(
        &resolve_args,
        &invocation_args,
        Arc::clone(&dbt_state),
        Macros::default(),
        Nodes::default(),
        GetRelationCalls::default(),
        GetColumnsInRelationCalls::default(),
        PatternedDanglingSources::default(),
        &token,
        listener_factory,
    )
    .await
    .map_err(|e| anyhow::anyhow!("dbt resolve failed: {e}"))?;

    let node_count = resolver_state.nodes.iter().count();
    info!(project = %project_name, nodes = node_count, "project resolved");

    // Capture compiled SQL and snapshot raw SQL into in-memory caches.
    // This lets activities use ephemeral temp dirs instead of sharing target/.
    let (compiled_sql_cache, snapshot_sql_cache, test_sql_cache) =
        build_sql_caches(&resolver_state.nodes, &out_dir);
    info!(
        project = %project_name,
        compiled = compiled_sql_cache.len(),
        snapshots = snapshot_sql_cache.len(),
        tests = test_sql_cache.len(),
        "SQL caches populated"
    );

    // Clean up the resolve output — everything is in memory now.
    if let Err(e) = std::fs::remove_dir_all(&out_dir) {
        tracing::warn!(path = %out_dir.display(), error = %e, "failed to clean up resolve output");
    }

    // Build adapter engine from profile, using resolved quoting from the project.
    let adapter_engine = adapter::build_adapter_engine(
        &dbt_state.dbt_profile.db_config,
        resolver_state.root_project_quoting,
        &token,
        auth_override.clone(),
    )?;

    // Collect package names for Jinja context.
    let packages: std::collections::BTreeSet<String> = dbt_state
        .packages
        .iter()
        .map(|p| p.dbt_project.name.clone())
        .collect();

    // Load project config (hooks + retry) from dbt_temporal.yml (if present).
    let (default_hooks, default_retry) = crate::hooks::load_project_config(&project_dir)
        .with_context(|| format!("loading config from {}", project_dir.display()))?;

    // Detect whether profiles.yml uses env_var() — gates per-workflow adapter rebuilding.
    let uses_env_vars = profile::profile_uses_env_vars(&profiles_path);

    let profile_name_in_project = dbt_state.dbt_profile.profile.clone();
    let default_target = dbt_state.dbt_profile.target.clone();

    if uses_env_vars {
        info!(
            project = %project_name,
            "profiles.yml uses env_var() — per-workflow adapter rebuilding enabled"
        );
    }

    let materialization_resolver =
        Arc::new(dbt_schemas::materialization_resolver::MaterializationResolver::new(
            &resolver_state.macros.macros,
            resolver_state.adapter_type,
            &resolver_state.root_project_name,
        ));

    // Capture the profile's startup schema/database for per-workflow patching.
    let default_schema = dbt_state.dbt_profile.schema.clone();
    let default_database = dbt_state.dbt_profile.database.clone();

    // Detect custom generate_schema_name/generate_database_name overrides.
    let has_custom_schema_name_macro = resolver_state
        .macros
        .macros
        .contains_key(&format!("macro.{project_name}.generate_schema_name"))
        || resolver_state
            .macros
            .macros
            .contains_key(&format!("macro.{project_name}.generate_database_name"));
    if has_custom_schema_name_macro && uses_env_vars {
        tracing::warn!(
            project = %project_name,
            "project overrides generate_schema_name or generate_database_name — \
             per-workflow env overrides may not correctly patch `this.schema`/`this.database` \
             context variables (the `target` global IS correct)"
        );
    }

    Ok(WorkerState {
        project_name,
        resolver_state: Arc::new(resolver_state),
        jinja_env,
        adapter_engine,
        io_args: io,
        packages,
        default_hooks,
        default_retry,
        profiles_path,
        profile_name_in_project,
        default_target,
        profile_uses_env_vars: uses_env_vars,
        compiled_sql_cache,
        snapshot_sql_cache,
        test_sql_cache,
        materialization_resolver,
        default_schema,
        default_database,
        has_custom_schema_name_macro,
        cancellation_source: cts,
        auth_override,
    })
}

/// Capture compiled SQL and snapshot raw SQL from the target directory into memory.
///
/// After the resolve step, dbt-fusion has written compiled SQL to `out_dir/compiled/`
/// and snapshot SQL to `out_dir/`. We read these into BTreeMaps keyed by the node's
/// `common_attr.path` so activities can write them into ephemeral temp dirs.
fn build_sql_caches(
    nodes: &dbt_schemas::schemas::Nodes,
    out_dir: &std::path::Path,
) -> (BTreeMap<String, String>, BTreeMap<String, String>, BTreeMap<String, String>) {
    use dbt_schemas::schemas::telemetry::NodeType;

    let mut compiled_sql_cache = BTreeMap::new();
    let mut snapshot_sql_cache = BTreeMap::new();
    let mut test_sql_cache = BTreeMap::new();

    for (_unique_id, node) in nodes.iter() {
        let path = node.common().path.to_string_lossy().to_string();

        // Compiled SQL: out_dir/compiled/<path>
        let compiled_path = out_dir.join("compiled").join(&path);
        if let Ok(sql) = std::fs::read_to_string(&compiled_path) {
            compiled_sql_cache.insert(path.clone(), sql);
        }

        // Snapshot raw SQL: out_dir/<path>
        if node.resource_type() == NodeType::Snapshot {
            let snapshot_path = out_dir.join(&path);
            if let Ok(sql) = std::fs::read_to_string(&snapshot_path) {
                snapshot_sql_cache.insert(path.clone(), sql);
            }
        }

        // Generic test generated SQL: out_dir/<path>
        if node.resource_type() == NodeType::Test {
            let test_path = out_dir.join(&path);
            if let Ok(sql) = std::fs::read_to_string(&test_path) {
                test_sql_cache.insert(path, sql);
            }
        }
    }

    (compiled_sql_cache, snapshot_sql_cache, test_sql_cache)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_project_sources_local_only() -> Result<()> {
        let tmp = std::env::temp_dir().join(format!("dbtt-resolve-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;
        std::fs::write(tmp.join("dbt_project.yml"), "name: test")?;

        let entries = vec![tmp.to_string_lossy().to_string()];
        let dirs = resolve_project_sources(&entries).await?;
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0], tmp);

        std::fs::remove_dir_all(&tmp)?;
        Ok(())
    }

    #[tokio::test]
    async fn resolve_project_sources_empty() -> Result<()> {
        let entries: Vec<String> = vec![];
        let dirs = resolve_project_sources(&entries).await?;
        assert!(dirs.is_empty());
        Ok(())
    }
}
