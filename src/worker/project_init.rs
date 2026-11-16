use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

use crate::config::DbtTemporalConfig;
use crate::worker_state::WorkerState;

use super::sql_caches::build_sql_caches;
use super::{adapter, defer_cache, profile, project_checks, relation_cache, schemas};

/// Load and parse a single dbt project, returning initialized WorkerState.
///
/// Runs under an `Invocation` root span: loading a project is a dbt invocation
/// in its own right, and dbt's telemetry data layer requires every span it sees
/// to descend from one.
pub async fn initialize_project(
    project_dir: &std::path::Path,
    config: &DbtTemporalConfig,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<WorkerState> {
    use tracing::Instrument as _;

    let span = dbt_common::tracing::create_root_info_span(dbt_telemetry::Invocation {
        invocation_id: uuid::Uuid::new_v4().to_string(),
        raw_command: "dbt parse".to_owned(),
        eval_args: None,
        process_info: None,
        metrics: None,
        parent_span_id: None,
    });
    // Boxed: the loader future is ~44KB, and this is called once per project at
    // startup, so the heap allocation is free next to keeping it on the stack.
    Box::pin(initialize_project_inner(project_dir, config, auth_override).instrument(span)).await
}

#[allow(clippy::too_many_lines, clippy::large_futures)]
// Loads dbt config, profile, manifest, and resolver state — sequential initialization steps.
async fn initialize_project_inner(
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
    let dbt_state = dbt_loader::load(
        &load_args,
        std::borrow::Cow::Borrowed(&invocation_args),
        None,
        &token,
        Arc::new(dbt_loader::loader_hooks::NoOpLoaderHooks),
        // Private `packages.yml` entries resolve to plain SSH clone URLs, so git
        // authenticates with the provider directly. The alternative resolver
        // mints credentials through dbt platform, which a self-hosted worker
        // has no session for.
        Arc::new(fs_deps::private_package::LocalPrivatePackageResolver),
    )
    .await
    .context("dbt load")?;
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

    // dbt caches which template answers `generate_schema_name` and its siblings
    // in a process-global map keyed by (dialect, package, macro) — with no
    // project in the key, because the CLI only ever loads one. A worker loads
    // several: without this, a package two projects share resolves to whichever
    // project's macro was cached first, so the second project either fails to
    // load ("Template not found") or silently names its relations with the
    // other project's rules. The cache is only read while resolving, and
    // projects resolve one at a time, so emptying it here isolates them.
    dbt_jinja_utils::utils::clear_template_cache();

    let (resolver_state, jinja_env) = dbt_parser::resolver::resolve(
        &resolve_args,
        &invocation_args,
        Arc::clone(&dbt_state),
        Macros::default(),
        Nodes::default(),
        // disabled_nodes: empty on a cold parse. Upstream threads this through
        // so incremental re-resolves can carry disabled nodes forward; the
        // worker always parses each project from scratch at startup.
        Nodes::default(),
        GetRelationCalls::default(),
        GetColumnsInRelationCalls::default(),
        PatternedDanglingSources::default(),
        &token,
        listener_factory,
        Arc::new(dbt_parser::resolver_hooks::NoOpResolverHooks),
        Arc::new(dbt_jinja_utils::DefaultJinjaFactory),
    )
    .await
    .context("dbt resolve")?;

    let node_count = resolver_state.nodes.iter().count();
    info!(project = %project_name, nodes = node_count, "project resolved");

    // Capture compiled SQL and snapshot raw SQL into in-memory caches.
    // This lets activities use ephemeral temp dirs instead of sharing target/.
    let (compiled_sql_cache, snapshot_sql_cache, test_sql_cache) =
        build_sql_caches(&resolver_state.nodes, &project_dir, &out_dir)?;
    info!(
        project = %project_name,
        compiled = compiled_sql_cache.len(),
        snapshots = snapshot_sql_cache.len(),
        tests = test_sql_cache.len(),
        "SQL caches populated"
    );

    // Write the project-check metadata before the resolve output goes: the epochs
    // are written from the in-memory resolved state into a directory this
    // worker owns, so the two are independent, but keeping them adjacent means
    // one place decides what survives the parse.
    //
    // A failure here is fatal rather than degraded. The alternative is a worker
    // that starts with no metadata and reports every check as unevaluable on every
    // run — a gate that never passes is worse than one that never starts.
    let project_checks = project_checks::build(&io, &dbt_state, &resolver_state)
        .context("writing the project-check metadata")?;

    // Clean up the resolve output — everything is in memory now.
    if let Err(e) = std::fs::remove_dir_all(&out_dir) {
        tracing::warn!(path = %out_dir.display(), error = %e, "failed to clean up resolve output");
    }

    // One engine per adapter the active target declares, so a node's `+adapter`
    // selection reaches the warehouse it names rather than the default's.
    // Declaration order comes straight from the profile's `IndexMap`, and only
    // an adapter's default connection is reachable.
    let target_configs: Vec<dbt_schemas::schemas::profiles::DbConfig> = dbt_state
        .dbt_profile
        .adapters
        .values()
        .map(|adapter| adapter.config().clone())
        .collect();
    // Query comment, behaviour flags and threads come from the loaded project;
    // an engine built without them silently runs with dbt's defaults replaced
    // by the adapter's.
    let adapter_settings = adapter::AdapterSettings::from_state(&dbt_state);
    let adapter_engines = adapter::build_adapter_engines(
        &target_configs,
        dbt_state.dbt_profile.default_adapter,
        resolver_state.root_project_quoting,
        &adapter_settings,
        auth_override.as_ref(),
    )?;
    if target_configs.len() > 1 {
        info!(
            project = %project_name,
            adapters = ?adapter_engines,
            "target declares several adapters — nodes route by their `+adapter` selection"
        );
    }

    // Collect package names for Jinja context.
    let packages: std::collections::BTreeSet<String> = dbt_state
        .packages
        .iter()
        .map(|p| p.dbt_project.name.clone())
        .collect();

    // Load project config (hooks + retry) from dbt_temporal.yml (if present).
    let (default_hooks, default_retry, default_timeouts) =
        crate::hooks::load_project_config(&project_dir)
            .with_context(|| format!("loading config from {}", project_dir.display()))?;

    // Pre-compile non-retryable-error regexes; invalid patterns log a warning and are dropped.
    let non_retryable_error_patterns =
        crate::error::compile_error_patterns(&default_retry.non_retryable_errors);

    // Which env vars profiles.yml reads — gates per-workflow adapter rebuilding.
    let profile_env_vars = profile::profile_env_vars(&profiles_path);

    let profile_name_in_project = dbt_state.dbt_profile.profile.clone();
    let default_target = dbt_state.dbt_profile.target.clone();

    match &profile_env_vars {
        profile::ProfileEnvVars::Keys(keys) if !keys.is_empty() => info!(
            project = %project_name,
            keys = ?keys,
            "profiles.yml reads env vars — a workflow overriding one of these rebuilds its adapters"
        ),
        profile::ProfileEnvVars::Unknown => info!(
            project = %project_name,
            "profiles.yml reads env vars this worker cannot name — every workflow \
             supplying overrides rebuilds its adapters"
        ),
        profile::ProfileEnvVars::Keys(_) => {}
    }

    let materialization_resolver =
        Arc::new(dbt_schemas::materialization_resolver::MaterializationResolver::new(
            &resolver_state.macros.macros,
            &resolver_state.root_project_name,
        ));

    // Capture the profile's startup schema/database for per-workflow patching.
    let default_schema = dbt_state.dbt_profile.schema.clone();
    let default_database = dbt_state.dbt_profile.database.clone();

    // Detect custom generate_schema_name/generate_database_name overrides.
    // Separately: a project overriding only one of them still wants dbt's own
    // pattern for the other, and running the missing macro would fail.
    let defines_macro = |name: &str| {
        resolver_state
            .macros
            .macros
            .contains_key(&format!("macro.{project_name}.{name}"))
    };
    let has_custom_schema_name_macro = defines_macro("generate_schema_name");
    let has_custom_database_name_macro = defines_macro("generate_database_name");
    if has_custom_schema_name_macro || has_custom_database_name_macro {
        info!(
            project = %project_name,
            schema_name_macro = has_custom_schema_name_macro,
            database_name_macro = has_custom_database_name_macro,
            "project overrides a dbt naming macro — per-workflow env overrides \
             re-execute it per node at activity time (env_var() reads and \
             target.schema both reflect the workflow env)"
        );
    }

    // Derived from the resolved project, so it cannot change while the worker
    // lives — so it is computed once here rather than by every microbatch
    // activity scanning every model.
    let event_time_columns = Arc::new(
        resolver_state
            .nodes
            .models
            .iter()
            .filter_map(|(unique_id, model)| {
                model
                    .deprecated_config
                    .event_time
                    .clone()
                    .map(|column| (unique_id.clone(), column))
            })
            .collect::<BTreeMap<_, _>>(),
    );

    Ok(WorkerState {
        defer_manifests: defer_cache::DeferManifestCache::default(),
        rebuilt_engines: profile::RebuildCache::default(),
        naming: relation_cache::NamingCache::default(),
        created_schemas: schemas::CreatedSchemas::default(),
        adapter_settings,
        event_time_columns,
        project_name,
        resolver_state: Arc::new(resolver_state),
        jinja_env,
        adapter_engines,
        io_args: io,
        packages,
        default_hooks,
        default_retry,
        default_timeouts,
        non_retryable_error_patterns,
        profiles_path,
        profile_name_in_project,
        default_target,
        profile_env_vars,
        override_policy: config.override_policy.clone(),
        compiled_sql_cache,
        snapshot_sql_cache,
        test_sql_cache,
        materialization_resolver,
        default_schema,
        default_database,
        has_custom_schema_name_macro,
        has_custom_database_name_macro,
        cancellation_source: cts,
        auth_override,
        project_checks,
        manifest: crate::activities::plan::ManifestCache::default(),
    })
}
