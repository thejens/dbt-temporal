use std::sync::Arc;

use anyhow::{Context, Result};

use crate::artifact_store::{ArtifactStore, LocalArtifactStore};
use crate::config::DbtTemporalConfig;
use crate::worker::engines::AdapterEngines;

/// The project- and profile-level execution settings an engine needs beyond
/// credentials.
///
/// dbt resolves all of these while loading the project. Building engines with
/// their defaults instead dropped every one: a project's `query_comment` never
/// reached the warehouse, so nothing on the platform could attribute a query to
/// the model that issued it (on BigQuery, `job-label` never set a label at
/// all); `flags:` behaviour overrides were ignored, so a project opting into or
/// out of an adapter behaviour got the opposite; and the target's `threads` was
/// never seen by the connection pool.
#[derive(Debug, Clone, Default)]
pub struct AdapterSettings {
    /// `query_comment:` from the root `dbt_project.yml`.
    pub query_comment: Option<dbt_schemas::schemas::project::QueryComment>,
    /// `flags:` from the root `dbt_project.yml`, reduced to the booleans the
    /// engine reads as behaviour overrides.
    pub behavior_flags: std::collections::BTreeMap<String, bool>,
    /// The active target's `threads`.
    pub threads: Option<usize>,
}

impl AdapterSettings {
    /// Read the settings out of a loaded project.
    pub fn from_state(state: &dbt_schemas::state::DbtState) -> Self {
        // The root project is the first package; the rest are its dependencies,
        // and dbt takes these settings from the root only.
        let root = state.packages.first();
        Self {
            query_comment: root.and_then(|p| (*p.dbt_project.query_comment).clone()),
            behavior_flags: root
                .and_then(|p| p.dbt_project.flags.as_ref())
                .and_then(dbt_yaml::Value::as_mapping)
                .map(|flags| {
                    flags
                        .iter()
                        .filter_map(|(key, value)| {
                            Some((key.as_str()?.to_string(), yml_flag_as_bool(value)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            threads: state.dbt_profile.threads,
        }
    }
}

/// Read one `flags:` entry as a behaviour override, the way upstream's adapter
/// factory does — a true boolean or the string "true", anything else false.
fn yml_flag_as_bool(value: &dbt_yaml::Value) -> bool {
    value.as_bool().unwrap_or_else(|| {
        value
            .as_str()
            .is_some_and(|s| s == "true" || s.parse::<bool>().unwrap_or(false))
    })
}

/// Build one engine per adapter the active target declares.
///
/// `configs` is the target's adapters in declaration order — one config each,
/// since only an adapter's default connection is reachable — and
/// `default_adapter` names the one unannotated nodes run on. Every engine gets
/// its own relation cache and connection pool, which is what keeps a node routed
/// to a non-default adapter off the default's warehouse.
pub fn build_adapter_engines(
    configs: &[dbt_schemas::schemas::profiles::DbConfig],
    default_adapter: dbt_adapter::AdapterType,
    quoting: dbt_schemas::schemas::common::ResolvedQuoting,
    settings: &AdapterSettings,
    auth_override: Option<&Arc<dyn dbt_auth::Auth>>,
) -> Result<AdapterEngines> {
    let engines = configs
        .iter()
        .map(|config| {
            let engine =
                build_adapter_engine(config, quoting, settings, auth_override.map(Arc::clone))
                    .with_context(|| {
                        format!("building the '{}' adapter engine", config.adapter_type())
                    })?;
            Ok((config.adapter_type(), engine))
        })
        .collect::<Result<Vec<_>>>()?;

    AdapterEngines::new(engines, default_adapter)
}

/// Build an AdapterEngine from a DbConfig.
pub fn build_adapter_engine(
    db_config: &dbt_schemas::schemas::profiles::DbConfig,
    quoting: dbt_schemas::schemas::common::ResolvedQuoting,
    settings: &AdapterSettings,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<Arc<dyn dbt_adapter::AdapterEngine>> {
    use dbt_adapter::adapter::adapter_factory::backend_of;
    use dbt_adapter::cache::RelationCache;
    use dbt_adapter::engine::AdbcEngine;
    use dbt_adapter::engine::query_comment::QueryCommentConfig;
    use dbt_adapter::sql_types::DefaultTypeOps;
    use dbt_adapter::stmt_splitter::DefaultStmtSplitter;

    let adapter_type = db_config.adapter_type();

    let base_auth: Arc<dyn dbt_auth::Auth> = auth_override.unwrap_or_else(|| {
        let backend = backend_of(adapter_type);
        dbt_auth::auth_for_backend(backend).into()
    });

    let mapping = db_config.to_mapping().context("serialising db config")?;

    let adapter_config = dbt_auth::AdapterConfig::new(mapping);

    let stmt_splitter: Arc<dyn dbt_adapter::stmt_splitter::StmtSplitter> =
        Arc::new(DefaultStmtSplitter);
    // `use_default = true` matches upstream's adapter factory: with no
    // `query_comment:` in the project, dbt still stamps its own comment on every
    // query, which is what makes a statement traceable back to an invocation.
    let query_comment = QueryCommentConfig::from_query_comment(
        settings.query_comment.clone(),
        adapter_type,
        true,
        None,
    );
    let type_ops: Arc<dyn dbt_adapter::sql_types::TypeOps> =
        Arc::new(DefaultTypeOps::new(adapter_type));
    let relation_cache = Arc::new(RelationCache::default());

    let engine = AdbcEngine::new(
        adapter_type,
        base_auth,
        adapter_config,
        quoting,
        query_comment,
        type_ops,
        stmt_splitter,
        relation_cache,
        settings.behavior_flags.clone(),
        settings.threads,
        None, // dbt_cloud_project_id — set by dbt platform, which a self-hosted
              // worker has no session for.
    );

    Ok(Arc::new(engine))
}

/// Build the artifact store based on config.
///
/// Cloud URLs (`gs://…`, `s3://…`) use the object_store backend.
/// Everything else is treated as a local filesystem path.
pub fn build_artifact_store(config: &DbtTemporalConfig) -> Result<Arc<dyn ArtifactStore>> {
    let loc = &config.artifact_store;
    if loc.starts_with("gs://") || loc.starts_with("s3://") {
        #[cfg(any(feature = "gcs", feature = "aws"))]
        {
            let store = crate::artifact_store::ObjectStoreArtifactStore::from_url(loc)?;
            return Ok(Arc::new(store));
        }
        #[cfg(not(any(feature = "gcs", feature = "aws")))]
        {
            let _ = loc;
            anyhow::bail!(
                "ARTIFACT_STORE with a cloud URL requires the 'gcs' or 'aws' feature flag"
            )
        }
    }
    Ok(Arc::new(LocalArtifactStore::new(loc.into())))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `flags:` values reach the engine as booleans, read the way upstream's
    /// adapter factory reads them — a project that writes `flag: "true"` in
    /// YAML means the same thing as `flag: true`.
    #[test]
    fn behaviour_flags_read_booleans_and_boolean_strings() {
        let yes = dbt_yaml::Value::bool(true);
        let no = dbt_yaml::Value::bool(false);
        assert!(yml_flag_as_bool(&yes));
        assert!(!yml_flag_as_bool(&no));

        assert!(yml_flag_as_bool(&dbt_yaml::Value::string("true".to_string())));
        assert!(!yml_flag_as_bool(&dbt_yaml::Value::string("false".to_string())));

        // Anything that is not a boolean is not an override.
        assert!(!yml_flag_as_bool(&dbt_yaml::Value::string("yes".to_string())));
        assert!(!yml_flag_as_bool(&dbt_yaml::Value::null()));
    }

    /// The default settings are what a locally-built engine (the project-check
    /// index) uses: no project to take a comment or flags from.
    #[test]
    fn default_settings_carry_nothing() {
        let settings = AdapterSettings::default();
        assert!(settings.query_comment.is_none());
        assert!(settings.behavior_flags.is_empty());
        assert!(settings.threads.is_none());
    }

    use crate::config::{DbtTemporalConfig, TemporalMetricsConfig, WorkerTuningConfig};

    fn test_config() -> DbtTemporalConfig {
        DbtTemporalConfig {
            temporal_address: "localhost:7233".into(),
            temporal_namespace: "default".into(),
            temporal_task_queue: "test".into(),
            temporal_api_key: None,
            temporal_tls_cert: None,
            temporal_tls_key: None,
            dbt_project_dirs: vec![],
            dbt_profiles_dir: None,
            dbt_target: None,
            health_file: None,
            health_port: None,
            write_artifacts: false,
            write_catalog: false,
            artifact_store: "/tmp/dbt-artifacts".into(),
            search_attributes: std::collections::BTreeMap::new(),
            write_run_log: true,
            worker_tuning: WorkerTuningConfig::Fixed {
                max_concurrent_workflow_tasks: 200,
                max_concurrent_activities: 100,
                max_concurrent_local_activities: 100,
            },
            sticky_queue_timeout_secs: 10,
            nonsticky_to_sticky_poll_ratio: 0.2,
            max_worker_activities_per_second: None,
            max_task_queue_activities_per_second: None,
            graceful_shutdown_secs: None,
            max_cached_workflows: 1000,
            deployment_name: None,
            poller_autoscaling: None,
            temporal_metrics: TemporalMetricsConfig::None,
            priority_scheduling: false,
            nexus_enabled: false,
        }
    }

    #[test]
    fn build_artifact_store_local_default() -> Result<()> {
        let config = test_config();
        let store = build_artifact_store(&config)?;
        // Just verify it doesn't error — the default path is /tmp/dbt-artifacts.
        drop(store);
        Ok(())
    }

    #[test]
    fn build_artifact_store_local_custom_path() -> Result<()> {
        let mut config = test_config();
        config.artifact_store = "/tmp/custom-artifacts".into();
        let store = build_artifact_store(&config)?;
        drop(store);
        Ok(())
    }

    #[test]
    fn build_artifact_store_cloud_url_without_feature_errors() {
        let mut config = test_config();
        config.artifact_store = "gs://my-bucket/prefix".into();
        // Without gcs/aws features, this should error.
        #[cfg(not(any(feature = "gcs", feature = "aws")))]
        assert!(build_artifact_store(&config).is_err());
    }
}
