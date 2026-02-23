use std::sync::Arc;

use anyhow::Result;

use crate::artifact_store::{ArtifactStore, LocalArtifactStore};
use crate::config::DbtTemporalConfig;

/// Build an AdapterEngine from a DbConfig.
pub fn build_adapter_engine(
    db_config: &dbt_schemas::schemas::profiles::DbConfig,
    quoting: dbt_schemas::schemas::common::ResolvedQuoting,
    token: &dbt_common::cancellation::CancellationToken,
    auth_override: Option<Arc<dyn dbt_auth::Auth>>,
) -> Result<Arc<dyn dbt_adapter::AdapterEngine>> {
    use dbt_adapter::adapter_engine::XdbcEngine;
    use dbt_adapter::base_adapter::backend_of;
    use dbt_adapter::cache::RelationCache;
    use dbt_adapter::query_comment::QueryCommentConfig;
    use dbt_adapter::sql_types::SATypeOpsImpl;
    use dbt_adapter::stmt_splitter::NaiveStmtSplitter;

    let adapter_type = db_config
        .adapter_type_if_supported()
        .ok_or_else(|| anyhow::anyhow!("unsupported adapter type: {}", db_config.adapter_type()))?;

    let base_auth: Arc<dyn dbt_auth::Auth> = auth_override.unwrap_or_else(|| {
        let backend = backend_of(adapter_type);
        dbt_auth::auth_for_backend(backend).into()
    });

    let mapping = db_config
        .to_mapping()
        .map_err(|e| anyhow::anyhow!("failed to serialize db config: {e}"))?;

    let adapter_config = dbt_auth::AdapterConfig::new(mapping);

    let stmt_splitter: Arc<dyn dbt_adapter::stmt_splitter::StmtSplitter> =
        Arc::new(NaiveStmtSplitter);
    let query_comment = QueryCommentConfig::from_query_comment(None, adapter_type, false);
    let type_ops: Box<dyn dbt_adapter::sql_types::TypeOps> =
        Box::new(SATypeOpsImpl::new(adapter_type));
    let relation_cache = Arc::new(RelationCache::default());

    let engine = XdbcEngine::new(
        adapter_type,
        base_auth,
        adapter_config,
        quoting,
        query_comment,
        type_ops,
        stmt_splitter,
        None, // query_cache
        relation_cache,
        std::collections::BTreeMap::new(), // behavior_flag_overrides
        token.clone(),
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
    use crate::config::{DbtTemporalConfig, WorkerTuningConfig};

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
