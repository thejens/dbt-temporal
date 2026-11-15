//! Moving a node's compiled SQL out of its result and into the artifact store.

use bytes::Bytes;
use tracing::warn;

use crate::types::NodeExecutionResult;

use super::super::DbtActivities;

/// Move the node's compiled SQL to the artifact store, leaving a reference.
///
/// The workflow accumulates every result, copies them into the checkpoint at
/// each continuation and clones them again for the artifact activity — so a
/// model's SQL crossed Temporal many times over to reach the one place that
/// reads it. Written once here rather than carried through the workflow, and read back by `store_artifacts`
/// when it assembles `run_results.json`.
///
/// Without artifact storage the SQL stays inline: nothing writes artifacts for
/// that run, so nothing would ever read the reference.
///
/// This runs after the node has already been materialized, so it must not
/// fail: an error here would make Temporal retry the activity and run the
/// model a second time. A failed write keeps the SQL inline, which is exactly
/// what a run without artifact storage carries, and `run_results.json` reads
/// it from there.
pub(super) async fn spill_compiled_sql(
    activities: &DbtActivities,
    invocation_id: &str,
    result: &mut NodeExecutionResult,
) {
    if !activities.write_artifacts.0 {
        return;
    }
    let (Some(store), Some(sql)) =
        (activities.artifact_store.as_ref(), result.compiled_code.take())
    else {
        return;
    };

    // Moves the buffer into the store; the inline fallback needs the string
    // back, so it is recovered from the failure path below.
    let bytes = Bytes::from(sql.into_bytes());
    match store
        .store(invocation_id, &compiled_sql_artifact_name(&result.unique_id), bytes.clone())
        .await
    {
        Ok(path) => result.compiled_code_ref = Some(path),
        Err(e) => {
            warn!(
                node = %result.unique_id,
                error = %format!("{e:#}"),
                "could not store compiled SQL; keeping it inline in the node result"
            );
            // The bytes came from a `String`, so they are valid UTF-8 by construction.
            result.compiled_code = String::from_utf8(bytes.to_vec()).ok();
        }
    }
}

/// Artifact name for one node's compiled SQL.
pub fn compiled_sql_artifact_name(unique_id: &str) -> String {
    format!("compiled/{unique_id}.sql")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::project_registry::ProjectRegistry;
    use crate::types::NodeStatus;

    // --- spill_compiled_sql ---

    /// A store whose writes always fail, standing in for an outage.
    struct UnwritableStore;

    #[async_trait::async_trait]
    impl crate::artifact_store::ArtifactStore for UnwritableStore {
        async fn store(&self, _: &str, _: &str, _: Bytes) -> anyhow::Result<String> {
            anyhow::bail!("store unavailable")
        }

        async fn retrieve(&self, path: &str) -> anyhow::Result<Bytes> {
            anyhow::bail!("nothing at {path}")
        }
    }

    fn activities_with_store(
        store: Arc<dyn crate::artifact_store::ArtifactStore>,
        write_artifacts: bool,
    ) -> DbtActivities {
        use crate::config::{
            PriorityScheduling, RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts,
            WriteCatalog, WriteRunLog,
        };
        DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: Some(store),
            search_attr_config: SearchAttributeConfig(BTreeMap::new()),
            registered_attrs: RegisteredSearchAttributes(std::collections::BTreeSet::new()),
            write_run_log: WriteRunLog(false),
            write_artifacts: WriteArtifacts(write_artifacts),
            write_catalog: WriteCatalog(false),
            priority_scheduling: PriorityScheduling(false),
        }
    }

    fn result_with_sql(sql: &str) -> NodeExecutionResult {
        NodeExecutionResult {
            unique_id: "model.p.m".into(),
            status: NodeStatus::Success,
            execution_time: 0.1,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: Some(sql.into()),
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }
    }

    #[tokio::test]
    async fn spilled_sql_is_replaced_by_a_reference_to_the_stored_object() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store =
            Arc::new(crate::artifact_store::LocalArtifactStore::new(dir.path().to_path_buf()));
        let activities = activities_with_store(Arc::clone(&store) as _, true);
        let mut result = result_with_sql("select 1 as id");

        spill_compiled_sql(&activities, "inv-1", &mut result).await;

        assert!(result.compiled_code.is_none());
        let reference = result.compiled_code_ref.as_deref().expect("a reference");
        let stored =
            crate::artifact_store::ArtifactStore::retrieve(store.as_ref(), reference).await?;
        assert_eq!(&stored[..], b"select 1 as id");
        Ok(())
    }

    /// The node has already been materialized by the time the SQL is spilled,
    /// so a store outage must leave a successful result rather than an error
    /// that would make Temporal run the model again.
    #[tokio::test]
    async fn a_failed_spill_keeps_the_sql_inline() {
        let activities = activities_with_store(Arc::new(UnwritableStore), true);
        let mut result = result_with_sql("select 1 as id");

        spill_compiled_sql(&activities, "inv-1", &mut result).await;

        assert_eq!(result.compiled_code.as_deref(), Some("select 1 as id"));
        assert!(result.compiled_code_ref.is_none());
        assert_eq!(result.status, NodeStatus::Success);
    }

    #[tokio::test]
    async fn sql_stays_inline_when_artifacts_are_not_written() {
        let activities = activities_with_store(Arc::new(UnwritableStore), false);
        let mut result = result_with_sql("select 1 as id");

        spill_compiled_sql(&activities, "inv-1", &mut result).await;

        assert_eq!(result.compiled_code.as_deref(), Some("select 1 as id"));
        assert!(result.compiled_code_ref.is_none());
    }
}
