// The `#[activities]` macro generates per-activity definition types without Debug impls.
#![allow(missing_debug_implementations)]

pub mod dag;
pub mod execute_node;
pub mod heartbeat;
pub mod node_helpers;
pub mod node_serialization;
pub mod plan;
pub mod project_hooks;
pub mod selectors;
pub mod store_artifacts;

use std::sync::Arc;

use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::artifact_store::ArtifactStore;
use crate::config::{
    RegisteredSearchAttributes, SearchAttributeConfig, WriteArtifacts, WriteRunLog,
};
use crate::project_registry::ProjectRegistry;
use crate::types::{
    DbtRunInput, ExecutionPlan, NodeExecutionInput, NodeExecutionResult, ProjectHooksInput,
    ResolveConfigInput, ResolvedProjectConfig, StoreArtifactsInput, StoreArtifactsOutput,
};

/// Shared state for all dbt activities, replacing the old `app_data()` DI pattern.
///
/// Created once at worker startup and registered via `worker.register_activities(Arc::new(..))`.
/// Each `#[activity]` method accesses shared state through `self: Arc<Self>`.
pub struct DbtActivities {
    pub registry: Arc<ProjectRegistry>,
    pub artifact_store: Option<Arc<dyn ArtifactStore>>,
    pub search_attr_config: SearchAttributeConfig,
    pub registered_attrs: RegisteredSearchAttributes,
    pub write_run_log: WriteRunLog,
    pub write_artifacts: WriteArtifacts,
}

impl std::fmt::Debug for DbtActivities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbtActivities")
            .field("search_attr_config", &self.search_attr_config)
            .field("write_run_log", &self.write_run_log)
            .field("write_artifacts", &self.write_artifacts)
            .finish_non_exhaustive()
    }
}

// The `#[activities]` macro generates per-activity definition types that lack Debug impls.
#[allow(missing_debug_implementations)]
#[activities]
impl DbtActivities {
    #[activity(name = "plan_project")]
    pub async fn plan_project(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: DbtRunInput,
    ) -> Result<ExecutionPlan, ActivityError> {
        plan::plan_project_inner(&self, &ctx, input)
            .await
            .map_err(|e| ActivityError::NonRetryable(e.into()))
    }

    #[activity(name = "execute_node")]
    pub async fn execute_node(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: NodeExecutionInput,
    ) -> Result<NodeExecutionResult, ActivityError> {
        execute_node::execute_node_outer(&self, ctx, input).await
    }

    #[activity(name = "store_artifacts")]
    pub async fn store_artifacts(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: StoreArtifactsInput,
    ) -> Result<StoreArtifactsOutput, ActivityError> {
        store_artifacts::store_artifacts_inner(&self, input)
            .await
            .map_err(|e| ActivityError::NonRetryable(e.into()))
    }

    #[activity(name = "resolve_config")]
    #[allow(clippy::unused_async)] // Must be async for #[activity] macro.
    pub async fn resolve_config(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ResolveConfigInput,
    ) -> Result<ResolvedProjectConfig, ActivityError> {
        crate::hooks::resolve_config_impl(&self.registry, input)
            .map_err(|e| ActivityError::NonRetryable(e.into()))
    }

    #[activity(name = "run_project_hooks")]
    pub async fn run_project_hooks(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: ProjectHooksInput,
    ) -> Result<(), ActivityError> {
        project_hooks::run_project_hooks_outer(&self, ctx, input).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn debug_redacts_registry_and_artifact_store_keeps_safe_fields() {
        // Building a real ArtifactStore + ProjectRegistry needs the full dbt-fusion
        // graph; for the Debug impl we only need a registry shape that prints. Use
        // an empty ProjectRegistry, which has its own Debug impl that just lists
        // project names — so no WorkerState construction is required.
        let activities = DbtActivities {
            registry: Arc::new(ProjectRegistry::new(BTreeMap::new())),
            artifact_store: None,
            search_attr_config: SearchAttributeConfig(
                std::iter::once(("env".to_string(), "test".to_string())).collect(),
            ),
            registered_attrs: RegisteredSearchAttributes(BTreeSet::new()),
            write_run_log: WriteRunLog(true),
            write_artifacts: WriteArtifacts(false),
        };
        let s = format!("{activities:?}");
        assert!(s.contains("DbtActivities"));
        assert!(s.contains("search_attr_config"));
        assert!(s.contains("write_run_log"));
        assert!(s.contains("write_artifacts"));
        // finish_non_exhaustive marker: registry/artifact_store omitted from Debug.
        assert!(s.contains(".."));
        assert!(!s.contains("registry"));
        assert!(!s.contains("artifact_store"));
    }
}
