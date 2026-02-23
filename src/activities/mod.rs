// The `#[activities]` macro generates per-activity definition types without Debug impls.
#![allow(missing_debug_implementations)]

pub mod dag;
pub mod execute_node;
pub mod node_helpers;
pub mod node_serialization;
pub mod plan;
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
    DbtRunInput, ExecutionPlan, NodeExecutionInput, NodeExecutionResult, ResolveConfigInput,
    ResolvedProjectConfig, StoreArtifactsInput, StoreArtifactsOutput,
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
}
