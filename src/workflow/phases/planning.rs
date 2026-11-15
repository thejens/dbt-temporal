//! Planning: the `plan_project` and `resolve_config` activities, the workflow
//! memo, and the output of a `list` run.

use std::time::Duration;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{LocalActivityOptions, MemoValue, WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    CommandMemo, DbtRunInput, DbtRunOutput, ExecutionPlan, NodeExecutionResult, NodeStatus,
    ResolveConfigInput, ResolvedProjectConfig,
};

use super::super::DbtRunWorkflow;
use super::super::helpers::plural;
use super::super::search_attributes::build_search_attribute_updates;
use super::activity_options::{activity_failure, bounded_activity_options, unresolved_retry};

/// Build the workflow-detail line shown after `plan_project` returns:
/// "planned 17 nodes across 8 levels".
pub fn build_planned_details(node_count: usize, level_count: usize) -> String {
    format!(
        "planned {node_count} node{} across {level_count} level{}",
        plural(node_count),
        plural(level_count),
    )
}

/// Build the input for the `resolve_config` activity. Pure projection of
/// project name + caller-supplied hook overrides.
pub fn build_resolve_config_input(plan: &ExecutionPlan, input: &DbtRunInput) -> ResolveConfigInput {
    ResolveConfigInput {
        project: Some(plan.project.clone()),
        input_hooks: input.hooks.clone(),
    }
}

pub fn write_command_memo(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
) -> Result<(), WorkflowTermination> {
    ctx.upsert_memo([("command", Some(MemoValue::new(CommandMemo::from(input))))])
        .map_err(|e| WorkflowTermination::failed_application(ApplicationFailure::non_retryable(e)))
}

/// Runs `plan_project`, upserts search attributes, sets the workflow's
/// current_details header. Returns the resolved `ExecutionPlan`.
pub async fn plan_and_announce(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan_timeout_secs: u64,
) -> Result<ExecutionPlan, WorkflowTermination> {
    let plan: ExecutionPlan = ctx
        .execute_activity(
            DbtActivities::plan_project,
            input.clone(),
            bounded_activity_options(Duration::from_secs(plan_timeout_secs), &unresolved_retry()),
        )
        .await
        .map_err(|e| activity_failure(ctx, "plan_project activity failed", &e))?;

    if !plan.search_attributes.is_empty() {
        ctx.upsert_search_attributes(build_search_attribute_updates(&plan.search_attributes)?);
    }

    ctx.set_current_details(build_planned_details(plan.nodes.len(), plan.levels.len()));

    Ok(plan)
}

/// Resolve hook/retry/timeout config for the run.
///
/// Runs as a **local activity**: it only reads config already parsed into
/// `WorkerState`, so dispatching it through the task queue would cost three
/// history events and a poll round-trip per run to do a map lookup. Local
/// activities execute inline on the worker holding the workflow task and are
/// recorded as a single marker.
pub async fn resolve_project_config(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
) -> Result<ResolvedProjectConfig, WorkflowTermination> {
    ctx.execute_local_activity(
        DbtActivities::resolve_config,
        build_resolve_config_input(plan, input),
        LocalActivityOptions::builder()
            .start_to_close_timeout(Duration::from_secs(10))
            .build(),
    )
    .await
    .map_err(|e| activity_failure(ctx, "resolve_config activity failed", &e))
}

/// Build the workflow output for a `list` command: returns all planned nodes as
/// success entries without executing any SQL or touching the warehouse.
pub fn build_list_output(plan: &ExecutionPlan, elapsed: f64) -> DbtRunOutput {
    let node_results = plan
        .nodes
        .values()
        .map(|node| {
            NodeExecutionResult::not_executed(node.unique_id.clone(), NodeStatus::Success, None)
        })
        .collect();
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: true,
        skipped: false,
        skip_reason: None,
        node_results,
        node_results_partial: false,
        elapsed_time: elapsed,
        artifacts: None,
        log_path: None,
        hook_errors: vec![],
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use dbt_schemas::schemas::telemetry::NodeType;

    use super::super::test_support::{empty_input, empty_plan};
    use super::*;
    use crate::types::HooksConfig;

    // --- build_list_output ---

    #[test]
    fn build_list_output_returns_all_nodes_as_success_without_execution() {
        use crate::types::NodeInfo;

        let plan = ExecutionPlan {
            nodes: BTreeMap::from([
                (
                    "model.shop.customers".to_string(),
                    NodeInfo {
                        unique_id: "model.shop.customers".to_string(),
                        name: "customers".to_string(),
                        resource_type: NodeType::Model,
                        materialization: Some("table".to_string()),
                        package_name: "shop".to_string(),
                        depends_on: vec![],
                        priority: None,
                        on_error: None,
                    },
                ),
                (
                    "model.shop.orders".to_string(),
                    NodeInfo {
                        unique_id: "model.shop.orders".to_string(),
                        name: "orders".to_string(),
                        resource_type: NodeType::Model,
                        materialization: Some("table".to_string()),
                        package_name: "shop".to_string(),
                        depends_on: vec!["model.shop.customers".to_string()],
                        priority: None,
                        on_error: None,
                    },
                ),
            ]),
            ..empty_plan()
        };

        let out = build_list_output(&plan, 0.05);

        assert!(out.success);
        assert!(!out.skipped);
        assert_eq!(out.invocation_id, "inv-42");
        assert_eq!(out.node_results.len(), 2, "one result per planned node");
        for r in &out.node_results {
            assert_eq!(r.status, NodeStatus::Success);
            assert!(
                (r.execution_time - 0.0).abs() < f64::EPSILON,
                "list doesn't execute — no execution time"
            );
            assert!(r.compiled_code.is_none());
            assert!(r.message.is_none());
        }
        assert!(out.artifacts.is_none());
        assert!(out.hook_errors.is_empty());
    }

    #[test]
    fn build_list_output_empty_plan_returns_empty_results() {
        let out = build_list_output(&empty_plan(), 0.0);
        assert!(out.success);
        assert!(out.node_results.is_empty());
    }

    // --- build_planned_details ---

    #[test]
    fn build_planned_details_pluralises_both_counts() {
        assert_eq!(build_planned_details(0, 0), "planned 0 nodes across 0 levels");
        assert_eq!(build_planned_details(1, 1), "planned 1 node across 1 level");
        assert_eq!(build_planned_details(17, 8), "planned 17 nodes across 8 levels");
    }

    // --- build_resolve_config_input ---

    #[test]
    fn build_resolve_config_input_carries_project_and_input_hooks() {
        let plan = ExecutionPlan {
            project: "shop".to_string(),
            ..empty_plan()
        };
        let input = DbtRunInput {
            hooks: Some(HooksConfig::default()),
            ..empty_input()
        };
        let resolved = build_resolve_config_input(&plan, &input);
        assert_eq!(resolved.project.as_deref(), Some("shop"));
        assert!(resolved.input_hooks.is_some());
    }

    #[test]
    fn build_resolve_config_input_passes_through_none_input_hooks() {
        let plan = empty_plan();
        let input = empty_input(); // hooks = None
        let resolved = build_resolve_config_input(&plan, &input);
        assert!(resolved.input_hooks.is_none());
    }
}
