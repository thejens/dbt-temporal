//! The `on-run-start` and `on-run-end` hooks declared in `dbt_project.yml`,
//! which run as the `run_project_hooks` activity.

use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    CompactProjectHooksInput, ExecutionPlan, HookError, NodeExecutionResult, ProjectHookPhase,
    ProjectHooksActivityInput, RetryConfig, RunScope,
};

use super::super::DbtRunWorkflow;
use super::super::helpers::ON_RUN_END_FAILS_RUN_PATCH;
use super::activity_options::{HookPolicy, activity_failure, hook_activity_options};

/// Build the input for `run_project_hooks` (on-run-start or on-run-end).
/// `node_results` and `prior_segments` are empty for on-run-start, populated
/// for on-run-end: `node_results` is this segment's, and `prior_segments` names
/// the checkpoints holding the earlier segments' results of a continued run.
pub fn build_project_hooks_input(
    phase: ProjectHookPhase,
    plan: &ExecutionPlan,
    run: RunScope,
    node_results: &[NodeExecutionResult],
    prior_segments: &[String],
    retry_config: &RetryConfig,
) -> ProjectHooksActivityInput {
    ProjectHooksActivityInput::compact(CompactProjectHooksInput {
        phase,
        project: plan.project.clone(),
        invocation_id: plan.invocation_id.clone(),
        run,
        node_results: node_results.to_vec(),
        retry_on_error: retry_config.project_hooks.allows(phase),
        prior_segments: prior_segments.to_vec(),
    })
}

pub async fn run_on_run_start(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    run_scope: &RunScope,
    policy: HookPolicy<'_>,
) -> Result<(), WorkflowTermination> {
    if !plan.has_on_run_start {
        return Ok(());
    }
    ctx.set_current_details("running on-run-start hooks".to_string());
    ctx.execute_activity(
        DbtActivities::run_project_hooks,
        build_project_hooks_input(
            ProjectHookPhase::OnRunStart,
            plan,
            run_scope.clone(),
            &[],
            &[],
            policy.retry,
        ),
        hook_activity_options(ProjectHookPhase::OnRunStart, policy),
    )
    .await
    .map_err(|e| activity_failure(ctx, "on-run-start hook failed", &e))
}

/// The results an `on-run-end` hook sees: this segment's, plus the checkpoints
/// of earlier segments the activity reads back and puts ahead of them.
#[derive(Clone, Copy)]
pub struct OnRunEndRecord<'a> {
    pub results: &'a [NodeExecutionResult],
    pub prior_segments: &'a [String],
}

/// Run the `on-run-end` hooks; `true` when one failed and the run has failed
/// with it.
///
/// As in dbt, a failing hook fails the invocation but leaves the nodes' own
/// results alone: the failure is recorded in `hook_errors` and the node
/// results and artifacts stay as the nodes produced them. The hooks fire even
/// when the run's nodes failed. A cancellation that reaches the activity ends
/// the workflow as cancelled rather than as a failed hook.
pub async fn run_on_run_end(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    run_scope: &RunScope,
    record: OnRunEndRecord<'_>,
    hook_errors: &mut Vec<HookError>,
    policy: HookPolicy<'_>,
) -> Result<bool, WorkflowTermination> {
    if !plan.has_on_run_end {
        return Ok(false);
    }
    ctx.set_current_details("running on-run-end hooks".to_string());
    let result = ctx
        .execute_activity(
            DbtActivities::run_project_hooks,
            build_project_hooks_input(
                ProjectHookPhase::OnRunEnd,
                plan,
                run_scope.clone(),
                record.results,
                record.prior_segments,
                policy.retry,
            ),
            hook_activity_options(ProjectHookPhase::OnRunEnd, policy),
        )
        .await;
    let Err(e) = result else {
        return Ok(false);
    };
    // Consulted only on this path, so a run whose hooks passed records no
    // marker.
    let fails_run = ctx.patched(ON_RUN_END_FAILS_RUN_PATCH);
    if fails_run && matches!(e, temporalio_sdk::ActivityExecutionError::Cancelled(_)) {
        return Err(WorkflowTermination::cancelled());
    }
    tracing::error!(error = %e, fails_run, "on-run-end hook failed");
    hook_errors.push(HookError {
        hook_workflow_type: "on_run_end".to_string(),
        event: "on_run_end".to_string(),
        error: e.to_string(),
    });
    Ok(fails_run)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::test_support::{empty_input, empty_plan};
    use super::*;

    // --- build_project_hooks_input ---

    #[test]
    fn build_project_hooks_input_on_run_start_has_no_node_results() {
        let plan = ExecutionPlan {
            project: "shop".to_string(),
            invocation_id: "inv-1".to_string(),
            ..empty_plan()
        };
        let env = BTreeMap::from([("DBT_TARGET".to_string(), "prod".to_string())]);
        let mut input = empty_input();
        input.target = Some("prod".to_string());

        let phs = build_project_hooks_input(
            ProjectHookPhase::OnRunStart,
            &plan,
            RunScope::new(&input, &env, None),
            &[], // on-run-start has no results yet
            &[],
            &RetryConfig::default(),
        )
        .resolve();
        assert_eq!(phs.phase, ProjectHookPhase::OnRunStart);
        assert_eq!(phs.project, "shop");
        assert_eq!(phs.invocation_id, "inv-1");
        assert_eq!(phs.target.as_deref(), Some("prod"));
        assert!(phs.node_results.is_empty());
        assert_eq!(phs.env.get("DBT_TARGET").map(String::as_str), Some("prod"));
    }

    #[test]
    fn build_project_hooks_input_on_run_end_carries_node_results() {
        use crate::types::{NodeExecutionResult, NodeStatus};
        let plan = empty_plan();
        let input = empty_input();
        let env = BTreeMap::new();
        let results = vec![NodeExecutionResult {
            unique_id: "model.shop.a".to_string(),
            status: NodeStatus::Success,
            execution_time: 0.1,
            message: None,
            adapter_response: BTreeMap::new(),
            compiled_code: None,
            timing: vec![],
            failures: None,
            freshness: None,
            compiled_code_ref: None,
            relation_name: None,
        }];
        let phs = build_project_hooks_input(
            ProjectHookPhase::OnRunEnd,
            &plan,
            RunScope::new(&input, &env, None),
            &results,
            &[],
            &RetryConfig::default(),
        )
        .resolve();
        assert_eq!(phs.phase, ProjectHookPhase::OnRunEnd);
        assert_eq!(phs.node_results.len(), 1);
        assert_eq!(phs.prior_segments, Vec::<String>::new());
    }

    /// The activity reads the earlier segments back from these references, so
    /// they must reach it in the order the chain was built.
    #[test]
    fn build_project_hooks_input_on_run_end_carries_prior_segments() {
        let prior = vec!["inv/seg_0.json".to_string(), "inv/seg_1.json".to_string()];
        let phs = build_project_hooks_input(
            ProjectHookPhase::OnRunEnd,
            &empty_plan(),
            RunScope::new(&empty_input(), &BTreeMap::new(), None),
            &[],
            &prior,
            &RetryConfig::default(),
        )
        .resolve();
        assert_eq!(phs.prior_segments, prior);
    }

    /// The input carries the resolved decision, so the activity does not have
    /// to re-read config on every attempt — and each phase carries its own.
    #[test]
    fn the_hooks_input_carries_the_resolved_opt_in_per_phase() {
        let retry = RetryConfig {
            project_hooks: crate::types::ProjectHookRetry {
                on_run_start: true,
                on_run_end: false,
            },
            ..RetryConfig::default()
        };
        let plan = empty_plan();
        let input = empty_input();
        let env = BTreeMap::new();

        let start = build_project_hooks_input(
            ProjectHookPhase::OnRunStart,
            &plan,
            RunScope::new(&input, &env, None),
            &[],
            &[],
            &retry,
        )
        .resolve();
        assert!(start.retry_on_error);

        let end = build_project_hooks_input(
            ProjectHookPhase::OnRunEnd,
            &plan,
            RunScope::new(&input, &env, None),
            &[],
            &[],
            &retry,
        )
        .resolve();
        assert!(!end.retry_on_error);
    }
}
