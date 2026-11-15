//! The project-checks gate: evaluated after planning, before any hook runs.

use std::time::Duration;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::activities::DbtActivities;
use crate::types::{
    DbtRunInput, ExecutionPlan, ProjectChecksInput, ProjectChecksOutput, TimeoutConfig,
};

use super::super::DbtRunWorkflow;
use super::activity_options::{activity_failure, bounded_activity_options, unresolved_retry};

/// The node ids a check's violations must name, or `None` for the whole project.
///
/// Keyed on whether the run *stated* a selection, not on what the plan
/// contains. An unselected run plans the project's default node set, which is
/// not a claim that violations elsewhere are uninteresting — and a check keyed
/// on groups or macros would have every violation filtered away into a
/// vacuous pass.
fn check_scope(input: &DbtRunInput, plan: &ExecutionPlan) -> Option<Vec<String>> {
    let scoped = input.select.is_some() || input.exclude.is_some();
    scoped.then(|| plan.nodes.keys().cloned().collect())
}

/// Evaluate the project's checks and stop the run if any of them is fatal.
///
/// Runs after planning, so a selector-scoped run only counts violations that
/// name a selected node, and before the pre-run hooks, so a project that fails
/// its gate performs no side effects at all.
///
/// Scoping follows the run's *stated* intent: `None` when the run named no
/// selector, since the default node set is not a statement about which
/// violations matter, and a check keyed on groups or macros would otherwise
/// have every violation filtered away into a vacuous pass.
pub async fn run_project_checks(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    timeouts: &TimeoutConfig,
) -> Result<(), WorkflowTermination> {
    if !plan.has_project_checks {
        return Ok(());
    }
    ctx.set_current_details("evaluating project checks".to_string());

    let checks_input = ProjectChecksInput {
        project: plan.project.clone(),
        invocation_id: plan.invocation_id.clone(),
        scope: check_scope(input, plan),
    };

    let output: ProjectChecksOutput = ctx
        .execute_activity(
            DbtActivities::run_project_checks,
            checks_input,
            // The gate reads the index built at parse time and issues no
            // warehouse query, so it is bounded by the same work planning is.
            bounded_activity_options(Duration::from_secs(timeouts.plan_secs), &unresolved_retry()),
        )
        .await
        .map_err(|e| activity_failure(ctx, "project checks could not be evaluated", &e))?;

    if output.failed == 0 {
        ctx.set_current_details(format!("{} project check(s) passed", output.results.len()));
        return Ok(());
    }

    // Every verdict is reported, not only the fatal ones: an operator reading
    // the failure needs to see what else the gate looked at.
    Err(WorkflowTermination::failed_application(ApplicationFailure::non_retryable(
        anyhow::anyhow!(
            "{} project check(s) failed:\n{}",
            output.failed,
            output.summary_lines().join("\n")
        ),
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::test_support::{empty_input, empty_plan, node_info};
    use super::*;

    /// A run that named no selector scopes nothing: the default node set is
    /// not a statement about which violations matter.
    #[test]
    fn an_unselected_run_scopes_checks_to_the_whole_project() {
        assert_eq!(check_scope(&empty_input(), &empty_plan()), None);
    }

    #[test]
    fn a_selected_run_scopes_checks_to_the_planned_nodes() {
        let mut input = empty_input();
        input.select = Some("tag:nightly".to_string());
        let mut plan = empty_plan();
        plan.nodes
            .insert("model.shop.a".to_string(), node_info("a"));
        plan.nodes
            .insert("model.shop.b".to_string(), node_info("b"));

        assert_eq!(
            check_scope(&input, &plan),
            Some(vec!["model.shop.a".to_string(), "model.shop.b".to_string()])
        );
    }

    /// `--exclude` alone is a selection too — scoping must not key on `select`.
    #[test]
    fn an_exclude_only_run_is_still_a_selected_run() {
        let mut input = empty_input();
        input.exclude = Some("tag:slow".to_string());
        assert_eq!(check_scope(&input, &empty_plan()), Some(vec![]));
    }
}
