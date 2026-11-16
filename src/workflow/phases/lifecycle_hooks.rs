//! The `pre_run`, `on_success` and `on_failure` lifecycle hooks, which run as
//! child workflows through `crate::hooks::execute_hooks`.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::time::SystemTime;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::hooks::{HooksError, execute_hooks};
use crate::types::{
    DbtRunInput, DbtRunOutput, ExecutionPlan, HookError, HookEvent, HookPayload, HooksConfig,
};

use super::super::DbtRunWorkflow;
use super::super::helpers::elapsed_secs;

/// Build the `HookPayload` that pre_run hooks receive. Output is always None
/// at this point — the run hasn't produced one yet.
pub fn build_pre_run_payload(plan: &ExecutionPlan, input: &DbtRunInput) -> HookPayload {
    HookPayload {
        event: HookEvent::PreRun,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: None,
    }
}

/// Build the `HookPayload` for on_success / on_failure hooks. The `event`
/// determines which list of hooks runs; the output snapshot is what the run
/// finished with so far.
pub fn build_post_hook_payload(
    event: HookEvent,
    plan: &ExecutionPlan,
    input: &DbtRunInput,
    output_snapshot: &DbtRunOutput,
) -> HookPayload {
    HookPayload {
        event,
        invocation_id: plan.invocation_id.clone(),
        input: input.clone(),
        plan: Some(plan.clone()),
        output: Some(output_snapshot.clone()),
    }
}

/// Build the `DbtRunOutput` returned when pre_run hooks signal skip. Drains
/// `hook_errors` so the caller can return the output without leaving stale
/// errors in the parent's accumulator.
pub fn build_skip_output(
    plan: &ExecutionPlan,
    skip_reason: Option<String>,
    hook_errors: &mut Vec<HookError>,
    elapsed: f64,
) -> DbtRunOutput {
    DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: true,
        skipped: true,
        skip_reason,
        node_results: vec![],
        node_results_partial: false,
        elapsed_time: elapsed,
        artifacts: None,
        log_path: None,
        hook_errors: std::mem::take(hook_errors),
    }
}

/// Pick the post-run hook list and event from the run's success flag.
pub fn select_post_run_hooks(
    success: bool,
    hooks: &HooksConfig,
) -> (&[crate::types::HookConfig], HookEvent) {
    if success {
        (&hooks.on_success, HookEvent::OnSuccess)
    } else {
        (&hooks.on_failure, HookEvent::OnFailure)
    }
}

/// Merge a successful `HookExecutionOutcome` into the parent run state and
/// decide whether the workflow should short-circuit.
///
/// `hook_errors` and `effective_env` are mutated in place; the returned
/// `ControlFlow` is `Break(output)` when the hooks signalled skip (the
/// caller returns `output` immediately) or `Continue(())` to proceed with
/// the run.
pub fn apply_pre_run_outcome(
    plan: &ExecutionPlan,
    outcome: crate::types::HookExecutionOutcome,
    effective_env: &mut BTreeMap<String, String>,
    hook_errors: &mut Vec<HookError>,
    elapsed: f64,
) -> ControlFlow<DbtRunOutput, ()> {
    hook_errors.extend(outcome.errors);
    effective_env.extend(outcome.extra_env);
    if outcome.skip {
        return ControlFlow::Break(build_skip_output(
            plan,
            outcome.skip_reason,
            hook_errors,
            elapsed,
        ));
    }
    ControlFlow::Continue(())
}

/// Merge the result of `execute_hooks` for a post-run hook batch. Returns the
/// new `success` flag for the workflow:
///
/// - On Ok: drains the outcome's collected non-fatal errors into the parent's
///   accumulator and preserves the incoming `success_in` value.
/// - On Err: a post-hook failure marks the workflow as failed (matches
///   dbt-core: failed `on_success` hooks turn the run into a failure).
pub fn apply_post_run_outcome(
    outcome_result: Result<crate::types::HookExecutionOutcome, anyhow::Error>,
    hook_errors: &mut Vec<HookError>,
    success_in: bool,
) -> bool {
    match outcome_result {
        Ok(outcome) => {
            hook_errors.extend(outcome.errors);
            success_in
        }
        Err(_) => false,
    }
}

/// `ControlFlow::Break(out)` means the pre_run hooks signalled `skip` — the
/// caller should return `out` immediately. `ControlFlow::Continue(())` means
/// hooks completed; `effective_env` and `hook_errors` are updated in place.
pub async fn run_pre_run_hooks(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    hooks: &HooksConfig,
    effective_env: &mut BTreeMap<String, String>,
    hook_errors: &mut Vec<HookError>,
    start: Option<SystemTime>,
) -> Result<ControlFlow<DbtRunOutput, ()>, WorkflowTermination> {
    if hooks.pre_run.is_empty() {
        return Ok(ControlFlow::Continue(()));
    }

    let payload = build_pre_run_payload(plan, input);
    match split_cancellation(execute_hooks(ctx, &hooks.pre_run, &payload).await)? {
        Ok(outcome) => {
            let elapsed = elapsed_secs(start, ctx.workflow_time());
            Ok(apply_pre_run_outcome(plan, outcome, effective_env, hook_errors, elapsed))
        }
        Err(e) => {
            Err(WorkflowTermination::failed_application(ApplicationFailure::non_retryable(e)))
        }
    }
}

/// Separate a cancellation of the run from a failure of its hooks: the first
/// ends the workflow as cancelled whatever the hook's `on_error` says, the
/// second is the hooks' own verdict for the caller to apply.
fn split_cancellation<T>(
    result: Result<T, HooksError>,
) -> Result<Result<T, anyhow::Error>, WorkflowTermination> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(HooksError::Failed(e)) => Ok(Err(e)),
        Err(HooksError::Cancelled) => Err(WorkflowTermination::cancelled()),
    }
}

/// Runs the on_success or on_failure hooks. Returns the post-hook `success`
/// flag — caller is responsible for writing it back into `output.success`
/// before deciding `Ok` vs `Err`. A cancellation that reaches a hook is an
/// `Err` instead: the run ends as cancelled, not as a failed hook.
pub async fn run_post_hooks(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    hooks: &HooksConfig,
    output_snapshot: &DbtRunOutput,
    success_in: bool,
    hook_errors: &mut Vec<HookError>,
) -> Result<bool, WorkflowTermination> {
    let (post_hooks, event) = select_post_run_hooks(success_in, hooks);

    if post_hooks.is_empty() {
        return Ok(success_in);
    }

    let payload = build_post_hook_payload(event, plan, input, output_snapshot);
    let result = split_cancellation(execute_hooks(ctx, post_hooks, &payload).await)?;
    if let Err(ref e) = result {
        tracing::error!(event = %event, error = %e, "post-hook failed, marking workflow as failed");
    }
    Ok(apply_post_run_outcome(result, hook_errors, success_in))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::test_support::{empty_input, empty_plan};
    use super::*;
    use crate::types::HookConfig;

    // --- build_pre_run_payload / build_post_hook_payload ---

    #[test]
    fn build_pre_run_payload_pins_event_invocation_and_omits_output() {
        let plan = ExecutionPlan {
            invocation_id: "inv-pre".to_string(),
            ..empty_plan()
        };
        let payload = build_pre_run_payload(&plan, &empty_input());
        assert_eq!(payload.event, HookEvent::PreRun);
        assert_eq!(payload.invocation_id, "inv-pre");
        assert!(payload.plan.is_some());
        // pre_run hasn't produced output yet — explicitly None.
        assert!(payload.output.is_none());
    }

    #[test]
    fn build_post_hook_payload_includes_output_snapshot() {
        let plan = empty_plan();
        let input = empty_input();
        let snapshot = DbtRunOutput {
            invocation_id: plan.invocation_id.clone(),
            success: true,
            skipped: false,
            skip_reason: None,
            node_results: vec![],
            node_results_partial: false,
            elapsed_time: 1.5,
            artifacts: None,
            log_path: None,
            hook_errors: vec![],
        };
        let payload = build_post_hook_payload(HookEvent::OnSuccess, &plan, &input, &snapshot);
        assert_eq!(payload.event, HookEvent::OnSuccess);
        let out = payload
            .output
            .expect("post-hook payload must carry the snapshot");
        assert!(out.success);
        assert!((out.elapsed_time - 1.5).abs() < f64::EPSILON);
    }

    // --- build_skip_output ---

    #[test]
    fn build_skip_output_drains_hook_errors_and_records_reason() {
        let plan = ExecutionPlan {
            invocation_id: "inv-skip".to_string(),
            ..empty_plan()
        };
        let mut hook_errors = vec![HookError {
            hook_workflow_type: "notify".to_string(),
            event: "pre_run".to_string(),
            error: "warn-1".to_string(),
        }];
        let out = build_skip_output(&plan, Some("no new data".to_string()), &mut hook_errors, 0.42);
        assert!(out.success); // skip is success — the run reached its end intentionally.
        assert!(out.skipped);
        assert_eq!(out.skip_reason.as_deref(), Some("no new data"));
        assert!(out.node_results.is_empty());
        assert!((out.elapsed_time - 0.42).abs() < f64::EPSILON);
        // Drained — caller's accumulator is reset.
        assert!(hook_errors.is_empty());
        assert_eq!(out.hook_errors.len(), 1);
    }

    #[test]
    fn build_skip_output_handles_no_skip_reason() {
        let plan = empty_plan();
        let mut hook_errors: Vec<HookError> = vec![];
        let out = build_skip_output(&plan, None, &mut hook_errors, 0.0);
        assert!(out.skipped);
        assert!(out.skip_reason.is_none());
    }

    // --- select_post_run_hooks ---

    fn dummy_hook(name: &str) -> HookConfig {
        HookConfig {
            workflow_type: name.to_string(),
            task_queue: "q".to_string(),
            timeout_secs: None,
            on_error: crate::types::HookErrorMode::default(),
            input: None,
            fire_and_forget: false,
        }
    }

    #[test]
    fn select_post_run_hooks_picks_on_success_when_run_succeeded() {
        let mut hooks = HooksConfig::default();
        hooks.on_success.push(dummy_hook("succ"));
        hooks.on_failure.push(dummy_hook("fail"));
        let (selected, event) = select_post_run_hooks(true, &hooks);
        assert_eq!(event, HookEvent::OnSuccess);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].workflow_type, "succ");
    }

    #[test]
    fn select_post_run_hooks_picks_on_failure_when_run_failed() {
        let mut hooks = HooksConfig::default();
        hooks.on_success.push(dummy_hook("succ"));
        hooks.on_failure.push(dummy_hook("fail"));
        let (selected, event) = select_post_run_hooks(false, &hooks);
        assert_eq!(event, HookEvent::OnFailure);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].workflow_type, "fail");
    }

    #[test]
    fn select_post_run_hooks_returns_empty_slice_when_corresponding_list_unset() {
        let hooks = HooksConfig::default();
        let (sel_succ, _) = select_post_run_hooks(true, &hooks);
        let (sel_fail, _) = select_post_run_hooks(false, &hooks);
        assert!(sel_succ.is_empty());
        assert!(sel_fail.is_empty());
    }

    // --- split_cancellation ---

    #[test]
    fn a_cancelled_hook_ends_the_workflow_as_cancelled() {
        let ended = split_cancellation::<()>(Err(HooksError::Cancelled));
        assert!(matches!(ended, Err(WorkflowTermination::Cancelled { .. })));
    }

    #[test]
    fn a_failed_hook_stays_the_hooks_own_verdict() {
        let split = split_cancellation::<()>(Err(HooksError::Failed(anyhow::anyhow!("boom"))));
        let Ok(Err(e)) = split else {
            panic!("a hook failure must reach the caller as a failure");
        };
        assert_eq!(e.to_string(), "boom");
    }

    #[test]
    fn a_completed_batch_passes_through() {
        assert!(matches!(split_cancellation(Ok(7)), Ok(Ok(7))));
    }

    // --- apply_post_run_outcome ---

    #[test]
    fn apply_post_run_outcome_ok_collects_errors_and_preserves_success() {
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[], 2);
        let success = apply_post_run_outcome(Ok(outcome), &mut hook_errors, true);
        assert!(success);
        assert_eq!(hook_errors.len(), 2);
    }

    #[test]
    fn apply_post_run_outcome_ok_preserves_failure_status() {
        // The post-hook ran cleanly but the run already failed → success
        // stays false.
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[], 0);
        let success = apply_post_run_outcome(Ok(outcome), &mut hook_errors, false);
        assert!(!success);
    }

    #[test]
    fn apply_post_run_outcome_err_marks_workflow_failed() {
        // post-hook itself errored — even on a run whose nodes all succeeded, the
        // workflow is marked failed. (dbt-core: a failed on_success turns the
        // run red.)
        let mut hook_errors = Vec::new();
        let success = apply_post_run_outcome(
            Err(anyhow::anyhow!("post-hook blew up")),
            &mut hook_errors,
            true,
        );
        assert!(!success);
        // Note: the err arm doesn't push to hook_errors — that loss is
        // intentional, the caller logs the error before calling here.
        assert!(hook_errors.is_empty());
    }

    // --- apply_pre_run_outcome ---

    fn make_outcome(
        skip: bool,
        reason: Option<&str>,
        extra_env: &[(&str, &str)],
        errors: usize,
    ) -> crate::types::HookExecutionOutcome {
        crate::types::HookExecutionOutcome {
            errors: (0..errors)
                .map(|i| HookError {
                    hook_workflow_type: format!("hook-{i}"),
                    event: "pre_run".to_string(),
                    error: format!("err-{i}"),
                })
                .collect(),
            skip,
            skip_reason: reason.map(String::from),
            extra_env: extra_env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    #[test]
    fn apply_pre_run_outcome_continues_when_no_skip_signal() {
        let plan = empty_plan();
        let mut env = BTreeMap::new();
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(false, None, &[("X", "y")], 1);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 0.5);
        assert!(matches!(action, ControlFlow::Continue(())));
        // Side effects: env merged, errors collected.
        assert_eq!(env.get("X").map(String::as_str), Some("y"));
        assert_eq!(hook_errors.len(), 1);
    }

    #[test]
    fn apply_pre_run_outcome_breaks_with_output_on_skip() {
        let plan = ExecutionPlan {
            invocation_id: "inv-skip".to_string(),
            ..empty_plan()
        };
        let mut env = BTreeMap::new();
        let mut hook_errors = Vec::new();
        let outcome = make_outcome(true, Some("not today"), &[("Y", "z")], 0);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 1.5);
        let ControlFlow::Break(out) = action else {
            panic!("skip outcome must Break");
        };
        assert!(out.success);
        assert!(out.skipped);
        assert_eq!(out.skip_reason.as_deref(), Some("not today"));
        // env still merged before the break (matches dbt-core: env state is
        // observable even on skip).
        assert_eq!(env.get("Y").map(String::as_str), Some("z"));
    }

    #[test]
    fn apply_pre_run_outcome_drains_hook_errors_on_skip() {
        // Skip path: any errors collected by the pre_run hooks are drained
        // into the returned DbtRunOutput.hook_errors so the caller can
        // surface them to the user.
        let plan = empty_plan();
        let mut env = BTreeMap::new();
        let mut hook_errors = vec![HookError {
            hook_workflow_type: "previous".to_string(),
            event: "pre_run".to_string(),
            error: "warn-1".to_string(),
        }];
        let outcome = make_outcome(true, None, &[], 1);

        let action = apply_pre_run_outcome(&plan, outcome, &mut env, &mut hook_errors, 0.0);
        let ControlFlow::Break(out) = action else {
            panic!("skip must Break");
        };
        // Outer accumulator drained.
        assert!(hook_errors.is_empty());
        // Output collected the previous warn + the new one from the outcome.
        assert_eq!(out.hook_errors.len(), 2);
    }
}
