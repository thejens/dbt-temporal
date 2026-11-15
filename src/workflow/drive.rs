//! The run driver: the phases of one workflow execution, in order.

use std::ops::ControlFlow;
use std::sync::Arc;

use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use super::DbtRunWorkflow;
use super::continuation::{
    build_resume_point, build_segment_state, continue_run_as_new, load_resume_state,
    next_segment_number,
};
use super::finish::{ExecutedRun, ExecutionRecord, RunEnd, RunProgress, StorePolicy};
use super::helpers::{
    READY_QUEUE_PATCH, RunClock, RunFacts, SINGLE_TERMINAL_MEMO_PATCH, elapsed_secs,
    upsert_memo_state,
};
use super::levels::{self, ResumePoint, execute_levels};
use super::phases::{
    HookPolicy, OnRunEndRecord, RunRecord, build_list_output, plan_and_announce,
    resolve_project_config, run_on_run_end, run_on_run_start, run_pre_run_hooks,
    run_project_checks, store_run_artifacts, write_command_memo,
};
use super::ready_queue::ReadyQueueRun;
use super::run_summary::append_run_summary;
use crate::types::{
    DbtCommand, DbtRunInput, DbtRunOutput, ExecutionPlan, NodeStatusTree, RetryConfig, RunPhase,
    RunScope, TimeoutConfig,
};

/// Run the phases of a dbt run in order, recording in `progress` what the
/// finisher needs if one of them fails.
///
/// Returns how the run ended when it got that far; an `Err` is an exit from a
/// phase, which `finish_run` turns into a terminal state.
#[allow(clippy::too_many_lines)] // A flat list of run phases reads better than nested helpers.
pub(super) async fn drive_run(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    start: Option<std::time::SystemTime>,
    progress: &mut RunProgress,
) -> Result<RunEnd, WorkflowTermination> {
    write_command_memo(ctx, input)?;

    // A continued run inherits its predecessor's plan and progress instead
    // of re-planning: re-running the selector could pick up a different
    // node set mid-run, and the pre_run / on-run-start hooks have already
    // fired for this logical run.
    let resumed = load_resume_state(ctx, input).await?;

    // A continuation restarts this execution's clock, but the artifacts
    // describe one logical run — so the first segment's start is the one
    // that counts, and it travels in the checkpoint.
    let run_started_at = resumed
        .as_ref()
        .and_then(|state| state.started_at)
        .map_or(start, |t| Some(std::time::SystemTime::from(t)));
    let run_started_at_utc = run_started_at.map(chrono::DateTime::<chrono::Utc>::from);

    ctx.state_mut(|s| s.status.phase = RunPhase::Planning);
    // Timeouts come from dbt_temporal.yml, but planning happens before that is
    // resolved — use the built-in default for the plan activity itself.
    let plan = match resumed.as_ref() {
        Some(state) => state.plan.clone(),
        None => plan_and_announce(ctx, input, TimeoutConfig::default().plan_secs).await?,
    };
    let plan = Arc::new(plan);
    progress.plan = Some(Arc::clone(&plan));
    ctx.state_mut(|s| {
        s.status.total_nodes = plan.levels.iter().map(Vec::len).sum();
        s.status.total_levels = plan.levels.len();
        s.status.phase = RunPhase::PreRunHooks;
    });

    // list: return the selected node set without executing any SQL.
    if DbtCommand::parse(&input.command) == Some(DbtCommand::List) {
        return Ok(RunEnd::Listed(build_list_output(
            &plan,
            elapsed_secs(start, ctx.workflow_time()),
        )));
    }

    let project_config = resolve_project_config(ctx, input, &plan).await?;
    let hooks = Arc::new(project_config.hooks);
    progress.hooks = Some(Arc::clone(&hooks));
    let retry_config = project_config.retry;
    let timeouts = project_config.timeouts;

    // The gate runs before any hook fires: a project that fails its checks
    // must not have executed side effects on the way to finding out. A
    // resumed run skips it, having already passed in its first segment.
    if resumed.is_none() {
        run_project_checks(ctx, input, &plan, &timeouts).await?;
    }

    // Effective env: workflow input env (with `_` set to the serialised
    // input), extended by pre_run hook extra_env. Used in NodeExecutionInput
    // so execute_node picks it up for env_var() rendering and per-workflow
    // adapter engine rebuilding (profiles.yml env_var() overrides).
    let mut effective_env;
    let mut hook_errors;
    if let Some(state) = resumed.as_ref() {
        // Both already ran in the first segment; re-running on-run-start
        // would re-execute its side effects.
        effective_env = state.effective_env.clone();
        hook_errors = state.hook_errors.clone();
    } else {
        effective_env = input.base_env();
        hook_errors = Vec::new();

        if let ControlFlow::Break(out) = run_pre_run_hooks(
            ctx,
            input,
            &plan,
            &hooks,
            &mut effective_env,
            &mut hook_errors,
            start,
        )
        .await?
        {
            return Ok(RunEnd::Skipped(out));
        }

        let hook_policy = HookPolicy {
            timeouts: &timeouts,
            retry: &retry_config,
        };
        let run_scope = RunScope::new(input, &effective_env, run_started_at_utc);
        run_on_run_start(ctx, &plan, &run_scope, hook_policy).await?;
    }

    // Stated once per run: every node's activity input carries a copy.
    let run_scope = RunScope::new(input, &effective_env, run_started_at_utc);
    ctx.state_mut(|s| s.status.phase = RunPhase::Executing);
    let resume_point = build_resume_point(&plan, resumed, plan.write_artifacts);
    let outcome =
        execute_nodes(ctx, input, &plan, &retry_config, &timeouts, &run_scope, resume_point)
            .await?;
    // From here the nodes have run, so every way out of this function leaves
    // their results with the finisher, which stores the artifacts of a run that
    // ends before this function does.
    let executed = progress.executed.insert(ExecutionRecord {
        levels: outcome,
        store: StorePolicy {
            timeouts: timeouts.clone(),
            retry: retry_config.clone(),
            env: effective_env.clone(),
            run_started_at,
        },
        artifacts_settled: false,
    });
    let levels = &mut executed.levels;

    // History is filling up: hand the remaining levels to a fresh run.
    if let Some(next_level) = levels.continue_at_level {
        let state = build_segment_state(
            &plan,
            levels,
            &effective_env,
            &hook_errors,
            next_level,
            next_segment_number(input.resume_from.as_ref()),
            run_started_at_utc,
        );
        return Err(continue_run_as_new(ctx, input, &plan, state).await);
    }
    ctx.state_mut(|s| {
        s.status.phase = RunPhase::Finalizing;
        // `tally` adds to the warned count instead of replacing it, and
        // both scheduling paths may already have published one.
        s.status.warned = 0;
        s.status.tally(&levels.node_status);
    });

    // A finished run's memo is written once, after the summary line. Only
    // the cancelled path returns before that write, so it publishes its
    // terminal state here. A history recorded without the patch marker
    // replays the unconditional write.
    if levels.was_cancelled || !ctx.patched(SINGLE_TERMINAL_MEMO_PATCH) {
        upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;
    }

    if levels.was_cancelled {
        return Err(WorkflowTermination::cancelled());
    }

    append_run_summary(levels, elapsed_secs(start, ctx.workflow_time()), &input.command);
    upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;

    let stored = store_run_artifacts(
        ctx,
        &plan,
        &input.command,
        RunRecord {
            results: &levels.all_results,
            log_lines: &levels.log_lines,
            prior_segments: &levels.prior_segments,
        },
        &timeouts,
        &retry_config,
        RunFacts {
            clock: RunClock {
                started_at: run_started_at,
                elapsed_secs: elapsed_secs(run_started_at, ctx.workflow_time()),
            },
            env: &effective_env,
            target: input.target.as_deref(),
        },
    )
    .await;
    let (artifacts, log_path) = match stored {
        Ok(stored) => {
            executed.artifacts_settled = true;
            stored
        }
        Err(e) => {
            // A write that failed is not tried again on the way out; one a
            // cancellation interrupted was never made, so the finisher makes it.
            executed.artifacts_settled = !matches!(e, WorkflowTermination::Cancelled { .. });
            return Err(e);
        }
    };

    let on_run_end_failed = run_on_run_end(
        ctx,
        &plan,
        &run_scope,
        OnRunEndRecord {
            results: &levels.all_results,
            prior_segments: &levels.prior_segments,
        },
        &mut hook_errors,
        HookPolicy {
            timeouts: &timeouts,
            retry: &retry_config,
        },
    )
    .await?;

    // Evaluated before `all_results` moves into the output.
    let node_results_partial = !levels.prior_segments.is_empty();
    let output = DbtRunOutput {
        invocation_id: plan.invocation_id.clone(),
        success: !levels.had_failure && !on_run_end_failed,
        skipped: false,
        skip_reason: None,
        node_results: std::mem::take(&mut levels.all_results),
        node_results_partial,
        elapsed_time: elapsed_secs(start, ctx.workflow_time()),
        log_path,
        artifacts,
        hook_errors: vec![],
    };
    Ok(RunEnd::Executed(Box::new(ExecutedRun {
        plan,
        hooks,
        output,
        node_status: NodeStatusTree {
            nodes: std::mem::take(&mut levels.node_status.nodes),
        },
        hook_errors,
    })))
}

/// Execute the plan's nodes, by readiness where the run may.
///
/// The level loop and the ready queue issue different command sequences. A run
/// whose history was recorded by the level loop has no patch marker, so
/// `patched` is false for it on replay and it keeps the level loop; an
/// execution that starts under this code records the marker and takes the ready
/// queue. That includes the successor of a run that continued as new from the
/// level loop: it is a new execution, and the checkpoint's node statuses and
/// failures are all the ready queue needs to pick up where the level loop
/// stopped.
pub(super) async fn execute_nodes(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    input: &DbtRunInput,
    plan: &ExecutionPlan,
    retry_config: &RetryConfig,
    timeouts: &TimeoutConfig,
    run_scope: &RunScope,
    resume: ResumePoint,
) -> Result<levels::LevelExecutionOutcome, WorkflowTermination> {
    if ctx.patched(READY_QUEUE_PATCH) {
        match ReadyQueueRun::new(plan, input, retry_config, timeouts, run_scope, &resume) {
            Ok(run) => return run.execute(ctx).await,
            // The plan's levels are valid by construction; edges that disagree
            // with them are not something to schedule by.
            Err(e) => {
                tracing::warn!(error = %e, "plan edges are not schedulable by readiness — running level by level");
            }
        }
    }
    execute_levels(ctx, input, plan, retry_config, timeouts, run_scope, resume).await
}
