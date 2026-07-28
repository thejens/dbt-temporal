//! Temporal workflow definition — must be fully deterministic.
//!
//! **No environment variable reads, filesystem I/O, wall-clock time, or random numbers.**
//! All non-deterministic work (SQL rendering, env var reads, DB access) belongs
//! in activities. This constraint is enforced by a test in this module.

// The `#[workflow_methods]` macro generates types without Debug impls.
#![allow(missing_debug_implementations)]

mod helpers;
mod levels;
mod phases;

use std::ops::ControlFlow;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{
    SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult, WorkflowTermination,
};

use crate::types::{DbtRunInput, DbtRunOutput, NodeStatus, RunStatusSnapshot, TimeoutConfig};

use self::helpers::{
    build_effective_env, build_summary_lines, elapsed_secs, format_final_details, upsert_memo_state,
};
use self::levels::execute_levels;
use self::phases::{
    build_list_output, plan_and_announce, resolve_project_config, run_on_run_end, run_on_run_start,
    run_post_hooks, run_pre_run_hooks, store_run_artifacts, upsert_terminal_status,
    write_command_memo,
};

/// The main dbt-temporal workflow: plan → execute levels → collect → store artifacts.
#[workflow]
pub struct DbtRunWorkflow {
    input: DbtRunInput,
    /// Live progress for the `run_status` query; updated at phase transitions
    /// and level boundaries.
    status: RunStatusSnapshot,
    /// Mid-run fail-fast override set by the `set_fail_fast` update; the level
    /// loop consults it at each level boundary.
    fail_fast_override: Option<bool>,
}

#[workflow_methods]
impl DbtRunWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, input: DbtRunInput) -> Self {
        let status = RunStatusSnapshot {
            phase: "initializing".to_string(),
            fail_fast: input.fail_fast,
            ..RunStatusSnapshot::default()
        };
        Self {
            input,
            status,
            fail_fast_override: None,
        }
    }

    /// Live run progress — cheaper for callers than `describe` + memo
    /// decoding, and not subject to memo truncation limits.
    ///
    /// `temporal workflow query -w <id> --type run_status`
    #[query]
    pub fn run_status(&self, _ctx: &WorkflowContextView) -> RunStatusSnapshot {
        self.status.clone()
    }

    /// Toggle fail-fast mid-run. Takes effect at the next level boundary:
    /// enabling it after a failure stops scheduling further levels; disabling
    /// it lets a run configured with fail_fast continue past failures.
    ///
    /// `temporal workflow update execute -w <id> --name set_fail_fast -i true`
    #[update]
    #[allow(clippy::missing_const_for_fn)] // Handler is registered by the macro; const adds nothing.
    pub fn set_fail_fast(&mut self, _ctx: &mut SyncWorkflowContext<Self>, enabled: bool) -> bool {
        self.fail_fast_override = Some(enabled);
        self.status.fail_fast = enabled;
        enabled
    }

    #[run(name = "dbt_run")]
    #[allow(
        clippy::needless_pass_by_ref_mut, // Required by the #[run] macro.
        clippy::future_not_send,          // WorkflowContext uses Rc internally.
        clippy::too_many_lines            // A flat list of run phases reads better than nested helpers.
    )]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<DbtRunOutput> {
        // Deterministic workflow time — the workflow's docstring forbids
        // wall-clock sources because they break history replay.
        let start = ctx.workflow_time();
        let input = ctx.state(|s| s.input.clone());

        write_command_memo(ctx, &input)?;
        ctx.state_mut(|s| s.status.phase = "planning".to_string());
        // Timeouts come from dbt_temporal.yml, but planning happens before that is
        // resolved — use the built-in default for the plan activity itself.
        let plan = plan_and_announce(ctx, &input, TimeoutConfig::default().plan_secs).await?;
        ctx.state_mut(|s| {
            s.status.total_nodes = plan.levels.iter().map(Vec::len).sum();
            s.status.total_levels = plan.levels.len();
            s.status.phase = "pre_run_hooks".to_string();
        });

        // list: return the selected node set without executing any SQL.
        if input.command == "list" {
            let out = build_list_output(&plan, elapsed_secs(start, ctx.workflow_time()));
            upsert_terminal_status(ctx, &plan, "passed")?;
            ctx.set_current_details("list".to_string());
            return Ok(out);
        }

        let project_config = resolve_project_config(ctx, &input, &plan).await?;
        let hooks = project_config.hooks;
        let retry_config = project_config.retry;
        let timeouts = project_config.timeouts;

        // Effective env: workflow input env (with `_` set to the serialised
        // input), extended by pre_run hook extra_env. Used in NodeExecutionInput
        // so execute_node picks it up for env_var() rendering and per-workflow
        // adapter engine rebuilding (profiles.yml env_var() overrides).
        let mut effective_env = build_effective_env(&input);
        let mut hook_errors = Vec::new();

        if let ControlFlow::Break(out) = run_pre_run_hooks(
            ctx,
            &input,
            &plan,
            &hooks,
            &mut effective_env,
            &mut hook_errors,
            start,
        )
        .await?
        {
            upsert_terminal_status(ctx, &plan, "skipped")?;
            ctx.set_current_details("skipped by pre_run hook".to_string());
            return Ok(out);
        }

        run_on_run_start(ctx, &input, &plan, &effective_env, &timeouts).await?;

        ctx.state_mut(|s| s.status.phase = "executing".to_string());
        let mut levels =
            execute_levels(ctx, &input, &plan, &retry_config, &timeouts, &effective_env).await?;
        ctx.state_mut(|s| {
            s.status.phase = "finalizing".to_string();
            s.status.tally(&levels.node_status);
        });

        // Final memo upsert with all nodes in terminal state.
        upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;

        if levels.was_cancelled {
            upsert_terminal_status(ctx, &plan, "cancelled")?;
            ctx.set_current_details("cancelled".to_string());
            return Err(WorkflowTermination::Cancelled);
        }

        append_run_summary(&mut levels, elapsed_secs(start, ctx.workflow_time()));
        upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;

        let (artifacts, log_path) =
            store_run_artifacts(ctx, &plan, &levels.all_results, &levels.log_lines, &timeouts)
                .await?;

        run_on_run_end(
            ctx,
            &input,
            &plan,
            &effective_env,
            &levels.all_results,
            &mut hook_errors,
            &timeouts,
        )
        .await;

        let mut output = DbtRunOutput {
            invocation_id: plan.invocation_id.clone(),
            success: !levels.had_failure,
            skipped: false,
            skip_reason: None,
            node_results: levels.all_results,
            elapsed_time: elapsed_secs(start, ctx.workflow_time()),
            log_path,
            artifacts,
            hook_errors: vec![],
        };

        let success = run_post_hooks(
            ctx,
            &input,
            &plan,
            &hooks,
            &output,
            !levels.had_failure,
            &mut hook_errors,
        )
        .await;

        output.success = success;
        output.hook_errors = hook_errors;

        ctx.set_current_details(format_final_details(
            &output.node_results,
            success,
            output.elapsed_time,
        ));
        upsert_terminal_status(ctx, &plan, if success { "passed" } else { "failed" })?;

        if success {
            Ok(output)
        } else {
            // Fail the workflow so Temporal marks it FAILED (not COMPLETED).
            // Structured output is already persisted in memo for callers to inspect.
            let error_count = output
                .node_results
                .iter()
                .filter(|r| r.status == NodeStatus::Error)
                .count();
            Err(WorkflowTermination::failed_application(ApplicationFailure::non_retryable(
                anyhow::anyhow!(
                    "dbt run failed: {error_count} node(s) errored (see workflow memo for details)"
                ),
            )))
        }
    }
}

/// Append the CLI-style run summary (pass/error/skip tallies) to the run log.
fn append_run_summary(levels: &mut levels::LevelExecutionOutcome, elapsed: f64) {
    let count_status = |s: NodeStatus| levels.all_results.iter().filter(|r| r.status == s).count();
    let pass = count_status(NodeStatus::Success);
    let error = count_status(NodeStatus::Error);
    let skip = count_status(NodeStatus::Skipped);
    levels
        .log_lines
        .extend(build_summary_lines(levels.total_nodes, elapsed, pass, error, skip));
}

#[cfg(test)]
mod tests {
    /// Sources of non-determinism that break Temporal history replay, as they
    /// appear in source. Suffix-matched, so both `std::env::var` and a bare
    /// `env::var` after `use std::env` are caught.
    const FORBIDDEN_IN_WORKFLOW: &[(&str, &str)] = &[
        ("env::var", "read env vars — move the read into an activity"),
        ("env::set_var", "mutate the process environment"),
        ("SystemTime::now", "read the wall clock — use ctx.workflow_time()"),
        ("Instant::now", "read the wall clock — use ctx.workflow_time()"),
        ("Utc::now", "read the wall clock — use ctx.workflow_time()"),
        ("Uuid::new_v4", "generate randomness — derive ids from the plan or input"),
        ("thread_rng", "generate randomness — derive ids from the plan or input"),
    ];

    /// Workflow code must be deterministic — replay re-executes it against a
    /// recorded history, so anything that can differ between the original run
    /// and the replay corrupts the workflow.
    ///
    /// Scans `src/workflow/*.rs` for the patterns above. Test modules are
    /// exempt (they are never replayed) and by convention sit at the end of
    /// each file, so the scan stops at the first `#[cfg(test)]`.
    #[test]
    #[allow(clippy::expect_used, clippy::unwrap_used)]
    fn workflow_code_is_deterministic() {
        let workflow_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/workflow");

        let mut checked = 0;
        for entry in std::fs::read_dir(&workflow_dir).expect("read src/workflow") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            let contents = std::fs::read_to_string(&path).expect("read file");
            let filename = path.file_name().unwrap().to_string_lossy().into_owned();
            let production = executable_code(production_code(&contents));
            checked += 1;

            for (pattern, why) in FORBIDDEN_IN_WORKFLOW {
                assert!(
                    !production.contains(pattern),
                    "src/workflow/{filename} contains `{pattern}` outside #[cfg(test)]. \
                     Workflow code must not {why}."
                );
            }
        }
        assert!(checked > 0, "determinism scan found no files — did the module move?");
    }

    /// The part of a source file that gets replayed: everything before the
    /// first `#[cfg(test)]`.
    fn production_code(source: &str) -> &str {
        source
            .find("#[cfg(test)]")
            .map_or(source, |cut| &source[..cut])
    }

    /// Drop comment lines. Prose explaining *why* a forbidden API is avoided
    /// ("use ctx.workflow_time() instead of Instant::now()") is exactly the
    /// documentation this rule wants, and must not trip the scan.
    fn executable_code(source: &str) -> String {
        source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn production_code_stops_at_the_test_module() {
        let input = "fn real() { env::var(\"X\"); }\n\
                     #[cfg(test)]\n\
                     mod tests { fn t() { env::var(\"Y\"); } }\n";
        let production = production_code(input);
        assert!(production.contains("env::var(\"X\")"));
        assert!(!production.contains("env::var(\"Y\")"));
    }

    #[test]
    fn production_code_returns_everything_when_there_is_no_test_module() {
        let input = "fn only_real() {}\n";
        assert_eq!(production_code(input), input);
    }

    /// The scan is suffix-based, so an unqualified `env::var` is caught too —
    /// the previous version only looked for the `std::`-prefixed spelling.
    #[test]
    fn scan_catches_unqualified_spellings() {
        let code = executable_code(production_code("use std::env;\nfn f() { env::var(\"X\"); }\n"));
        assert!(
            FORBIDDEN_IN_WORKFLOW
                .iter()
                .any(|(pattern, _)| code.contains(pattern))
        );
    }

    #[test]
    fn scan_ignores_forbidden_apis_named_in_comments() {
        let code = executable_code("/// Prefer workflow_time() over Instant::now().\nfn f() {}\n");
        assert!(!code.contains("Instant::now"), "comment should be stripped: {code}");
    }
}
