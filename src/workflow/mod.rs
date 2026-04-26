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
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};

use crate::types::{DbtRunInput, DbtRunOutput, NodeStatus};

use self::helpers::{build_effective_env, build_summary_lines, elapsed_secs, upsert_memo_state};
use self::levels::execute_levels;
use self::phases::{
    plan_and_announce, resolve_project_config, run_on_run_end, run_on_run_start, run_post_hooks,
    run_pre_run_hooks, store_run_artifacts, write_command_memo,
};

/// The main dbt-temporal workflow: plan → execute levels → collect → store artifacts.
#[workflow]
pub struct DbtRunWorkflow {
    input: DbtRunInput,
}

#[workflow_methods]
impl DbtRunWorkflow {
    #[init]
    #[allow(clippy::missing_const_for_fn)] // #[init] macro requires non-const.
    fn new(_ctx: &WorkflowContextView, input: DbtRunInput) -> Self {
        Self { input }
    }

    #[run(name = "dbt_run")]
    #[allow(
        clippy::needless_pass_by_ref_mut, // Required by the #[run] macro.
        clippy::future_not_send           // WorkflowContext uses Rc internally.
    )]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<DbtRunOutput> {
        // Deterministic workflow time — the workflow's docstring forbids
        // wall-clock sources because they break history replay.
        let start = ctx.workflow_time();
        let input = ctx.state(|s| s.input.clone());

        write_command_memo(ctx, &input)?;
        let plan = plan_and_announce(ctx, &input).await?;
        let project_config = resolve_project_config(ctx, &input, &plan).await?;
        let hooks = project_config.hooks;
        let retry_config = project_config.retry;

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
            return Ok(out);
        }

        run_on_run_start(ctx, &input, &plan, &effective_env).await?;

        let mut levels = execute_levels(ctx, &input, &plan, &retry_config, &effective_env).await?;

        // Final memo upsert with all nodes in terminal state.
        upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;

        if levels.was_cancelled {
            return Err(temporalio_sdk::WorkflowTermination::Cancelled);
        }

        // Run-log summary lines.
        let pass = levels
            .all_results
            .iter()
            .filter(|r| r.status == NodeStatus::Success)
            .count();
        let error = levels
            .all_results
            .iter()
            .filter(|r| r.status == NodeStatus::Error)
            .count();
        let skip = levels
            .all_results
            .iter()
            .filter(|r| r.status == NodeStatus::Skipped)
            .count();
        let elapsed = elapsed_secs(start, ctx.workflow_time());
        levels.log_lines.extend(build_summary_lines(
            levels.total_nodes,
            elapsed,
            pass,
            error,
            skip,
        ));
        upsert_memo_state(ctx, &levels.node_status, &levels.log_lines)?;

        let (artifacts, log_path) =
            store_run_artifacts(ctx, &plan, &levels.all_results, &levels.log_lines).await?;

        run_on_run_end(ctx, &input, &plan, &effective_env, &levels.all_results, &mut hook_errors)
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
            Err(temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                "dbt run failed: {error_count} node(s) errored (see workflow memo for details)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    /// Workflow code must be deterministic — env var reads break Temporal replay.
    ///
    /// This test scans the workflow module source files for `std::env::var` calls.
    /// If someone adds an env var read to workflow code, this test will fail and
    /// remind them to move it into an activity.
    #[test]
    #[allow(clippy::expect_used, clippy::unwrap_used)]
    fn workflow_code_does_not_read_env_vars() {
        let workflow_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/workflow");

        for entry in std::fs::read_dir(&workflow_dir).expect("read src/workflow") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "rs") {
                let contents = std::fs::read_to_string(&path).expect("read file");
                let filename = path.file_name().unwrap().to_string_lossy();

                // Skip #[cfg(test)] blocks — test code is not replayed.
                let non_test_content = strip_cfg_test_blocks(&contents);

                assert!(
                    !non_test_content.contains("std::env::var"),
                    "src/workflow/{filename} contains `std::env::var` outside #[cfg(test)]. \
                     Workflow code must be deterministic — move env var reads into an activity."
                );
                assert!(
                    !non_test_content.contains("std::env::set_var"),
                    "src/workflow/{filename} contains `std::env::set_var` outside #[cfg(test)]. \
                     Workflow code must never mutate process environment."
                );
            }
        }
    }

    /// Strip `#[cfg(test)] mod tests { ... }` blocks from source, leaving only
    /// production code for the determinism scan.
    fn strip_cfg_test_blocks(source: &str) -> String {
        // Simple heuristic: find `#[cfg(test)]` and skip until matching closing brace.
        let mut result = String::new();
        let mut chars = source.chars();
        let mut buf = String::new();

        while let Some(c) = chars.next() {
            buf.push(c);

            // Detect #[cfg(test)]
            if buf.ends_with("#[cfg(test)]") {
                // Remove the #[cfg(test)] from result
                let prefix_len = buf.len() - "#[cfg(test)]".len();
                result.push_str(&buf[..prefix_len]);
                buf.clear();

                // Skip until we find the opening brace, then skip the balanced block.
                let mut depth = 0u32;
                for c in chars.by_ref() {
                    if c == '{' {
                        depth += 1;
                    } else if c == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                continue;
            }

            // Flush buffer periodically to avoid unbounded growth.
            if buf.len() > 128 {
                let keep = "#[cfg(test)]".len();
                let mut flush = buf.len() - keep;
                // Ensure we split on a char boundary.
                while flush > 0 && !buf.is_char_boundary(flush) {
                    flush -= 1;
                }
                result.push_str(&buf[..flush]);
                buf = buf[flush..].to_string();
            }
        }
        result.push_str(&buf);
        result
    }

    #[test]
    fn strip_cfg_test_blocks_works() {
        let input = r#"
fn real_code() { std::env::var("X"); }
#[cfg(test)]
mod tests {
    fn test_code() { std::env::var("Y"); }
}
fn more_real() {}
"#;
        let stripped = strip_cfg_test_blocks(input);
        assert!(stripped.contains("std::env::var(\"X\")"));
        assert!(!stripped.contains("std::env::var(\"Y\")"));
        assert!(stripped.contains("more_real"));
    }
}
