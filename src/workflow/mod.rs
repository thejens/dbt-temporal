//! Temporal workflow definition — must be fully deterministic.
//!
//! **No environment variable reads, filesystem I/O, wall-clock time, or random numbers.**
//! All non-deterministic work (SQL rendering, env var reads, DB access) belongs
//! in activities. This constraint is enforced by a test in this module.

// The `#[workflow_methods]` macro generates types without Debug impls.
#![allow(missing_debug_implementations)]
// WorkflowContext uses Rc internally, so no future touching it is Send —
// levels.rs, phases.rs and the run driver silence the same lint for the same reason.
#![allow(clippy::future_not_send)]

mod continuation;
mod drive;
mod finish;
mod helpers;
mod levels;
mod node_activity;
mod phases;
mod ready_queue;
mod run_summary;
pub mod scheduler;
mod search_attributes;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

use self::continuation::inherited_fail_fast_override;
use self::drive::drive_run;
use self::finish::{RunProgress, finish_run};
use crate::types::{DbtRunInput, DbtRunOutput, RunPhase, RunStatusSnapshot};

/// The main dbt-temporal workflow: plan → execute levels → collect → store artifacts.
#[workflow]
pub struct DbtRunWorkflow {
    input: DbtRunInput,
    /// Live progress for the `run_status` query; updated at phase transitions
    /// and level boundaries.
    status: RunStatusSnapshot,
    /// Mid-run fail-fast override set by the `set_fail_fast` update; the level
    /// loop consults it at each level boundary, the ready queue before each
    /// scheduling decision.
    fail_fast_override: Option<bool>,
}

#[workflow_methods]
impl DbtRunWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, input: DbtRunInput) -> Self {
        let fail_fast_override = inherited_fail_fast_override(&input);
        let status = RunStatusSnapshot {
            phase: RunPhase::Initializing,
            fail_fast: fail_fast_override.unwrap_or(input.fail_fast),
            ..RunStatusSnapshot::default()
        };
        Self {
            input,
            status,
            fail_fast_override,
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

    /// Toggle fail-fast mid-run. Takes effect at the next scheduling decision
    /// (the next level boundary when scheduling by level): enabling it after a
    /// failure stops starting further nodes; disabling it lets a run configured
    /// with fail_fast continue past failures.
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
    #[allow(clippy::needless_pass_by_ref_mut)] // Required by the #[run] macro.
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<DbtRunOutput> {
        // Deterministic workflow time — the workflow's docstring forbids
        // wall-clock sources because they break history replay.
        let start = ctx.workflow_time();
        let input = ctx.state(|s| s.input.clone());

        // Every way out of the run — success, failure, cancellation — passes
        // through `finish_run`, so the run's recorded state cannot be left
        // behind by an early return in a phase.
        let mut progress = RunProgress::default();
        let ended = drive_run(ctx, &input, start, &mut progress).await;
        finish_run(ctx, &input, start, progress, ended).await
    }
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
    /// Scans `src/workflow/**/*.rs` for the patterns above. Test modules are
    /// exempt (they are never replayed) and by convention sit at the end of
    /// each file, so the scan stops at the first `#[cfg(test)]`.
    #[test]
    #[allow(clippy::expect_used, clippy::unwrap_used)]
    fn workflow_code_is_deterministic() {
        let workflow_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/workflow");

        let mut checked = 0;
        for path in workflow_sources(&workflow_dir) {
            let contents = std::fs::read_to_string(&path).expect("read file");
            let filename = path
                .strip_prefix(&workflow_dir)
                .unwrap()
                .to_string_lossy()
                .into_owned();
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

    /// Every replayed source file under `dir`, submodule directories included.
    /// Files that hold only test code (`tests.rs`, `tests/`, `test_support.rs`)
    /// are never replayed and carry no `#[cfg(test)]` marker of their own, so
    /// they are skipped by name.
    #[allow(clippy::expect_used)]
    fn workflow_sources(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).expect("read workflow dir") {
            let path = entry.expect("dir entry").path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if matches!(stem, "tests" | "test_support") {
                continue;
            }
            if path.is_dir() {
                files.extend(workflow_sources(&path));
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
        files
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
