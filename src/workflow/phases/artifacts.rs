//! Storing a run's artifacts through the `store_artifacts` activity.

use std::time::Duration;

use temporalio_sdk::{
    ActivityOptions, WorkflowCancellationToken, WorkflowContext, WorkflowTermination,
};

use crate::activities::DbtActivities;
use crate::types::{
    ExecutionPlan, NodeExecutionResult, RetryConfig, StoreArtifactsInput, StoreArtifactsOutput,
    TimeoutConfig,
};

use super::super::DbtRunWorkflow;
use super::super::helpers::RunFacts;
use super::activity_options::{activity_failure, bounded_activity_options};

/// What the run produced, as the artifact activity needs it.
///
/// A continued run's earlier segments are named, not carried: their results and
/// log stay in their own checkpoints until the activity collects them.
#[derive(Clone, Copy)]
pub struct RunRecord<'a> {
    pub results: &'a [NodeExecutionResult],
    pub log_lines: &'a [String],
    pub prior_segments: &'a [String],
}

/// Build the input for `store_artifacts` from the plan + run accumulators.
///
/// `command` travels with the results because the artifact set is
/// command-specific: only a freshness run writes `sources.json` /
/// `freshness.json`, and which of the two it writes depends on the spelling.
pub fn build_store_artifacts_input(
    plan: &ExecutionPlan,
    command: &str,
    record: RunRecord<'_>,
    facts: RunFacts<'_>,
) -> StoreArtifactsInput {
    StoreArtifactsInput {
        invocation_id: plan.invocation_id.clone(),
        project: Some(plan.project.clone()),
        command: Some(command.to_string()),
        node_results: record.results.to_vec(),
        prior_segments: record.prior_segments.to_vec(),
        manifest_json: plan.manifest_json.clone(),
        manifest_ref: plan.manifest_ref.clone(),
        run_log: Some(record.log_lines.join("\n")),
        env: facts.env.clone(),
        target: facts.target.map(ToString::to_string),
        started_at: facts
            .clock
            .started_at
            .map(chrono::DateTime::<chrono::Utc>::from),
        elapsed_time: facts.clock.elapsed_secs,
    }
}

pub async fn store_run_artifacts(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    command: &str,
    record: RunRecord<'_>,
    timeouts: &TimeoutConfig,
    retry: &RetryConfig,
    facts: RunFacts<'_>,
) -> Result<(Option<StoreArtifactsOutput>, Option<String>), WorkflowTermination> {
    if !plan.write_artifacts {
        return Ok((None, None));
    }
    let options =
        bounded_activity_options(Duration::from_secs(timeouts.store_artifacts_secs), retry);
    let artifacts = schedule_store_artifacts(ctx, plan, command, record, options, facts).await?;
    let log_path = artifacts.log_path.clone();
    Ok((Some(artifacts), log_path))
}

/// [`store_run_artifacts`] for a run that is already ending, cancelled or
/// failed.
///
/// The activity is detached from the workflow's cancellation. An activity
/// scheduled under a cancelled workflow is cancelled before it starts, and the
/// artifacts of a cancelled run are the ones its `retry_from` needs.
pub async fn store_artifacts_of_ended_run(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    command: &str,
    record: RunRecord<'_>,
    timeouts: &TimeoutConfig,
    retry: &RetryConfig,
    facts: RunFacts<'_>,
) -> Result<(), WorkflowTermination> {
    let mut options =
        bounded_activity_options(Duration::from_secs(timeouts.store_artifacts_secs), retry);
    options.cancellation_token = Some(WorkflowCancellationToken::new());
    schedule_store_artifacts(ctx, plan, command, record, options, facts)
        .await
        .map(|_| ())
}

async fn schedule_store_artifacts(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: &ExecutionPlan,
    command: &str,
    record: RunRecord<'_>,
    options: ActivityOptions,
    facts: RunFacts<'_>,
) -> Result<StoreArtifactsOutput, WorkflowTermination> {
    ctx.execute_activity(
        DbtActivities::store_artifacts,
        build_store_artifacts_input(plan, command, record, facts),
        options,
    )
    .await
    .map_err(|e| activity_failure(ctx, "store_artifacts activity failed", &e))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::test_support::empty_plan;
    use super::*;
    use crate::workflow::helpers::RunClock;

    /// Artifact inputs in these tests are about the manifest and the log, not
    /// the run's clock or connection.
    fn test_facts() -> RunFacts<'static> {
        static NO_ENV: std::sync::LazyLock<BTreeMap<String, String>> =
            std::sync::LazyLock::new(BTreeMap::new);
        RunFacts {
            clock: RunClock::default(),
            env: &NO_ENV,
            target: None,
        }
    }

    // --- build_store_artifacts_input ---

    #[test]
    fn build_store_artifacts_input_joins_log_lines_and_carries_manifest() {
        let plan = ExecutionPlan {
            invocation_id: "inv-store".to_string(),
            manifest_json: Some("{\"k\":1}".to_string()),
            manifest_ref: None,
            ..empty_plan()
        };
        let log_lines = vec!["line one".to_string(), "line two".to_string()];
        let store_input = build_store_artifacts_input(
            &plan,
            "build",
            RunRecord {
                results: &[],
                log_lines: &log_lines,
                prior_segments: &[],
            },
            test_facts(),
        );
        assert_eq!(store_input.invocation_id, "inv-store");
        assert_eq!(store_input.manifest_json.as_deref(), Some("{\"k\":1}"));
        assert!(store_input.manifest_ref.is_none());
        assert_eq!(store_input.run_log.as_deref(), Some("line one\nline two"));
    }

    #[test]
    fn build_store_artifacts_input_uses_manifest_ref_when_provided() {
        let plan = ExecutionPlan {
            invocation_id: "inv".to_string(),
            manifest_json: None,
            manifest_ref: Some("/path/to/manifest.json".to_string()),
            ..empty_plan()
        };
        let store_input = build_store_artifacts_input(
            &plan,
            "build",
            RunRecord {
                results: &[],
                log_lines: &[],
                prior_segments: &[],
            },
            test_facts(),
        );
        assert!(store_input.manifest_json.is_none());
        assert_eq!(store_input.manifest_ref.as_deref(), Some("/path/to/manifest.json"));
        // Empty log_lines join to empty string — still wrapped in Some.
        assert_eq!(store_input.run_log.as_deref(), Some(""));
    }
}
