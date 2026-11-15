//! Activity options and failure mapping shared by every phase that schedules
//! an activity outside the node loop.

use std::time::Duration;

use temporalio_sdk::error::ApplicationFailure;
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowTermination};

use crate::types::{ProjectHookPhase, RetryConfig, TimeoutConfig};

use super::super::DbtRunWorkflow;
use super::super::helpers::{
    FAILURE_FINALIZATION_PATCH, activity_error_message, build_retry_policy,
};

/// How the workflow ends when an activity outside the node loop does not
/// succeed.
///
/// A cancelled activity means the workflow itself was cancelled, and ends it as
/// cancelled. Anything else is a non-retryable workflow failure naming the
/// activity that failed and the reason it returned (see
/// [`activity_error_message`] for why that reason needs the `cause` chain),
/// so the workflow fails outright instead of the workflow task being retried
/// into the same activity error.
pub(super) fn activity_failure(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    what: &str,
    e: &temporalio_sdk::ActivityExecutionError,
) -> WorkflowTermination {
    // Histories recorded before cancellation was reported as such ended in
    // failure here; the marker is only consulted on this path.
    if matches!(e, temporalio_sdk::ActivityExecutionError::Cancelled(_))
        && ctx.patched(FAILURE_FINALIZATION_PATCH)
    {
        return WorkflowTermination::cancelled();
    }
    WorkflowTermination::failed_application(ApplicationFailure::non_retryable(anyhow::anyhow!(
        "{what}: {}",
        activity_error_message(e)
    )))
}

/// What a project-hook activity is allowed to do: how long it may run, and
/// whether it may retry.
///
/// Bundled because the two always travel together to both hook phases, and the
/// call sites already carry enough arguments.
#[derive(Clone, Copy)]
pub struct HookPolicy<'a> {
    pub timeouts: &'a TimeoutConfig,
    pub retry: &'a RetryConfig,
}

/// Activity options for a project-hook phase.
///
/// A retry policy is attached only when the phase opted in. Temporal's default
/// policy retries an unlimited number of times, so a hook that opts in without
/// one would hammer a failing warehouse forever; bounding it with the project's
/// own `retry` settings keeps hooks on the same budget as nodes. When the phase
/// has not opted in the activity reports every failure as non-retryable, so no
/// policy is needed.
pub(super) fn hook_activity_options(
    phase: ProjectHookPhase,
    policy: HookPolicy<'_>,
) -> ActivityOptions {
    let options = ActivityOptions::with_start_to_close_timeout(Duration::from_secs(
        policy.timeouts.hook_secs,
    ))
    .heartbeat_timeout(Duration::from_secs(policy.timeouts.hook_heartbeat_secs));
    if policy.retry.project_hooks.allows(phase) {
        options
            .retry_policy(build_retry_policy(policy.retry))
            .build()
    } else {
        options.build()
    }
}

/// Options for an activity outside the node loop, bounded by `retry`.
///
/// Temporal's default policy retries without limit, so an activity that keeps
/// failing for a reason its error cannot mark permanent would hold the run open
/// forever. The policy lists the same non-retryable failure types the node
/// activities do, so a failure the activity classifies as permanent (a bad
/// configuration, a missing bucket, access denied) ends the run at once.
pub(super) fn bounded_activity_options(timeout: Duration, retry: &RetryConfig) -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(timeout)
        .retry_policy(build_retry_policy(retry))
        .build()
}

/// Retry budget for the activities that run before the project's own
/// `dbt_temporal.yml` settings are resolved: planning, and restoring a
/// continued run's state.
pub(super) fn unresolved_retry() -> RetryConfig {
    RetryConfig::default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Unbounded retries are Temporal's default; every activity the workflow
    /// schedules outside the node loop must carry an explicit, bounded policy.
    #[test]
    fn a_bounded_activity_carries_the_projects_attempt_limit_and_timeout() {
        let retry = RetryConfig {
            max_attempts: 4,
            ..RetryConfig::default()
        };
        let opts = bounded_activity_options(Duration::from_secs(90), &retry);

        assert!(
            matches!(
                opts.close_timeouts,
                temporalio_common::ActivityCloseTimeouts::StartToClose(t) if t == Duration::from_secs(90)
            ),
            "{:?}",
            opts.close_timeouts
        );
        let policy = opts
            .retry_policy
            .expect("the activity must bound its retries");
        assert_eq!(policy.maximum_attempts(), 4);
    }

    /// The failure types the activities stamp on permanent errors must be
    /// listed, or Temporal retries them regardless of the non-retryable flag's
    /// intent whenever the type is the only thing a policy sees.
    #[test]
    fn a_bounded_activity_does_not_retry_permanent_failure_types() {
        let opts = bounded_activity_options(Duration::from_secs(5), &RetryConfig::default());
        let policy = opts.retry_policy.expect("bounded");

        assert_eq!(
            policy.non_retryable_error_types(),
            crate::error::DbtTemporalError::NON_RETRYABLE_TYPE_NAMES
        );
        assert!(
            policy
                .non_retryable_error_types()
                .iter()
                .any(|name| name == "DbtTemporalError::ArtifactStorePermanent")
        );
    }

    /// Planning and checkpoint reads run before the project's own retry settings
    /// are resolved, so they get the built-in budget.
    #[test]
    fn activities_before_config_resolution_use_the_default_retry_budget() {
        assert_eq!(unresolved_retry().max_attempts, RetryConfig::default().max_attempts);
    }

    // --- project hook activity options ---

    /// Without an explicit policy Temporal retries an activity indefinitely, so
    /// an opted-in hook must carry the project's bounded one — otherwise
    /// enabling retries would turn a failing warehouse into an endless loop.
    #[test]
    fn an_opted_in_phase_gets_the_projects_bounded_retry_policy() {
        let retry = RetryConfig {
            max_attempts: 7,
            project_hooks: crate::types::ProjectHookRetry {
                on_run_start: true,
                on_run_end: false,
            },
            ..RetryConfig::default()
        };
        let timeouts = TimeoutConfig::default();
        let opts = hook_activity_options(
            ProjectHookPhase::OnRunStart,
            HookPolicy {
                timeouts: &timeouts,
                retry: &retry,
            },
        );
        let policy = opts
            .retry_policy
            .expect("an opted-in phase must bound its retries");
        assert_eq!(policy.maximum_attempts(), 7);
    }

    /// The phase that did not opt in reports every failure as non-retryable, so
    /// it needs no policy — and must not silently acquire one.
    #[test]
    fn a_phase_that_did_not_opt_in_gets_no_retry_policy() {
        let retry = RetryConfig {
            project_hooks: crate::types::ProjectHookRetry {
                on_run_start: true,
                on_run_end: false,
            },
            ..RetryConfig::default()
        };
        let timeouts = TimeoutConfig::default();
        let opts = hook_activity_options(
            ProjectHookPhase::OnRunEnd,
            HookPolicy {
                timeouts: &timeouts,
                retry: &retry,
            },
        );
        assert!(opts.retry_policy.is_none(), "on_run_end did not opt in");
    }

    /// Timeouts apply either way — opting in changes retries, not deadlines.
    #[test]
    fn hook_timeouts_apply_regardless_of_the_opt_in() {
        let timeouts = TimeoutConfig {
            hook_secs: 111,
            hook_heartbeat_secs: 60,
            ..TimeoutConfig::default()
        };
        for on in [true, false] {
            let retry = RetryConfig {
                project_hooks: crate::types::ProjectHookRetry {
                    on_run_start: on,
                    on_run_end: on,
                },
                ..RetryConfig::default()
            };
            let opts = hook_activity_options(
                ProjectHookPhase::OnRunStart,
                HookPolicy {
                    timeouts: &timeouts,
                    retry: &retry,
                },
            );
            assert_eq!(opts.heartbeat_timeout, Some(Duration::from_mins(1)));
        }
    }
}
