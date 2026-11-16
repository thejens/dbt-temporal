//! The plan activity: select, order and describe the nodes of one run.

use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;
use temporalio_sdk::activities::ActivityContext;
use tracing::warn;

use crate::types::{DbtRunInput, ExecutionPlan};
use crate::worker_state::WorkerState;

use super::super::DbtActivities;
use super::super::indirect::IndirectSelection;
use super::super::selectors::selectors_use_state;
use super::empty::{hooks_skipped, nothing_selected_warnings};
use super::graph::{PlannedGraph, filter_selection, plan_graph};
use super::search_attributes::build_search_attributes;
use super::select::select_command_node_ids;

/// Parse the workflow's `indirect_selection`, defaulting to dbt's `eager`.
///
/// A typo is rejected rather than silently falling back — picking the wrong
/// mode changes which tests run, which is exactly the kind of difference that
/// goes unnoticed in a green run.
fn parse_indirect_selection(raw: Option<&str>) -> Result<IndirectSelection, anyhow::Error> {
    raw.map_or_else(
        || Ok(IndirectSelection::default()),
        |value| {
            value
                .parse::<IndirectSelection>()
                .map_err(|e| anyhow::anyhow!(e))
        },
    )
}

/// Tag an artifact-store read/write as retryable.
///
/// The planner's own failures are permanent (bad selector, missing project),
/// but its object-store round-trips fail transiently like any network call and
/// must not sink the run on a single 5xx.
fn artifact_io_error(e: anyhow::Error) -> anyhow::Error {
    crate::error::DbtTemporalError::ArtifactStore(e).into()
}

/// Check the microbatch window before any node runs.
///
/// The window is two strings on the workflow input. If every node activity
/// parsed them for itself, a typo, a reversed pair, or only one of the two
/// would be discovered once per node, or not at all: with just one of them
/// set, the windowing would not activate and the run would quietly rebuild
/// every batch of every microbatch model.
fn validate_microbatch_window(input: &DbtRunInput) -> Result<(), anyhow::Error> {
    let (start, end) = match (&input.event_time_start, &input.event_time_end) {
        (None, None) => return Ok(()),
        (Some(_), None) => {
            anyhow::bail!(
                "event_time_start is set without event_time_end — a microbatch window needs both, and one alone silently disables windowing"
            )
        }
        (None, Some(_)) => {
            anyhow::bail!(
                "event_time_end is set without event_time_start — a microbatch window needs both, and one alone silently disables windowing"
            )
        }
        (Some(start), Some(end)) => (start, end),
    };

    let parsed_start: chrono::DateTime<chrono::Utc> = start
        .parse()
        .with_context(|| format!("parsing event_time_start: {start}"))?;
    let parsed_end: chrono::DateTime<chrono::Utc> = end
        .parse()
        .with_context(|| format!("parsing event_time_end: {end}"))?;

    anyhow::ensure!(
        parsed_start < parsed_end,
        "microbatch window starts at or after it ends ({start} .. {end}); no batch can fall inside it"
    );
    Ok(())
}

/// Store the run's manifest and return its artifact-store reference.
///
/// The manifest never travels inline. `ExecutionPlan` is an activity result, so
/// anything carried in it becomes a Temporal history payload, and a manifest
/// routinely exceeds the 2 MB payload limit on its own — before the rest of the
/// plan, JSON string escaping, and every later copy of the plan are added.
///
/// A missing store is an error rather than a fallback: the end-of-run
/// `store_artifacts` activity needs the same store, so a run configured to
/// write artifacts without one cannot finish anyway. Failing here surfaces it
/// before a single node executes.
async fn store_manifest_json(
    artifact_store: Option<&Arc<dyn crate::artifact_store::ArtifactStore>>,
    invocation_id: &str,
    manifest_json: String,
) -> Result<String, anyhow::Error> {
    let store = artifact_store.ok_or_else(|| {
        anyhow::anyhow!("ArtifactStore not configured but write_artifacts is enabled")
    })?;
    store
        .store(invocation_id, "manifest.json", Bytes::from(manifest_json))
        .await
        .map_err(|e| artifact_io_error(e.context("storing manifest.json")))
}

/// Render this run's manifest and store it, returning its reference.
async fn store_run_manifest(
    activities: &DbtActivities,
    state: &Arc<WorkerState>,
    invocation_id: &str,
) -> Result<String, anyhow::Error> {
    let for_build = Arc::clone(state);
    let template = state
        .manifest
        .get_or_build(move || {
            dbt_schemas::schemas::manifest::build_manifest("", &for_build.resolver_state)
        })
        .await?;
    // The body is tens of megabytes on a large project: copying it into the
    // run's document is not work for an async thread either.
    let run_id = invocation_id.to_owned();
    let manifest_json = crate::dbt_pool::run(move || {
        template.render(
            &run_id,
            chrono::Utc::now(),
            dbt_common::constants::collect_dbt_custom_envs(),
        )
    })
    .await??;
    store_manifest_json(activities.artifact_store.as_ref(), invocation_id, manifest_json).await
}

/// Fetch a previous run's artifact the plan depends on.
async fn fetch_previous_artifact(
    activities: &DbtActivities,
    reference: &str,
    needed_for: &str,
    what: &str,
) -> Result<Bytes, anyhow::Error> {
    let artifact_store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "{needed_for} requires artifact storage to be configured \
             (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
        )
    })?;
    artifact_store
        .retrieve(reference)
        .await
        .map_err(|e| artifact_io_error(e.context(format!("loading {what} from {reference}"))))
}

/// Fetch the previous manifest `state:` selectors compare against, when the
/// run's selectors use one.
///
/// Loaded up front so a missing `state_manifest_ref` fails with a clear error
/// instead of silently matching nothing.
async fn fetch_state_manifest(
    activities: &DbtActivities,
    input: &DbtRunInput,
) -> Result<Option<Bytes>, anyhow::Error> {
    if !selectors_use_state(input.select.as_deref(), input.exclude.as_deref())? {
        return Ok(None);
    }
    let manifest_ref = input.state_manifest_ref.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "select/exclude use state: methods — set state_manifest_ref to a previous \
             manifest.json artifact"
        )
    })?;
    fetch_previous_artifact(activities, manifest_ref, "state_manifest_ref", "state manifest")
        .await
        .map(Some)
}

/// Plan activity inner logic — called from DbtActivities::plan_project.
///
/// Planning alternates between artifact-store round-trips, which are awaited
/// here, and CPU work over the whole project, which runs on the dbt pool: on a
/// large project selection, the DAG build and manifest serialization each take
/// long enough to stall the tokio worker thread, and with it the heartbeats and
/// workflow tasks of everything else on this worker. The activity sets no
/// heartbeat of its own because the workflow gives it only a start-to-close
/// timeout.
pub async fn plan_project_inner(
    activities: &DbtActivities,
    ctx: &ActivityContext,
    input: DbtRunInput,
) -> Result<ExecutionPlan, anyhow::Error> {
    let state = Arc::clone(activities.registry.get(input.project.as_deref())?);

    let invocation_id = ctx
        .info()
        .workflow_run_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // First thing the run asks of the worker: a refused override must stop the
    // run before any node is selected, let alone executed.
    state
        .override_policy
        .check(&input.env, input.target.as_deref(), &state.default_target)?;

    validate_microbatch_window(&input)?;
    // The mode is parsed up front so an unrecognised value is rejected before
    // any graph work, and on every run rather than only on filtered ones.
    let indirect_mode = parse_indirect_selection(input.indirect_selection.as_deref())?;
    let input = Arc::new(input);

    let selected_ids = {
        let (state, input) = (Arc::clone(&state), Arc::clone(&input));
        crate::dbt_pool::run(move || select_command_node_ids(&state, &input)).await??
    };

    let state_manifest = fetch_state_manifest(activities, &input).await?;

    let selected_ids = {
        let (state, input) = (Arc::clone(&state), Arc::clone(&input));
        crate::dbt_pool::run(move || {
            filter_selection(&state, &input, indirect_mode, selected_ids, state_manifest.as_deref())
        })
        .await??
    };

    // An empty selection is not an error, as in dbt: the plan simply holds no
    // nodes and `nothing_selected_warnings` says so. Everything above that can
    // reject a selector (syntax, unknown methods, `state:` without a manifest)
    // has already run, so only a valid selection that matches nothing gets here.
    let selected_before_retry = selected_ids.len();

    let previous_run_results = match input.retry_from.as_deref() {
        Some(retry_ref) => Some(
            fetch_previous_artifact(activities, retry_ref, "retry_from", "previous run_results")
                .await?,
        ),
        None => None,
    };

    let priority_scheduling = activities.priority_scheduling.0;
    let PlannedGraph {
        levels,
        nodes,
        selected,
    } = {
        let (state, input) = (Arc::clone(&state), Arc::clone(&input));
        crate::dbt_pool::run(move || {
            plan_graph(
                &state,
                &input,
                selected_ids,
                previous_run_results.as_deref(),
                priority_scheduling,
            )
        })
        .await??
    };

    let write_artifacts = activities.write_artifacts.0;

    // Build the manifest only when artifact writing is enabled — it can run to
    // hundreds of KB (especially with adapter macro packages) and nothing else
    // in the run needs it.
    let manifest_ref = if write_artifacts {
        Some(store_run_manifest(activities, &state, &invocation_id).await?)
    } else {
        None
    };

    let search_attributes = build_search_attributes(
        &activities.search_attr_config.0,
        &state.project_name,
        &input.command,
        input.target.as_deref(),
        input.select.as_deref(),
        &activities.registered_attrs.0,
    );

    let warnings = nothing_selected_warnings(&input, selected_before_retry, selected);
    for line in &warnings {
        warn!("{line}");
    }

    // dbt runs no hooks for an empty selection of these commands; see
    // `DbtCommand::skips_hooks_when_nothing_selected`.
    let hooks_skipped = hooks_skipped(&input.command, selected);
    let has_on_run_start =
        !hooks_skipped && !state.resolver_state.operations.on_run_start.is_empty();
    let has_on_run_end = !hooks_skipped && !state.resolver_state.operations.on_run_end.is_empty();
    let has_project_checks = state
        .project_checks
        .as_ref()
        .is_some_and(|checks| !checks.checks.is_empty());

    Ok(ExecutionPlan {
        project: state.project_name.clone(),
        levels,
        nodes,
        manifest_json: None,
        manifest_ref,
        invocation_id,
        search_attributes,
        write_artifacts,
        has_on_run_start,
        has_on_run_end,
        priority_scheduling,
        has_project_checks,
        warnings,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn window_input(start: Option<&str>, end: Option<&str>) -> DbtRunInput {
        serde_json::from_value(serde_json::json!({
            "command": "run",
            "event_time_start": start,
            "event_time_end": end,
        }))
        .expect("valid input")
    }

    #[test]
    fn a_run_without_a_microbatch_window_is_fine() {
        assert!(validate_microbatch_window(&window_input(None, None)).is_ok());
    }

    /// Half a window would activate nothing: the run would rebuild every batch
    /// of every microbatch model and report success.
    #[test]
    fn half_a_microbatch_window_is_rejected() {
        for (start, end, missing) in [
            (Some("2026-01-01T00:00:00Z"), None, "event_time_end"),
            (None, Some("2026-01-02T00:00:00Z"), "event_time_start"),
        ] {
            let err = validate_microbatch_window(&window_input(start, end))
                .expect_err("half a window must be refused");
            assert!(err.to_string().contains(missing), "should name what is missing: {err}");
        }
    }

    #[test]
    fn a_microbatch_window_that_does_not_move_forward_is_rejected() {
        let err = validate_microbatch_window(&window_input(
            Some("2026-01-02T00:00:00Z"),
            Some("2026-01-01T00:00:00Z"),
        ))
        .expect_err("a backwards window must be refused");
        assert!(err.to_string().contains("starts at or after it ends"), "{err}");

        let err = validate_microbatch_window(&window_input(
            Some("2026-01-01T00:00:00Z"),
            Some("2026-01-01T00:00:00Z"),
        ))
        .expect_err("an empty window must be refused");
        assert!(err.to_string().contains("starts at or after it ends"), "{err}");
    }

    #[test]
    fn an_unparseable_microbatch_bound_names_itself() {
        let err = validate_microbatch_window(&window_input(
            Some("yesterday"),
            Some("2026-01-02T00:00:00Z"),
        ))
        .expect_err("a non-timestamp must be refused");
        assert!(format!("{err:#}").contains("event_time_start"), "{err:#}");
    }

    #[test]
    fn a_well_formed_microbatch_window_is_accepted() {
        assert!(
            validate_microbatch_window(&window_input(
                Some("2026-01-01T00:00:00Z"),
                Some("2026-01-02T00:00:00Z"),
            ))
            .is_ok()
        );
    }

    // --- store_manifest_json ---

    #[tokio::test]
    async fn store_manifest_json_writes_to_the_store_and_returns_its_reference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn crate::artifact_store::ArtifactStore> =
            Arc::new(crate::artifact_store::LocalArtifactStore::new(dir.path().to_path_buf()));

        let manifest = r#"{"nodes":{}}"#;
        let path = store_manifest_json(Some(&store), "inv-1", manifest.to_string())
            .await
            .expect("storing the manifest should succeed");

        assert!(path.ends_with("manifest.json"), "{path}");
        assert_eq!(std::fs::read_to_string(&path).expect("stored file"), manifest);
    }

    /// Without a store there is nowhere to put the manifest, and the run could
    /// not write its artifacts at the end either — so planning fails now rather
    /// than after every node has executed.
    #[tokio::test]
    async fn store_manifest_json_without_a_store_names_the_missing_configuration() {
        let err = store_manifest_json(None, "inv-1", "{}".to_string())
            .await
            .expect_err("a missing store must fail");
        assert!(
            err.to_string().contains("ArtifactStore not configured"),
            "unexpected error: {err:#}"
        );
    }
}
