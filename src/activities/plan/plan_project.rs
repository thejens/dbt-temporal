//! The plan activity: select, order and describe the nodes of one run.

use std::collections::BTreeMap;

use anyhow::Context as _;
use bytes::Bytes;
use temporalio_sdk::activities::ActivityContext;
use tracing::{info, warn};

use crate::types::{DbtCommand, DbtRunInput, ExecutionPlan};

use super::super::DbtActivities;
use super::super::dag::{
    build_dependency_map, independent_nodes, inject_test_gates, inject_unit_test_gates,
    topological_levels,
};
use super::super::indirect::{IndirectSelection, expand_indirect_selection};
use super::super::selectors::{StateSelector, apply_selectors, selectors_use_state};
use super::nodes::build_node_info;
use super::priorities::compute_priorities;
use super::retry::parse_retry_node_ids;
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
    artifact_store: Option<&std::sync::Arc<dyn crate::artifact_store::ArtifactStore>>,
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

/// Plan activity inner logic — called from DbtActivities::plan_project.
#[allow(clippy::too_many_lines)]
pub async fn plan_project_inner(
    activities: &DbtActivities,
    ctx: &ActivityContext,
    input: DbtRunInput,
) -> Result<ExecutionPlan, anyhow::Error> {
    let state = activities.registry.get(input.project.as_deref())?;

    let invocation_id = ctx
        .info()
        .workflow_run_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    validate_microbatch_window(&input)?;

    let selected_ids = select_command_node_ids(state, &input)?;
    // The command's own node-type filter bounds what indirect selection may
    // add later: `run` never executes tests, so it must never gain any.
    let command_eligible: std::collections::BTreeSet<String> =
        selected_ids.iter().cloned().collect();

    // state: selectors compare against a previous manifest, loaded up front so
    // a missing state_manifest_ref fails with a clear error instead of
    // silently matching nothing.
    let state_selector = if selectors_use_state(input.select.as_deref(), input.exclude.as_deref())?
    {
        let manifest_ref = input.state_manifest_ref.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "select/exclude use state: methods — set state_manifest_ref to a previous \
                 manifest.json artifact"
            )
        })?;
        let artifact_store = activities.artifact_store.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "state_manifest_ref requires artifact storage to be configured \
                 (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
            )
        })?;
        let bytes = artifact_store.retrieve(manifest_ref).await.map_err(|e| {
            artifact_io_error(e.context(format!("loading state manifest from {manifest_ref}")))
        })?;
        let manifest: serde_json::Value =
            serde_json::from_slice(&bytes).context("parsing state manifest JSON")?;
        Some(StateSelector::from_previous_manifest(&state.resolver_state.nodes, &manifest))
    } else {
        None
    };

    // Apply --select/--exclude, pulling in the tests hanging off what --select
    // matched in between. Without that expansion `--select my_model` runs the
    // model and silently skips every test on it; running it before --exclude
    // is what keeps an explicitly excluded test excluded.
    //
    // `apply_selectors` returns early when neither filter is set, so the
    // expansion never runs for an unfiltered `build` — which already contains
    // every test.
    //
    // The mode is parsed up front so an unrecognised value is rejected before
    // any graph work, and on every run rather than only on filtered ones.
    let indirect_mode = parse_indirect_selection(input.indirect_selection.as_deref())?;
    let nodes = &state.resolver_state.nodes;
    let selected_ids = apply_selectors(
        selected_ids,
        nodes,
        input.select.as_deref(),
        input.exclude.as_deref(),
        state_selector.as_ref(),
        &|ids, maps| {
            let before = ids.len();
            let expanded =
                expand_indirect_selection(ids, nodes, maps, &command_eligible, indirect_mode);
            if expanded.len() > before {
                info!(
                    added = expanded.len() - before,
                    mode = ?indirect_mode,
                    "indirect selection added tests for the selected nodes"
                );
            }
            expanded
        },
    )?;

    if selected_ids.is_empty() {
        anyhow::bail!("no nodes matched after applying selectors");
    }

    // Retry-from-failure: narrow the selection to the nodes `dbt retry` would
    // re-run from a previous run (errored, failed and skipped ones; skipped
    // nodes were blocked by a failure).
    let selected_ids = if let Some(ref retry_ref) = input.retry_from {
        let artifact_store = activities.artifact_store.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "retry_from requires artifact storage to be configured \
                 (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
            )
        })?;
        let bytes = artifact_store.retrieve(retry_ref).await.map_err(|e| {
            artifact_io_error(e.context(format!("loading previous run_results from {retry_ref}")))
        })?;
        let retry_ids = parse_retry_node_ids(&bytes)?;
        let filtered: Vec<String> = selected_ids
            .into_iter()
            .filter(|id| retry_ids.contains(id))
            .collect();
        anyhow::ensure!(
            !filtered.is_empty(),
            "retry_from: no selected node errored, failed or was skipped in the previous run — nothing to retry"
        );
        warn!(
            retrying = filtered.len(),
            from = %retry_ref,
            "retry_from: narrowed plan to nodes that errored, failed or were skipped previously"
        );
        filtered
    } else {
        selected_ids
    };

    // Compute topological levels from the dependency graph.
    // Tests act as gates: non-test downstream nodes must wait for all tests on their
    // upstream model to pass before starting. If a test fails, downstreams are skipped.
    //
    // The freshness commands are the exception: they have no task graph at all,
    // so every measured node goes into one level.
    let deps = if DbtCommand::parse(&input.command).is_some_and(DbtCommand::is_freshness) {
        independent_nodes(&selected_ids)
    } else {
        let mut deps = build_dependency_map(&state.resolver_state.nodes, &selected_ids);
        // Unit test rewiring must run before data-test gates so unit tests
        // inherit the tested model's original inputs, not gate edges.
        inject_unit_test_gates(&state.resolver_state.nodes, &selected_ids, &mut deps);
        inject_test_gates(&state.resolver_state.nodes, &selected_ids, &mut deps);
        deps
    };
    let levels = topological_levels(&deps)?;

    // Priority keys from critical-path depth: nodes with longer downstream
    // chains block more of the run, so they get higher priority when activity
    // slots are scarce.
    let priorities = if activities.priority_scheduling.0 {
        compute_priorities(&levels, &deps)
    } else {
        BTreeMap::new()
    };

    // Build NodeInfo for each selected node, using the computed deps (which include
    // test gates and ephemeral promotions) so the workflow's skip logic is correct.
    let mut nodes = BTreeMap::new();
    for unique_id in &selected_ids {
        if let Some(mut info) = build_node_info(&state.resolver_state.nodes, unique_id) {
            // Override depends_on with the computed deps (includes test gates).
            if let Some(computed_deps) = deps.get(unique_id) {
                info.depends_on = computed_deps.iter().cloned().collect();
            }
            info.priority = priorities.get(unique_id).copied();
            nodes.insert(unique_id.clone(), info);
        }
    }

    let write_artifacts = activities.write_artifacts.0;

    // Build the manifest only when artifact writing is enabled — it can run to
    // hundreds of KB (especially with adapter macro packages) and nothing else
    // in the run needs it.
    let manifest_ref = if write_artifacts {
        let template = state
            .manifest
            .get_or_build(|| {
                dbt_schemas::schemas::manifest::build_manifest("", &state.resolver_state)
            })
            .await?;
        let manifest_json = template.render(
            &invocation_id,
            chrono::Utc::now(),
            dbt_common::constants::collect_dbt_custom_envs(),
        )?;
        Some(
            store_manifest_json(activities.artifact_store.as_ref(), &invocation_id, manifest_json)
                .await?,
        )
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

    let has_on_run_start = !state.resolver_state.operations.on_run_start.is_empty();
    let has_on_run_end = !state.resolver_state.operations.on_run_end.is_empty();
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
        priority_scheduling: activities.priority_scheduling.0,
        has_project_checks,
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
        let store: std::sync::Arc<dyn crate::artifact_store::ArtifactStore> = std::sync::Arc::new(
            crate::artifact_store::LocalArtifactStore::new(dir.path().to_path_buf()),
        );

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
