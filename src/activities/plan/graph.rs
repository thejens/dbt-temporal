//! The synchronous stages of planning: filtering the selection and turning it
//! into levels and per-node metadata.
//!
//! All of it is CPU work over the project's resolved nodes, plus parsing the
//! manifests the async half of planning has already fetched. On a large project
//! it runs for long enough to stall every other task sharing a tokio worker
//! thread, heartbeats and workflow tasks included, so the caller runs each
//! function here on the dbt pool (see `crate::dbt_pool`) rather than inline.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context as _;
use tracing::{info, warn};

use crate::types::{DbtCommand, DbtRunInput, NodeInfo};
use crate::worker_state::WorkerState;

use super::super::dag::{
    build_dependency_map, independent_nodes, inject_test_gates, inject_unit_test_gates,
    topological_levels,
};
use super::super::indirect::{IndirectSelection, expand_indirect_selection};
use super::super::selectors::{StateSelector, apply_selectors};
use super::empty::narrow_to_retry;
use super::nodes::build_node_info;
use super::priorities::compute_priorities;
use super::retry::parse_retry_node_ids;

/// What planning hands the workflow: the order nodes run in and what is known
/// about each.
pub(super) struct PlannedGraph {
    pub levels: Vec<Vec<String>>,
    pub nodes: BTreeMap<String, NodeInfo>,
    /// How many nodes the plan selected once `retry_from` narrowed it; zero
    /// makes the run a "nothing to do".
    pub selected: usize,
}

/// Apply `--select`/`--exclude` to the command's nodes, pulling in the tests
/// hanging off what `--select` matched in between. Without that expansion
/// `--select my_model` runs the model and silently skips every test on it;
/// running it before `--exclude` is what keeps an explicitly excluded test
/// excluded.
///
/// `apply_selectors` returns early when neither filter is set, so the expansion
/// never runs for an unfiltered `build` — which already contains every test.
///
/// `state_manifest` is the previous manifest for `state:` selectors, fetched by
/// the caller so that a missing `state_manifest_ref` fails with a clear error
/// instead of silently matching nothing.
pub(super) fn filter_selection(
    state: &WorkerState,
    input: &DbtRunInput,
    indirect_mode: IndirectSelection,
    selected_ids: Vec<String>,
    state_manifest: Option<&[u8]>,
) -> Result<Vec<String>, anyhow::Error> {
    let nodes = &state.resolver_state.nodes;
    // The command's own node-type filter bounds what indirect selection may
    // add later: `run` never executes tests, so it must never gain any.
    let command_eligible: BTreeSet<String> = selected_ids.iter().cloned().collect();

    let state_selector = state_manifest
        .map(|bytes| {
            let manifest: serde_json::Value =
                serde_json::from_slice(bytes).context("parsing state manifest JSON")?;
            Ok::<_, anyhow::Error>(StateSelector::from_previous_manifest(nodes, &manifest))
        })
        .transpose()?;

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

    // An empty result is a valid selection that matched nothing, which the
    // plan reports as a warning; a selector that cannot be evaluated has
    // already failed above.
    Ok(selected_ids)
}

/// Order the selected nodes into levels and describe each of them.
///
/// `previous_run_results` is the `run_results.json` of the run `retry_from`
/// names; it narrows the selection to the nodes `dbt retry` would re-run
/// (errored, failed and skipped ones; skipped nodes were blocked by a failure).
pub(super) fn plan_graph(
    state: &WorkerState,
    input: &DbtRunInput,
    selected_ids: Vec<String>,
    previous_run_results: Option<&[u8]>,
    priority_scheduling: bool,
) -> Result<PlannedGraph, anyhow::Error> {
    let selected_ids = match (input.retry_from.as_deref(), previous_run_results) {
        (Some(retry_ref), Some(bytes)) => retry_selection(selected_ids, retry_ref, bytes)?,
        _ => selected_ids,
    };
    let resolved = &state.resolver_state.nodes;

    // Tests act as gates: non-test downstream nodes must wait for all tests on their
    // upstream model to pass before starting. If a test fails, downstreams are skipped.
    //
    // The freshness commands are the exception: they have no task graph at all,
    // so every measured node goes into one level.
    let deps = if DbtCommand::parse(&input.command).is_some_and(DbtCommand::is_freshness) {
        independent_nodes(&selected_ids)
    } else {
        let mut deps = build_dependency_map(resolved, &selected_ids);
        // Unit test rewiring must run before data-test gates so unit tests
        // inherit the tested model's original inputs, not gate edges.
        inject_unit_test_gates(resolved, &selected_ids, &mut deps);
        inject_test_gates(resolved, &selected_ids, &mut deps);
        deps
    };
    let levels = topological_levels(&deps)?;

    // Priority keys from critical-path depth: nodes with longer downstream
    // chains block more of the run, so they get higher priority when activity
    // slots are scarce.
    let priorities = if priority_scheduling {
        compute_priorities(&levels, &deps)
    } else {
        BTreeMap::new()
    };

    // Build NodeInfo for each selected node, using the computed deps (which include
    // test gates and ephemeral promotions) so the workflow's skip logic is correct.
    let mut nodes = BTreeMap::new();
    for unique_id in &selected_ids {
        if let Some(mut info) = build_node_info(resolved, unique_id) {
            // Override depends_on with the computed deps (includes test gates).
            if let Some(computed_deps) = deps.get(unique_id) {
                info.depends_on = computed_deps.iter().cloned().collect();
            }
            info.priority = priorities.get(unique_id).copied();
            nodes.insert(unique_id.clone(), info);
        }
    }
    Ok(PlannedGraph {
        levels,
        nodes,
        selected: selected_ids.len(),
    })
}

/// Keep the selected nodes that errored, failed or were skipped in the run
/// `run_results` belongs to. Narrowing to nothing is not an error: the previous
/// run left nothing unfinished, and the plan says so in its warnings.
fn retry_selection(
    selected_ids: Vec<String>,
    retry_ref: &str,
    run_results: &[u8],
) -> Result<Vec<String>, anyhow::Error> {
    let retry_ids = parse_retry_node_ids(run_results)?;
    let filtered = narrow_to_retry(selected_ids, &retry_ids);
    warn!(
        retrying = filtered.len(),
        from = %retry_ref,
        "retry_from: narrowed plan to nodes that errored, failed or were skipped previously"
    );
    Ok(filtered)
}
