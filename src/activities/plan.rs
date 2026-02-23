use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;
use temporalio_sdk::activities::ActivityContext;

use crate::types::{DbtRunInput, ExecutionPlan, NodeInfo};

use super::DbtActivities;
use super::dag::{build_dependency_map, inject_test_gates, topological_levels};
use super::selectors::apply_selectors;

const MANIFEST_INLINE_THRESHOLD: usize = 3 * 1024 * 1024; // 3 MB

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
        .workflow_execution
        .as_ref()
        .map_or_else(|| uuid::Uuid::new_v4().to_string(), |we| we.run_id.clone());

    // Collect all node IDs that match the command (run vs build).
    // Ephemeral models are excluded — they're inlined as CTEs wherever they're ref()'d
    // and never executed as standalone activities.
    let selected_ids: Vec<String> = state
        .resolver_state
        .nodes
        .iter()
        .filter(|(_, node)| {
            let rt = node.resource_type();
            let is_ephemeral = node
                .base()
                .materialized
                .to_string()
                .eq_ignore_ascii_case("ephemeral");
            if is_ephemeral {
                return false;
            }
            match input.command.as_str() {
                "run" => matches!(rt, NodeType::Model),
                "build" => matches!(
                    rt,
                    NodeType::Model | NodeType::Test | NodeType::Seed | NodeType::Snapshot
                ),
                _ => false,
            }
        })
        .map(|(id, _)| id.clone())
        .collect();

    if selected_ids.is_empty() {
        anyhow::bail!("no nodes found for command '{}'", input.command);
    }

    // Apply --select/--exclude filters.
    let selected_ids = apply_selectors(
        selected_ids,
        &state.resolver_state.nodes,
        input.select.as_deref(),
        input.exclude.as_deref(),
    )?;

    if selected_ids.is_empty() {
        anyhow::bail!("no nodes matched after applying selectors");
    }

    // Compute topological levels from the dependency graph.
    // Tests act as gates: non-test downstream nodes must wait for all tests on their
    // upstream model to pass before starting. If a test fails, downstreams are skipped.
    let mut deps = build_dependency_map(&state.resolver_state.nodes, &selected_ids);
    inject_test_gates(&state.resolver_state.nodes, &selected_ids, &mut deps);
    let levels = topological_levels(&deps)?;

    // Build NodeInfo for each selected node, using the computed deps (which include
    // test gates and ephemeral promotions) so the workflow's skip logic is correct.
    let mut nodes = BTreeMap::new();
    for unique_id in &selected_ids {
        if let Some(mut info) = build_node_info(&state.resolver_state.nodes, unique_id) {
            // Override depends_on with the computed deps (includes test gates).
            if let Some(computed_deps) = deps.get(unique_id) {
                info.depends_on = computed_deps.iter().cloned().collect();
            }
            nodes.insert(unique_id.clone(), info);
        }
    }

    let write_artifacts = activities.write_artifacts.0;

    // Build manifest only when artifact writing is enabled — the manifest can be
    // hundreds of KB (especially with adapter macro packages) and would otherwise
    // bloat Temporal workflow history for every run.
    let (inline_manifest, manifest_ref) = if write_artifacts {
        let manifest =
            dbt_schemas::schemas::manifest::build_manifest(&invocation_id, &state.resolver_state);
        let manifest_json = serde_json::to_string(&manifest)?;

        if manifest_json.len() < MANIFEST_INLINE_THRESHOLD {
            (Some(manifest_json), None)
        } else {
            let artifact_store = activities.artifact_store.as_ref().ok_or_else(|| {
                anyhow::anyhow!("ArtifactStore not configured but write_artifacts is enabled")
            })?;
            let path = artifact_store
                .store(&invocation_id, "manifest.json", manifest_json.as_bytes())
                .await?;
            (None, Some(path))
        }
    } else {
        (None, None)
    };

    // Build search attributes: start with static config, then add dynamic values.
    // Only attributes registered on the Temporal namespace are kept; unregistered
    // ones are dropped with a warning to prevent workflow task failures.
    let mut search_attributes = activities.search_attr_config.0.clone();
    search_attributes
        .entry("DbtProject".to_string())
        .or_insert_with(|| state.project_name.clone());
    search_attributes
        .entry("DbtCommand".to_string())
        .or_insert_with(|| input.command.clone());
    if let Some(ref target) = input.target {
        search_attributes
            .entry("DbtTarget".to_string())
            .or_insert_with(|| target.clone());
    }

    // Filter to only registered attributes.
    let registered = &activities.registered_attrs.0;
    if !registered.is_empty() {
        let before = search_attributes.len();
        search_attributes.retain(|k, _| {
            if registered.contains(k) {
                true
            } else {
                tracing::warn!(
                    attribute = %k,
                    "search attribute not registered on namespace — skipping \
                     (register it with `temporal operator search-attribute create`)"
                );
                false
            }
        });
        if before > 0 && search_attributes.is_empty() {
            tracing::warn!(
                "all search attributes were skipped — none are registered on the namespace"
            );
        }
    }

    Ok(ExecutionPlan {
        project: state.project_name.clone(),
        levels,
        nodes,
        manifest_json: inline_manifest,
        manifest_ref,
        invocation_id,
        search_attributes,
        write_artifacts,
    })
}

/// Extract NodeInfo from Nodes for a given unique_id.
fn build_node_info(nodes: &dbt_schemas::schemas::Nodes, unique_id: &str) -> Option<NodeInfo> {
    let node = nodes.get_node(unique_id)?;
    let common = node.common();
    let base = node.base();

    Some(NodeInfo {
        unique_id: common.unique_id.clone(),
        name: common.name.clone(),
        resource_type: node.resource_type().as_str_name().to_string(),
        materialization: Some(base.materialized.to_string()),
        package_name: common.package_name.clone(),
        depends_on: base
            .depends_on
            .nodes_with_ref_location
            .iter()
            .map(|(id, _)| id.clone())
            .collect(),
    })
}
