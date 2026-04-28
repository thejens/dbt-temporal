use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;
use temporalio_sdk::activities::ActivityContext;
use tracing::warn;

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
    // Generic-test macro definitions (`{% test foo(model, column_name) %}…{% endtest %}`)
    // are excluded — dbt-fusion's parser registers them as runnable test nodes with
    // empty `depends_on`, but the body is a macro definition, not a runnable query.
    // Executing them produces empty SQL and a `Syntax error: Unexpected ")"`.
    let mut skipped_macro_defs: Vec<String> = Vec::new();
    let selected_ids: Vec<String> = state
        .resolver_state
        .nodes
        .iter()
        .filter(|(id, node)| {
            let rt = node.resource_type();
            let is_ephemeral = node
                .base()
                .materialized
                .to_string()
                .eq_ignore_ascii_case("ephemeral");
            if is_ephemeral {
                return false;
            }
            if !command_includes_node_type(input.command.as_str(), rt) {
                return false;
            }
            if rt == NodeType::Test
                && node
                    .common()
                    .raw_code
                    .as_deref()
                    .is_some_and(raw_code_is_generic_test_macro_def)
            {
                skipped_macro_defs.push((*id).clone());
                return false;
            }
            true
        })
        .map(|(id, _)| id.clone())
        .collect();

    if !skipped_macro_defs.is_empty() {
        warn!(
            count = skipped_macro_defs.len(),
            ids = ?skipped_macro_defs,
            "skipping generic-test macro definitions misregistered as runnable tests \
             (file body is `{{% test ... %}}...{{% endtest %}}`, not an applied test)"
        );
    }

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

    let search_attributes = build_search_attributes(
        &activities.search_attr_config.0,
        &state.project_name,
        &input.command,
        input.target.as_deref(),
        &activities.registered_attrs.0,
    );

    let has_on_run_start = !state.resolver_state.operations.on_run_start.is_empty();
    let has_on_run_end = !state.resolver_state.operations.on_run_end.is_empty();

    Ok(ExecutionPlan {
        project: state.project_name.clone(),
        levels,
        nodes,
        manifest_json: inline_manifest,
        manifest_ref,
        invocation_id,
        search_attributes,
        write_artifacts,
        has_on_run_start,
        has_on_run_end,
    })
}

/// True if `raw_code` is the body of a generic-test macro definition rather than
/// an applied test: `{% test name(model, column_name) %}…{% endtest %}` (with
/// optional Jinja whitespace marks). dbt-fusion's parser misregisters these
/// files in `test-paths/` as runnable test nodes, but the body is a macro
/// definition, so executing it produces empty SQL.
fn raw_code_is_generic_test_macro_def(raw_code: &str) -> bool {
    let s = raw_code.trim_start();
    let Some(s) = s.strip_prefix("{%") else {
        return false;
    };
    // Optional whitespace-control marks: `{%-`, `{%+`.
    let s = s.strip_prefix(['-', '+']).unwrap_or(s);
    let s = s.trim_start();
    let Some(after_tag) = s.strip_prefix("test") else {
        return false;
    };
    // The tag name must end here — `{% test foo(...)` and `{% test(` count, but
    // `{% test_anything %}` (a different macro/keyword) does not.
    matches!(after_tag.chars().next(), Some(c) if c.is_whitespace() || c == '(')
}

/// Decide whether a node of resource type `rt` belongs in the plan for `command`.
///
/// `compile` renders SQL templates without executing — seeds are CSV (no SQL),
/// so they're excluded from compile to match `dbt compile` semantics. `run`
/// matches dbt's "models only" default; `build` matches its full graph.
fn command_includes_node_type(command: &str, rt: NodeType) -> bool {
    match command {
        "run" => matches!(rt, NodeType::Model),
        "build" => {
            matches!(rt, NodeType::Model | NodeType::Test | NodeType::Seed | NodeType::Snapshot)
        }
        "compile" => matches!(rt, NodeType::Model | NodeType::Test | NodeType::Snapshot),
        // test runs only test nodes — mirrors `dbt test` which assumes models already exist.
        "test" => matches!(rt, NodeType::Test),
        // list selects the full graph (same as build) — the workflow returns node metadata
        // without executing; no SQL is compiled or sent to the warehouse.
        "list" => {
            matches!(rt, NodeType::Model | NodeType::Test | NodeType::Seed | NodeType::Snapshot)
        }
        _ => false,
    }
}

/// Build the search-attribute map upserted on every workflow run.
///
/// Starts from the worker-startup static config, then layers the always-set
/// dynamic attributes (`DbtProject`, `DbtCommand`, `DbtTarget`) — without
/// overriding caller-provided values for the same keys. Finally filters to
/// only attributes registered on the namespace; unregistered ones are dropped
/// with a per-attribute warning so workflow task failures don't surprise the
/// user.
fn build_search_attributes(
    static_config: &BTreeMap<String, String>,
    project: &str,
    command: &str,
    target: Option<&str>,
    registered: &std::collections::BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut search_attributes = static_config.clone();
    search_attributes
        .entry("DbtProject".to_string())
        .or_insert_with(|| project.to_string());
    search_attributes
        .entry("DbtCommand".to_string())
        .or_insert_with(|| command.to_string());
    if let Some(target) = target {
        search_attributes
            .entry("DbtTarget".to_string())
            .or_insert_with(|| target.to_string());
    }

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

    search_attributes
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn run_command_only_includes_models() {
        assert!(command_includes_node_type("run", NodeType::Model));
        assert!(!command_includes_node_type("run", NodeType::Test));
        assert!(!command_includes_node_type("run", NodeType::Seed));
        assert!(!command_includes_node_type("run", NodeType::Snapshot));
    }

    #[test]
    fn build_command_includes_full_graph() {
        for rt in [
            NodeType::Model,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
        ] {
            assert!(command_includes_node_type("build", rt), "build should include {rt:?}");
        }
    }

    #[test]
    fn compile_command_includes_sql_nodes_but_not_seeds() {
        // Regression: an earlier version returned false for any non-{run, build}
        // command, producing "no nodes found for command 'compile'" on real projects.
        assert!(command_includes_node_type("compile", NodeType::Model));
        assert!(command_includes_node_type("compile", NodeType::Test));
        assert!(command_includes_node_type("compile", NodeType::Snapshot));
        // Seeds are CSV — there's no SQL to compile.
        assert!(!command_includes_node_type("compile", NodeType::Seed));
    }

    #[test]
    fn unknown_command_excludes_everything() {
        assert!(!command_includes_node_type("freshness", NodeType::Model));
        assert!(!command_includes_node_type("", NodeType::Model));
    }

    #[test]
    fn test_command_includes_only_tests() {
        assert!(command_includes_node_type("test", NodeType::Test));
        assert!(!command_includes_node_type("test", NodeType::Model));
        assert!(!command_includes_node_type("test", NodeType::Seed));
        assert!(!command_includes_node_type("test", NodeType::Snapshot));
    }

    #[test]
    fn list_command_includes_full_graph_like_build() {
        for rt in [
            NodeType::Model,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
        ] {
            assert!(command_includes_node_type("list", rt), "list should include {rt:?}");
        }
    }

    /// Regression: panda-cascade phantom-test macro defs must be detected so the
    /// planner can skip them. dbt-fusion registers each generic-test definition
    /// in `test-paths/` as a runnable test; the body looks like a macro def and
    /// renders to empty SQL → `Syntax error: Unexpected ")"`.
    #[test]
    fn raw_code_macro_def_detected_for_phantom_test() {
        let body = "{% test not_empty(model, column_name) %}\n\
            SELECT 'Table {{ model.table }} has no rows' AS error_message\n\
            FROM `{{ model.project }}.{{ model.dataset }}.__TABLES__`\n\
            WHERE table_id = '{{ model.table }}' AND row_count = 0\n\
            {% endtest %}\n";
        assert!(raw_code_is_generic_test_macro_def(body));
    }

    #[test]
    fn raw_code_macro_def_detected_with_jinja_whitespace_marks() {
        assert!(raw_code_is_generic_test_macro_def("{%- test x(m) -%}body{%- endtest -%}"));
        assert!(raw_code_is_generic_test_macro_def("{%+ test x(m) +%}body{%+ endtest +%}"));
        assert!(raw_code_is_generic_test_macro_def("  \n{% test x(m) %}body{% endtest %}"));
    }

    #[test]
    fn raw_code_macro_def_not_triggered_by_applied_test_or_unrelated_jinja() {
        // Real one-off test using ref() — the body starts with SQL, not `{% test`.
        assert!(!raw_code_is_generic_test_macro_def(
            "SELECT * FROM {{ ref('marketing_spend_daily') }} WHERE country_code IS NULL"
        ));
        // A user macro whose name happens to start with "test_" must not match.
        assert!(!raw_code_is_generic_test_macro_def(
            "{% macro test_something(x) %}select 1{% endmacro %}"
        ));
        // Bare `{% set ... %}` etc. obviously not.
        assert!(!raw_code_is_generic_test_macro_def("{% set x = 1 %}select 1"));
        // Empty / whitespace-only.
        assert!(!raw_code_is_generic_test_macro_def(""));
        assert!(!raw_code_is_generic_test_macro_def("   \n  "));
    }

    // --- build_node_info ---

    #[test]
    fn build_node_info_returns_none_for_unknown_id() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        assert!(build_node_info(&nodes, "model.shop.nope").is_none());
    }

    // --- build_search_attributes ---

    use std::collections::BTreeSet;

    fn registered_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn build_search_attributes_layers_dynamic_over_static() {
        let static_config = BTreeMap::new();
        // Empty registered set means filtering is skipped (all kept).
        let registered = BTreeSet::new();
        let attrs =
            build_search_attributes(&static_config, "shop", "build", Some("prod"), &registered);
        assert_eq!(attrs.get("DbtProject").map(String::as_str), Some("shop"));
        assert_eq!(attrs.get("DbtCommand").map(String::as_str), Some("build"));
        assert_eq!(attrs.get("DbtTarget").map(String::as_str), Some("prod"));
    }

    #[test]
    fn build_search_attributes_static_config_wins_over_dynamic() {
        // If the worker config explicitly pinned DbtProject (or DbtCommand
        // etc.), the dynamic value must not overwrite it. `entry().or_insert`
        // gives the static config priority.
        let mut static_config = BTreeMap::new();
        static_config.insert("DbtProject".to_string(), "pinned-project".to_string());
        let registered = BTreeSet::new();
        let attrs = build_search_attributes(&static_config, "shop", "build", None, &registered);
        assert_eq!(attrs.get("DbtProject").map(String::as_str), Some("pinned-project"));
    }

    #[test]
    fn build_search_attributes_skips_dbttarget_when_input_target_absent() {
        let static_config = BTreeMap::new();
        let registered = BTreeSet::new();
        let attrs = build_search_attributes(&static_config, "shop", "build", None, &registered);
        assert!(!attrs.contains_key("DbtTarget"));
    }

    #[test]
    fn build_search_attributes_filters_to_registered_when_set() {
        let mut static_config = BTreeMap::new();
        static_config.insert("env".to_string(), "test".to_string());
        static_config.insert("team".to_string(), "data".to_string());

        // Only DbtProject is registered → static + DbtCommand + DbtTarget all dropped.
        let registered = registered_set(&["DbtProject"]);
        let attrs =
            build_search_attributes(&static_config, "shop", "build", Some("prod"), &registered);
        assert_eq!(attrs.len(), 1);
        assert!(attrs.contains_key("DbtProject"));
        assert!(!attrs.contains_key("env"));
        assert!(!attrs.contains_key("DbtCommand"));
    }

    #[test]
    fn build_search_attributes_keeps_all_when_registered_set_empty() {
        // Empty registered set is the "all attributes are valid" default —
        // skip the filter entirely so users without a configured registry
        // still get DbtProject/DbtCommand etc.
        let mut static_config = BTreeMap::new();
        static_config.insert("env".to_string(), "test".to_string());
        let registered = BTreeSet::new();
        let attrs = build_search_attributes(&static_config, "shop", "build", None, &registered);
        assert_eq!(attrs.len(), 3); // env + DbtProject + DbtCommand
        assert!(attrs.contains_key("env"));
        assert!(attrs.contains_key("DbtProject"));
        assert!(attrs.contains_key("DbtCommand"));
    }

    #[test]
    fn build_search_attributes_returns_empty_when_nothing_registered() {
        // Static config is non-empty but none of the keys are registered.
        let mut static_config = BTreeMap::new();
        static_config.insert("foo".to_string(), "bar".to_string());
        let registered = registered_set(&["DbtTarget"]); // present but no match
        let attrs = build_search_attributes(&static_config, "shop", "build", None, &registered);
        assert!(attrs.is_empty());
    }

    #[test]
    fn build_node_info_extracts_fields_and_dependencies() {
        use std::sync::Arc;

        use dbt_common::CodeLocationWithFile;
        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::common::NodeDependsOn;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

        let mut nodes = Nodes::default();
        let common = CommonAttributes {
            unique_id: "model.shop.orders".to_string(),
            name: "orders".to_string(),
            package_name: "shop".to_string(),
            ..CommonAttributes::default()
        };
        let depends_on = NodeDependsOn {
            nodes_with_ref_location: vec![
                ("model.shop.stg_orders".to_string(), CodeLocationWithFile::default()),
                ("model.shop.stg_customers".to_string(), CodeLocationWithFile::default()),
            ],
            ..NodeDependsOn::default()
        };
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: common,
                __base_attr__: NodeBaseAttributes {
                    depends_on,
                    ..NodeBaseAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let info = build_node_info(&nodes, "model.shop.orders").expect("orders is in the registry");
        assert_eq!(info.unique_id, "model.shop.orders");
        assert_eq!(info.name, "orders");
        assert_eq!(info.package_name, "shop");
        assert_eq!(info.resource_type, "NODE_TYPE_MODEL");
        assert_eq!(info.depends_on.len(), 2);
        assert!(
            info.depends_on
                .contains(&"model.shop.stg_orders".to_string())
        );
        assert!(
            info.depends_on
                .contains(&"model.shop.stg_customers".to_string())
        );
    }
}
