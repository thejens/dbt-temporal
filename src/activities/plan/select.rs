//! Which nodes a command considers at all, before selectors narrow them.

use anyhow::Context as _;
use dbt_common::io_args::ClapResourceType;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::telemetry::NodeType;
use tracing::warn;

use crate::types::{DbtCommand, DbtRunInput};
use crate::worker_state::WorkerState;

use super::super::execute_node::freshness;
use super::vars::warn_parse_time_vars;

/// Collect all node IDs that match the command (run vs build), applying the
/// exclusions that happen before selectors.
///
/// Ephemeral models are excluded — they're inlined as CTEs wherever they're
/// ref()'d and never executed as standalone activities. Generic-test macro
/// definitions (`{% test foo(model, column_name) %}…{% endtest %}`) are
/// excluded — dbt-fusion's parser registers them as runnable test nodes with
/// empty `depends_on`, but the body is a macro definition, not a runnable
/// query. Executing them produces empty SQL and a `Syntax error: Unexpected ")"`.
///
/// Pure over `state`/`input` (no `ActivityContext`), so it's unit-testable
/// without a live Temporal worker — also the entry point for integration
/// tests that drive selection against an embedded engine (e.g. the DuckDB
/// scenario harness), where standing up an `ActivityContext` would add nothing.
pub fn select_command_node_ids(
    state: &WorkerState,
    input: &DbtRunInput,
) -> Result<Vec<String>, anyhow::Error> {
    let command = DbtCommand::parse(&input.command).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown command {:?} — expected one of: {}",
            input.command,
            DbtCommand::ALL.map(DbtCommand::as_str).join(", ")
        )
    })?;
    let include_types =
        parse_resource_types(&input.resource_types).context("invalid --resource-type")?;
    let exclude_types = parse_resource_types(&input.exclude_resource_types)
        .context("invalid --exclude-resource-type")?;
    let freshness_command = command.is_freshness();
    let nodes = &state.resolver_state.nodes;
    let mut skipped_macro_defs: Vec<String> = Vec::new();
    let mut unmeasurable_slas: Vec<String> = Vec::new();
    let mut invalid_rules: Vec<String> = Vec::new();
    let selected_ids: Vec<String> = nodes
        .iter()
        .filter(|(id, node)| {
            let rt = node.resource_type();
            if node.base().materialized == DbtMaterialization::Ephemeral {
                return false;
            }
            if !command_includes_node_type(command, rt) {
                return false;
            }
            if !resource_type_allowed(rt, &include_types, &exclude_types) {
                return false;
            }
            if freshness_command {
                // A freshness plan holds only nodes we can actually measure:
                // criteria with a usable rule plus a loaded_at field/query.
                let Some(freshness_node) = freshness::as_freshness_node(nodes, id.as_str(), rt)
                else {
                    return false;
                };
                if let Err(e) = freshness::validate_freshness_rules(freshness_node) {
                    invalid_rules.push(e.to_string());
                    return false;
                }
                if freshness::declares_unmeasurable_sla(freshness_node) {
                    unmeasurable_slas.push((*id).clone());
                }
                return freshness::node_has_freshness_check(freshness_node);
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

    // A half-filled rule aborts the whole command rather than silently
    // dropping one node, matching dbt: it validates every selected node's rules
    // up front, before any warehouse query runs.
    if !invalid_rules.is_empty() {
        anyhow::bail!("invalid freshness criteria:\n{}", invalid_rules.join("\n"));
    }

    if !unmeasurable_slas.is_empty() {
        warn!(
            count = unmeasurable_slas.len(),
            ids = ?unmeasurable_slas,
            "skipping freshness nodes that declare warn_after/error_after but no \
             loaded_at_field or loaded_at_query — dbt would fall back to relation \
             metadata, which dbt-temporal does not query"
        );
    }

    warn_unsupported_resource_types(state, command);
    warn_parse_time_vars(state, &input.vars);

    Ok(selected_ids)
}

/// Warn about resource types this planner never schedules.
///
/// dbt Core v2 resolves exposures, metrics, saved queries and semantic models
/// into the same node graph as models, but dbt-temporal has no execution path
/// for them. Silently dropping them makes a run look complete when part of the
/// project was never built, so say so once per plan. Only graph-building
/// commands warn — the single-resource commands (`run`, `test`, `seed`, …)
/// legitimately ignore everything outside their own type.
///
/// Functions are absent from this list because they *are* executed — see
/// `command_includes_node_type`.
fn warn_unsupported_resource_types(state: &WorkerState, command: DbtCommand) {
    if !command.builds_whole_graph() {
        return;
    }
    let nodes = &state.resolver_state.nodes;
    let unsupported: Vec<(&str, usize)> = [
        ("exposures", nodes.exposures.len()),
        ("metrics", nodes.metrics.len()),
        ("saved_queries", nodes.saved_queries.len()),
        ("semantic_models", nodes.semantic_models.len()),
    ]
    .into_iter()
    .filter(|(_, count)| *count > 0)
    .collect();

    if !unsupported.is_empty() {
        warn!(
            resources = ?unsupported,
            "project defines resource types dbt-temporal does not execute — \
             they are excluded from the plan and will not be built"
        );
    }
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

/// Parse `--resource-type` / `--exclude-resource-type` values.
///
/// Routed through dbt's own `ClapResourceType` rather than a local table so
/// the accepted spellings cannot drift from the CLI's, and so an unknown value
/// is rejected with serde's "expected one of ..." listing rather than a bare
/// "invalid".
fn parse_resource_types(raw: &[String]) -> Result<Vec<NodeType>, anyhow::Error> {
    raw.iter()
        .map(|value| {
            let parsed: ClapResourceType =
                serde_json::from_value(serde_json::Value::String(value.clone()))
                    .with_context(|| format!("resource type '{value}'"))?;
            Ok(NodeType::from(&parsed))
        })
        .collect()
}

/// Whether `rt` survives `--resource-type` / `--exclude-resource-type`.
///
/// Both lists only ever narrow the command's own node-type filter: an empty
/// include list means "no restriction", and an exclusion always wins. Applied
/// to every command, not just freshness — dbt treats them the same way.
fn resource_type_allowed(rt: NodeType, include: &[NodeType], exclude: &[NodeType]) -> bool {
    (include.is_empty() || include.contains(&rt)) && !exclude.contains(&rt)
}

/// Decide whether a node of resource type `rt` belongs in the plan for `command`.
///
/// `compile` renders SQL templates without executing — seeds are CSV (no SQL),
/// so they're excluded from compile to match `dbt compile` semantics. `run`
/// matches dbt's "models only" default; `build` matches its full graph.
const fn command_includes_node_type(command: DbtCommand, rt: NodeType) -> bool {
    match command {
        DbtCommand::Run => matches!(rt, NodeType::Model),
        // `dbt source freshness`: checks sources only. Sources without
        // freshness criteria are filtered out separately in the planner.
        DbtCommand::SourceFreshness => matches!(rt, NodeType::Source),
        // `dbt freshness`: sources plus models carrying a freshness SLA.
        // Which of those are actually measurable — and which declare a rule
        // with no way to measure it — is decided in the planner.
        DbtCommand::Freshness => matches!(rt, NodeType::Source | NodeType::Model),
        DbtCommand::Build => matches!(
            rt,
            NodeType::Model
                | NodeType::Test
                | NodeType::Seed
                | NodeType::Snapshot
                | NodeType::UnitTest
                | NodeType::Function
        ),
        // Unit tests are hard-excluded from compile, matching dbt-core: their
        // SQL is assembled at execution time from fixtures + warehouse probes.
        DbtCommand::Compile => {
            matches!(rt, NodeType::Model | NodeType::Test | NodeType::Snapshot | NodeType::Function)
        }
        // `dbt test` runs data tests and unit tests, and assumes the models
        // they read already exist.
        DbtCommand::Test => matches!(rt, NodeType::Test | NodeType::UnitTest),
        // seed / snapshot mirror the single-resource dbt commands of the same
        // name. Both assume the rest of the graph is already in place.
        DbtCommand::Seed => matches!(rt, NodeType::Seed),
        DbtCommand::Snapshot => matches!(rt, NodeType::Snapshot),
        // list selects the full graph (same as build) — the workflow returns node metadata
        // without executing; no SQL is compiled or sent to the warehouse.
        DbtCommand::List => matches!(
            rt,
            NodeType::Model
                | NodeType::Test
                | NodeType::Seed
                | NodeType::Snapshot
                | NodeType::UnitTest
                | NodeType::Function
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn run_command_only_includes_models() {
        assert!(command_includes_node_type(DbtCommand::Run, NodeType::Model));
        assert!(!command_includes_node_type(DbtCommand::Run, NodeType::Test));
        assert!(!command_includes_node_type(DbtCommand::Run, NodeType::Seed));
        assert!(!command_includes_node_type(DbtCommand::Run, NodeType::Snapshot));
    }

    #[test]
    fn build_command_includes_full_graph() {
        for rt in [
            NodeType::Model,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
        ] {
            assert!(
                command_includes_node_type(DbtCommand::Build, rt),
                "build should include {rt:?}"
            );
        }
    }

    #[test]
    fn compile_command_includes_sql_nodes_but_not_seeds() {
        // Commands other than {run, build} must not be treated as excluding
        // everything, or planning fails with "no nodes found for command
        // 'compile'" on real projects.
        assert!(command_includes_node_type(DbtCommand::Compile, NodeType::Model));
        assert!(command_includes_node_type(DbtCommand::Compile, NodeType::Test));
        assert!(command_includes_node_type(DbtCommand::Compile, NodeType::Snapshot));
        // Seeds are CSV — there's no SQL to compile.
        assert!(!command_includes_node_type(DbtCommand::Compile, NodeType::Seed));
    }

    /// An unknown command would match no arm and plan an empty run, which
    /// then fails with "no nodes found" — technically an error, but one that
    /// blames the selection rather than the typo.
    #[test]
    fn an_unknown_command_is_not_a_command() {
        assert!(DbtCommand::parse("materialize").is_none());
        assert!(DbtCommand::parse("").is_none());
        assert!(DbtCommand::parse("Build").is_none(), "spelling is dbt's, not case-insensitive");
    }

    /// Every command round-trips through its wire spelling, so the enum cannot
    /// silently rename one.
    #[test]
    fn every_command_round_trips_through_its_wire_name() {
        for command in DbtCommand::ALL {
            assert_eq!(DbtCommand::parse(command.as_str()), Some(command));
        }
    }

    #[test]
    fn freshness_commands_cover_the_node_types_they_measure() {
        // `dbt source freshness` predates model freshness and stays
        // sources-only; the unified spelling adds SLA-carrying models.
        assert!(command_includes_node_type(DbtCommand::SourceFreshness, NodeType::Source));
        assert!(!command_includes_node_type(DbtCommand::SourceFreshness, NodeType::Model));
        assert!(command_includes_node_type(DbtCommand::Freshness, NodeType::Source));
        assert!(command_includes_node_type(DbtCommand::Freshness, NodeType::Model));
        for rt in [
            NodeType::Seed,
            NodeType::Snapshot,
            NodeType::Test,
            NodeType::UnitTest,
        ] {
            assert!(
                !command_includes_node_type(DbtCommand::Freshness, rt),
                "{rt:?} is not measurable"
            );
        }
        // Sources never take part in a build.
        assert!(!command_includes_node_type(DbtCommand::Build, NodeType::Source));
    }

    #[test]
    fn resource_types_only_narrow_the_command_filter() {
        let models = parse_resource_types(&["model".to_string()]).expect("valid");
        let sources = parse_resource_types(&["source".to_string()]).expect("valid");
        assert_eq!(models, vec![NodeType::Model]);

        // Empty include list means "no restriction".
        assert!(resource_type_allowed(NodeType::Source, &[], &[]));
        assert!(resource_type_allowed(NodeType::Model, &models, &[]));
        assert!(!resource_type_allowed(NodeType::Source, &models, &[]));
        // Exclusion always wins, even over an explicit include.
        assert!(!resource_type_allowed(NodeType::Source, &sources, &sources));
        assert!(!resource_type_allowed(NodeType::Source, &[], &sources));
    }

    #[test]
    fn resource_types_use_dbts_own_spellings() {
        assert_eq!(
            parse_resource_types(&["unit_test".to_string()]).expect("valid"),
            vec![NodeType::UnitTest]
        );
        let err = parse_resource_types(&["sources".to_string()]).expect_err("plural is not a type");
        // serde names the valid spellings, which is the useful half of the message.
        assert!(format!("{err:#}").contains("source"), "{err:#}");
    }

    #[test]
    fn test_command_includes_only_tests() {
        assert!(command_includes_node_type(DbtCommand::Test, NodeType::Test));
        assert!(command_includes_node_type(DbtCommand::Test, NodeType::UnitTest));
        assert!(!command_includes_node_type(DbtCommand::Test, NodeType::Model));
        assert!(!command_includes_node_type(DbtCommand::Test, NodeType::Seed));
        assert!(!command_includes_node_type(DbtCommand::Test, NodeType::Snapshot));
    }

    /// Functions are dbt Core v2 nodes with a real materialization, so the
    /// graph-building commands schedule them like any other buildable node.
    #[test]
    fn graph_commands_schedule_functions() {
        for command in [DbtCommand::Build, DbtCommand::Compile, DbtCommand::List] {
            assert!(
                command_includes_node_type(command, NodeType::Function),
                "{command} should schedule functions"
            );
        }
        // `run` is models-only, matching dbt.
        assert!(!command_includes_node_type(DbtCommand::Run, NodeType::Function));
    }

    #[test]
    fn seed_command_includes_only_seeds() {
        assert!(command_includes_node_type(DbtCommand::Seed, NodeType::Seed));
        assert!(!command_includes_node_type(DbtCommand::Seed, NodeType::Model));
        assert!(!command_includes_node_type(DbtCommand::Seed, NodeType::Test));
        assert!(!command_includes_node_type(DbtCommand::Seed, NodeType::Snapshot));
    }

    #[test]
    fn snapshot_command_includes_only_snapshots() {
        assert!(command_includes_node_type(DbtCommand::Snapshot, NodeType::Snapshot));
        assert!(!command_includes_node_type(DbtCommand::Snapshot, NodeType::Model));
        assert!(!command_includes_node_type(DbtCommand::Snapshot, NodeType::Test));
        assert!(!command_includes_node_type(DbtCommand::Snapshot, NodeType::Seed));
    }

    /// Resource types with no execution path must stay out of every command's
    /// plan — they are reported via `warn_unsupported_resource_types` instead.
    /// Functions are deliberately absent: they *are* executed.
    #[test]
    fn no_command_schedules_unsupported_resource_types() {
        for command in DbtCommand::ALL {
            for rt in [
                NodeType::Exposure,
                NodeType::Metric,
                NodeType::SavedQuery,
                NodeType::SemanticModel,
                NodeType::Analysis,
            ] {
                assert!(
                    !command_includes_node_type(command, rt),
                    "{command} should not schedule {rt:?}"
                );
            }
        }
    }

    #[test]
    fn list_command_includes_full_graph_like_build() {
        for rt in [
            NodeType::Model,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
            NodeType::UnitTest,
        ] {
            assert!(command_includes_node_type(DbtCommand::List, rt), "list should include {rt:?}");
        }
    }

    /// Phantom-test macro defs (seen in the panda-cascade project) must be detected so the
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
}
