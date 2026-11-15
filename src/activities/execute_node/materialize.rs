//! The execute phase: create the target schema, render the materialization
//! and read back what it stored.

use std::collections::BTreeMap;

use dbt_schemas::schemas::telemetry::NodeType;

use crate::error::DbtTemporalError;
use crate::types::TimingEntry;

use super::super::node_helpers::render_materialization;
use super::super::render_env::RenderEnv;
use super::context::{CompiledNode, NodeRun, Timeline};
use super::outcome::{build_timing_entries, finalize_compiled_code};

/// What the materialization produced, beyond the results it stored.
pub(super) struct Materialized {
    pub(super) materialization: String,
    /// The resolved `<package>.<macro>` that ran.
    pub(super) template: String,
    pub(super) compiled_code: Option<String>,
    pub(super) timing: Vec<TimingEntry>,
    pub(super) execution_time: f64,
}

/// Run the node's materialization against the warehouse.
pub(super) fn materialize_node(
    run: &NodeRun<'_>,
    render_env: &RenderEnv,
    compiled: &mut CompiledNode,
    timeline: &Timeline,
    execute_start: chrono::DateTime<chrono::Utc>,
) -> Result<Materialized, anyhow::Error> {
    let state = run.state;
    let rt = run.resource_type;
    let unique_id = run.unique_id;
    let jinja_env = &render_env.jinja_env;
    let node_context = &mut compiled.node_context;

    // Ensure the target schema/dataset exists (dbt does this before materializations).
    // Dispatches to the adapter-specific create_schema macro (e.g. CREATE SCHEMA IF NOT EXISTS).
    // Uses `this` which is the target relation (database + schema + identifier).
    // Tests normally only read, but with store_failures they write failing
    // rows into the audit schema — which may not exist yet.
    let needs_schema = is_create_schema_eligible(rt)
        || (rt == NodeType::Test && test_stores_failures(&state.resolver_state.nodes, unique_id));
    if needs_schema {
        ensure_target_schema(state, &run.input.invocation_id, run.base, jinja_env, node_context)?;
    }

    // Resolve the materialization template using dbt-fusion's MaterializationResolver.
    // Dispatches with adapter prefix inheritance (e.g. redshift→postgres→default) and
    // package precedence (Root > Imported > Core for builtins).
    let materialization = select_materialization_name(rt, &run.base.materialized.to_string());

    // Extract compiled SQL from the node context before rendering.
    let compiled_sql = node_context
        .get("sql")
        .and_then(|v| v.as_str().map(ToString::to_string));

    // Taken after the SQL is recorded: the result keeps the body as written,
    // and only the materialization sees it without its terminator.
    if rt == NodeType::Model {
        strip_body_terminator(
            node_context,
            run.common.language.as_deref(),
            render_env.adapter.engine().splitter(),
            run.base.adapter,
        );
    }

    // Resolve against the adapter the node actually runs on: `base.adapter` is the
    // node's `+adapter` selection when it made one, and the run's default adapter
    // otherwise. The resolver holds no default of its own.
    let template = state
        .materialization_resolver
        .find_materialization_macro_by_name(&materialization, run.base.adapter)
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "no materialization found for node {unique_id} (materialization={materialization}): {e:#}"
            ))
        })?;

    // Render the materialization template (triggers SQL execution through BridgeAdapter).
    let rendered = render_materialization(jinja_env, &template, node_context)?;

    // Prefer the compiled SQL from the context; fall back to rendered output if non-empty.
    let compiled_code = finalize_compiled_code(compiled_sql, &rendered);

    let execute_end = chrono::Utc::now();

    let execution_time = timeline.elapsed_secs();

    let timing = build_timing_entries(
        timeline.compile_start,
        timeline.compile_end,
        execute_start,
        execute_end,
    );

    Ok(Materialized {
        materialization,
        template,
        compiled_code,
        timing,
        execution_time,
    })
}

/// Pick the materialization name to dispatch on. Seeds are forced to "seed"
/// because dbt-fusion still reports their `base.materialized` as "table"
/// (issue #1345); without the override `materialization_table_default` would
/// be invoked with empty SQL and produce invalid CREATE statements. Unit
/// tests always dispatch to the bundled `unit` materialization.
pub(super) fn select_materialization_name(rt: NodeType, base_materialized: &str) -> String {
    match rt {
        NodeType::Seed => "seed".to_string(),
        NodeType::UnitTest => "unit".to_string(),
        _ => base_materialized.to_lowercase(),
    }
}

/// Create the node's target schema, once per run rather than once per node.
///
/// A project with three hundred models in one schema would otherwise issue
/// three hundred identical `CREATE SCHEMA IF NOT EXISTS` statements and the
/// metadata round trips behind them. Creation is single-flight per schema: a
/// node that finds another node's `CREATE` in flight blocks until it finishes,
/// because materializing into a schema that does not exist yet fails. The
/// memory is keyed by run, not by worker: a schema dropped between runs must be
/// created again.
///
/// A non-retryable failure is not fatal — a run against an existing schema on a
/// role without CREATE is a legitimate setup, and the materialization gives a
/// much better error if the schema really is absent. It settles the schema, so
/// the failing statement is issued once per run rather than once per node. A
/// *retryable* failure (a dropped connection, a throttle) is propagated and the
/// schema forgotten, so the activity is retried.
fn ensure_target_schema(
    state: &crate::worker_state::WorkerState,
    invocation_id: &str,
    base: &dbt_schemas::schemas::nodes::NodeBaseAttributes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Result<(), DbtTemporalError> {
    // The relation `this` resolves to, which is what `create_schema` reads —
    // per-workflow overrides have already patched it into the context.
    let (database, schema) = node_context
        .get("this")
        .and_then(|this| {
            let database = this.get_attr("database").ok()?.as_str()?.to_string();
            let schema = this.get_attr("schema").ok()?.as_str()?.to_string();
            Some((database, schema))
        })
        .unwrap_or_else(|| (base.database.clone(), base.schema.clone()));

    state
        .created_schemas
        .ensure(invocation_id, base.adapter, &database, &schema, || {
            let Err(e) = jinja_env.render_str("{% do create_schema(this) %}", node_context, &[])
            else {
                return Ok(());
            };
            let classified = crate::error::classify_adapter_execution_error(
                &*e,
                &format!("creating schema {database}.{schema}"),
            );
            if classified.is_retryable() {
                return Err(classified);
            }
            tracing::warn!(
                database = %database,
                schema = %schema,
                error = %classified,
                "create_schema failed (non-fatal)"
            );
            Ok(())
        })
}

/// True if the node is one we expect `create_schema(this)` to be called for
/// before materialization. Tests and operations don't get a schema-create
/// pass — they only read. Unit tests qualify because the `unit`
/// materialization creates a temp table in the target schema to probe column
/// types, and may run before anything else has created that schema. Functions
/// are created *in* a schema like any other relation.
const fn is_create_schema_eligible(rt: NodeType) -> bool {
    matches!(
        rt,
        NodeType::Model
            | NodeType::Seed
            | NodeType::Snapshot
            | NodeType::UnitTest
            | NodeType::Function
    )
}

/// True if this test node persists failing rows to the warehouse
/// (`store_failures` / `store_failures_as: table|view`). Such tests need
/// their audit schema created before the materialization runs.
fn test_stores_failures(nodes: &dbt_schemas::schemas::Nodes, unique_id: &str) -> bool {
    use dbt_schemas::schemas::common::StoreFailuresAs;

    nodes.tests.get(unique_id).is_some_and(|test| {
        let config = &test.deprecated_config;
        matches!(config.store_failures_as, Some(StoreFailuresAs::Table | StoreFailuresAs::View))
            || (config.store_failures_as.is_none() && config.store_failures == Some(true))
    })
}

/// Drop a model body's trailing `;` before a materialization splices it in.
///
/// Materializations wrap the body in a larger query — `create table as ( ... )`,
/// contract enforcement's `select * from ( ... ) as __dbt_sbq` — so a terminal
/// `;` lands inside the parentheses and makes the statement invalid. dbt strips
/// it at the same point (dbt-core#15779). The splitter only strips a body that
/// holds exactly one statement: a genuine multi-statement body keeps its
/// terminators and the warehouse still rejects it, rather than having the
/// user's SQL rewritten. Python bodies are not SQL and pass through untouched.
fn strip_body_terminator(
    node_context: &mut BTreeMap<String, minijinja::Value>,
    language: Option<&str>,
    splitter: &dyn dbt_adapter::stmt_splitter::StmtSplitter,
    adapter_type: dbt_adapter::AdapterType,
) {
    if language == Some("python") {
        return;
    }
    let Some(sql) = node_context.get("sql").and_then(minijinja::Value::as_str) else {
        return;
    };
    let body = splitter.strip_trailing_statement_terminator(sql, adapter_type);
    if body.len() == sql.len() {
        return;
    }
    let body = minijinja::Value::from(body.to_owned());
    node_context.insert("sql".to_owned(), body.clone());
    node_context.insert("compiled_code".to_owned(), body);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    // --- select_materialization_name ---

    #[test]
    fn select_materialization_name_forces_seed_for_seed_nodes() {
        // base.materialized still says "table" for seeds in dbt-fusion (#1345).
        assert_eq!(select_materialization_name(NodeType::Seed, "table"), "seed");
        assert_eq!(select_materialization_name(NodeType::Seed, "view"), "seed");
    }

    #[test]
    fn select_materialization_name_lowercases_for_non_seed() {
        assert_eq!(select_materialization_name(NodeType::Model, "Table"), "table");
        assert_eq!(select_materialization_name(NodeType::Model, "VIEW"), "view");
        assert_eq!(select_materialization_name(NodeType::Snapshot, "snapshot"), "snapshot");
        assert_eq!(select_materialization_name(NodeType::Test, "test"), "test");
    }

    // --- is_create_schema_eligible ---

    #[test]
    fn is_create_schema_eligible_for_writers_only() {
        assert!(is_create_schema_eligible(NodeType::Model));
        assert!(is_create_schema_eligible(NodeType::Seed));
        assert!(is_create_schema_eligible(NodeType::Snapshot));
        // Tests and operations don't need a schema-create pass.
        assert!(!is_create_schema_eligible(NodeType::Test));
        assert!(!is_create_schema_eligible(NodeType::Operation));
    }

    // --- test_stores_failures ---

    fn nodes_with_test(
        store_failures: Option<bool>,
        store_failures_as: Option<dbt_schemas::schemas::common::StoreFailuresAs>,
    ) -> dbt_schemas::schemas::Nodes {
        use dbt_schemas::schemas::nodes::DbtTest;

        let mut test = DbtTest::default();
        test.deprecated_config.store_failures = store_failures;
        test.deprecated_config.store_failures_as = store_failures_as;
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes.tests.insert("test.p.t".to_string(), Arc::new(test));
        nodes
    }

    #[test]
    fn test_stores_failures_on_flag_or_persistent_as() {
        use dbt_schemas::schemas::common::StoreFailuresAs;

        assert!(test_stores_failures(&nodes_with_test(Some(true), None), "test.p.t"));
        assert!(test_stores_failures(
            &nodes_with_test(None, Some(StoreFailuresAs::Table)),
            "test.p.t"
        ));
        assert!(test_stores_failures(
            &nodes_with_test(Some(false), Some(StoreFailuresAs::View)),
            "test.p.t"
        ));
    }

    #[test]
    fn test_stores_failures_off_for_ephemeral_default_or_unknown() {
        use dbt_schemas::schemas::common::StoreFailuresAs;

        assert!(!test_stores_failures(&nodes_with_test(None, None), "test.p.t"));
        assert!(!test_stores_failures(&nodes_with_test(Some(false), None), "test.p.t"));
        // store_failures_as: ephemeral never persists, even with the flag on.
        assert!(!test_stores_failures(
            &nodes_with_test(Some(true), Some(StoreFailuresAs::Ephemeral)),
            "test.p.t"
        ));
        assert!(!test_stores_failures(&nodes_with_test(Some(true), None), "test.p.missing"));
    }
}
