//! catalog.json generation (`dbt docs generate` equivalent).
//!
//! When `WRITE_CATALOG=1`, `store_artifacts` fetches column metadata for
//! every relation the run built (plus the project's sources) through
//! `adapter.get_columns_in_relation` — the standard, adapter-generic dbt
//! path — and assembles a partial dbt catalog v1 artifact: columns come from
//! the warehouse, the table type from the node's materialization, while
//! comments/owner/stats are omitted. Catalog problems never fail the run —
//! the caller logs and continues.

use std::sync::Arc;

use crate::types::{NodeExecutionResult, NodeStatus};
use crate::worker_state::WorkerState;

/// A relation we want catalog metadata for.
struct Target {
    unique_id: String,
    database: String,
    schema: String,
    identifier: String,
    quoting: dbt_schemas::schemas::common::ResolvedQuoting,
    /// dbt catalog table type, e.g. `BASE TABLE` or `VIEW`.
    table_type: String,
    is_source: bool,
}

/// A target enriched with warehouse columns: (name, type) in ordinal order.
struct CatalogEntry<'a> {
    target: &'a Target,
    columns: Vec<(String, String)>,
}

/// Build the dbt catalog v1 JSON for the run's successfully built relations
/// and the project's sources.
pub fn build_catalog_json(
    state: &WorkerState,
    node_results: &[NodeExecutionResult],
    invocation_id: &str,
) -> Result<String, anyhow::Error> {
    let targets = collect_targets(state, node_results);
    anyhow::ensure!(!targets.is_empty(), "no relations to catalog");

    let entries = fetch_columns(state, &targets)?;
    let artifact = assemble_catalog(&entries, invocation_id);
    serde_json::to_string_pretty(&artifact).map_err(Into::into)
}

/// Map a node materialization to the dbt catalog's table type label.
fn table_type_label(materialized: &str) -> String {
    match materialized.to_lowercase().as_str() {
        "view" => "VIEW".to_string(),
        "materialized_view" => "MATERIALIZED VIEW".to_string(),
        _ => "BASE TABLE".to_string(),
    }
}

/// Successfully built models/seeds/snapshots from this run, plus all sources.
fn collect_targets(state: &WorkerState, node_results: &[NodeExecutionResult]) -> Vec<Target> {
    let nodes = &state.resolver_state.nodes;
    let mut targets = Vec::new();

    for result in node_results {
        if result.status != NodeStatus::Success {
            continue;
        }
        let uid = &result.unique_id;
        if !(uid.starts_with("model.") || uid.starts_with("seed.") || uid.starts_with("snapshot."))
        {
            continue;
        }
        let Some(node) = nodes.get_node(uid) else {
            continue;
        };
        let base = node.base();
        let materialized = base.materialized.to_string();
        if materialized.eq_ignore_ascii_case("ephemeral") {
            continue;
        }
        targets.push(Target {
            unique_id: uid.clone(),
            database: base.database.clone(),
            schema: base.schema.clone(),
            identifier: base.alias.clone(),
            quoting: base.quoting,
            table_type: table_type_label(&materialized),
            is_source: false,
        });
    }

    for (uid, source) in &nodes.sources {
        let base = &source.__base_attr__;
        targets.push(Target {
            unique_id: uid.clone(),
            database: base.database.clone(),
            schema: base.schema.clone(),
            identifier: source.__source_attr__.identifier.clone(),
            quoting: base.quoting,
            table_type: "BASE TABLE".to_string(),
            is_source: true,
        });
    }

    targets
}

/// Fetch warehouse columns for all targets via Jinja-evaluated
/// `adapter.get_columns_in_relation` (one call per relation, dispatched per
/// adapter exactly like dbt's own macros). Targets whose relations are
/// missing from the warehouse come back empty and are dropped, matching
/// dbt's catalog behavior.
fn fetch_columns<'a>(
    state: &WorkerState,
    targets: &'a [Target],
) -> Result<Vec<CatalogEntry<'a>>, anyhow::Error> {
    let mut jinja_env = (*state.jinja_env).clone();
    let adapter_impl = dbt_adapter::AdapterImpl::new(Arc::clone(&state.adapter_engine), None);
    let adapter = Arc::new(dbt_adapter::Adapter::new(
        Arc::new(adapter_impl),
        None,
        state.cancellation_source.token(),
    ));
    dbt_jinja_utils::phases::configure_compile_and_run_jinja_environment(&mut jinja_env, adapter);

    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();
    let mut ctx = dbt_jinja_utils::phases::build_compile_and_run_base_context(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        None,
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
    );

    let columns_expr = jinja_env
        .compile_expression("adapter.get_columns_in_relation(__dbt_catalog_relation__)")
        .map_err(|e| anyhow::anyhow!("compiling catalog expression: {e}"))?;

    let mut entries = Vec::new();
    for target in targets {
        let relation = dbt_adapter::relation::do_create_relation(
            state.resolver_state.adapter_type,
            target.database.clone(),
            target.schema.clone(),
            Some(target.identifier.clone()),
            None,
            target.quoting,
        )
        .map_err(|e| anyhow::anyhow!("building catalog relation for {}: {e}", target.unique_id))?;
        ctx.insert(
            "__dbt_catalog_relation__".to_owned(),
            dbt_adapter::relation::RelationObject::new(Arc::from(relation)).into_value(),
        );

        let value = columns_expr
            .eval(&ctx, &[])
            .map_err(|e| anyhow::anyhow!("fetching columns for {}: {e}", target.unique_id))?;
        let columns = column_pairs(&value)?;
        if columns.is_empty() {
            // Relation absent from the warehouse (e.g. a source that's never
            // been loaded) — dbt omits it from the catalog too.
            continue;
        }
        entries.push(CatalogEntry { target, columns });
    }
    Ok(entries)
}

/// Extract (name, data_type) pairs from the Jinja column-object list.
fn column_pairs(value: &minijinja::Value) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut columns = Vec::new();
    let iter = value
        .try_iter()
        .map_err(|e| anyhow::anyhow!("column list not iterable: {e}"))?;
    for column in iter {
        let name = column
            .get_attr("name")
            .ok()
            .filter(|v| !v.is_undefined() && !v.is_none())
            .ok_or_else(|| anyhow::anyhow!("column object has no name attribute"))?
            .to_string();
        let data_type = column
            .get_attr("data_type")
            .ok()
            .filter(|v| !v.is_undefined() && !v.is_none())
            .or_else(|| {
                column
                    .get_attr("dtype")
                    .ok()
                    .filter(|v| !v.is_undefined() && !v.is_none())
            })
            .map_or_else(|| "unknown".to_string(), |v| v.to_string());
        columns.push((name, data_type));
    }
    Ok(columns)
}

/// Assemble the dbt catalog v1 artifact.
fn assemble_catalog(entries: &[CatalogEntry<'_>], invocation_id: &str) -> serde_json::Value {
    let mut nodes = serde_json::Map::new();
    let mut sources = serde_json::Map::new();

    for entry in entries {
        let target = entry.target;
        let mut columns = serde_json::Map::new();
        for (index, (name, sql_type)) in entry.columns.iter().enumerate() {
            columns.insert(
                name.clone(),
                serde_json::json!({
                    "type": sql_type,
                    "index": index + 1,
                    "name": name,
                    "comment": serde_json::Value::Null,
                }),
            );
        }

        let value = serde_json::json!({
            "metadata": {
                "type": target.table_type,
                "schema": target.schema,
                "name": target.identifier,
                "database": target.database,
                "comment": serde_json::Value::Null,
                "owner": serde_json::Value::Null,
            },
            "columns": columns,
            "stats": {},
            "unique_id": target.unique_id,
        });
        if target.is_source {
            sources.insert(target.unique_id.clone(), value);
        } else {
            nodes.insert(target.unique_id.clone(), value);
        }
    }

    serde_json::json!({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "dbt_version": env!("CARGO_PKG_VERSION"),
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "invocation_id": invocation_id,
            "env": {},
        },
        "nodes": nodes,
        "sources": sources,
        "errors": serde_json::Value::Null,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn target(unique_id: &str, identifier: &str, table_type: &str, is_source: bool) -> Target {
        Target {
            unique_id: unique_id.to_string(),
            database: "waffle_hut".to_string(),
            schema: "public".to_string(),
            identifier: identifier.to_string(),
            quoting: dbt_schemas::schemas::common::ResolvedQuoting::trues(),
            table_type: table_type.to_string(),
            is_source,
        }
    }

    #[test]
    fn table_type_label_maps_materializations() {
        assert_eq!(table_type_label("view"), "VIEW");
        assert_eq!(table_type_label("VIEW"), "VIEW");
        assert_eq!(table_type_label("materialized_view"), "MATERIALIZED VIEW");
        assert_eq!(table_type_label("table"), "BASE TABLE");
        assert_eq!(table_type_label("incremental"), "BASE TABLE");
        assert_eq!(table_type_label("seed"), "BASE TABLE");
    }

    #[test]
    fn assemble_catalog_groups_columns_and_splits_sources() {
        let model = target("model.p.orders", "orders", "VIEW", false);
        let source = target("source.p.raw.payments", "payments", "BASE TABLE", true);
        let entries = vec![
            CatalogEntry {
                target: &model,
                columns: vec![
                    ("order_id".to_string(), "integer".to_string()),
                    ("status".to_string(), "text".to_string()),
                ],
            },
            CatalogEntry {
                target: &source,
                columns: vec![("id".to_string(), "integer".to_string())],
            },
        ];
        let artifact = assemble_catalog(&entries, "inv-1");

        let node = &artifact["nodes"]["model.p.orders"];
        assert_eq!(node["metadata"]["name"], "orders");
        assert_eq!(node["metadata"]["type"], "VIEW");
        assert_eq!(node["columns"]["order_id"]["index"], 1);
        assert_eq!(node["columns"]["order_id"]["type"], "integer");
        assert_eq!(node["columns"]["status"]["index"], 2);
        assert_eq!(node["unique_id"], "model.p.orders");

        let src = &artifact["sources"]["source.p.raw.payments"];
        assert_eq!(src["metadata"]["schema"], "public");
        assert!(artifact["nodes"].get("source.p.raw.payments").is_none());
        assert!(artifact["errors"].is_null());
        assert_eq!(
            artifact["metadata"]["dbt_schema_version"],
            "https://schemas.getdbt.com/dbt/catalog/v1.json"
        );
    }
}
