use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use dbt_adapter::load_store::ResultStore;

use crate::error::DbtTemporalError;

const CTE_PREFIX: &str = "__dbt__cte__";

/// Find a materialization template by searching for a suffix match in the Jinja environment.
/// Templates are registered with package prefixes (e.g. "dbt_postgres.materialization_view_postgres").
///
/// Prefer `MaterializationResolver::find_materialization_macro_by_name()` for production use;
/// this function is kept for the debug_render example.
#[allow(dead_code)]
pub fn find_materialization_template(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    suffix: &str,
) -> Option<String> {
    let dot_suffix = format!(".{suffix}");
    for (name, _) in jinja_env.env.templates() {
        if name.ends_with(&dot_suffix) {
            return Some(name.to_string());
        }
    }
    None
}

/// Invoke a materialization template by evaluating it to state.
///
/// Looks up the materialization macro and calls it. `{% materialization %}` blocks define
/// callable macros — `template.render()` returns empty because the macro body
/// is never executed. This follows the DispatchObject pattern from dbt-fusion.
pub fn render_materialization(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    template_name: &str,
    context: &BTreeMap<String, minijinja::Value>,
) -> Result<String, DbtTemporalError> {
    let template = jinja_env.env.get_template(template_name).map_err(|e| {
        DbtTemporalError::Compilation(format!("template {template_name} not found: {e}"))
    })?;

    // Evaluate the template to get State with macro definitions registered.
    let state = template.eval_to_state(context, &[]).map_err(|e| {
        DbtTemporalError::Compilation(format!("eval_to_state {template_name}: {e}"))
    })?;

    // The macro name is the leaf part after the last dot
    // (e.g. "dbt_bigquery.materialization_view_bigquery" -> "materialization_view_bigquery").
    let macro_name = template_name
        .split('.')
        .next_back()
        .unwrap_or(template_name);

    let func = state.lookup(macro_name).ok_or_else(|| {
        DbtTemporalError::Compilation(format!(
            "macro '{macro_name}' not found in template '{template_name}'"
        ))
    })?;

    // Call the macro — this executes the materialization body (DDL/DML via adapter).
    let result = func.call(&state, &[], &[]).map_err(|e| {
        DbtTemporalError::Compilation(format!("calling {macro_name} in {template_name}: {e}"))
    })?;

    Ok(result.as_str().map(ToString::to_string).unwrap_or_default())
}

/// Inject ephemeral model CTEs into compiled SQL.
///
/// When a model refs an ephemeral model, the ref resolves to `__dbt__cte__<name>`.
/// This function finds the referenced ephemeral models, compiles their SQL, and
/// prepends them as a WITH clause. Handles transitive (nested) ephemeral refs.
pub fn inject_ephemeral_ctes(
    compiled_sql: &str,
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    in_dir: &Path,
) -> Result<String, DbtTemporalError> {
    if !compiled_sql.contains(CTE_PREFIX) {
        return Ok(compiled_sql.to_string());
    }

    // Collect all CTE names referenced in the SQL.
    let cte_names = extract_cte_names(compiled_sql);
    if cte_names.is_empty() {
        return Ok(compiled_sql.to_string());
    }

    // Compile each ephemeral model and collect CTE definitions.
    // Track visited to avoid infinite recursion on circular refs.
    let mut cte_defs: Vec<(String, String)> = Vec::new();
    let mut visited = BTreeSet::new();
    compile_ephemeral_ctes(
        &cte_names,
        nodes,
        jinja_env,
        node_context,
        in_dir,
        &mut cte_defs,
        &mut visited,
    )?;

    if cte_defs.is_empty() {
        return Ok(compiled_sql.to_string());
    }

    // Build WITH clause: CTEs in dependency order (deepest first).
    let ctes = cte_defs
        .iter()
        .map(|(name, sql)| format!("{CTE_PREFIX}{name} as (\n{sql}\n)"))
        .collect::<Vec<_>>()
        .join(",\n");

    Ok(format!("with {ctes}\n{compiled_sql}"))
}

/// Extract `__dbt__cte__<name>` model names from SQL.
fn extract_cte_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut pos = 0;
    while let Some(start) = sql[pos..].find(CTE_PREFIX) {
        pos += start + CTE_PREFIX.len();
        let name_end = sql[pos..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or_else(|| sql[pos..].len());
        let name = sql[pos..pos + name_end].to_string();
        if !name.is_empty() && !names.contains(&name) {
            names.push(name);
        }
        pos += name_end;
    }
    names
}

/// Recursively compile ephemeral models and their transitive dependencies.
fn compile_ephemeral_ctes(
    names: &[String],
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    in_dir: &Path,
    cte_defs: &mut Vec<(String, String)>,
    visited: &mut BTreeSet<String>,
) -> Result<(), DbtTemporalError> {
    for name in names {
        if visited.contains(name) {
            continue;
        }
        visited.insert(name.clone());

        // Find the ephemeral node by model name.
        let node = nodes.iter().find(|(_, n)| {
            n.common().name == *name
                && n.base()
                    .materialized
                    .to_string()
                    .eq_ignore_ascii_case("ephemeral")
        });
        let Some((_, node)) = node else {
            continue;
        };

        // Read and compile the ephemeral model's raw SQL.
        let raw_path = in_dir.join(&node.common().original_file_path);
        let raw_sql = std::fs::read_to_string(&raw_path).map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "reading ephemeral model '{}' at {}: {e}",
                name,
                raw_path.display()
            ))
        })?;

        let compiled = jinja_env
            .render_str(&raw_sql, node_context, &[])
            .map_err(|e| {
                DbtTemporalError::Compilation(format!("compiling ephemeral model '{name}': {e}"))
            })?;

        // Check for transitive ephemeral refs (ephemeral model referencing another ephemeral).
        let transitive = extract_cte_names(&compiled);
        if !transitive.is_empty() {
            compile_ephemeral_ctes(
                &transitive,
                nodes,
                jinja_env,
                node_context,
                in_dir,
                cte_defs,
                visited,
            )?;
        }

        cte_defs.push((name.clone(), compiled));
    }
    Ok(())
}

/// Extract the test failure count from the ResultStore.
///
/// The test materialization wraps the test SQL in `get_test_sql()` which produces a single row
/// with `failures`, `should_warn`, `should_error` columns. The actual failure count is in the
/// `failures` column of the agate_table, NOT in `rows_affected` (which is always 1 for tests).
pub fn extract_test_failures(result_store: &ResultStore) -> Option<i64> {
    let load_fn = result_store.load_result();
    let result = load_fn(&[minijinja::Value::from("main")]).ok()?;
    if result.is_none() || result.is_undefined() {
        return None;
    }
    let table = result.get_attr("table").ok()?;
    if table.is_none() || table.is_undefined() {
        return None;
    }
    // AgateTable rows: table.rows is a list of Row objects; each Row supports index/attr access.
    let rows = table.get_attr("rows").ok()?;
    let first_row = rows.get_item(&minijinja::Value::from(0)).ok()?;
    // Try column name "failures" first (the standard dbt test column).
    let failures_val = first_row
        .get_item(&minijinja::Value::from("failures"))
        .ok()
        .filter(|v| !v.is_none() && !v.is_undefined());
    if let Some(val) = failures_val {
        return val.as_i64().or_else(|| {
            // May be returned as a string.
            val.as_str().and_then(|s| s.parse::<i64>().ok())
        });
    }
    None
}

/// Extract adapter response metadata from the ResultStore after rendering.
/// Materialization macros store results via `store_result('main', response)`.
pub fn extract_adapter_response(result_store: &ResultStore) -> BTreeMap<String, serde_json::Value> {
    // Call load_result("main") via the closure.
    let load_fn = result_store.load_result();
    let result = load_fn(&[minijinja::Value::from("main")]);

    let mut response_map = BTreeMap::new();
    if let Ok(result_val) = result {
        if result_val.is_none() || result_val.is_undefined() {
            return response_map;
        }
        // ResultObject exposes "response" key which is an AdapterResponse.
        if let Ok(response) = result_val.get_attr("response")
            && !response.is_none()
            && !response.is_undefined()
        {
            if let Some(msg) = response
                .get_attr("message")
                .ok()
                .and_then(|v| v.as_str().map(ToString::to_string))
            {
                response_map.insert("message".to_string(), serde_json::Value::String(msg));
            }
            if let Some(code) = response
                .get_attr("code")
                .ok()
                .and_then(|v| v.as_str().map(ToString::to_string))
            {
                response_map.insert("code".to_string(), serde_json::Value::String(code));
            }
            if let Ok(rows) = response.get_attr("rows_affected")
                && let Some(n) = rows.as_i64()
            {
                response_map
                    .insert("rows_affected".to_string(), serde_json::Value::Number(n.into()));
            }
            if let Some(qid) = response
                .get_attr("query_id")
                .ok()
                .and_then(|v| v.as_str().map(ToString::to_string))
            {
                response_map.insert("query_id".to_string(), serde_json::Value::String(qid));
            }
        }
    }
    response_map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_adapter_response_empty_store() {
        let store = ResultStore::default();
        let result = extract_adapter_response(&store);
        assert!(result.is_empty(), "empty store should return empty map");
    }

    #[test]
    fn extract_adapter_response_with_stored_result() -> anyhow::Result<()> {
        let store = ResultStore::default();
        // store_raw_result uses keyword args; invoke via a minijinja Environment.
        let mut env = minijinja::Environment::new();
        env.add_function("store_raw_result", store.store_raw_result());
        // Render a template that calls store_raw_result with known values.
        let tmpl = env
            .template_from_str(
                "{{ store_raw_result(name='main', message='CREATE VIEW', code='SUCCESS', rows_affected='42') }}",
            )?;
        tmpl.render(minijinja::context!(), &[])?;

        let result = extract_adapter_response(&store);
        assert_eq!(
            result.get("message"),
            Some(&serde_json::Value::String("CREATE VIEW".to_string()))
        );
        assert_eq!(result.get("code"), Some(&serde_json::Value::String("SUCCESS".to_string())));
        assert_eq!(result.get("rows_affected"), Some(&serde_json::Value::Number(42.into())));
        Ok(())
    }

    #[test]
    fn extract_adapter_response_partial_fields() -> anyhow::Result<()> {
        let store = ResultStore::default();
        let mut env = minijinja::Environment::new();
        env.add_function("store_raw_result", store.store_raw_result());
        let tmpl = env.template_from_str("{{ store_raw_result(name='main', message='OK') }}")?;
        tmpl.render(minijinja::context!(), &[])?;

        let result = extract_adapter_response(&store);
        assert_eq!(result.get("message"), Some(&serde_json::Value::String("OK".to_string())));
        // Fields not stored should be absent.
        assert!(!result.contains_key("query_id"));
        Ok(())
    }
}
