use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;

use dbt_adapter::load_store::ResultStore;
use dbt_common::constants::DBT_CTE_PREFIX;
use dbt_jinja_utils::utils::{inject_and_persist_ephemeral_models, render_sql_with_listeners};
use minijinja::MacroSpans;
use minijinja::listener::RenderingEventListener;

use crate::error::DbtTemporalError;

/// Empty listener slice for SQL renders that don't subscribe to events.
/// Threaded through `render_sql_with_listeners` to keep call sites uniform.
const NO_LISTENERS: &[Rc<dyn RenderingEventListener>] = &[];

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
        DbtTemporalError::Compilation(format!("template {template_name} not found: {e:#}"))
    })?;

    // Evaluate the template to get State with macro definitions registered.
    let state = template.eval_to_state(context, &[]).map_err(|e| {
        DbtTemporalError::Compilation(format!("eval_to_state {template_name}: {e:#}"))
    })?;

    // The macro name is the leaf part after the last dot
    // (e.g. "dbt_bigquery.materialization_view_bigquery" -> "materialization_view_bigquery").
    let macro_name = template_name
        .split('.')
        .next_back()
        .unwrap_or(template_name);

    let func = state.lookup(macro_name, &[]).ok_or_else(|| {
        DbtTemporalError::Compilation(format!(
            "macro '{macro_name}' not found in template '{template_name}'"
        ))
    })?;

    // Call the macro — this executes the materialization body (DDL/DML via adapter).
    let result = func.call(&state, &[], &[]).map_err(|e| {
        DbtTemporalError::Compilation(format!("calling {macro_name} in {template_name}: {e:#}"))
    })?;

    Ok(result.as_str().map(ToString::to_string).unwrap_or_default())
}

/// Inject ephemeral model CTEs into compiled SQL.
///
/// Walks the user's compiled SQL for `__dbt__cte__<name>` references, compiles
/// each transitive ephemeral dependency through Jinja, and persists per-ephemeral
/// CTE chains to `ephemeral_dir`. The final wrap is delegated to
/// `dbt_jinja_utils::utils::inject_and_persist_ephemeral_models`, which produces
/// SQL with `--EPHEMERAL-SELECT-WRAPPER-START/END` markers — matching vanilla
/// dbt-fusion's compile output and consumable by dbt-fusion's adapter SQL
/// tokenizer / diff (`crates/dbt-adapter/src/sql/{tokenizer,diff}.rs`), which
/// rely on those markers to reason about ephemeral wrapping.
///
/// `ephemeral_dir` must be unique per-activity; the persist step writes
/// `<name>.sql` files there, so concurrent activities sharing a directory would
/// race on the cumulative-CTE-chain layout.
pub fn inject_ephemeral_ctes(
    compiled_sql: &str,
    user_node_name: &str,
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    in_dir: &Path,
    ephemeral_dir: &Path,
) -> Result<String, DbtTemporalError> {
    if !compiled_sql.contains(DBT_CTE_PREFIX) {
        return Ok(compiled_sql.to_string());
    }

    // Walk the dependency DAG of ephemerals, persisting each leaf-first via
    // dbt-fusion's `inject_and_persist_ephemeral_models`. Each call writes a
    // cumulative CTE chain for that ephemeral into `ephemeral_dir`, so the
    // final user-model call only has to read one file per direct dep.
    let mut visited = BTreeSet::new();
    persist_ephemeral_chain(
        compiled_sql,
        nodes,
        jinja_env,
        node_context,
        in_dir,
        ephemeral_dir,
        &mut visited,
    )?;

    let mut spans = MacroSpans::default();
    inject_and_persist_ephemeral_models(
        compiled_sql.to_string(),
        &mut spans,
        user_node_name,
        false, // not an ephemeral — wraps and returns without persisting
        ephemeral_dir,
    )
    .map_err(|e| {
        DbtTemporalError::Compilation(format!(
            "wrapping ephemeral CTEs for {user_node_name}: {e:#}"
        ))
    })
}

/// Recursively compile each ephemeral referenced (directly or transitively) by
/// the given SQL, persisting each leaf-first via
/// `inject_and_persist_ephemeral_models(is_current_model_ephemeral=true)`.
///
/// Leaf-first ordering is required: the function reads each direct dep's
/// persisted file when processing a parent ephemeral, so dependencies must
/// already be on disk.
fn persist_ephemeral_chain(
    sql: &str,
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    in_dir: &Path,
    ephemeral_dir: &Path,
    visited: &mut BTreeSet<String>,
) -> Result<(), DbtTemporalError> {
    for name in extract_ephemeral_names(sql) {
        if !visited.insert(name.clone()) {
            continue;
        }

        let Some((_, node)) = nodes.iter().find(|(_, n)| {
            n.common().name == name
                && n.base()
                    .materialized
                    .to_string()
                    .eq_ignore_ascii_case("ephemeral")
        }) else {
            continue;
        };

        let raw_path = in_dir.join(&node.common().original_file_path);
        let raw_sql = std::fs::read_to_string(&raw_path).map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "reading ephemeral model '{name}' at {}: {e:#}",
                raw_path.display()
            ))
        })?;
        let compiled =
            render_sql_with_listeners(&raw_sql, jinja_env, node_context, NO_LISTENERS, &raw_path)
                .map_err(|e| {
                DbtTemporalError::Compilation(format!("compiling ephemeral model '{name}': {e:#}"))
            })?;

        // Recurse first so this ephemeral's deps land on disk before we persist it.
        persist_ephemeral_chain(
            &compiled,
            nodes,
            jinja_env,
            node_context,
            in_dir,
            ephemeral_dir,
            visited,
        )?;

        let mut spans = MacroSpans::default();
        inject_and_persist_ephemeral_models(
            compiled,
            &mut spans,
            &name,
            true, // ephemeral — persists cumulative CTE chain to disk
            ephemeral_dir,
        )
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "persisting ephemeral CTE chain for '{name}': {e:#}"
            ))
        })?;
    }
    Ok(())
}

/// Extract ephemeral model names from `__dbt__cte__<name>` references in SQL.
///
/// dbt-fusion has a private equivalent (`extract_ephemeral_model_names` in
/// `dbt-jinja-utils/src/utils.rs`); we keep our own because we need it to walk
/// the dep DAG before each ephemeral is persisted, and the upstream version
/// isn't exported.
fn extract_ephemeral_names(sql: &str) -> Vec<String> {
    #[allow(clippy::expect_used)]
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(&format!(r"{DBT_CTE_PREFIX}(\w+)")).expect("ephemeral CTE name regex")
    });
    RE.captures_iter(sql)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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

/// Patch the `target` (and `env`) Jinja global with per-workflow schema/database.
///
/// The startup `target` global has the profile schema/database from worker init.
/// When per-workflow env overrides change the profile (e.g. different schema via env_var()),
/// all Jinja macros that access `target.schema` / `target.database` must see the new values.
/// This includes `generate_schema_name`, `generate_database_name`, materialization templates,
/// and any custom macros.
pub(super) fn patch_target_global(
    jinja_env: &mut dbt_jinja_utils::jinja_environment::JinjaEnv,
    schema: &str,
    database: &str,
    target_name: Option<&str>,
) {
    // Extract current target as JSON, modify fields, re-inject as a native BTreeMap Value.
    let target_json = jinja_env
        .render_str("{{ target | tojson }}", BTreeMap::<String, minijinja::Value>::new(), &[])
        .unwrap_or_default();

    let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&target_json) else {
        return;
    };
    let Some(obj) = json_val.as_object() else {
        return;
    };

    let mut new_target: BTreeMap<String, minijinja::Value> = obj
        .iter()
        .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
        .collect();

    new_target.insert("schema".to_string(), minijinja::Value::from(schema));
    new_target.insert("database".to_string(), minijinja::Value::from(database));
    if let Some(name) = target_name {
        new_target.insert("name".to_string(), minijinja::Value::from(name));
        new_target.insert("target_name".to_string(), minijinja::Value::from(name));
    }

    let val = minijinja::Value::from(new_target);
    jinja_env.env.add_global("target", val.clone());
    // In dbt, `env` is an alias for `target`.
    jinja_env.env.add_global("env", val);
}

/// Convert a `serde_json::Value` to a native `minijinja::Value`.
///
/// Produces native minijinja types (Vec, BTreeMap) that support iteration,
/// attribute access, and `.get()` in Jinja templates — unlike `from_serialize`
/// which creates "plain objects" with limited Jinja interop.
#[allow(clippy::option_if_let_else)]
pub(super) fn json_to_minijinja(v: &serde_json::Value) -> minijinja::Value {
    match v {
        serde_json::Value::Null => minijinja::Value::from(()),
        serde_json::Value::Bool(b) => minijinja::Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(f) = n.as_f64() {
                minijinja::Value::from(f)
            } else {
                minijinja::Value::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => minijinja::Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            minijinja::Value::from(arr.iter().map(json_to_minijinja).collect::<Vec<_>>())
        }
        serde_json::Value::Object(obj) => {
            let map: BTreeMap<String, minijinja::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
                .collect();
            minijinja::Value::from(map)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn extract_ephemeral_names_finds_each_unique_reference() {
        let sql = "with __dbt__cte__alpha as (select 1), __dbt__cte__beta as (select 2)\n\
                   select * from __dbt__cte__alpha join __dbt__cte__beta using(id)";
        let names = extract_ephemeral_names(sql);
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn extract_ephemeral_names_returns_empty_when_absent() {
        assert!(extract_ephemeral_names("select 1").is_empty());
    }

    #[test]
    fn upstream_ephemeral_wrap_handles_user_with_clause() -> anyhow::Result<()> {
        // Regression for the BigQuery `Expected keyword DEPTH` failure: when
        // the user's compiled SQL starts with `WITH`, naively prepending the
        // ephemeral CTEs (our old code) produced two `WITH` keywords in a row.
        // BigQuery's parser then tried to read the second `WITH` as part of a
        // recursive CTE's `... CYCLE ... DEPTH` clause and rejected it. The
        // dbt-fusion helper wraps the user SQL in `select * from (...)` plus
        // `--EPHEMERAL-SELECT-WRAPPER-START/END` markers so a leading user
        // `WITH` becomes a valid subselect CTE list.
        let dir = tempfile::tempdir()?;

        // Simulate the persist step for a single ephemeral named `alpha` —
        // mirrors what we do at runtime in `persist_ephemeral_chain`.
        let mut spans = MacroSpans::default();
        inject_and_persist_ephemeral_models(
            "select 1 as k, 2 as v".to_string(),
            &mut spans,
            "alpha",
            true,
            dir.path(),
        )
        .map_err(|e| anyhow::anyhow!("persist alpha: {e:#}"))?;

        let user_sql = "WITH user_cte AS (\n  SELECT k, v FROM __dbt__cte__alpha\n)\n\
                        SELECT * FROM user_cte";
        let mut user_spans = MacroSpans::default();
        let wrapped = inject_and_persist_ephemeral_models(
            user_sql.to_string(),
            &mut user_spans,
            "user_model",
            false,
            dir.path(),
        )
        .map_err(|e| anyhow::anyhow!("wrap user model: {e:#}"))?;

        assert!(
            wrapped.contains("--EPHEMERAL-SELECT-WRAPPER-START"),
            "expected start marker; got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("--EPHEMERAL-SELECT-WRAPPER-END"),
            "expected end marker; got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("__dbt__cte__alpha as (\nselect 1 as k, 2 as v\n)"),
            "expected alpha to be inlined; got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("select * from (\nWITH user_cte AS"),
            "expected user WITH wrapped in subselect; got:\n{wrapped}"
        );
        // The original two-WITH-in-a-row pathology: assert that the user
        // `WITH` appears strictly *inside* the wrap, not as a sibling to the
        // outer `with __dbt__cte__alpha ...`. The wrapper-start marker is the
        // boundary; nothing before it should mention the user CTE.
        let start_idx = wrapped
            .find("--EPHEMERAL-SELECT-WRAPPER-START")
            .ok_or_else(|| anyhow::anyhow!("start marker missing"))?;
        let user_idx = wrapped
            .find("WITH user_cte")
            .ok_or_else(|| anyhow::anyhow!("user WITH missing"))?;
        assert!(
            user_idx > start_idx,
            "user WITH must appear after the wrapper start; got:\n{wrapped}"
        );
        assert!(
            !wrapped[..start_idx].contains("WITH user_cte"),
            "user WITH must not appear before the wrapper start; got:\n{wrapped}"
        );
        Ok(())
    }

    #[test]
    fn upstream_ephemeral_wrap_is_noop_without_cte_references() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let mut spans = MacroSpans::default();
        let plain = "select 1 as id".to_string();
        let out = inject_and_persist_ephemeral_models(
            plain.clone(),
            &mut spans,
            "user_model",
            false,
            dir.path(),
        )
        .map_err(|e| anyhow::anyhow!("noop wrap: {e:#}"))?;
        assert_eq!(out, plain);
        Ok(())
    }

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

    // --- json_to_minijinja ---

    #[test]
    fn json_to_minijinja_null_maps_to_unit() {
        let v = json_to_minijinja(&serde_json::Value::Null);
        assert!(v.is_none() || v.is_undefined());
    }

    #[test]
    fn json_to_minijinja_primitives() {
        assert_eq!(json_to_minijinja(&serde_json::json!(true)).to_string(), "True");
        assert_eq!(json_to_minijinja(&serde_json::json!(42)).to_string(), "42");
        assert_eq!(json_to_minijinja(&serde_json::json!(-3)).to_string(), "-3");
        assert_eq!(json_to_minijinja(&serde_json::json!(2.5)).to_string(), "2.5");
        assert_eq!(json_to_minijinja(&serde_json::json!("hi")).as_str(), Some("hi"));
    }

    #[test]
    fn json_to_minijinja_array_yields_list() {
        let v = json_to_minijinja(&serde_json::json!([1, 2, "x"]));
        let items: Vec<String> = v.try_iter().unwrap().map(|i| i.to_string()).collect();
        assert_eq!(items, vec!["1", "2", "x"]);
    }

    #[test]
    fn json_to_minijinja_object_supports_attribute_access() {
        let v = json_to_minijinja(&serde_json::json!({"a": 1, "b": "two"}));
        assert_eq!(v.get_attr("a").unwrap().to_string(), "1");
        assert_eq!(v.get_attr("b").unwrap().as_str(), Some("two"));
    }

    #[test]
    fn json_to_minijinja_large_unsigned_falls_back_to_f64() {
        // u64::MAX doesn't fit in i64; we fall through to as_f64 which always
        // succeeds for serde_json::Number (so the to_string() arm is dead but
        // typesafe). Exercise the f64 branch — the value stringifies as a float.
        let v = json_to_minijinja(&serde_json::json!(u64::MAX));
        assert!(v.to_string().contains("e19") || v.to_string().contains("18446744"));
    }

    // --- patch_target_global ---

    fn make_jinja_env_with_target(
        initial: &serde_json::Value,
    ) -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        let mut env = minijinja::Environment::new();
        env.add_global("target", json_to_minijinja(initial));
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(env)
    }

    #[test]
    fn patch_target_global_overrides_schema_and_database() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "old_schema",
            "database": "old_db",
            "name": "dev",
        }));
        patch_target_global(&mut env, "new_schema", "new_db", None);

        let rendered = env
            .render_str(
                "{{ target.schema }}|{{ target.database }}|{{ target.name }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "new_schema|new_db|dev");
    }

    #[test]
    fn patch_target_global_overrides_name_when_provided() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "s",
            "database": "d",
            "name": "old",
        }));
        patch_target_global(&mut env, "s", "d", Some("prod"));

        let rendered = env
            .render_str(
                "{{ target.name }}|{{ target.target_name }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "prod|prod");
    }

    #[test]
    fn patch_target_global_aliases_env_to_target() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "s",
            "database": "d",
        }));
        patch_target_global(&mut env, "s2", "d2", None);

        let rendered = env
            .render_str(
                "{{ env.schema }}|{{ env.database }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "s2|d2");
    }

    #[test]
    fn patch_target_global_returns_silently_when_target_not_object() {
        // `target` rendered as plain string → tojson produces a JSON string (not object);
        // the patch should be a no-op rather than panic.
        let mut env = minijinja::Environment::new();
        env.add_global("target", minijinja::Value::from("not_an_object"));
        let mut jenv = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);
        patch_target_global(&mut jenv, "s", "d", None);
        // No assertion needed beyond "didn't panic".
    }

    // --- find_materialization_template ---

    fn jinja_env_with_templates(
        templates: &[(&str, &str)],
    ) -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        let mut env = minijinja::Environment::new();
        for (name, body) in templates {
            env.add_template_owned(name.to_string(), body.to_string(), None)
                .expect("add template");
        }
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(env)
    }

    #[test]
    fn find_materialization_template_returns_first_suffix_match() {
        let env = jinja_env_with_templates(&[
            ("dbt_postgres.materialization_view_postgres", "ok"),
            ("dbt.materialization_table_default", "ok"),
            ("unrelated.macro", "ok"),
        ]);
        let found = find_materialization_template(&env, "materialization_view_postgres");
        assert_eq!(found.as_deref(), Some("dbt_postgres.materialization_view_postgres"));
    }

    #[test]
    fn find_materialization_template_returns_none_for_unknown_suffix() {
        let env = jinja_env_with_templates(&[("dbt.materialization_table_default", "ok")]);
        assert!(find_materialization_template(&env, "nope").is_none());
    }

    #[test]
    fn find_materialization_template_requires_dot_separator() {
        // A template named `materialization_view_postgres` (no package prefix)
        // would not have the leading dot, so it should not match.
        let env = jinja_env_with_templates(&[("materialization_view_postgres", "ok")]);
        assert!(find_materialization_template(&env, "materialization_view_postgres").is_none());
    }

    // --- render_materialization ---

    #[test]
    fn render_materialization_invokes_named_macro_and_returns_its_output() {
        // The macro body is a tiny stand-in for a materialization template — what
        // matters here is that `render_materialization` resolves the leaf macro name
        // and calls it. The body just emits a sentinel string.
        let env = jinja_env_with_templates(&[(
            "dbt.materialization_view_default",
            "{% macro materialization_view_default() %}rendered-ok{% endmacro %}",
        )]);
        let ctx = BTreeMap::new();
        let out = render_materialization(&env, "dbt.materialization_view_default", &ctx).unwrap();
        assert_eq!(out, "rendered-ok");
    }

    #[test]
    fn render_materialization_errors_when_template_missing() {
        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::new();
        let err = render_materialization(&env, "dbt.materialization_view_default", &ctx)
            .expect_err("missing template should error");
        let msg = err.to_string();
        assert!(msg.contains("template"));
        assert!(msg.contains("dbt.materialization_view_default"));
    }
}
