use anyhow::Context as _;
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

    // Call the macro — this executes the materialization body (DDL/DML via
    // adapter). A transient warehouse failure (dropped connection, throttling,
    // timeout) arrives here wrapped in a `minijinja::Error`; classify it so it
    // becomes a retryable `Adapter` error rather than a permanent `Compilation`
    // one — otherwise Temporal never retries a briefly-unreachable warehouse.
    let result = func.call(&state, &[], &[]).map_err(|e| {
        crate::error::classify_adapter_execution_error(
            &e,
            &format!("calling {macro_name} in {template_name}"),
        )
    })?;

    Ok(result.as_str().map(ToString::to_string).unwrap_or_default())
}

/// Inject ephemeral model CTEs into compiled SQL.
///
/// Walks the user's compiled SQL for `__dbt__cte__<name>` references, compiles
/// each transitive ephemeral dependency through Jinja, and persists per-ephemeral
/// CTE chains to `ephemeral_dir`. The final wrap is delegated to
/// `dbt_jinja_utils::utils::inject_and_persist_ephemeral_models`, matching
/// vanilla dbt-fusion's compile output.
///
/// That helper picks one of two shapes. SQL that already opens with `WITH` gets
/// the ephemeral CTEs spliced into its existing chain as siblings; everything
/// else is wrapped in `select * from (...)` and marked with
/// `--EPHEMERAL-SELECT-WRAPPER-START/END`, which dbt-fusion's adapter SQL
/// tokenizer / diff (`crates/dbt-adapter/src/sql/{tokenizer,diff}.rs`) read to
/// reason about ephemeral wrapping. Do not assume the markers are always
/// present.
///
/// `ephemeral_dir` must be unique per-activity; the persist step writes
/// `<name>.sql` files there, so concurrent activities sharing a directory would
/// race on the cumulative-CTE-chain layout.
pub fn inject_ephemeral_ctes(
    compiled_sql: &str,
    user_node_name: &str,
    depends_on: &[String],
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    dirs: EphemeralDirs<'_>,
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
        depends_on,
        nodes,
        jinja_env,
        node_context,
        dirs,
        &mut visited,
    )?;

    let mut spans = MacroSpans::default();
    inject_and_persist_ephemeral_models(
        compiled_sql.to_string(),
        &mut spans,
        user_node_name,
        false, // not an ephemeral — wraps and returns without persisting
        dirs.ephemeral_dir,
    )
    .map_err(|e| {
        DbtTemporalError::Compilation(format!(
            "wrapping ephemeral CTEs for {user_node_name}: {e:#}"
        ))
    })
}

/// Where the ephemeral walk reads from and writes to.
///
/// `ephemeral_dir` must be unique per activity: the persist step writes
/// `<name>.sql` files there, so concurrent activities sharing one would race on
/// the cumulative-CTE-chain layout.
#[derive(Debug, Clone, Copy)]
pub struct EphemeralDirs<'a> {
    /// Project source root, where an ephemeral's raw SQL is read from.
    pub in_dir: &'a Path,
    /// The activity's private scratch directory for persisted CTE chains.
    pub ephemeral_dir: &'a Path,
}

/// The ephemeral models among a node's resolved dependencies, indexed by the
/// name their CTE carries.
///
/// Two dependencies with the same name would produce two `__dbt__cte__<name>`
/// markers that no reader can tell apart, so that is refused rather than
/// silently resolved to one of them.
fn ephemeral_dependencies<'a>(
    depends_on: &[String],
    nodes: &'a dbt_schemas::schemas::Nodes,
) -> Result<
    BTreeMap<&'a str, (&'a str, &'a dyn dbt_schemas::schemas::nodes::InternalDbtNode)>,
    DbtTemporalError,
> {
    let mut by_name: BTreeMap<&str, (&str, &dyn dbt_schemas::schemas::nodes::InternalDbtNode)> =
        BTreeMap::new();
    for (unique_id, node) in nodes.iter() {
        if !depends_on.iter().any(|dep| dep == unique_id) {
            continue;
        }
        if !node
            .base()
            .materialized
            .to_string()
            .eq_ignore_ascii_case("ephemeral")
        {
            continue;
        }
        let name = node.common().name.as_str();
        if let Some((existing, _)) = by_name.insert(name, (unique_id.as_str(), node)) {
            return Err(DbtTemporalError::Compilation(format!(
                "two ephemeral dependencies are both named '{name}' ({existing} and \
                 {unique_id}); their CTEs cannot be told apart in compiled SQL"
            )));
        }
    }
    Ok(by_name)
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
    depends_on: &[String],
    nodes: &dbt_schemas::schemas::Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    dirs: EphemeralDirs<'_>,
    visited: &mut BTreeSet<String>,
) -> Result<(), DbtTemporalError> {
    let candidates = ephemeral_dependencies(depends_on, nodes)?;

    for name in extract_ephemeral_names(sql) {
        // The CTE name is what the SQL carries, but *which* node it names is
        // decided by the depending node's resolved dependencies — not by
        // scanning the whole project for a model with that name, which picks
        // an arbitrary one when two packages both define `base`.
        let Some(&(unique_id, node)) = candidates.get(name.as_str()) else {
            return Err(DbtTemporalError::Compilation(format!(
                "compiled SQL references ephemeral model '{name}', which is not among the \
                 node's ephemeral dependencies ({})",
                if candidates.is_empty() {
                    "none".to_string()
                } else {
                    candidates.keys().copied().collect::<Vec<_>>().join(", ")
                }
            )));
        };
        if !visited.insert(unique_id.to_string()) {
            continue;
        }

        let raw_path = dirs.in_dir.join(&node.common().original_file_path);
        let raw_sql = std::fs::read_to_string(&raw_path).map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "reading ephemeral model '{name}' at {}: {e:#}",
                raw_path.display()
            ))
        })?;
        let compiled = render_sql_with_listeners(
            &raw_sql,
            jinja_env,
            node_context,
            NO_LISTENERS,
            &[],
            &raw_path,
        )
        .map_err(|e| {
            // Same reasoning as the model render path: an ephemeral's body can
            // reach the warehouse through `run_query` or introspection, so a
            // transient failure here has to stay retryable.
            crate::error::classify_adapter_execution_error(
                &*e,
                &format!("compiling ephemeral model '{name}'"),
            )
        })?;

        // Recurse first so this ephemeral's deps land on disk before we persist
        // it — and from *its* dependencies, so a nested ephemeral is resolved
        // against the node that actually references it.
        persist_ephemeral_chain(
            &compiled,
            &node.base().depends_on.nodes,
            nodes,
            jinja_env,
            node_context,
            dirs,
            visited,
        )?;

        let mut spans = MacroSpans::default();
        inject_and_persist_ephemeral_models(
            compiled,
            &mut spans,
            &name,
            true, // ephemeral — persists cumulative CTE chain to disk
            dirs.ephemeral_dir,
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

/// What dbt's test materialization reported about one test.
///
/// `should_warn` and `should_error` are the `warn_if` / `error_if` expressions
/// evaluated by the warehouse itself, not something to be re-derived from
/// `failures`: a test configured `error_if: ">100"` is passing at 100 failures,
/// and only the SQL knows that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TestOutcome {
    pub failures: i64,
    pub should_warn: bool,
    pub should_error: bool,
}

/// Read the test result table dbt's test materialization stored.
///
/// `get_test_sql` selects `failures`, `should_warn`, `should_error`. Column
/// names are matched case-insensitively because warehouses do not agree on
/// case, and a table without them falls back to that fixed column order —
/// both matching upstream's own reader.
///
/// A missing or unreadable table is an error, never an empty outcome: this is
/// only called for test nodes, so "no result" means the materialization never
/// ran `statement('main')`, and reporting that as zero failures turns a broken
/// test into a green one.
pub fn extract_test_outcome(result_store: &ResultStore) -> Result<TestOutcome, anyhow::Error> {
    let load_fn = result_store.load_result();
    let result = load_fn(&[minijinja::Value::from("main")])
        .map_err(|e| anyhow::anyhow!("loading test result: {e}"))?;
    let table_val = result
        .get_attr("table")
        .map_err(|e| anyhow::anyhow!("reading the test result table: {e}"))?;
    let table = table_val
        .downcast_object::<dbt_agate::AgateTable>()
        .ok_or_else(|| {
            anyhow::anyhow!("test produced no result table — statement('main') did not run")
        })?;

    let batch = table.original_record_batch();
    anyhow::ensure!(
        batch.num_rows() > 0,
        "test result table has no rows; expected the row get_test_sql selects"
    );

    let (failures_idx, warn_idx, error_idx) = test_result_columns(batch.as_ref())?;

    // Normally one row. A test that reports per column returns one row each, and
    // such a test has failed if any of its rows did — so the rows are folded
    // rather than assumed to be single.
    let mut outcome = TestOutcome::default();
    for row in 0..batch.num_rows() {
        outcome.failures += cell_as_i64(batch.as_ref(), failures_idx, row)?;
        outcome.should_warn |= cell_as_bool(batch.as_ref(), warn_idx, row)?;
        outcome.should_error |= cell_as_bool(batch.as_ref(), error_idx, row)?;
    }
    Ok(outcome)
}

/// Locate `failures`, `should_warn` and `should_error` in the result table.
fn test_result_columns(
    batch: &arrow_array::RecordBatch,
) -> Result<(usize, usize, usize), anyhow::Error> {
    let index_of = |name: &str| {
        batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(name))
    };

    if let (Some(f), Some(w), Some(e)) =
        (index_of("failures"), index_of("should_warn"), index_of("should_error"))
    {
        return Ok((f, w, e));
    }

    // Same fallback upstream uses: an adapter that renamed the columns still
    // returns them in the order `get_test_sql` selects them.
    anyhow::ensure!(
        batch.num_columns() == 3,
        "test result table should name failures/should_warn/should_error or have exactly \
         3 columns, but has {} columns: {:?}",
        batch.num_columns(),
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
    Ok((0, 1, 2))
}

/// Render one cell as text. Warehouses disagree on the types they return for
/// these columns, and every one of them prints.
fn cell_text(
    batch: &arrow_array::RecordBatch,
    column: usize,
    row: usize,
) -> Result<String, anyhow::Error> {
    let array = batch.column(column);
    if array.is_null(row) {
        return Ok(String::new());
    }
    arrow_cast::display::array_value_to_string(array, row)
        .map_err(|e| anyhow::anyhow!("reading test result column {column}: {e}"))
}

fn cell_as_i64(
    batch: &arrow_array::RecordBatch,
    column: usize,
    row: usize,
) -> Result<i64, anyhow::Error> {
    let text = cell_text(batch, column, row)?;
    if text.is_empty() {
        return Ok(0);
    }
    if let Ok(n) = text.parse::<i64>() {
        return Ok(n);
    }
    // A custom `fail_calc` can return a decimal (`sum(amount)`), which dbt still
    // treats as a count — truncate toward zero. `as` saturates at the i64
    // bounds rather than wrapping.
    #[allow(clippy::cast_possible_truncation)]
    let truncated = text
        .parse::<f64>()
        .map(|f| f as i64)
        .map_err(|_| anyhow::anyhow!("test failure count is not a number: {text:?}"))?;
    Ok(truncated)
}

/// Coerce a test-result cell to bool. Some adapters have no boolean literal and
/// return the text "true"/"false"; others return 1/0.
fn cell_as_bool(
    batch: &arrow_array::RecordBatch,
    column: usize,
    row: usize,
) -> Result<bool, anyhow::Error> {
    let text = cell_text(batch, column, row)?;
    match text.trim() {
        "" => Ok(false),
        t if t.eq_ignore_ascii_case("true") || t == "1" => Ok(true),
        t if t.eq_ignore_ascii_case("false") || t == "0" => Ok(false),
        other => anyhow::bail!("test result flag is not a boolean: {other:?}"),
    }
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
///
/// Every failure here is an error rather than a silent return. This runs only
/// when a workflow resolved a different profile than the worker started on, so
/// giving up leaves `target` describing the *startup* warehouse while the
/// adapter is connected to another one — every `target.schema` in the project
/// then names a schema the run is not writing to.
pub(super) fn patch_target_global(
    jinja_env: &mut dbt_jinja_utils::jinja_environment::JinjaEnv,
    schema: &str,
    database: Option<&str>,
    target_name: Option<&str>,
) -> Result<(), anyhow::Error> {
    // Extract current target as JSON, modify fields, re-inject as a native BTreeMap Value.
    let target_json = jinja_env
        .render_str("{{ target | tojson }}", BTreeMap::<String, minijinja::Value>::new(), &[])
        .map_err(|e| anyhow::anyhow!("reading the current Jinja target: {e}"))?;

    let json_val: serde_json::Value = serde_json::from_str(&target_json)
        .with_context(|| format!("parsing the current Jinja target: {target_json}"))?;
    let obj = json_val
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("the Jinja target is not an object: {json_val}"))?;

    let mut new_target: BTreeMap<String, minijinja::Value> = obj
        .iter()
        .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
        .collect();

    new_target.insert("schema".to_string(), minijinja::Value::from(schema));
    // A target that declares no database keeps the one the adapter derived —
    // DuckDB names it after the file, and overwriting that with an empty
    // string puts `""` into every relation the project renders.
    if let Some(database) = database {
        new_target.insert("database".to_string(), minijinja::Value::from(database));
    }
    if let Some(name) = target_name {
        new_target.insert("name".to_string(), minijinja::Value::from(name));
        new_target.insert("target_name".to_string(), minijinja::Value::from(name));
    }

    let val = minijinja::Value::from(new_target);
    jinja_env.env.add_global("target", val.clone());
    // In dbt, `env` is an alias for `target`.
    jinja_env.env.add_global("env", val);
    Ok(())
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
            // `u64` before `f64`: a JSON integer above `i64::MAX` is exact as
            // an unsigned, and lossy as a float. Warehouse ids and epoch
            // nanoseconds land in that range, and a var silently rounded to
            // the nearest representable double changes what the model selects.
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(u) = n.as_u64() {
                minijinja::Value::from(u)
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
        // recursive CTE's `... CYCLE ... DEPTH` clause and rejected it.
        //
        // The dbt-fusion helper splices the ephemeral CTEs into the user's
        // existing chain, so both become siblings under one `WITH`. The
        // `select * from (...)` wrapper and its `--EPHEMERAL-SELECT-WRAPPER-*`
        // markers are the *other* branch, taken only when there is no leading
        // `WITH` to splice into — see `..._emits_wrapper_markers` below.
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
            wrapped.contains("__dbt__cte__alpha as (\nselect 1 as k, 2 as v\n)"),
            "expected alpha to be inlined; got:\n{wrapped}"
        );
        // Splice, not wrap: the ephemeral lands inside the user's own chain and
        // `user_cte` follows it as a sibling behind a comma.
        assert!(
            wrapped.starts_with("WITH  __dbt__cte__alpha as ("),
            "expected alpha spliced into the leading WITH; got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("), user_cte AS ("),
            "expected user_cte to remain a sibling CTE; got:\n{wrapped}"
        );
        assert!(
            !wrapped.contains("--EPHEMERAL-SELECT-WRAPPER"),
            "splice path must not emit wrapper markers; got:\n{wrapped}"
        );
        // The pathology itself: exactly one `WITH` keyword may open the
        // statement. Two in a row is what BigQuery rejected.
        assert_eq!(
            wrapped.to_ascii_uppercase().matches("WITH").count(),
            1,
            "expected a single WITH keyword; got:\n{wrapped}"
        );
        Ok(())
    }

    /// Build Nodes containing one ephemeral model whose raw SQL is `body` and
    /// whose original_file_path will resolve relative to `in_dir`.
    fn build_nodes_with_ephemeral(
        in_dir: &Path,
        name: &str,
        body: &str,
    ) -> std::io::Result<dbt_schemas::schemas::Nodes> {
        use std::sync::Arc as A;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::common::DbtMaterialization;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

        let rel = format!("models/{name}.sql");
        let abs = in_dir.join(&rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&abs, body)?;

        let common = CommonAttributes {
            unique_id: format!("model.shop.{name}"),
            name: name.to_string(),
            original_file_path: std::path::PathBuf::from(&rel).into(),
            ..CommonAttributes::default()
        };
        let base = NodeBaseAttributes {
            materialized: DbtMaterialization::Ephemeral,
            ..NodeBaseAttributes::default()
        };
        let model = DbtModel {
            __common_attr__: common,
            __base_attr__: base,
            ..DbtModel::default()
        };

        let mut nodes = Nodes::default();
        nodes
            .models
            .insert(format!("model.shop.{name}"), A::new(model));
        Ok(nodes)
    }

    #[test]
    fn inject_ephemeral_ctes_walks_dependency_and_emits_wrapper_markers() -> anyhow::Result<()> {
        // End-to-end: a user model SQL that refs an ephemeral via the
        // __dbt__cte__alpha prefix. inject_ephemeral_ctes must (a) compile +
        // persist the ephemeral's raw SQL leaf-first, (b) wrap the user SQL
        // with the EPHEMERAL-SELECT-WRAPPER markers.
        let dir = tempfile::tempdir()?;
        let in_dir = dir.path().join("project");
        let ephemeral_dir = dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)?;

        let nodes = build_nodes_with_ephemeral(&in_dir, "alpha", "select 1 as k, 2 as v")?;
        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::<String, minijinja::Value>::new();

        let user_sql = "select * from __dbt__cte__alpha";
        let out = inject_ephemeral_ctes(
            user_sql,
            "user_model",
            &["model.shop.alpha".to_string()],
            &nodes,
            &env,
            &ctx,
            EphemeralDirs {
                in_dir: &in_dir,
                ephemeral_dir: &ephemeral_dir,
            },
        )?;

        assert!(
            out.contains("--EPHEMERAL-SELECT-WRAPPER-START"),
            "expected wrapper start marker; got:\n{out}"
        );
        assert!(out.contains("--EPHEMERAL-SELECT-WRAPPER-END"), "got:\n{out}");
        // alpha's compiled body should appear inline.
        assert!(out.contains("select 1 as k, 2 as v"), "got:\n{out}");
        Ok(())
    }

    #[test]
    fn inject_ephemeral_ctes_errors_when_ephemeral_source_missing_on_disk() -> anyhow::Result<()> {
        // Nodes registers an ephemeral model with an original_file_path that
        // doesn't exist on disk → persist_ephemeral_chain fails to read it.
        // The error must mention the model name + the file path so users can
        // diagnose it.
        use std::sync::Arc as A;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::common::DbtMaterialization;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

        let dir = tempfile::tempdir()?;
        let in_dir = dir.path().to_path_buf();
        let ephemeral_dir = dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)?;

        let common = CommonAttributes {
            unique_id: "model.shop.missing".to_string(),
            name: "missing".to_string(),
            original_file_path: std::path::PathBuf::from("models/missing.sql").into(),
            ..CommonAttributes::default()
        };
        let base = NodeBaseAttributes {
            materialized: DbtMaterialization::Ephemeral,
            ..NodeBaseAttributes::default()
        };
        let model = DbtModel {
            __common_attr__: common,
            __base_attr__: base,
            ..DbtModel::default()
        };
        let mut nodes = Nodes::default();
        nodes
            .models
            .insert("model.shop.missing".to_string(), A::new(model));

        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::<String, minijinja::Value>::new();

        let err = inject_ephemeral_ctes(
            "select * from __dbt__cte__missing",
            "user_model",
            &["model.shop.missing".to_string()],
            &nodes,
            &env,
            &ctx,
            EphemeralDirs {
                in_dir: &in_dir,
                ephemeral_dir: &ephemeral_dir,
            },
        )
        .expect_err("missing ephemeral source must fail");
        let msg = err.to_string();
        assert!(msg.contains("missing"), "error should mention model name: {msg}");
        Ok(())
    }

    /// Two packages may each define an ephemeral called `base`. Scanning the
    /// whole project for a model with that name picked whichever the node map
    /// yielded first, so a model could be compiled around a different
    /// package's ephemeral entirely. The depending node's own dependencies say
    /// which one it meant.
    #[test]
    fn inject_ephemeral_ctes_picks_the_dependency_not_the_first_same_named_model()
    -> anyhow::Result<()> {
        use std::sync::Arc as A;

        use dbt_schemas::schemas::common::DbtMaterialization;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel, NodeBaseAttributes};

        let dir = tempfile::tempdir()?;
        let in_dir = dir.path().join("project");
        let ephemeral_dir = dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)?;

        // `model.shop.base` sorts before `model.vendor.base`, so a first-match
        // scan finds the wrong one.
        let mut nodes = build_nodes_with_ephemeral(&in_dir, "base", "select 'shop' as who")?;
        let rel = "models/vendor_base.sql";
        let abs = in_dir.join(rel);
        std::fs::write(&abs, "select 'vendor' as who")?;
        nodes.models.insert(
            "model.vendor.base".to_string(),
            A::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: "model.vendor.base".to_string(),
                    name: "base".to_string(),
                    original_file_path: std::path::PathBuf::from(rel).into(),
                    ..CommonAttributes::default()
                },
                __base_attr__: NodeBaseAttributes {
                    materialized: DbtMaterialization::Ephemeral,
                    ..NodeBaseAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::<String, minijinja::Value>::new();
        let out = inject_ephemeral_ctes(
            "select * from __dbt__cte__base",
            "user_model",
            &["model.vendor.base".to_string()],
            &nodes,
            &env,
            &ctx,
            EphemeralDirs {
                in_dir: &in_dir,
                ephemeral_dir: &ephemeral_dir,
            },
        )?;

        assert!(out.contains("select 'vendor' as who"), "got:\n{out}");
        assert!(!out.contains("select 'shop' as who"), "got:\n{out}");
        Ok(())
    }

    /// A CTE marker naming a dependency that is not ephemeral has no body to
    /// inline. It used to be skipped, and the failure surfaced later as a
    /// missing persisted file; it is now reported against the name in the SQL.
    #[test]
    fn inject_ephemeral_ctes_reports_a_non_ephemeral_dependency() -> anyhow::Result<()> {
        use std::sync::Arc as A;

        use dbt_schemas::schemas::Nodes;
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let dir = tempfile::tempdir()?;
        let in_dir = dir.path().join("project");
        std::fs::create_dir_all(&in_dir)?;
        let ephemeral_dir = dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)?;

        // Default DbtModel is materialized = Snapshot (not Ephemeral).
        let common = CommonAttributes {
            unique_id: "model.shop.alpha".to_string(),
            name: "alpha".to_string(),
            ..CommonAttributes::default()
        };
        let model = DbtModel {
            __common_attr__: common,
            ..DbtModel::default()
        };
        let mut nodes = Nodes::default();
        nodes
            .models
            .insert("model.shop.alpha".to_string(), A::new(model));

        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::<String, minijinja::Value>::new();

        let err = inject_ephemeral_ctes(
            "select * from __dbt__cte__alpha",
            "user_model",
            &["model.shop.alpha".to_string()],
            &nodes,
            &env,
            &ctx,
            EphemeralDirs {
                in_dir: &in_dir,
                ephemeral_dir: &ephemeral_dir,
            },
        )
        .expect_err("a non-ephemeral dependency has no CTE body to inline");
        let msg = err.to_string();
        assert!(msg.contains("alpha"), "should name the ephemeral in the SQL: {msg}");
        Ok(())
    }

    #[test]
    fn inject_ephemeral_ctes_short_circuits_when_prefix_absent() -> anyhow::Result<()> {
        // Early return path: SQL without DBT_CTE_PREFIX — neither dependency
        // walk nor wrap step runs, returns the input verbatim. This is the
        // common case for non-ephemeral nodes.
        let dir = tempfile::tempdir()?;
        let nodes = dbt_schemas::schemas::Nodes::default();
        let env = jinja_env_with_templates(&[]);
        let ctx = BTreeMap::<String, minijinja::Value>::new();

        let plain = "select 1 as id";
        let out = inject_ephemeral_ctes(
            plain,
            "user_model",
            &[],
            &nodes,
            &env,
            &ctx,
            EphemeralDirs {
                in_dir: dir.path(),
                ephemeral_dir: dir.path(),
            },
        )?;
        assert_eq!(out, plain);
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
    fn json_to_minijinja_large_unsigned_keeps_every_digit() {
        // Above i64::MAX the value has to go through the u64 branch: as an f64
        // it would round to the nearest double and come back as a different
        // number, which is how a var reaches SQL as the wrong id.
        let v = json_to_minijinja(&serde_json::json!(u64::MAX));
        assert_eq!(v.to_string(), u64::MAX.to_string());

        let id = 9_007_199_254_740_993_u64; // 2^53 + 1 — not representable as f64
        let v = json_to_minijinja(&serde_json::json!(id));
        assert_eq!(v.to_string(), id.to_string());
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
        patch_target_global(&mut env, "new_schema", Some("new_db"), None)
            .expect("patching the target succeeds");

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
        patch_target_global(&mut env, "s", Some("d"), Some("prod"))
            .expect("patching the target succeeds");

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
        patch_target_global(&mut env, "s2", Some("d2"), None)
            .expect("patching the target succeeds");

        let rendered = env
            .render_str(
                "{{ env.schema }}|{{ env.database }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "s2|d2");
    }

    /// A `target` that is not an object cannot be patched. Returning quietly
    /// left it describing the startup warehouse while the adapter was connected
    /// to another one, so every `target.schema` in the project named a schema
    /// the run was not writing to.
    #[test]
    fn patch_target_global_reports_a_target_it_cannot_patch() {
        let mut env = minijinja::Environment::new();
        env.add_global("target", minijinja::Value::from("not_an_object"));
        let mut jenv = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);

        let err = patch_target_global(&mut jenv, "s", None, None)
            .expect_err("a non-object target must be reported");
        assert!(err.to_string().contains("not an object"), "should say what was wrong: {err}");
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

    // NOTE: the macro-missing branch (template exists, leaf macro absent) is
    // deliberately untested — fusion's `State::lookup` recurses without bound
    // on a missing name and overflows the stack. Re-confirmed against the
    // 2026-07-06 pin (`37ba42bd`); see docs/workarounds.md.

    #[test]
    fn render_materialization_surfaces_macro_call_errors() {
        let env = jinja_env_with_templates(&[(
            "dbt.materialization_boom_default",
            "{% macro materialization_boom_default() %}{{ no_such_function() }}{% endmacro %}",
        )]);
        let ctx = BTreeMap::new();
        let err = render_materialization(&env, "dbt.materialization_boom_default", &ctx)
            .expect_err("erroring macro should propagate");
        assert!(err.to_string().contains("materialization_boom_default"));
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
