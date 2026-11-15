//! Reading what dbt's materializations stored in the result store.

use std::collections::BTreeMap;

use dbt_adapter::load_store::ResultStore;

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
