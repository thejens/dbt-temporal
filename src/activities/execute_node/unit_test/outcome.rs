//! Comparing the actual and expected partitions of an executed unit test.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array as _, RecordBatch, StringArray};
use arrow_schema::DataType;
use dbt_adapter::load_store::ResultStore;

use crate::error::DbtTemporalError;

/// Maximum number of differing rows included in the failure diff message.
/// Keeps Temporal failure payloads bounded for tests over large fixtures.
const MAX_DIFF_LINES: usize = 20;

/// Outcome of comparing the executed unit test result table.
#[derive(Debug)]
pub struct UnitTestOutcome {
    pub passed: bool,
    /// Number of row instances that differ between actual and expected.
    pub failures: i64,
    /// Human-readable diff of the differing rows (bounded).
    pub diff: String,
    /// Total actual rows, for the success message.
    pub actual_rows: usize,
}

/// Load the executed unit test result from the activity's result store and
/// compare actual vs expected partitions.
pub fn extract_unit_test_outcome(
    result_store: &ResultStore,
) -> Result<UnitTestOutcome, DbtTemporalError> {
    let load_fn = result_store.load_result();
    let result = load_fn(&[minijinja::Value::from("main")])
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("loading unit test result: {e}")))?;
    let table_val = result
        .get_attr("table")
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("unit test result table: {e}")))?;
    let table = table_val
        .downcast_object::<dbt_agate::AgateTable>()
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "unit test produced no result table — statement('main') did not run"
            ))
        })?;
    compare_actual_expected(table.original_record_batch().as_ref())
}

/// Compare the actual vs expected partitions of the executed unit test
/// result as multisets of stringified rows. Order-insensitive by design.
pub(super) fn compare_actual_expected(
    batch: &RecordBatch,
) -> Result<UnitTestOutcome, DbtTemporalError> {
    let schema = batch.schema();
    let label_idx = schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case("actual_or_expected"))
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "unit test result is missing the actual_or_expected column"
            ))
        })?;
    // The adapter normalizes every string column of a query result to
    // `Utf8View` before handing it back (dbt-adapter's
    // `concat_batches::to_view_types`, which sidesteps the i32 offset overflow
    // that concatenating large `Utf8` batches hits). Cast the label column back
    // to `Utf8` once so the row loop can borrow `&str` directly; the data
    // columns need no such treatment because they go through
    // `arrow_cast::display`, which renders every string representation alike.
    let label_column = batch.column(label_idx);
    let label_column = match label_column.data_type() {
        DataType::Utf8 => Arc::clone(label_column),
        DataType::Utf8View | DataType::LargeUtf8 => {
            arrow_cast::cast(label_column.as_ref(), &DataType::Utf8).map_err(|e| {
                DbtTemporalError::Adapter(anyhow::anyhow!(
                    "casting actual_or_expected column to Utf8: {e}"
                ))
            })?
        }
        other => {
            return Err(DbtTemporalError::Adapter(anyhow::anyhow!(
                "actual_or_expected column is not a string column (got {other})"
            )));
        }
    };
    let labels = label_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "actual_or_expected column is not a string column"
            ))
        })?;

    let data_columns: Vec<usize> = (0..schema.fields().len())
        .filter(|i| *i != label_idx)
        .collect();
    let header = data_columns
        .iter()
        .map(|i| schema.field(*i).name().clone())
        .collect::<Vec<_>>()
        .join(" | ");

    let mut actual: BTreeMap<String, i64> = BTreeMap::new();
    let mut expected: BTreeMap<String, i64> = BTreeMap::new();
    let mut actual_rows = 0usize;
    for row in 0..batch.num_rows() {
        let key = data_columns
            .iter()
            .map(|col| format_cell(batch.column(*col).as_ref(), row))
            .collect::<Result<Vec<_>, _>>()?
            .join(" | ");
        match labels.value(row) {
            "actual" => {
                actual_rows += 1;
                *actual.entry(key).or_default() += 1;
            }
            "expected" => *expected.entry(key).or_default() += 1,
            other => {
                return Err(DbtTemporalError::Adapter(anyhow::anyhow!(
                    "unexpected actual_or_expected label {other:?}"
                )));
            }
        }
    }

    let mut failures = 0i64;
    let mut lines: Vec<String> = Vec::new();
    let all_keys: std::collections::BTreeSet<&String> =
        actual.keys().chain(expected.keys()).collect();
    for key in all_keys {
        let a = actual.get(key).copied().unwrap_or(0);
        let e = expected.get(key).copied().unwrap_or(0);
        if a == e {
            continue;
        }
        failures += (a - e).abs();
        if lines.len() < MAX_DIFF_LINES {
            if e > a {
                lines.push(format!("- expected (x{}): {key}", e - a));
            } else {
                lines.push(format!("+ actual   (x{}): {key}", a - e));
            }
        }
    }

    let passed = failures == 0;
    let diff = if passed {
        String::new()
    } else {
        let mut out = format!("columns: {header}\n");
        out.push_str(&lines.join("\n"));
        if lines.len() == MAX_DIFF_LINES {
            out.push_str("\n… (diff truncated)");
        }
        out
    };

    Ok(UnitTestOutcome {
        passed,
        failures,
        diff,
        actual_rows,
    })
}

/// Stringify one cell for multiset comparison and diff display.
fn format_cell(column: &dyn arrow_array::Array, row: usize) -> Result<String, DbtTemporalError> {
    if column.is_null(row) {
        return Ok("NULL".to_string());
    }
    arrow_cast::display::array_value_to_string(column, row)
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("formatting result cell: {e}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use arrow_array::{ArrayRef, Int64Array, LargeStringArray, StringViewArray};

    fn batch(ids: &[i64], labels: &[&str]) -> RecordBatch {
        let id_col: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        let label_col: ArrayRef = Arc::new(StringArray::from(
            labels.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
        ));
        RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)]).unwrap()
    }

    /// The shape a real query result arrives in: the adapter normalizes string
    /// columns to `Utf8View` before returning them.
    fn batch_with_view_labels(ids: &[i64], labels: &[&str]) -> RecordBatch {
        let id_col: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        let label_col: ArrayRef = Arc::new(StringViewArray::from(labels.to_vec()));
        RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)]).unwrap()
    }

    #[test]
    fn compare_reads_utf8view_label_columns() {
        // The adapter widens every result string column to
        // `Utf8View`, so a plain `StringArray` downcast fails on real results
        // and the unit test errors with "not a string column" instead of
        // comparing anything.
        let b =
            batch_with_view_labels(&[2, 1, 1, 2], &["actual", "actual", "expected", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed);
        assert_eq!(outcome.actual_rows, 2);
    }

    #[test]
    fn compare_reads_largeutf8_label_columns() {
        // Drivers may hand back 64-bit-offset strings rather than `Utf8View`;
        // both normalize through the same cast.
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 1]));
        let label_col: ArrayRef = Arc::new(LargeStringArray::from(vec!["actual", "expected"]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)])
            .unwrap();
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed);
        assert_eq!(outcome.actual_rows, 1);
    }

    #[test]
    fn compare_detects_mismatch_with_utf8view_labels() {
        let b = batch_with_view_labels(&[1, 3], &["actual", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, 2);
    }

    #[test]
    fn compare_passes_when_multisets_match_regardless_of_order() {
        let b = batch(&[2, 1, 1, 2], &["actual", "actual", "expected", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed);
        assert_eq!(outcome.failures, 0);
        assert_eq!(outcome.actual_rows, 2);
    }

    #[test]
    fn compare_fails_on_value_mismatch_with_diff() {
        let b = batch(&[1, 3], &["actual", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, 2); // one extra actual + one missing expected
        assert!(outcome.diff.contains("+ actual"));
        assert!(outcome.diff.contains("- expected"));
        assert!(outcome.diff.contains("columns: id"));
    }

    #[test]
    fn compare_fails_on_duplicate_count_mismatch() {
        // Same value but different multiplicities must fail — a set-based
        // comparison would wrongly pass this.
        let b = batch(&[1, 1, 1], &["actual", "actual", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, 1);
    }

    #[test]
    fn compare_handles_null_cells_as_equal() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let label_col: ArrayRef =
            Arc::new(StringArray::from(vec!["actual".to_string(), "expected".to_string()]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)])
            .unwrap();
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed, "NULL must compare equal to NULL");
    }

    #[test]
    fn compare_truncates_long_diffs() {
        let n = i64::try_from(MAX_DIFF_LINES).expect("small constant") + 10;
        let ids: Vec<i64> = (0..n).collect();
        let labels: Vec<&str> = (0..n).map(|_| "actual").collect();
        let b = batch(&ids, &labels);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, n);
        assert!(outcome.diff.contains("diff truncated"));
    }

    #[test]
    fn compare_rejects_non_string_label_column() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let label_col: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)])
            .unwrap();
        assert!(compare_actual_expected(&b).is_err());
    }

    #[test]
    fn compare_rejects_unknown_label_values() {
        let b = batch(&[1], &["maybe"]);
        assert!(compare_actual_expected(&b).is_err());
    }

    #[test]
    fn compare_errors_without_label_column() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col)]).unwrap();
        assert!(compare_actual_expected(&b).is_err());
    }
}
