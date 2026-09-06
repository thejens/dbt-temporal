//! What counts as a violation, and when zero rows is not a pass.
//!
//! A project check is SQL whose result set *is* its violation list: every row
//! it returns is something wrong with the project, and zero rows means the rule
//! looked and found nothing. Everything here decides which of a check's rows
//! survive the run's node selection, and whether what survived says anything at
//! all.

use std::collections::BTreeSet;

use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use dbt_index_core::format::FMT_OPTS;
use dbt_schemas::schemas::project::SelectionFilterOn;

/// Which output column(s) the run's node selection scopes a check's rows by.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SelectionFilter {
    /// Scope on `unique_id` when the check outputs one, otherwise not at all.
    #[default]
    Auto,
    /// Never scope — the check always evaluates the whole project, which is
    /// what an aggregate check ("no more than 20% of models are views") wants.
    None,
    /// Scope on these output columns: a row survives when the node id in *any*
    /// of them is selected. A named column missing from the output is an error,
    /// not an empty filter — silently keeping every row would turn a
    /// misconfigured check into a confident verdict.
    Columns(Vec<String>),
}

/// Map a check's resolved `selection_filter_on` config onto a [`SelectionFilter`].
pub fn selection_filter_for(configured: Option<&SelectionFilterOn>) -> SelectionFilter {
    match configured {
        None => SelectionFilter::Auto,
        Some(SelectionFilterOn::One(s)) if s.eq_ignore_ascii_case("none") => SelectionFilter::None,
        Some(SelectionFilterOn::One(s)) => SelectionFilter::Columns(vec![s.clone()]),
        Some(SelectionFilterOn::Many(cols)) => SelectionFilter::Columns(cols.clone()),
    }
}

/// The resource-type prefix of a unique id: `model.p.a` -> `model.`.
///
/// An id carrying no prefix is its own kind, so an exact match still works.
fn kind_of(id: &str) -> String {
    id.find('.')
        .map_or_else(|| id.to_string(), |i| id[..=i].to_string())
}

/// Whether a zero-row result means "nothing was examined" rather than "nothing
/// is wrong".
///
/// Reporting the first as a pass claims a validation that never ran, which is
/// the one outcome a check must never produce. There are two ways to get there,
/// and an empty scope is only the obvious one:
///
/// - **Nothing selected at all.** Every row is filtered away.
/// - **Nothing of the right *kind* selected.** A selector that resolves to
///   seeds only, against a check reporting model ids, drops every row — yet the
///   scope is not empty, so an emptiness test would call it a genuine pass.
///
/// `reported_kinds` is the resource-type prefixes of the ids the check emitted
/// in its scoping columns *before* filtering. Empty means the check reported no
/// rows anywhere, which is a real pass: the rule looked project-wide and found
/// nothing.
pub fn zero_rows_is_vacuous(
    scope: Option<&BTreeSet<String>>,
    filter: &SelectionFilter,
    reported_kinds: &BTreeSet<String>,
) -> bool {
    // No selector: the whole project was examined.
    let Some(scope) = scope else { return false };
    // The check opted out of scoping, so the selection never touched its rows.
    if matches!(filter, SelectionFilter::None) {
        return false;
    }
    if scope.is_empty() {
        return true;
    }
    // The rule found nothing to report project-wide, so the scope dropped nothing.
    if reported_kinds.is_empty() {
        return false;
    }
    !reported_kinds
        .iter()
        .any(|kind| scope.iter().any(|id| id.starts_with(kind)))
}

/// A check's rows, evaluated against the selection.
#[derive(Debug, Default)]
pub struct Evaluation {
    /// Rows that survived scoping. Zero is a pass unless it is vacuous — see
    /// [`zero_rows_is_vacuous`].
    pub violations: u64,
    /// The first few surviving rows, rendered `col=value, …` for the report.
    pub preview: Vec<String>,
    /// Resource-type prefixes of the ids found in the scoping columns *before*
    /// filtering, so a zero-row result can be told apart from one whose scope
    /// held nothing of the right kind.
    pub reported_kinds: BTreeSet<String>,
}

/// Count a check's violations and build a preview of the first `max_rows`.
///
/// `scope` is the run's selected node set, or `None` when the run had no
/// selector at all.
pub fn evaluate_batch(
    batch: &RecordBatch,
    filter: &SelectionFilter,
    scope: Option<&BTreeSet<String>>,
    max_rows: usize,
) -> Result<Evaluation, String> {
    let mut eval = Evaluation::default();
    let Ok(formatters) = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &FMT_OPTS))
        .collect::<Result<Vec<_>, _>>()
    else {
        // A column Arrow cannot render is not a violation count we can trust.
        return Err("check output has a column that cannot be rendered".to_string());
    };
    let schema = batch.schema();

    // `unique_id` already identifies the node, so `name` beside it is noise.
    // `message` is dropped always: check SQL often aliases a prose column
    // there, and the preview is the row's identity, not a second copy of that.
    let drop_name = schema.index_of("unique_id").is_ok();
    let filter_idxs = scoping_columns(&schema, filter, scope)?;

    for row in 0..batch.num_rows() {
        // Recorded before the row can be filtered away: what a check reports on
        // is a property of the check, not of the selection.
        for &i in &filter_idxs {
            eval.reported_kinds
                .insert(kind_of(&formatters[i].value(row).to_string()));
        }
        if let Some(selected) = scope
            && !filter_idxs.is_empty()
            && !filter_idxs
                .iter()
                .any(|&i| selected.contains(&formatters[i].value(row).to_string()))
        {
            continue;
        }
        eval.violations += 1;
        if eval.preview.len() < max_rows {
            let cells: Vec<String> = formatters
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    let name = schema.field(*i).name();
                    name != "message" && !(drop_name && name == "name")
                })
                .map(|(i, f)| format!("{}={}", schema.field(i).name(), f.value(row)))
                .collect();
            if !cells.is_empty() {
                eval.preview.push(cells.join(", "));
            }
        }
    }
    Ok(eval)
}

/// Output-column indices whose values are matched against the selection.
///
/// Empty means this check's rows are not scoped at all, so every row counts.
fn scoping_columns(
    schema: &arrow_schema::Schema,
    filter: &SelectionFilter,
    scope: Option<&BTreeSet<String>>,
) -> Result<Vec<usize>, String> {
    match (scope, filter) {
        (None, _) | (Some(_), SelectionFilter::None) => Ok(Vec::new()),
        (Some(_), SelectionFilter::Auto) => {
            Ok(schema.index_of("unique_id").ok().into_iter().collect())
        }
        (Some(_), SelectionFilter::Columns(cols)) => cols
            .iter()
            .map(|c| {
                schema.index_of(c).map_err(|_| {
                    format!(
                        "selection_filter_on: column '{c}' is not in the check's output columns"
                    )
                })
            })
            .collect(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn ids(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    /// A single all-Utf8 batch from `(column, values)` pairs.
    fn batch(cols: &[(&str, &[&str])]) -> RecordBatch {
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field, Schema};
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
            .collect();
        let arrays: Vec<Arc<dyn arrow_array::Array>> = cols
            .iter()
            .map(|(_, vals)| {
                Arc::new(StringArray::from(vals.iter().map(|s| Some(*s)).collect::<Vec<_>>())) as _
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn selection_filter_reads_the_config_spellings() {
        assert_eq!(selection_filter_for(None), SelectionFilter::Auto);
        assert_eq!(
            selection_filter_for(Some(&SelectionFilterOn::One("NONE".to_string()))),
            SelectionFilter::None,
            "`none` opts out of scoping regardless of case"
        );
        assert_eq!(
            selection_filter_for(Some(&SelectionFilterOn::One("parent_id".to_string()))),
            SelectionFilter::Columns(vec!["parent_id".to_string()])
        );
        assert_eq!(
            selection_filter_for(Some(&SelectionFilterOn::Many(vec![
                "a".to_string(),
                "b".to_string()
            ]))),
            SelectionFilter::Columns(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn every_row_is_a_violation_without_a_selector() {
        let b = batch(&[("unique_id", &["model.p.a", "model.p.b"])]);
        let eval = evaluate_batch(&b, &SelectionFilter::Auto, None, 5).unwrap();
        assert_eq!(eval.violations, 2);
    }

    #[test]
    fn rows_outside_the_selection_are_dropped() {
        let b = batch(&[("unique_id", &["model.p.a", "model.p.b"])]);
        let scope = ids(&["model.p.a"]);
        let eval = evaluate_batch(&b, &SelectionFilter::Auto, Some(&scope), 5).unwrap();
        assert_eq!(eval.violations, 1);
        assert_eq!(eval.preview, vec!["unique_id=model.p.a".to_string()]);
    }

    /// `selection_filter_on: none` is what an aggregate check sets, and it must
    /// survive a selector that names none of the rows it reports.
    #[test]
    fn opting_out_of_scoping_keeps_every_row() {
        let b = batch(&[("unique_id", &["model.p.a", "model.p.b"])]);
        let scope = ids(&["model.p.zzz"]);
        let eval = evaluate_batch(&b, &SelectionFilter::None, Some(&scope), 5).unwrap();
        assert_eq!(eval.violations, 2);
    }

    /// A row survives when *any* named column names a selected node.
    #[test]
    fn multi_column_scoping_keeps_a_row_matched_by_either_side() {
        let b = batch(&[
            ("parent", &["model.p.a", "model.p.x"]),
            ("child", &["model.p.y", "model.p.b"]),
        ]);
        let scope = ids(&["model.p.b"]);
        let filter = SelectionFilter::Columns(vec!["parent".to_string(), "child".to_string()]);
        let eval = evaluate_batch(&b, &filter, Some(&scope), 5).unwrap();
        assert_eq!(eval.violations, 1);
    }

    /// A named column the check does not output is a configuration error. The
    /// alternative — no filtering — would report a project-wide verdict for a
    /// run that asked for a subset.
    #[test]
    fn a_missing_scoping_column_is_an_error() {
        let b = batch(&[("unique_id", &["model.p.a"])]);
        let scope = ids(&["model.p.a"]);
        let filter = SelectionFilter::Columns(vec!["nope".to_string()]);
        let err = evaluate_batch(&b, &filter, Some(&scope), 5).unwrap_err();
        assert!(err.contains("nope"), "should name the missing column: {err}");
    }

    /// The preview identifies the row; `name` next to `unique_id` and any
    /// `message` column are the check's prose, not its identity.
    #[test]
    fn preview_drops_the_redundant_and_prose_columns() {
        let b = batch(&[
            ("unique_id", &["model.p.a"]),
            ("name", &["a"]),
            ("column_name", &["id"]),
            ("message", &["column has no description"]),
        ]);
        let eval = evaluate_batch(&b, &SelectionFilter::None, None, 5).unwrap();
        assert_eq!(eval.preview, vec!["unique_id=model.p.a, column_name=id".to_string()]);
    }

    #[test]
    fn preview_stops_at_the_row_limit_but_the_count_does_not() {
        let b = batch(&[("unique_id", &["model.p.a", "model.p.b", "model.p.c"])]);
        let eval = evaluate_batch(&b, &SelectionFilter::Auto, None, 2).unwrap();
        assert_eq!(eval.violations, 3);
        assert_eq!(eval.preview.len(), 2);
    }

    /// Kinds are recorded from the rows the check emitted, before the selection
    /// gets to drop any of them — that is what makes the vacuity test possible.
    #[test]
    fn reported_kinds_survive_filtering() {
        let b = batch(&[("unique_id", &["model.p.a", "seed.p.s"])]);
        let scope = ids(&["snapshot.p.x"]);
        let eval = evaluate_batch(&b, &SelectionFilter::Auto, Some(&scope), 5).unwrap();
        assert_eq!(eval.violations, 0);
        assert_eq!(eval.reported_kinds, ids(&["model.", "seed."]));
    }

    #[test]
    fn zero_rows_without_a_selector_is_a_genuine_pass() {
        assert!(!zero_rows_is_vacuous(None, &SelectionFilter::Auto, &ids(&["model."])));
    }

    #[test]
    fn an_empty_scope_examines_nothing() {
        assert!(zero_rows_is_vacuous(
            Some(&BTreeSet::new()),
            &SelectionFilter::Auto,
            &ids(&["model."])
        ));
    }

    /// The subtle case: a non-empty scope holding nothing of the kind this
    /// check reports on is just as vacuous as an empty one.
    #[test]
    fn a_scope_of_the_wrong_kind_examines_nothing() {
        assert!(zero_rows_is_vacuous(
            Some(&ids(&["seed.p.s"])),
            &SelectionFilter::Auto,
            &ids(&["model."])
        ));
        assert!(
            !zero_rows_is_vacuous(
                Some(&ids(&["model.p.a"])),
                &SelectionFilter::Auto,
                &ids(&["model."])
            ),
            "a model in scope and a check about models is a real verdict"
        );
    }

    /// A check that reported nothing project-wide had nothing for the scope to
    /// drop, so its zero rows are a finding, not a gap.
    #[test]
    fn a_check_that_reported_nothing_anywhere_passes() {
        assert!(!zero_rows_is_vacuous(
            Some(&ids(&["model.p.a"])),
            &SelectionFilter::Auto,
            &BTreeSet::new()
        ));
    }

    /// A check that opted out of scoping was never narrowed, so its zero rows
    /// always mean the whole project is clean.
    #[test]
    fn opting_out_of_scoping_is_never_vacuous() {
        assert!(!zero_rows_is_vacuous(
            Some(&BTreeSet::new()),
            &SelectionFilter::None,
            &ids(&["model."])
        ));
    }

    #[test]
    fn kind_of_splits_at_the_first_separator() {
        assert_eq!(kind_of("model.pkg.a"), "model.");
        assert_eq!(kind_of("bare"), "bare");
    }
}
