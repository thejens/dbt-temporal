//! What a run says when its selection matches nothing.
//!
//! dbt treats an empty selection as a warning, not an error: the command ends
//! with exit code 0 after `NoNodesSelected` ("Nothing to do"), so slim CI with
//! `state:modified+` on an unchanged project, or `seed` on a project without
//! seeds, is a green run. The wording here follows `dbt-main`'s
//! `compilation.rs` (selection criterion, then "Nothing to do") and
//! `dbt-freshness`'s `nothing_to_do_message` for the freshness commands.

use std::collections::BTreeSet;

use crate::types::{DbtCommand, DbtRunInput};

/// Keep the selected nodes a previous run left unfinished.
///
/// `retry_ids` is what [`super::retry::parse_retry_node_ids`] read; nodes the
/// previous run finished well are dropped, as `dbt retry` drops them.
pub(super) fn narrow_to_retry(
    selected_ids: Vec<String>,
    retry_ids: &BTreeSet<String>,
) -> Vec<String> {
    selected_ids
        .into_iter()
        .filter(|id| retry_ids.contains(id))
        .collect()
}

/// The warnings for a plan holding `selected` nodes, of which
/// `selected_before_retry` were selected ahead of any `retry_from` narrowing.
///
/// Empty when anything is selected. The cause matters to the reader: a
/// selector that matched nothing sends them to the selector, while a retry that
/// found nothing unfinished means the previous run was already complete.
pub(super) fn nothing_selected_warnings(
    input: &DbtRunInput,
    selected_before_retry: usize,
    selected: usize,
) -> Vec<String> {
    if selected > 0 {
        return Vec::new();
    }
    let mut warnings = Vec::with_capacity(2);
    if input.retry_from.is_some() && selected_before_retry > 0 {
        warnings.push(
            "retry_from: no selected node errored, failed or was skipped in the previous run"
                .to_string(),
        );
    } else if let Some(select) = input.select.as_deref().filter(|s| !s.trim().is_empty()) {
        warnings
            .push(format!("The selection criterion '{select}' does not match any enabled nodes"));
    }
    warnings.push(nothing_to_do(DbtCommand::parse(&input.command)).to_string());
    warnings
}

/// Whether the run's project hooks are skipped because nothing was selected.
///
/// dbt ends these commands before any hook when the schedule is empty; see
/// [`DbtCommand::skips_hooks_when_nothing_selected`].
pub(super) fn hooks_skipped(command: &str, selected: usize) -> bool {
    selected == 0
        && DbtCommand::parse(command).is_some_and(DbtCommand::skips_hooks_when_nothing_selected)
}

const fn nothing_to_do(command: Option<DbtCommand>) -> &'static str {
    match command {
        Some(DbtCommand::SourceFreshness) => "Nothing to do. No sources with freshness config.",
        Some(DbtCommand::Freshness) => {
            "Nothing to do. No sources or models with a freshness config."
        }
        _ => "Nothing to do. Try checking your model configs and model specification args",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn input(command: &str, select: Option<&str>, retry_from: Option<&str>) -> DbtRunInput {
        serde_json::from_value(serde_json::json!({
            "command": command,
            "select": select,
            "retry_from": retry_from,
        }))
        .expect("valid input")
    }

    const NOTHING: &str =
        "Nothing to do. Try checking your model configs and model specification args";

    #[test]
    fn a_plan_with_nodes_has_no_warnings() {
        assert_eq!(
            nothing_selected_warnings(&input("build", Some("m"), None), 3, 1),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_selector_that_matched_nothing_is_quoted_before_nothing_to_do() {
        assert_eq!(
            nothing_selected_warnings(&input("build", Some("state:modified+"), None), 0, 0),
            vec![
                "The selection criterion 'state:modified+' does not match any enabled nodes"
                    .to_string(),
                NOTHING.to_string(),
            ]
        );
    }

    #[test]
    fn an_empty_selection_without_a_selector_only_says_nothing_to_do() {
        assert_eq!(
            nothing_selected_warnings(&input("seed", None, None), 0, 0),
            vec![NOTHING.to_string()]
        );
        // A blank selector is no selector.
        assert_eq!(
            nothing_selected_warnings(&input("seed", Some("  "), None), 0, 0),
            vec![NOTHING.to_string()]
        );
    }

    #[test]
    fn a_retry_with_nothing_unfinished_names_the_retry_not_the_selector() {
        assert_eq!(
            nothing_selected_warnings(&input("build", Some("tag:daily"), Some("prev.json")), 4, 0),
            vec![
                "retry_from: no selected node errored, failed or was skipped in the previous run"
                    .to_string(),
                NOTHING.to_string(),
            ]
        );
    }

    #[test]
    fn a_retry_whose_selection_was_already_empty_blames_the_selector() {
        assert_eq!(
            nothing_selected_warnings(&input("build", Some("nope"), Some("prev.json")), 0, 0),
            vec![
                "The selection criterion 'nope' does not match any enabled nodes".to_string(),
                NOTHING.to_string(),
            ]
        );
    }

    #[test]
    fn freshness_commands_keep_their_own_wording() {
        assert_eq!(
            nothing_selected_warnings(&input("source-freshness", None, None), 0, 0),
            vec!["Nothing to do. No sources with freshness config.".to_string()]
        );
        assert_eq!(
            nothing_selected_warnings(&input("freshness", None, None), 0, 0),
            vec!["Nothing to do. No sources or models with a freshness config.".to_string()]
        );
    }

    #[test]
    fn hooks_are_skipped_only_for_commands_dbt_short_circuits() {
        for command in ["run", "test", "build", "seed", "snapshot"] {
            assert!(hooks_skipped(command, 0), "{command}");
            assert!(!hooks_skipped(command, 1), "{command}");
        }
        for command in [
            "compile",
            "list",
            "source-freshness",
            "freshness",
            "unknown",
        ] {
            assert!(!hooks_skipped(command, 0), "{command}");
        }
    }

    #[test]
    fn narrow_to_retry_keeps_only_unfinished_nodes() {
        let retry: BTreeSet<String> = ["model.p.b".to_string()].into();
        let ids = vec!["model.p.a".to_string(), "model.p.b".to_string()];
        assert_eq!(narrow_to_retry(ids.clone(), &retry), vec!["model.p.b".to_string()]);
        assert_eq!(narrow_to_retry(ids, &BTreeSet::new()), Vec::<String>::new());
    }
}
