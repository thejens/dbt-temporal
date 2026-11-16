//! Workflow `--vars` that the parse-time project cannot honour.

use std::collections::BTreeMap;

use tracing::warn;

use crate::worker_state::WorkerState;

/// Warn when a workflow's `--vars` are read at parse time, where they cannot
/// take effect.
///
/// The worker parses each project once at startup, so `var()` calls evaluated
/// during parsing — `{{ config(...) }}` blocks, `dbt_project.yml` — are already
/// baked in by the time a workflow runs. Render-time `var()` in model bodies,
/// macros and hooks *is* overridden (see `render_env`). Naming the affected
/// nodes is more useful than a blanket "vars may not work": it points at the
/// exact models whose config would silently keep the startup value.
pub(super) fn warn_parse_time_vars(
    state: &WorkerState,
    vars: &BTreeMap<String, serde_json::Value>,
) {
    let affected = nodes_reading_vars_in_config(state, vars);
    if !affected.is_empty() {
        warn!(
            nodes = ?affected,
            "workflow vars are referenced inside config() blocks, which are evaluated \
             when the worker parses the project — those uses keep their startup value. \
             Only render-time var() calls reflect the workflow's vars"
        );
    }
}

/// The nodes whose `config()` block reads one of `vars`, capped at 20: enough
/// to name the pattern in a warning without flooding it on a large project.
pub fn nodes_reading_vars_in_config(
    state: &WorkerState,
    vars: &BTreeMap<String, serde_json::Value>,
) -> Vec<String> {
    if vars.is_empty() {
        return Vec::new();
    }
    state
        .resolver_state
        .nodes
        .iter()
        .filter(|(_, node)| {
            node.common()
                .raw_code
                .as_deref()
                .is_some_and(|code| config_block_reads_var(code, vars.keys()))
        })
        .map(|(id, _)| id.clone())
        .take(20)
        .collect()
}

/// True if `raw_code` has a `config(...)` block mentioning `var('<name>')` for
/// any of the given names.
///
/// Deliberately a substring scan rather than a Jinja parse: a false positive
/// costs one warning line, and parsing every node's template at plan time to
/// remove it would not be worth the cost.
fn config_block_reads_var<'a>(raw_code: &str, var_names: impl Iterator<Item = &'a String>) -> bool {
    let Some(config_start) = raw_code.find("config(") else {
        return false;
    };
    let Some(config_len) = config_call_len(&raw_code[config_start..]) else {
        return false;
    };
    let block = &raw_code[config_start..config_start + config_len];
    var_names.into_iter().any(|name| {
        block.contains(&format!("var('{name}'")) || block.contains(&format!("var(\"{name}\""))
    })
}

/// Byte length of the call starting at `call` (which begins with `config(`),
/// up to its matching `)`, or `None` when it never closes.
///
/// Parentheses nest (`config(alias=upper(var('a')), ...)`), and one inside a
/// string literal belongs to the string, so the scan balances them outside
/// quotes. A bare `find(')')` would end the block at the first inner call.
fn config_call_len(call: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (idx, c) in call.char_indices() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (None, '\'' | '"') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_block_reads_var_detects_both_quote_styles() {
        let names = ["region".to_string()];
        assert!(config_block_reads_var(
            "{{ config(schema=var('region')) }}\nselect 1",
            names.iter()
        ));
        assert!(config_block_reads_var(
            "{{ config(schema=var(\"region\")) }}\nselect 1",
            names.iter()
        ));
    }

    #[test]
    fn config_block_reads_var_sees_past_a_nested_call() {
        let names = ["region".to_string()];
        assert!(config_block_reads_var(
            "{{ config(alias=upper('a'), schema=var('region')) }}\nselect 1",
            names.iter()
        ));
    }

    #[test]
    fn config_block_reads_var_ignores_parens_inside_strings() {
        let names = ["region".to_string()];
        assert!(config_block_reads_var(
            "{{ config(post_hook=\"select ')'\", schema=var('region')) }}",
            names.iter()
        ));
    }

    #[test]
    fn config_block_reads_var_stops_at_the_matching_paren() {
        let names = ["region".to_string()];
        assert!(!config_block_reads_var(
            "{{ config(alias=upper('a')) }}\nselect var('region')",
            names.iter()
        ));
    }

    #[test]
    fn config_block_reads_var_ignores_vars_outside_the_config_block() {
        // The warning exists for config() blocks specifically: those are
        // evaluated at parse time and keep the worker's startup value. A var
        // in the model body is rendered per run and needs no warning.
        let names = ["region".to_string()];
        assert!(!config_block_reads_var(
            "{{ config(materialized='table') }}\nselect '{{ var('region') }}' as r",
            names.iter()
        ));
    }

    #[test]
    fn config_block_reads_var_handles_absent_or_unterminated_config() {
        let names = ["region".to_string()];
        assert!(!config_block_reads_var("select 1", names.iter()));
        // Unterminated `config(` — no closing paren anywhere to bound the
        // block, so there is nothing to scan.
        assert!(!config_block_reads_var("{{ config(schema=var('region'", names.iter()));
    }

    #[test]
    fn config_block_reads_var_is_false_for_unrelated_var_names() {
        let names = ["other".to_string()];
        assert!(!config_block_reads_var("{{ config(schema=var('region')) }}", names.iter()));
    }
}
