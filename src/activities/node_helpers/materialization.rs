//! Finding and rendering a materialization template.

use std::collections::BTreeMap;

use crate::error::DbtTemporalError;

/// Find a template named `<package>.<suffix>` in the Jinja environment.
///
/// Templates are registered with package prefixes (e.g.
/// "dbt_postgres.materialization_view_postgres"). The first match in the environment's
/// iteration order wins, so callers that need package precedence must try the package they
/// prefer explicitly first.
///
/// Used by the schema-naming macro lookup and the `debug_render` example. Resolving a node's
/// materialization goes through `MaterializationResolver::find_materialization_macro_by_name()`
/// instead, which applies adapter inheritance and package precedence.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::test_support::jinja_env_with_templates;
    use super::*;

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
