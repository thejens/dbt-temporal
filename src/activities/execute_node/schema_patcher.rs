//! Detect a pattern that breaks per-workflow env_var() overrides:
//! `config(schema=env_var(...))` or `config(database=env_var(...))`
//! inside a node's compiled SQL.
//!
//! The config block is evaluated at resolver time (worker startup) and
//! baked into node metadata. Per-workflow env changes have no effect on
//! it, so the workflow would write into a stale schema/database. Callers
//! reject the workflow when this returns true.

/// True if the SQL contains `config(schema=env_var(...))` or
/// `config(database=env_var(...))` inside a Jinja `{{ ... }}` block.
pub fn has_env_var_in_config_schema_or_database(sql: &str) -> bool {
    // The `[^}]*` segments keep the regex from spanning multiple Jinja
    // blocks, e.g. matching across `}}` … `{{ env_var(...) }}` later in
    // the file.
    #[allow(clippy::expect_used)]
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?si)\{\{[^}]*config\s*\([^}]*(schema|database)\s*=\s*env_var\s*\(")
            .expect("env_var config detection regex")
    });
    if !sql.contains("env_var(") || !sql.contains("config(") {
        return false;
    }
    RE.is_match(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_schema_env_var_inline() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{ config(schema=env_var('MY_SCHEMA')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn detects_database_env_var_inline() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{ config(database=env_var('MY_DB')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn detects_env_var_with_other_kwargs() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{ config(materialized='table', schema=env_var('MY_SCHEMA')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn detects_env_var_with_default() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{ config(schema=env_var('MY_SCHEMA', 'fallback')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn detects_multiline_config() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{\n  config(\n    materialized='table',\n    schema = env_var('MY_SCHEMA')\n  )\n}}\nSELECT 1"
        ));
    }

    #[test]
    fn detects_spaces_around_equals() {
        assert!(has_env_var_in_config_schema_or_database(
            "{{ config(schema = env_var('S')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn no_false_positive_static_schema() {
        assert!(!has_env_var_in_config_schema_or_database(
            "{{ config(schema='staging') }}\nSELECT 1"
        ));
    }

    #[test]
    fn no_false_positive_env_var_in_sql_body() {
        // env_var() in model SQL (not in config) is fine — it's re-rendered per-workflow.
        assert!(!has_env_var_in_config_schema_or_database(
            "{{ config(materialized='table') }}\nSELECT '{{ env_var(\"MY_VAR\") }}' as val"
        ));
    }

    #[test]
    fn no_false_positive_env_var_in_different_config_kwarg() {
        // env_var() in config(materialized=...) is not a schema/database concern.
        assert!(!has_env_var_in_config_schema_or_database(
            "{{ config(materialized=env_var('MAT_TYPE', 'table')) }}\nSELECT 1"
        ));
    }

    #[test]
    fn no_false_positive_no_config() {
        assert!(!has_env_var_in_config_schema_or_database(
            "SELECT * FROM {{ ref('stg_customers') }} WHERE schema = env_var('X')"
        ));
    }

    #[test]
    fn no_false_positive_plain_sql() {
        assert!(!has_env_var_in_config_schema_or_database("SELECT 1 FROM my_table"));
    }

    #[test]
    fn no_false_positive_config_without_env_var() {
        assert!(!has_env_var_in_config_schema_or_database(
            "{{ config(materialized='view', schema='analytics') }}\nSELECT 1"
        ));
    }

    #[test]
    fn no_false_positive_env_var_outside_config_block() {
        // env_var used in a separate Jinja block, schema in config — should NOT match.
        assert!(!has_env_var_in_config_schema_or_database(
            "{{ config(schema='staging') }}\n{% set x = env_var('FOO') %}\nSELECT 1"
        ));
    }
}
