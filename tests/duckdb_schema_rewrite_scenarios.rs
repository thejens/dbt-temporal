//! Custom `generate_schema_name` macro re-execution (`build_schema_rewrite_map` /
//! `apply_schema_map_to_context` / `patch_sql_with_schema_map` in
//! `src/activities/execute_node/schema_patch.rs`), exercised end-to-end through
//! `execute_node_inner` against a real DuckDB project.
//!
//! The pure helper functions have direct unit tests in `schema_patch.rs`
//! itself; these scenarios cover the integration this repo's own workaround
//! actually needs — a project whose profile uses `env_var()` (triggering the
//! per-workflow adapter rebuild) *and* overrides `generate_schema_name` with a
//! custom macro (triggering the schema-rewrite-map path instead of the
//! default-pattern one).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;

/// Schema driven by `env_var()` (so `profile_uses_env_vars` is true) and a
/// `generate_schema_name` macro that prefixes with `custom_` and appends any
/// model-level `custom_schema_name` — deliberately different from dbt's
/// default `<target_schema>[_<custom>]` pattern, so a correct implementation
/// must re-execute this exact macro rather than guess at the pattern.
const CUSTOM_MACRO_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

const CUSTOM_SCHEMA_NAME_MACRO: &str = "{% macro generate_schema_name(custom_schema_name, node) %}\
    {%- if custom_schema_name is none -%}\
        custom_{{ target.schema }}\
    {%- else -%}\
        custom_{{ target.schema }}_{{ custom_schema_name }}\
    {%- endif -%}\
{% endmacro %}";

const CUSTOM_MACRO_FILES: &[(&str, &str)] = &[
    ("macros/generate_schema_name.sql", CUSTOM_SCHEMA_NAME_MACRO),
    ("models/plain.sql", "select 1 as id"),
    ("models/overridden.sql", "{{ config(schema='marts') }}\nselect 2 as id"),
];

#[tokio::test]
async fn custom_generate_schema_name_macro_is_re_executed_per_workflow() {
    let harness = Harness::build_files_with_profile(CUSTOM_MACRO_FILES, CUSTOM_MACRO_PROFILE).await;
    assert!(
        harness.state().has_custom_schema_name_macro,
        "project defines its own generate_schema_name — should be detected as custom"
    );

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    // No config(schema=...) override: generate_schema_name(none, node) with
    // the per-workflow target.schema — the custom macro emits "custom_workflow42".
    let result = harness
        .run_uid_with_env("model.spike.plain", &env)
        .await
        .unwrap();
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success);
}

#[tokio::test]
async fn custom_generate_schema_name_macro_honours_per_model_schema_override() {
    let harness = Harness::build_files_with_profile(CUSTOM_MACRO_FILES, CUSTOM_MACRO_PROFILE).await;

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    // config(schema='marts'): generate_schema_name('marts', node) — the custom
    // macro emits "custom_workflow42_marts", not the default "workflow42_marts".
    let result = harness
        .run_uid_with_env("model.spike.overridden", &env)
        .await
        .unwrap();
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success);
}

#[tokio::test]
async fn custom_macro_schema_rewrite_is_a_no_op_without_env_override() {
    // No per-workflow env at all: execute_node_inner's rebuild guard
    // (`!input.env.is_empty()`) never fires, so neither schema-rewrite path
    // runs — the node materializes using the profile's own default() schema.
    let harness = Harness::build_files_with_profile(CUSTOM_MACRO_FILES, CUSTOM_MACRO_PROFILE).await;
    let result = harness.run("plain").await.unwrap();
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success);
}
