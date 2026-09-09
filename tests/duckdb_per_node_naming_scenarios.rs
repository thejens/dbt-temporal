//! Naming macros evaluated per node.
//!
//! Its own test binary: dbt's macro registry is process-global, so a project
//! that defines `generate_schema_name` poisons every project loaded after it
//! in the same process — including another project's `generate_schema_name`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;

/// Schema driven by `env_var()`, so a per-workflow override rebuilds the
/// profile and the macro sees the workflow's `target.schema`.
const PER_NODE_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

/// A macro that branches on the node sends two models out of the same startup
/// schema to different ones. A schema-to-schema map could only carry one of
/// the two answers, so whichever model lost got its `ref()`s pointed at a
/// relation the run never wrote.
const PER_NODE_MACRO: &str = "{% macro generate_schema_name(custom_schema_name, node) %}\
    {%- if 'marts' in node.tags -%}\
        mart_{{ target.schema }}\
    {%- else -%}\
        {{ target.schema }}\
    {%- endif -%}\
{% endmacro %}";

const PER_NODE_FILES: &[(&str, &str)] = &[
    ("macros/generate_schema_name.sql", PER_NODE_MACRO),
    ("models/plain.sql", "{{ config(materialized='table') }}\nselect 1 as id"),
    (
        "models/marty.sql",
        "{{ config(materialized='table', tags=['marts']) }}\nselect 2 as id",
    ),
    (
        "models/joined.sql",
        "{{ config(materialized='table') }}\n\
         select (select id from {{ ref('plain') }}) + (select id from {{ ref('marty') }}) as id",
    ),
];

#[tokio::test]
async fn the_naming_macro_is_evaluated_against_each_node() {
    let harness = Harness::build_files_with_profile(PER_NODE_FILES, PER_NODE_PROFILE).await;

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    for model in ["plain", "marty", "joined"] {
        let result = harness
            .run_uid_with_env(&format!("model.spike.{model}"), &env)
            .await
            .unwrap_or_else(|e| panic!("{model} should succeed: {e:#}"));
        assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{model}");
    }

    // The tag decided where each one went…
    assert_eq!(harness.query_scalar("select id from workflow42.plain"), "1");
    assert_eq!(harness.query_scalar("select id from mart_workflow42.marty"), "2");
    // …and the downstream's compiled `ref()`s followed both.
    assert_eq!(harness.query_scalar("select id from workflow42.joined"), "3");
}
