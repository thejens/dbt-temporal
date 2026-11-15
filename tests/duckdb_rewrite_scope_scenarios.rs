//! Which upstream relations a node's per-workflow naming resolves.
//!
//! Its own test binary: dbt's macro registry is process-global, so a project
//! that defines `generate_schema_name` poisons every project loaded after it
//! in the same process.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;

const PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

/// `root` fails to resolve under the workflow's schema, so a run that touches
/// it fails; `raw` moves to a schema of its own, so a reader of it must be
/// rewritten to find it. At parse time (schema `main`) both resolve normally.
const MACRO: &str = "{% macro generate_schema_name(custom_schema_name, node) %}\
    {%- if node.name == 'root' and target.schema == 'workflow42' -%}\
        {{ exceptions.raise_compiler_error('root must not be resolved') }}\
    {%- elif node.name == 'raw' -%}\
        raw_{{ target.schema }}\
    {%- else -%}\
        {{ target.schema }}\
    {%- endif -%}\
{% endmacro %}";

const FILES: &[(&str, &str)] = &[
    ("macros/generate_schema_name.sql", MACRO),
    ("models/root.sql", "{{ config(materialized='table') }}\nselect 1 as id"),
    (
        "models/mid1.sql",
        "{{ config(materialized='table') }}\nselect id from {{ ref('root') }}",
    ),
    (
        "models/mid2.sql",
        "{{ config(materialized='table') }}\nselect id from {{ ref('mid1') }}",
    ),
    (
        "models/tail.sql",
        "{{ config(materialized='table') }}\nselect id + 1 as id from {{ ref('mid2') }}",
    ),
    ("models/raw.sql", "{{ config(materialized='table') }}\nselect 10 as id"),
    (
        "models/eph.sql",
        "{{ config(materialized='ephemeral') }}\nselect id * 2 as id from {{ ref('raw') }}",
    ),
    (
        "models/final.sql",
        "{{ config(materialized='table') }}\nselect id + 1 as id from {{ ref('eph') }}",
    ),
];

fn workflow_env() -> BTreeMap<String, String> {
    BTreeMap::from([("DBTT_SCHEMA".to_string(), "workflow42".to_string())])
}

/// Only a node's direct dependencies can appear in its SQL, so running it must
/// not resolve the ancestors further up: here `root`, which fails to resolve
/// under the workflow's schema, is three hops from the node that runs.
#[tokio::test]
async fn a_node_resolves_its_direct_dependencies_and_no_further() {
    let harness = Harness::build_files_with_profile(FILES, PROFILE).await;
    harness
        .exec("create schema workflow42; create table workflow42.mid2 as select 7 as id")
        .await;

    let result = harness
        .run_uid_with_env("model.spike.tail", &workflow_env())
        .await
        .unwrap_or_else(|e| panic!("tail should not resolve root: {e:#}"));
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success);
    // `mid2` exists only under the workflow's schema, so the statement ran
    // against the rewritten relation.
    assert_eq!(result.relation_name.as_deref(), Some("spike.workflow42.tail"));
}

/// An ephemeral model is inlined into its consumer, so the relations *it*
/// reads are in the consumer's SQL even though the consumer never `ref()`s
/// them.
#[tokio::test]
async fn the_dependencies_of_an_inlined_ephemeral_model_are_resolved() {
    let harness = Harness::build_files_with_profile(FILES, PROFILE).await;
    let env = workflow_env();
    harness
        .run_uid_with_env("model.spike.raw", &env)
        .await
        .unwrap_or_else(|e| panic!("raw should succeed: {e:#}"));
    let result = harness
        .run_uid_with_env("model.spike.final", &env)
        .await
        .unwrap_or_else(|e| panic!("final should succeed: {e:#}"));
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success);

    assert_eq!(
        harness
            .query_scalar("select id from workflow42.final")
            .await,
        "21"
    );
}
