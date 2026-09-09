//! The **default** `generate_schema_name` path: schemas reconstructed from
//! dbt's `<target_schema>[_<custom>]` pattern, and the relations that pattern
//! leaves baked into already-compiled SQL.
//!
//! Its own test binary on purpose. dbt's macro registry is process-global, so
//! a project that defines `generate_schema_name` poisons every project loaded
//! after it in the same process — the sibling
//! `duckdb_schema_rewrite_scenarios` suite does exactly that, and a
//! default-macro project loaded behind it fails to resolve at all.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;

/// Default `generate_schema_name`, so the reconstruction path — with a model
/// configured into `<target>_marts`. Its downstream's compiled SQL names that
/// suffixed schema, which the single-token substitution never rewrote: the
/// node materialized into the per-workflow schema and read from the startup
/// one.
const DEFAULT_MACRO_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

const SUFFIXED_SCHEMA_FILES: &[(&str, &str)] = &[
    (
        "models/upstream.sql",
        "{{ config(schema='marts', materialized='table') }}\nselect 7 as id",
    ),
    (
        "models/downstream.sql",
        "{{ config(materialized='table') }}\nselect * from {{ ref('upstream') }}",
    ),
];

#[tokio::test]
async fn default_macro_rewrites_a_custom_suffixed_upstream_schema() {
    let harness =
        Harness::build_files_with_profile(SUFFIXED_SCHEMA_FILES, DEFAULT_MACRO_PROFILE).await;
    assert!(
        !harness.state().has_custom_schema_name_macro,
        "project defines no generate_schema_name — should use the default-pattern path"
    );

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    // Materializes into workflow42_marts, not the startup <schema>_marts.
    let upstream = harness
        .run_uid_with_env("model.spike.upstream", &env)
        .await
        .unwrap();
    assert_eq!(upstream.status, dbt_temporal::types::NodeStatus::Success);

    // Its ref() must resolve to workflow42_marts too — the schema the previous
    // run actually wrote.
    let downstream = harness
        .run_uid_with_env("model.spike.downstream", &env)
        .await
        .unwrap();
    assert_eq!(downstream.status, dbt_temporal::types::NodeStatus::Success);
    assert_eq!(harness.query_scalar("select id from workflow42.downstream"), "7");
}

/// A source names a table dbt did not create. Rewriting its schema alongside
/// the models' — which is what happens whenever a source shares the profile
/// schema, the ordinary case in a dev project — points `source()` at a
/// relation that was never there.
const SOURCE_FILES: &[(&str, &str)] = &[
    (
        "models/sources.yml",
        "version: 2\nsources:\n  - name: landing\n    schema: main\n    tables:\n      - name: events\n",
    ),
    (
        "models/from_source.sql",
        "{{ config(materialized='table') }}\nselect * from {{ source('landing', 'events') }}",
    ),
];

#[tokio::test]
async fn a_source_keeps_its_declared_schema_under_an_env_override() {
    let harness = Harness::build_files_with_profile(SOURCE_FILES, DEFAULT_MACRO_PROFILE).await;

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    // The source table is deliberately absent, so the run fails naming the
    // relation it looked for — which is the assertion. Seeding it first is not
    // an option here: opening the DuckDB file on the startup engine stops the
    // per-workflow rebuilt engine from seeing its own writes.
    let err = harness
        .run_err_uid_with_env("model.spike.from_source", &env)
        .await;
    let message = format!("{err:#}");
    assert!(
        message.contains(r#""main"."events""#),
        "the source keeps its declared schema: {message}"
    );
    assert!(
        !message.contains("workflow42\".\"events"),
        "the source must not be moved with the models: {message}"
    );
}
