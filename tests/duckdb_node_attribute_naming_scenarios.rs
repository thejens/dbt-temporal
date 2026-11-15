//! The naming macro's `node` argument under a per-workflow override.
//!
//! Its own test binary: dbt's macro registry is process-global, so two
//! projects defining different `generate_schema_name` macros in one process
//! would run each other's.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;

/// Schema driven by `env_var()`, so a per-workflow override rebuilds the
/// profile and the macro sees the workflow's `target.schema`.
const OVERRIDE_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

/// Same shape of profile, with the workflow's schema written into it: what a
/// fresh parse under the override's environment sees.
const FRESH_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: workflow42\n      threads: 1\n";

/// Every node attribute a naming macro commonly branches on, one branch each.
/// The `custom_schema_name` branch comes last so each earlier branch is
/// reachable only through the node.
const NODE_ATTRIBUTE_MACRO: &str = "{% macro generate_schema_name(custom_schema_name, node) %}\
    {%- if node.resource_type == 'seed' -%}\
        {{ target.schema }}_seeds\
    {%- elif node.fqn[1] == 'staging' -%}\
        {{ target.schema }}_stg\
    {%- elif node.path.startswith('audit/') -%}\
        {{ target.schema }}_audit\
    {%- elif node.config.materialized == 'view' -%}\
        {{ target.schema }}_views\
    {%- elif node.name == 'special' -%}\
        {{ node.schema }}_special\
    {%- elif custom_schema_name is not none -%}\
        {{ target.schema }}_{{ custom_schema_name | trim }}\
    {%- else -%}\
        {{ target.schema }}\
    {%- endif -%}\
{% endmacro %}";

const NODE_ATTRIBUTE_FILES: &[(&str, &str)] = &[
    ("macros/generate_schema_name.sql", NODE_ATTRIBUTE_MACRO),
    ("seeds/lookup.csv", "id\n1\n"),
    ("models/staging/stg_a.sql", "{{ config(materialized='table') }}\nselect 1 as id"),
    ("models/audit/audited.sql", "{{ config(materialized='table') }}\nselect 2 as id"),
    ("models/viewed.sql", "{{ config(materialized='view') }}\nselect 3 as id"),
    ("models/special.sql", "{{ config(materialized='table') }}\nselect 4 as id"),
    (
        "models/customised.sql",
        "{{ config(materialized='table', schema='finance') }}\nselect 5 as id",
    ),
    ("models/plain.sql", "{{ config(materialized='table') }}\nselect 6 as id"),
];

/// Under an env override the macro must see what it sees at parse time: the
/// node's resource type, fqn, path, config and name. Where each relation lands
/// has to equal where a fresh parse under that environment puts it.
#[tokio::test]
async fn the_override_path_names_relations_as_a_fresh_parse_would() {
    let overridden =
        Harness::build_files_with_profile(NODE_ATTRIBUTE_FILES, OVERRIDE_PROFILE).await;
    let fresh = Harness::build_files_with_profile(NODE_ATTRIBUTE_FILES, FRESH_PROFILE).await;

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "workflow42".to_string());

    let ids = [
        "model.spike.stg_a",
        "model.spike.audited",
        "model.spike.viewed",
        "model.spike.special",
        "model.spike.customised",
        "model.spike.plain",
        "seed.spike.lookup",
    ];
    for id in ids {
        let result = overridden
            .run_uid_with_env(id, &env)
            .await
            .unwrap_or_else(|e| panic!("{id} should succeed: {e:#}"));
        assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{id}");
    }

    // Read only after every node has run: the harness's query connection keeps
    // the catalog it first saw.
    for id in ids {
        let (schema, alias) = fresh.node_relation(id);
        let landed = overridden
            .query_scalar(&format!(
                "select count(*) from information_schema.tables \
                 where table_schema = '{schema}' and table_name = '{alias}'"
            ))
            .await;
        assert_eq!(landed, "1", "{id} should land in {schema}.{alias}, as a fresh parse names it");
    }
}
