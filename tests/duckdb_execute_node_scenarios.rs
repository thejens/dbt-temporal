//! Additional `execute_node` error paths not covered by the main scenario
//! matrix in `duckdb_scenarios.rs`: empty raw SQL, an unresolvable
//! materialization name, and the per-workflow env-override rebuild failure
//! as it propagates through `execute_node_inner` (as opposed to calling
//! `rebuild_adapter_engines_with_env` directly, covered in
//! `duckdb_profile_scenarios.rs`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::large_futures,
    unsafe_code
)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;
use dbt_temporal::error::DbtTemporalError;

#[tokio::test]
async fn empty_model_sql_is_a_compilation_error() {
    let harness = Harness::build(&[("blank", "")]).await;
    let err = harness.run_err("blank").await;
    let rendered = format!("{err:#}");
    assert!(rendered.contains("empty"), "got: {rendered}");
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

#[tokio::test]
async fn whitespace_only_model_sql_is_a_compilation_error() {
    // Same dispatch as a truly empty file — `raw_sql.trim().is_empty()`.
    let harness = Harness::build(&[("blank", "   \n\t\n  ")]).await;
    let err = harness.run_err("blank").await;
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

#[tokio::test]
async fn unresolvable_materialization_name_is_a_compilation_error() {
    let harness = Harness::build(&[(
        "bogus",
        "{{ config(materialized='not_a_real_materialization_xyz') }}\nselect 1 as id",
    )])
    .await;
    let err = harness.run_err("bogus").await;
    let rendered = format!("{err:#}");
    assert!(rendered.contains("no materialization found"), "got: {rendered}");
    assert!(
        matches!(err, DbtTemporalError::Compilation(_)),
        "expected Compilation, got: {err:?}"
    );
}

/// An override of a variable the profile never reads changes nothing about the
/// connection, so it must not cost a profile re-render and a fresh engine — the
/// cost every run used to pay, because `build_effective_env` puts the
/// serialized workflow input in `_` on every one of them.
#[tokio::test]
async fn an_override_the_profile_does_not_read_reuses_the_startup_engine() {
    let profile_yml = "spike:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      \
                        path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_EXEC_SCHEMA') }}\"\n      \
                        threads: 1\n";
    let guard = EnvVarGuard::set("DBTT_EXEC_SCHEMA", "main");
    let harness =
        Harness::build_files_with_profile(&[("models/m.sql", "select 1 as id")], profile_yml).await;
    // Removed, so any rebuild would fail on the missing required variable.
    guard.remove();

    let mut env = BTreeMap::new();
    env.insert("UNRELATED".to_string(), "value".to_string());
    env.insert("_".to_string(), "{\"command\":\"run\"}".to_string());

    harness
        .run_uid_with_env("model.spike.m", &env)
        .await
        .expect("an unread override must not trigger a rebuild");
}

#[tokio::test]
async fn env_override_rebuild_failure_inside_execute_node_is_a_configuration_error() {
    // Mirrors duckdb_profile_scenarios.rs's direct rebuild_adapter_engine_with_env
    // test, but through the real execute_node_inner integration: a missing
    // required env_var during the per-workflow rebuild is wrapped as
    // Configuration (not Compilation/Adapter) at the execute_node call site.
    //
    // The override names `DBTT_EXEC_THREADS`, which this profile reads — that is
    // what asks for a rebuild. An override of some variable the profile never
    // mentions would not, and must not.
    let profile_yml = "spike:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      \
                        path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_EXEC_SCHEMA') }}\"\n      \
                        threads: \"{{ env_var('DBTT_EXEC_THREADS', '1') }}\"\n";
    let guard = EnvVarGuard::set("DBTT_EXEC_SCHEMA", "startup-value");
    let harness =
        Harness::build_files_with_profile(&[("models/m.sql", "select 1 as id")], profile_yml).await;
    guard.remove();

    let mut env = BTreeMap::new();
    env.insert("DBTT_EXEC_THREADS".to_string(), "2".to_string());
    let err = harness.run_err_uid_with_env("model.spike.m", &env).await;
    assert!(
        matches!(err, DbtTemporalError::Configuration(_)),
        "expected Configuration, got: {err:?}"
    );
    assert!(format!("{err:#}").contains("DBTT_EXEC_SCHEMA"), "got: {err:#}");
}

/// RAII guard restoring a process env var to its prior state on drop (even on
/// panic) — see `duckdb_profile_scenarios.rs` for the full rationale (dbt-fusion's
/// startup profile loader reads `env_var()` straight from the process env).
struct EnvVarGuard {
    key: &'static str,
    prior: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prior = std::env::var(key).ok();
        // SAFETY: test-only, single-threaded (--test-threads=1), unique var name.
        unsafe { std::env::set_var(key, value) };
        Self { key, prior }
    }

    fn remove(&self) {
        // SAFETY: test-only, single-threaded (--test-threads=1).
        unsafe { std::env::remove_var(self.key) };
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: test-only, single-threaded (--test-threads=1).
        unsafe {
            match &self.prior {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
