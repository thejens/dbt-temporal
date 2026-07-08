//! Per-workflow env-override adapter rebuild (`worker::profile::rebuild_adapter_engine_with_env`),
//! exercised directly against a real `WorkerState` built by the DuckDB harness.
//!
//! `render_profile_with_env` (the YAML-rendering half) already has extensive
//! unit tests in `src/worker/profile.rs`; these scenarios cover the half that
//! needs a real, loaded project: building a fresh `AdapterEngine` from the
//! re-rendered config, and the real `RebuildResult` `Debug` impl.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::large_futures,
    unsafe_code
)]

mod common;

use std::collections::BTreeMap;

use common::duckdb::Harness;
use dbt_temporal::worker::profile::rebuild_adapter_engine_with_env;

/// A profile whose `schema` is driven by `env_var(...)`, so `profile_uses_env_vars`
/// is true and a per-workflow `env` override triggers a real adapter rebuild.
const ENV_VAR_SCHEMA_MODEL: &[(&str, &str)] = &[("m", "select 1 as id")];

const ENV_VAR_SCHEMA_PROFILE: &str = "spike:\n  target: dev\n  outputs:\n    dev:\n      \
     type: duckdb\n      path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_SCHEMA', 'main') }}\"\n      \
     threads: 1\n";

#[tokio::test]
async fn rebuild_adapter_engine_with_env_override_picks_up_the_new_schema() {
    let harness =
        Harness::build_files_with_profile(ENV_VAR_SCHEMA_MODEL, ENV_VAR_SCHEMA_PROFILE).await;

    let mut env = BTreeMap::new();
    env.insert("DBTT_SCHEMA".to_string(), "custom_schema".to_string());

    let rebuild = rebuild_adapter_engine_with_env(harness.state(), None, &env)
        .expect("rebuild should succeed with a valid env override");
    assert_eq!(rebuild.schema, "custom_schema");

    // Exercise the real `RebuildResult` Debug impl (not a stand-in mock) —
    // schema/database are projected, the engine itself is not leaked.
    let rendered = format!("{rebuild:?}");
    assert!(rendered.contains("custom_schema"), "got: {rendered}");
    assert!(
        rendered.contains(".."),
        "expected finish_non_exhaustive marker, got: {rendered}"
    );
}

/// RAII guard restoring a process env var to its prior state on drop (even on
/// panic) — needed because dbt-fusion's own startup profile loader reads
/// `env_var()` straight from the process env, so the var must exist while
/// `Harness::build_files_with_profile` runs `initialize_project`, then be
/// removed to exercise `rebuild_adapter_engine_with_env`'s own "not found"
/// error (which checks the workflow override map, then falls back to the
/// process env — with no default in the profile, both must be absent).
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

#[tokio::test]
async fn rebuild_adapter_engine_with_env_missing_required_var_errors() {
    // No default this time — a required env_var with nothing supplied must fail.
    let profile_yml = "spike:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      \
                        path: \"{DB_PATH}\"\n      schema: \"{{ env_var('DBTT_REQUIRED_SCHEMA') }}\"\n      \
                        threads: 1\n";
    // Present at startup (dbt-fusion's own loader reads the process env
    // directly) so `initialize_project` succeeds; removed before the rebuild
    // call so the override map + process-env fallback both miss.
    let guard = EnvVarGuard::set("DBTT_REQUIRED_SCHEMA", "startup-value");
    let harness = Harness::build_files_with_profile(ENV_VAR_SCHEMA_MODEL, profile_yml).await;
    guard.remove();

    let env = BTreeMap::new();
    let err = rebuild_adapter_engine_with_env(harness.state(), None, &env)
        .expect_err("missing required env_var should error");
    assert!(format!("{err:#}").contains("DBTT_REQUIRED_SCHEMA"), "got: {err:#}");
}

#[tokio::test]
async fn rebuild_adapter_engine_with_env_unknown_target_override_errors() {
    let harness =
        Harness::build_files_with_profile(ENV_VAR_SCHEMA_MODEL, ENV_VAR_SCHEMA_PROFILE).await;

    let env = BTreeMap::new();
    let err = rebuild_adapter_engine_with_env(harness.state(), Some("nonexistent_target"), &env)
        .expect_err("unknown target override should error");
    assert!(format!("{err:#}").contains("nonexistent_target"), "got: {err:#}");
}
