//! Per-workflow profile handling: re-rendering `profiles.yml` with the
//! workflow's `env` overrides and rebuilding the adapter engines from it.

pub mod render;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};

pub use render::{RenderedTarget, render_profile_with_env};

use crate::worker::engines::AdapterEngines;
use crate::worker_state::WorkerState;

/// Result of rebuilding the adapter engines with per-workflow env overrides.
pub struct RebuildResult {
    /// One engine per adapter the re-rendered target declares. A node still
    /// routes by its own adapter here: an override that changes the target can
    /// change the adapter set, and collapsing to the default would send the node
    /// to the wrong warehouse.
    pub engines: AdapterEngines,
    /// Profile-level schema after applying env overrides.
    pub schema: String,
    /// Profile-level database after applying env overrides.
    pub database: String,
}

impl std::fmt::Debug for RebuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildResult")
            .field("schema", &self.schema)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

/// Re-render profiles.yml with per-workflow env overrides and build fresh
/// adapter engines.
///
/// Called from `prepare_render_env` when the workflow provides `env` overrides
/// and profiles.yml uses `env_var()`. This avoids touching process-level env
/// vars, so parallel workflows are fully isolated.
///
/// Every adapter the target declares is rebuilt, not just the default one:
/// credentials for a non-default adapter come from the same profile and go stale
/// under an override the same way.
///
/// Returns the engines plus the resolved schema/database of the *default*
/// adapter — that is what node relation metadata was baked from at worker
/// startup, and therefore what schema patching compares against.
pub fn rebuild_adapter_engines_with_env(
    state: &WorkerState,
    target_override: Option<&str>,
    env_overrides: &BTreeMap<String, String>,
) -> Result<RebuildResult> {
    let target = target_override.unwrap_or(&state.default_target);
    let rendered = render_profile_with_env(
        &state.profiles_path,
        &state.profile_name_in_project,
        target,
        env_overrides,
    )?;

    let default_config = rendered
        .default_config()
        .with_context(|| format!("target '{target}' declares no config for its default adapter"))?;
    let schema = default_config.get_schema().cloned().unwrap_or_default();
    let database = default_config.get_database().cloned().unwrap_or_default();

    let engines = super::adapter::build_adapter_engines(
        &rendered.configs,
        rendered.default_adapter,
        state.resolver_state.root_project_quoting,
        state.auth_override.as_ref(),
    )?;

    Ok(RebuildResult {
        engines,
        schema,
        database,
    })
}

/// Heuristic: does this profiles.yml call `env_var` anywhere in the raw file?
/// A textual match deliberately, not a YAML walk:
///
/// - The cost of a false positive is one extra adapter rebuild per workflow.
/// - The cost of a false negative is silently stale credentials at runtime.
///
/// So we lean toward the harmless direction in every uncertain case. Strings
/// that merely *mention* the call (comments, doc keys) trigger the rebuild,
/// whitespace before the paren still counts because Jinja accepts
/// `env_var ('KEY')`, and a profile that cannot be read is assumed to use them:
/// an unreadable file here says nothing about what it contains, and guessing
/// "no dependencies" would pin every later run to startup credentials.
pub fn profile_uses_env_vars(profiles_path: &Path) -> bool {
    std::fs::read_to_string(profiles_path).map_or(true, |content| mentions_env_var(&content))
}

/// True when `content` contains an `env_var` call — the identifier followed by
/// its opening paren, with any Jinja-legal whitespace between them.
fn mentions_env_var(content: &str) -> bool {
    #[allow(clippy::expect_used)]
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"\benv_var\s*\(").expect("env_var call regex")
    });
    RE.is_match(content)
}

/// Write a throwaway `profiles.yml` into its own temp directory, for the tests
/// of both halves of this module. The caller removes the directory.
#[cfg(test)]
fn write_temp_profiles(content: &str) -> Result<std::path::PathBuf> {
    let dir = std::env::temp_dir().join(format!("dbtt-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("profiles.yml");
    std::fs::write(&path, content)?;
    Ok(path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_uses_env_vars_detects_usage() -> Result<()> {
        let path = write_temp_profiles(
            r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      port: 5432
"#,
        )?;
        assert!(profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    #[test]
    fn test_profile_uses_env_vars_no_env_var() -> Result<()> {
        let path = write_temp_profiles(
            r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
",
        )?;
        assert!(!profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    #[test]
    fn test_profile_uses_env_vars_detects_whitespace_before_paren() -> Result<()> {
        let path = write_temp_profiles(
            r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var ('DB_HOST', 'localhost') }}"
"#,
        )?;
        assert!(profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    /// A name that merely ends in `env_var` is not a call, and must not cost
    /// every workflow an adapter rebuild.
    #[test]
    fn test_profile_uses_env_vars_ignores_longer_identifier() -> Result<()> {
        let path = write_temp_profiles(
            r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: my_env_variable
",
        )?;
        assert!(!profile_uses_env_vars(&path));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    /// Unreadable says nothing about the contents: assume env vars are in play
    /// rather than pinning the run to startup credentials.
    #[test]
    fn test_profile_uses_env_vars_missing_file_assumes_usage() {
        let path = std::path::PathBuf::from("/tmp/nonexistent-dbtt-test/profiles.yml");
        assert!(profile_uses_env_vars(&path));
    }

    /// The contract of the `Debug` impl: a rebuild is logged by the target it
    /// resolved to, never by what the engines hold — those carry credentials.
    #[test]
    fn rebuild_result_debug_projects_the_target_and_not_the_engines() -> Result<()> {
        let config = dbt_schemas::schemas::profiles::DbConfig::DuckDB(Box::new(
            dbt_schemas::schemas::profiles::DuckDbConfig {
                path: Some(":memory:".to_string()),
                ..Default::default()
            },
        ));
        let engines = crate::worker::adapter::build_adapter_engines(
            std::slice::from_ref(&config),
            dbt_adapter::AdapterType::DuckDB,
            dbt_schemas::schemas::common::ResolvedQuoting::default(),
            None,
        )?;

        let rendered = format!(
            "{:?}",
            RebuildResult {
                engines,
                schema: "wf_42".to_string(),
                database: "warehouse".to_string(),
            }
        );
        assert!(rendered.contains("RebuildResult"), "{rendered}");
        assert!(rendered.contains("wf_42"), "{rendered}");
        assert!(rendered.contains("warehouse"), "{rendered}");
        assert!(rendered.contains(".."), "expected finish_non_exhaustive marker: {rendered}");
        assert!(
            !rendered.contains("duckdb"),
            "the engines must not reach log output: {rendered}"
        );
        Ok(())
    }
}
