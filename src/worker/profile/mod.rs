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
        // The same project settings the startup engines were built with: a
        // workflow that overrides credentials is not also opting out of the
        // project's query comment or behaviour flags.
        &state.adapter_settings,
        state.auth_override.as_ref(),
    )?;

    Ok(RebuildResult {
        engines,
        schema,
        database,
    })
}

/// Which environment variables a `profiles.yml` reads.
///
/// Deliberately textual rather than a YAML walk. The cost of naming one key too
/// many is an extra adapter rebuild; the cost of missing one is silently stale
/// credentials at runtime, so every uncertain case leans the harmless way.
#[derive(Debug, Clone)]
pub enum ProfileEnvVars {
    /// The profile reads exactly these keys, all of them written as literals.
    Keys(std::collections::BTreeSet<String>),
    /// The profile reads env vars, but not all of them can be named — a
    /// computed key (`env_var(some_var)`), or a file that could not be read.
    /// Any override has to be assumed relevant.
    Unknown,
}

impl ProfileEnvVars {
    /// Whether an override of these keys can change what the profile resolves
    /// to, and so requires rebuilding the adapter engines.
    ///
    /// This is the whole reason the type exists. `build_effective_env` injects
    /// the serialized workflow input as `_` on *every* run, so "the workflow
    /// supplied env overrides" was true always — and a profile that read any
    /// env var at all therefore re-read, re-rendered and rebuilt every engine
    /// once per node activity, handling credentials each time. `_` is transport
    /// metadata for `env_var('_')` in model SQL; a profile that does not read it
    /// is not affected by it.
    pub fn affected_by<'a>(&self, overridden: impl Iterator<Item = &'a String>) -> bool {
        match self {
            Self::Keys(keys) => overridden.into_iter().any(|key| keys.contains(key)),
            Self::Unknown => overridden.into_iter().next().is_some(),
        }
    }
}

/// Read the env vars a `profiles.yml` refers to.
///
/// A file that cannot be read says nothing about what it contains, so it counts
/// as [`ProfileEnvVars::Unknown`] rather than as "no dependencies" — the one
/// guess that would pin every later run to startup credentials.
pub fn profile_env_vars(profiles_path: &Path) -> ProfileEnvVars {
    std::fs::read_to_string(profiles_path)
        .map_or(ProfileEnvVars::Unknown, |content| parse_env_var_keys(&content))
}

/// Pull the literal keys out of every `env_var(...)` call in the text.
///
/// Whitespace before the paren counts, because Jinja accepts `env_var ('KEY')`.
/// A call whose key is not a quoted literal makes the whole answer `Unknown`:
/// its key is only known at render time, so nothing here can rule an override
/// out.
fn parse_env_var_keys(content: &str) -> ProfileEnvVars {
    #[allow(clippy::expect_used)]
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"\benv_var\s*\(\s*(?:'([^']*)'|"([^"]*)")?"#)
            .expect("env_var call regex")
    });

    let mut keys = std::collections::BTreeSet::new();
    for call in CALL.captures_iter(content) {
        match call.get(1).or_else(|| call.get(2)) {
            Some(key) => {
                keys.insert(key.as_str().to_string());
            }
            // A call with no quoted literal after the paren — the key is an
            // expression, and only rendering can say what it is.
            None => return ProfileEnvVars::Unknown,
        }
    }
    ProfileEnvVars::Keys(keys)
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

    fn keys_of(path: &Path) -> std::collections::BTreeSet<String> {
        match profile_env_vars(path) {
            ProfileEnvVars::Keys(keys) => keys,
            ProfileEnvVars::Unknown => panic!("expected named keys"),
        }
    }

    #[test]
    fn profile_env_vars_names_the_keys_it_reads() -> Result<()> {
        let path = write_temp_profiles(
            r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      user: "{{ env_var ('DB_USER') }}"
      port: 5432
"#,
        )?;
        assert_eq!(
            keys_of(&path),
            ["DB_HOST".to_string(), "DB_USER".to_string()]
                .into_iter()
                .collect(),
            "both spellings count, whitespace before the paren included"
        );
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    #[test]
    fn profile_env_vars_is_empty_for_a_static_profile() -> Result<()> {
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
        assert!(keys_of(&path).is_empty());
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    /// A name that merely ends in `env_var` is not a call.
    #[test]
    fn profile_env_vars_ignores_a_longer_identifier() -> Result<()> {
        let path = write_temp_profiles(
            r"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: my_env_variable
",
        )?;
        assert!(keys_of(&path).is_empty());
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    /// A key only rendering can resolve makes every override potentially
    /// relevant — the conservative answer, not a guess at nothing.
    #[test]
    fn profile_env_vars_is_unknown_for_a_computed_key() -> Result<()> {
        let path = write_temp_profiles(
            r#"my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var(host_var) }}"
"#,
        )?;
        assert!(matches!(profile_env_vars(&path), ProfileEnvVars::Unknown));
        std::fs::remove_dir_all(path.parent().context("no parent")?).ok();
        Ok(())
    }

    /// Unreadable says nothing about the contents: assume env vars are in play
    /// rather than pinning the run to startup credentials.
    #[test]
    fn profile_env_vars_is_unknown_when_the_file_cannot_be_read() {
        let path = std::path::PathBuf::from("/tmp/nonexistent-dbtt-test/profiles.yml");
        assert!(matches!(profile_env_vars(&path), ProfileEnvVars::Unknown));
    }

    /// The point of naming keys: `build_effective_env` puts the serialized
    /// workflow input in `_` on every run, so a profile that does not read `_`
    /// must not rebuild its engines once per node because of it.
    #[test]
    fn the_underscore_override_alone_does_not_affect_a_profile_that_ignores_it() {
        let reads_db_host = ProfileEnvVars::Keys(std::iter::once("DB_HOST".to_string()).collect());

        let only_underscore = ["_".to_string()];
        assert!(!reads_db_host.affected_by(only_underscore.iter()));

        let with_db_host = ["_".to_string(), "DB_HOST".to_string()];
        assert!(reads_db_host.affected_by(with_db_host.iter()));

        let unrelated = ["_".to_string(), "SOME_MODEL_VAR".to_string()];
        assert!(
            !reads_db_host.affected_by(unrelated.iter()),
            "an override the profile never reads changes nothing about the connection"
        );
    }

    /// When the keys cannot be named, any override is assumed relevant.
    #[test]
    fn unknown_keys_treat_every_override_as_relevant() {
        let unknown = ProfileEnvVars::Unknown;
        assert!(unknown.affected_by(std::iter::once(&"_".to_string())));
        assert!(!unknown.affected_by(std::iter::empty()));
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
            &crate::worker::adapter::AdapterSettings::default(),
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
