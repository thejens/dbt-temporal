//! Per-workflow profile handling: re-rendering `profiles.yml` with the
//! workflow's `env` overrides and rebuilding the adapter engines from it.

pub mod render;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

pub use render::{RenderedTarget, render_profile_with_env};

use crate::worker::single_flight::SingleFlightCache;

use crate::worker::engines::AdapterEngines;
use crate::worker_state::WorkerState;

/// Result of rebuilding the adapter engines with per-workflow env overrides.
pub struct RebuildResult {
    /// One engine per adapter the re-rendered target declares. A node still
    /// routes by its own adapter here: an override that changes the target can
    /// change the adapter set, and collapsing to the default would send the node
    /// to the wrong warehouse.
    pub engines: AdapterEngines,
    /// The resolved schema and database of *each* adapter, keyed by adapter.
    ///
    /// Per adapter, not just the default's: a target declaring several adapters
    /// gives each its own schema and database, and applying the default's to a
    /// node routed elsewhere told its Jinja `target` about a warehouse it was
    /// not connected to.
    pub targets: Vec<(dbt_adapter::AdapterType, AdapterTarget)>,
    /// The adapter unannotated nodes run on after the rebuild.
    pub default_adapter: dbt_adapter::AdapterType,
}

/// The schema and database one adapter's target resolves to.
///
/// `database` stays `None` when the target does not declare one — DuckDB and
/// SQLite name their database by their file. Flattening that to an empty
/// string reads as "the workflow moved every relation to the database called
/// ``", which is what then goes into the compiled SQL.
#[derive(Debug, Clone, Default)]
pub struct AdapterTarget {
    pub schema: String,
    pub database: Option<String>,
}

impl RebuildResult {
    /// The resolved target for one adapter, falling back to the default
    /// adapter's when the rebuilt profile does not declare that one.
    pub fn target_for(&self, adapter: dbt_adapter::AdapterType) -> AdapterTarget {
        let find = |wanted: dbt_adapter::AdapterType| {
            self.targets
                .iter()
                .find(|(declared, _)| *declared == wanted)
                .map(|(_, target)| target.clone())
        };
        find(adapter)
            .or_else(|| find(self.default_adapter))
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for RebuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildResult")
            .field("targets", &self.targets)
            .field("default_adapter", &self.default_adapter)
            .finish_non_exhaustive()
    }
}

/// Read one rendered target's schema and database.
fn adapter_target(config: &dbt_schemas::schemas::profiles::DbConfig) -> AdapterTarget {
    AdapterTarget {
        schema: config.get_schema().cloned().unwrap_or_default(),
        database: config.get_database().cloned().filter(|d| !d.is_empty()),
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
/// Returns the engines plus each adapter's resolved schema and database, so a
/// node routed to a non-default adapter gets that adapter's target rather than
/// the default's.
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

    rendered
        .default_config()
        .with_context(|| format!("target '{target}' declares no config for its default adapter"))?;

    let targets: Vec<(dbt_adapter::AdapterType, AdapterTarget)> = rendered
        .configs
        .iter()
        .map(|config| (config.adapter_type(), adapter_target(config)))
        .collect();

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
        targets,
        default_adapter: rendered.default_adapter,
    })
}

/// How many distinct rebuilds a worker keeps.
///
/// Each entry is a set of engines with their connection pools, so the bound is
/// what keeps a worker that serves ever-changing overrides (a credential per
/// run, say) from accumulating idle warehouse connections. Least recently used
/// goes first; a miss costs one profile render and one engine build.
const MAX_CACHED_REBUILDS: usize = 16;

/// Everything that decides what [`rebuild_adapter_engines_with_env`] produces:
/// the target, and the env overrides the profile can actually read.
///
/// The values are the full strings, compared in full. They can hold
/// credentials, so [`Debug`](std::fmt::Debug) names the keys and never prints a
/// value.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RebuildKey {
    target: String,
    env: BTreeMap<String, String>,
}

impl RebuildKey {
    /// `target` is the name the rebuild resolves, after the worker's default
    /// has been applied: an override naming the default target and no override
    /// at all build the same engines and must share an entry.
    pub fn new(
        target: &str,
        profile_env_vars: &ProfileEnvVars,
        env_overrides: &BTreeMap<String, String>,
    ) -> Self {
        Self {
            target: target.to_string(),
            env: profile_env_vars.relevant(env_overrides),
        }
    }
}

impl std::fmt::Debug for RebuildKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildKey")
            .field("target", &self.target)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Rebuilt engines, shared by every activity that resolves to the same
/// [`RebuildKey`].
pub struct RebuildCache(SingleFlightCache<RebuildKey, RebuildResult>);

impl Default for RebuildCache {
    fn default() -> Self {
        Self(SingleFlightCache::new(MAX_CACHED_REBUILDS))
    }
}

impl std::fmt::Debug for RebuildCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildCache").finish_non_exhaustive()
    }
}

/// [`rebuild_adapter_engines_with_env`], built once per distinct
/// [`RebuildKey`] and shared.
///
/// A run's nodes all resolve to one key, so a run builds its engines once
/// rather than once per node; so do later runs with the same overrides. The
/// engines hold no per-activity state — the cancellation token travels with the
/// `Adapter` each activity wraps around its engine, never with the engine — so
/// sharing them is as safe as sharing the worker's startup engines.
///
/// Blocking: the first caller builds on the calling thread and the rest wait for
/// it, so this must be called from the dbt pool like the build itself.
pub fn rebuild_adapter_engines_shared(
    state: &WorkerState,
    target_override: Option<&str>,
    env_overrides: &BTreeMap<String, String>,
) -> Result<Arc<RebuildResult>> {
    let key = RebuildKey::new(
        target_override.unwrap_or(&state.default_target),
        &state.profile_env_vars,
        env_overrides,
    );
    // Build from the key's own env, not the caller's: the key is exactly the
    // inputs the profile can read, and overrides it ignores must not be able to
    // make two callers of one key build different things.
    let build_env = key.env.clone();
    state
        .rebuilt_engines
        .0
        .get_or_build(key, || rebuild_adapter_engines_with_env(state, target_override, &build_env))
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

impl ProfileEnvVars {
    /// The overrides that can influence what the profile resolves to.
    ///
    /// For a profile naming its keys that is only those; every other override
    /// is invisible to it. When keys cannot be named, any of them might be read,
    /// so all are kept.
    pub fn relevant(&self, overrides: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        match self {
            Self::Keys(keys) => overrides
                .iter()
                .filter(|(key, _)| keys.contains(*key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            Self::Unknown => overrides.clone(),
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
/// A call whose key is not a lone quoted literal — a variable, or an expression
/// such as `'A' ~ suffix` — makes the whole answer `Unknown`: its key is only
/// known at render time, so nothing here can rule an override out.
fn parse_env_var_keys(content: &str) -> ProfileEnvVars {
    #[allow(clippy::expect_used)]
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"\benv_var\s*\(\s*(?:'([^']*)'|"([^"]*)")?(\s*[,)])?"#)
            .expect("env_var call regex")
    });

    let mut keys = std::collections::BTreeSet::new();
    for call in CALL.captures_iter(content) {
        match (call.get(1).or_else(|| call.get(2)), call.get(3)) {
            // The literal is the whole first argument: it ends at a comma
            // (a default follows) or the closing paren.
            (Some(key), Some(_)) => {
                keys.insert(key.as_str().to_string());
            }
            // No quoted literal after the paren, or one that continues into an
            // expression — only rendering can say what the key is.
            _ => return ProfileEnvVars::Unknown,
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
mod tests;
