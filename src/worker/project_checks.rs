//! The metadata a project check reads, written once per project at startup.
//!
//! A project check is a SQL file under `check-paths` (`checks/` by default)
//! that queries the project's own metadata: `dbt.models`, `dbt.checks`,
//! `dbt.node_columns`, … Those relations are not in the warehouse — they are
//! views over the parse epochs, the parquet `save_parse_state` writes under
//! `<root>/private/metadata/parse/`. This module reproduces that one step dbt
//! takes between resolving a project and gating on its checks.
//!
//! It runs at worker startup, inside `initialize_project`, because its input is
//! the resolved project — which this worker parses exactly once and then holds
//! for its lifetime. Nothing a workflow does can invalidate the epochs, so the
//! per-run gate is a pure read of what is written here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use dbt_common::constants::default_metadata_dir;
use dbt_common::io_args::IoArgs;
use dbt_schemas::schemas::DbtCheck;
use dbt_schemas::state::{DbtState, ResolverState};
use tempfile::TempDir;
use tracing::info;

/// A project's checks and the metadata they read.
pub struct ProjectChecks {
    /// Owns the metadata on disk. Dropped with the project's `WorkerState`, which
    /// lives for the worker process — so the directory outlives every run that
    /// queries it.
    _root: TempDir,
    /// Where the parse epochs were written; the gate's views read them here.
    pub metadata_dir: PathBuf,
    /// Enabled checks, in `unique_id` order.
    ///
    /// Disabled ones are dropped rather than recorded: dbt keeps them only so
    /// that naming one on the command line stays a no-op success instead of an
    /// unknown-name error, and this worker has no per-check naming to
    /// disambiguate.
    pub checks: Vec<Arc<DbtCheck>>,
}

impl std::fmt::Debug for ProjectChecks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectChecks")
            .field("metadata_dir", &self.metadata_dir)
            .field("checks", &self.checks.len())
            .finish_non_exhaustive()
    }
}

/// Write the metadata for a resolved project, or `None` when it declares no
/// enabled checks.
///
/// Returning `None` is the common case: a project without checks pays nothing
/// for the gate.
///
/// `io` supplies the invocation identity and project directory; only its output
/// directory is replaced, so the epochs land in a directory this worker owns
/// rather than in the resolve output, which `initialize_project` deletes.
pub fn build(
    io: &IoArgs,
    dbt_state: &DbtState,
    resolver_state: &ResolverState,
) -> Result<Option<ProjectChecks>> {
    // BTreeMap iteration order, so the per-check results a run reports come
    // back sorted by unique_id rather than in whatever order resolve produced.
    let checks: Vec<Arc<DbtCheck>> = resolver_state.nodes.checks.values().cloned().collect();
    if checks.is_empty() {
        return Ok(None);
    }

    let root =
        TempDir::with_prefix("dbtt-checks-").context("creating the check metadata directory")?;
    let metadata_dir = default_metadata_dir(root.path());

    write_parse_epochs(io, root.path(), dbt_state, resolver_state)?;

    info!(
        checks = checks.len(),
        metadata_dir = %metadata_dir.display(),
        "wrote the project-check metadata"
    );

    Ok(Some(ProjectChecks {
        _root: root,
        metadata_dir,
        checks,
    }))
}

/// Write the parse epochs the checks read.
///
/// `changed_nodes: None` means a cold write — every node — which is the only
/// correct choice here: the worker resolves each project from scratch and keeps
/// no previous parse state to diff against.
fn write_parse_epochs(
    io: &IoArgs,
    root: &Path,
    dbt_state: &DbtState,
    resolver_state: &ResolverState,
) -> Result<()> {
    let io = IoArgs {
        out_dir: root.to_path_buf(),
        ..io.clone()
    };
    // The `env_var()` reads the parse collected, which the views publish as
    // `dbt.project_env_vars`. dbt reads the same global at its own call site;
    // a lock poisoned by an unrelated panic costs that one view, not the gate.
    let env_vars: HashMap<String, String> = dbt_jinja_utils::utils::ENV_VARS
        .lock()
        .map(|vars| vars.clone())
        .unwrap_or_default();

    dbt_metadata::partial_parse::save_parse_state(
        &io,
        dbt_state,
        resolver_state,
        // Startup has no `--vars`: they arrive per workflow and never reach
        // parse, so there is nothing to hash into the epoch.
        &None,
        env_vars,
        None,
    )
    .map_err(|e| anyhow::anyhow!("writing parse metadata for project checks: {e}"))?;
    Ok(())
}

/// A `ProjectChecks` whose directory holds no metadata at all.
///
/// Lets the gate's refuse-to-read-missing-metadata path be exercised without
/// standing up a parse.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn without_metadata(checks: Vec<Arc<DbtCheck>>) -> ProjectChecks {
    let root = TempDir::new().expect("creating a temp dir");
    let metadata_dir = default_metadata_dir(root.path());
    ProjectChecks {
        _root: root,
        metadata_dir,
        checks,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The metadata directory is the actionable half of a gate failure, and
    /// the checks themselves have no useful `Debug`, so the count stands in.
    #[test]
    fn debug_reports_the_metadata_directory_and_how_many_checks_it_serves() {
        let checks = without_metadata(vec![Arc::new(DbtCheck::default())]);
        let rendered = format!("{checks:?}");
        assert!(rendered.contains("ProjectChecks"), "{rendered}");
        assert!(rendered.contains("metadata"), "{rendered}");
        assert!(rendered.contains('1'), "the check count belongs in it: {rendered}");
    }
}
