//! The metadata index a project check reads, built once per project at startup.
//!
//! A project check is a SQL file under `check-paths` (`checks/` by default)
//! that queries the project's own metadata: `dbt.models`, `dbt.checks`,
//! `dbt.node_columns`, … Those relations are not in the warehouse — they are
//! parquet the parse writes and an ingest pass turns into an index. So before
//! any check can run, this module reproduces the two steps dbt takes between
//! resolving a project and gating on its checks:
//!
//! 1. `save_parse_state` writes the parse epochs (nodes, columns, alive set)
//!    under `<root>/private/metadata/parse/`.
//! 2. `ingest_from_metadata_direct` converts those epochs into the index's
//!    `dbt.*.parquet` under `<root>/private/index/`.
//!
//! Both run at worker startup, inside `initialize_project`, because their input
//! is the resolved project — which this worker parses exactly once and then
//! holds for its lifetime. Nothing a workflow does can invalidate the index, so
//! the per-run gate is a pure read of what is built here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use dbt_common::constants::{default_index_dir, default_metadata_dir};
use dbt_common::io_args::IoArgs;
use dbt_index_core::WriteSource;
use dbt_index_core::ingest::{IngestState, ingest_from_metadata_direct};
use dbt_schemas::schemas::DbtCheck;
use dbt_schemas::state::{DbtState, ResolverState};
use tempfile::TempDir;
use tracing::{info, warn};

/// A project's checks and the index they read.
pub struct ProjectChecks {
    /// Owns the index on disk. Dropped with the project's `WorkerState`, which
    /// lives for the worker process — so the directory outlives every run that
    /// queries it.
    _root: TempDir,
    /// Where the parse epochs were written. The gate consults it to confirm the
    /// index still reflects them before querying: a check is a pure reader and
    /// must refuse a stale index rather than report its zero rows as a pass.
    pub metadata_dir: PathBuf,
    /// Directory holding the index's `dbt.*.parquet`.
    pub index_dir: PathBuf,
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
            .field("index_dir", &self.index_dir)
            .field("checks", &self.checks.len())
            .finish_non_exhaustive()
    }
}

/// Build the index for a resolved project, or `None` when it declares no
/// enabled checks.
///
/// Returning `None` is the common case and the reason the whole pipeline is
/// gated on it: a project without checks pays neither the parquet write nor the
/// ingest, exactly as dbt skips its early index publish when `nodes.checks` is
/// empty.
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
        TempDir::with_prefix("dbtt-checks-").context("creating the check index directory")?;
    let metadata_dir = default_metadata_dir(root.path());
    let index_dir = default_index_dir(root.path());

    write_parse_epochs(io, root.path(), dbt_state, resolver_state)?;
    ingest_index(&metadata_dir, &index_dir, root.path())?;

    info!(
        checks = checks.len(),
        index_dir = %index_dir.display(),
        "built the project-check index"
    );

    Ok(Some(ProjectChecks {
        _root: root,
        metadata_dir,
        index_dir,
        checks,
    }))
}

/// Write the parse epochs the ingest reads.
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
    // The `env_var()` reads the parse collected, which the index publishes as
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

/// Convert the parse epochs into the index the checks query.
fn ingest_index(metadata_dir: &Path, index_dir: &Path, root: &Path) -> Result<()> {
    let mut state = IngestState::default();
    ingest_from_metadata_direct(metadata_dir, index_dir, &mut state)
        .map_err(|e| anyhow::anyhow!("ingesting the project-check index: {e}"))?;

    // Provenance bookkeeping that marks the directory a published index rather
    // than a directory of parquet nothing vouched for. A failure leaves the
    // index itself queryable, so it warns rather than aborting startup.
    if let Err(e) =
        dbt_index_core::save_artifact_meta(index_dir, root, WriteSource::DirectWrite, None)
    {
        warn!(error = %e, "could not record index provenance metadata");
    }
    Ok(())
}

/// A `ProjectChecks` whose directories hold no index at all.
///
/// Lets the gate's refuse-to-read-a-missing-index path be exercised without
/// standing up a parse: what matters there is only that `index_is_current`
/// says no.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn without_an_index(checks: Vec<Arc<DbtCheck>>) -> ProjectChecks {
    let root = TempDir::new().expect("creating a temp dir");
    let metadata_dir = default_metadata_dir(root.path());
    let index_dir = default_index_dir(root.path());
    ProjectChecks {
        _root: root,
        metadata_dir,
        index_dir,
        checks,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The index directory is the actionable half of a gate failure, and the
    /// checks themselves have no useful `Debug`, so the count stands in.
    #[test]
    fn debug_reports_the_index_directory_and_how_many_checks_it_serves() {
        let checks = without_an_index(vec![Arc::new(DbtCheck::default())]);
        let rendered = format!("{checks:?}");
        assert!(rendered.contains("ProjectChecks"), "{rendered}");
        assert!(rendered.contains("index"), "{rendered}");
        assert!(rendered.contains('1'), "the check count belongs in it: {rendered}");
    }
}
