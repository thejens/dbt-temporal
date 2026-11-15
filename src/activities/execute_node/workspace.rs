//! Per-activity scratch space and the on-disk compiled SQL it is seeded with.

use std::collections::BTreeMap;

use anyhow::Context;
use dbt_schemas::schemas::telemetry::NodeType;

/// Where dbt-fusion looks for a node's compiled SQL.
///
/// `model.compiled_code` is not a value we can set — the `model` object in the
/// node context is a `LazyModelWrapper` that reads this file on every access.
/// Materializations that go through the attribute rather than the top-level
/// `compiled_code` variable (function DDL is one) therefore render an empty
/// body unless the file is exactly here. The layout is fusion's
/// (`out_dir/compiled/<package>/<original_file_path>`, with per-resource-type
/// quirks for snapshots and unit tests), so ask fusion rather than rebuild it.
pub(super) fn compiled_sql_path(
    node: &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
    in_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::path::PathBuf {
    node.get_node_path_abs(dbt_schemas::schemas::nodes::NodePathKind::Compiled, in_dir, out_dir)
}

/// Per-activity scratch space for one node execution.
///
/// Activities must never share `target/`: dbt-fusion writes compiled SQL and
/// per-ephemeral cumulative CTE chains there, and concurrent workflows on the
/// same worker would race on those files. Each activity therefore gets a fresh
/// temp dir seeded from the worker's in-memory SQL caches.
///
/// Holding the `TempDir` keeps the directory alive; dropping this struct
/// deletes it, so it must outlive every render that reads from `io_args`.
pub(super) struct ActivityWorkspace {
    _temp_dir: tempfile::TempDir,
    pub(super) ephemeral_dir: std::path::PathBuf,
    pub(super) io_args: dbt_common::io_args::IoArgs,
}

impl ActivityWorkspace {
    pub(super) fn new(
        state: &crate::worker_state::WorkerState,
        node: &dyn dbt_schemas::schemas::nodes::InternalDbtNode,
        invocation_id: &str,
    ) -> Result<Self, anyhow::Error> {
        let temp_dir = tempfile::tempdir().context("creating temp dir for activity")?;
        let out_dir = temp_dir.path().to_path_buf();

        let ephemeral_dir = temp_dir.path().join("ephemeral");
        std::fs::create_dir_all(&ephemeral_dir)
            .with_context(|| format!("creating ephemeral dir {}", ephemeral_dir.display()))?;

        let node_path = node.common().path.clone();
        let cache_key = node.common().unique_id.as_str();
        write_cached_sql(
            &state.compiled_sql_cache,
            cache_key,
            &compiled_sql_path(node, &state.io_args.in_dir, &out_dir),
        )?;
        if node.resource_type() == NodeType::Snapshot {
            write_cached_sql(&state.snapshot_sql_cache, cache_key, &out_dir.join(&node_path))?;
        }

        Ok(Self {
            _temp_dir: temp_dir,
            ephemeral_dir,
            io_args: dbt_common::io_args::IoArgs {
                in_dir: state.io_args.in_dir.clone(),
                out_dir,
                invocation_id: parse_invocation_id(invocation_id)?,
                ..Default::default()
            },
        })
    }
}

/// Write a cached SQL string to disk under `dest`, creating parent dirs as
/// needed. No-op when `cache` doesn't contain `node_path`.
///
/// Used at activity startup to seed the per-activity temp `out_dir` with the
/// SQL captured during the worker's startup resolve — so materialization
/// macros that read `target/compiled/<path>` see the right content even
/// though each activity gets a fresh temp dir.
fn write_cached_sql(
    cache: &BTreeMap<String, String>,
    node_path: &str,
    dest: &std::path::Path,
) -> std::io::Result<()> {
    let Some(sql) = cache.get(node_path) else {
        return Ok(());
    };
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(dest, sql)
}

/// Parse the invocation ID from the workflow input. Rejects malformed IDs
/// with an actionable error message — falling back to the worker-state
/// invocation_id would silently mis-tag artifacts and run-results under a
/// different run.
fn parse_invocation_id(raw: &str) -> Result<uuid::Uuid, anyhow::Error> {
    raw.parse()
        .map_err(|e| anyhow::anyhow!("invalid invocation_id {raw:?}: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- write_cached_sql ---

    #[test]
    fn write_cached_sql_writes_when_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = BTreeMap::new();
        cache.insert("models/m.sql".to_string(), "SELECT 1".to_string());

        let dest = dir.path().join("compiled/models/m.sql");
        write_cached_sql(&cache, "models/m.sql", &dest).unwrap();
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "SELECT 1");
    }

    #[test]
    fn write_cached_sql_noop_when_cache_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let dest = dir.path().join("compiled/models/m.sql");
        write_cached_sql(&cache, "models/m.sql", &dest).unwrap();
        assert!(!dest.exists());
    }

    #[test]
    fn write_cached_sql_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = BTreeMap::new();
        cache.insert("a/b/c.sql".to_string(), "x".to_string());

        // dest has multiple non-existent parent levels; the helper should
        // create them all.
        let dest = dir.path().join("nested/dirs/a/b/c.sql");
        write_cached_sql(&cache, "a/b/c.sql", &dest).unwrap();
        assert!(dest.exists());
    }

    // --- parse_invocation_id ---

    #[test]
    fn parse_invocation_id_accepts_valid_uuid() {
        let raw = "00000000-0000-0000-0000-000000000001";
        let id = parse_invocation_id(raw).unwrap();
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn parse_invocation_id_rejects_garbage_with_actionable_message() {
        let err = parse_invocation_id("not-a-uuid").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid invocation_id"));
        assert!(msg.contains("not-a-uuid"));
    }
}
