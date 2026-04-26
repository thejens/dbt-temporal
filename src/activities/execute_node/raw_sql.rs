//! Resolve a node's raw SQL string before Jinja compilation.
//!
//! Tests fall through up to four on-disk candidates because dbt-fusion writes
//! the inlined-kwargs SQL to `out_dir` while `raw_code` may carry the
//! pre-resolution template; pulling from `out_dir` first matches what the
//! `materialization_test_default` macro expects.

use std::path::PathBuf;

use dbt_schemas::schemas::nodes::CommonAttributes;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::worker_state::WorkerState;

/// On miss, returns the path that was tried last and the underlying I/O error
/// so the caller can produce an actionable message.
pub fn resolve_raw_sql(
    state: &WorkerState,
    common: &CommonAttributes,
    rt: NodeType,
    node_path: &str,
) -> Result<String, (PathBuf, std::io::Error)> {
    if rt == NodeType::Seed {
        // Seeds have no SQL body — data comes from the agate_table loaded
        // separately. Reading the .csv path here would embed the CSV verbatim
        // in CREATE TABLE AS (csv...) producing invalid SQL.
        return Ok(String::new());
    }

    if rt == NodeType::Test {
        if let Some(sql) = state.test_sql_cache.get(node_path) {
            return Ok(sql.clone());
        }
        let candidates: [(PathBuf, &'static str); 4] = [
            (
                state.io_args.out_dir.join(&common.original_file_path),
                "out_dir/original_file_path",
            ),
            (state.io_args.out_dir.join(&common.path), "out_dir/path"),
            (
                state.io_args.in_dir.join(&common.original_file_path),
                "in_dir/original_file_path",
            ),
            (state.io_args.in_dir.join(&common.path), "in_dir/path"),
        ];
        if let Some(sql) = read_first_non_empty_sql(&candidates) {
            return Ok(sql);
        }
        // Last resort: raw_code on the node (may lack inlined kwargs, so this
        // is a degraded path, but better than failing outright on test nodes
        // that don't write to disk).
        return common
            .raw_code
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "--placeholder--")
            .map(ToString::to_string)
            .ok_or_else(|| {
                (
                    state.io_args.out_dir.join(&common.original_file_path),
                    std::io::Error::from(std::io::ErrorKind::NotFound),
                )
            });
    }

    // Models, snapshots, hooks: prefer raw_code; fall back to in_dir.
    let raw_sql_from_node = common
        .raw_code
        .as_deref()
        .filter(|s| !s.is_empty() && *s != "--placeholder--")
        .map(ToString::to_string);
    raw_sql_from_node.map_or_else(
        || {
            let raw_sql_path = state.io_args.in_dir.join(&common.original_file_path);
            std::fs::read_to_string(&raw_sql_path).map_err(|e| (raw_sql_path, e))
        },
        Ok,
    )
}

fn read_first_non_empty_sql(candidates: &[(PathBuf, &'static str)]) -> Option<String> {
    for (path, _source) in candidates {
        let Ok(sql) = std::fs::read_to_string(path) else {
            continue;
        };
        if !sql.trim().is_empty() {
            return Some(sql);
        }
    }
    None
}
