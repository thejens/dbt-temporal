//! Resolve a node's raw SQL string before Jinja compilation.
//!
//! Tests fall through up to four on-disk candidates because dbt-fusion writes
//! the inlined-kwargs SQL to `out_dir` while `raw_code` may carry the
//! pre-resolution template; pulling from `out_dir` first matches what the
//! `materialization_test_default` macro expects.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
    resolve_raw_sql_inner(
        &state.test_sql_cache,
        &state.io_args.in_dir,
        &state.io_args.out_dir,
        common,
        rt,
        node_path,
    )
}

fn resolve_raw_sql_inner(
    test_sql_cache: &BTreeMap<String, String>,
    in_dir: &Path,
    out_dir: &Path,
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
        if let Some(sql) = test_sql_cache.get(node_path) {
            return Ok(sql.clone());
        }
        let candidates: [(PathBuf, &'static str); 4] = [
            (out_dir.join(&common.original_file_path), "out_dir/original_file_path"),
            (out_dir.join(&common.path), "out_dir/path"),
            (in_dir.join(&common.original_file_path), "in_dir/original_file_path"),
            (in_dir.join(&common.path), "in_dir/path"),
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
                    out_dir.join(&common.original_file_path),
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
            let raw_sql_path = in_dir.join(&common.original_file_path);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn make_common(
        original_file_path: &str,
        path: &str,
        raw_code: Option<&str>,
    ) -> CommonAttributes {
        CommonAttributes {
            original_file_path: PathBuf::from(original_file_path),
            path: PathBuf::from(path),
            raw_code: raw_code.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn seed_returns_empty_string() {
        let cache = BTreeMap::new();
        let common = make_common("seeds/x.csv", "seeds/x.csv", None);
        let r = resolve_raw_sql_inner(
            &cache,
            Path::new("/in"),
            Path::new("/out"),
            &common,
            NodeType::Seed,
            "seed.x",
        );
        assert_eq!(r.unwrap(), "");
    }

    #[test]
    fn test_uses_cache_first() {
        let mut cache = BTreeMap::new();
        cache.insert("models/t.sql".to_string(), "SELECT 1".to_string());
        let common = make_common("models/t.sql", "models/t.sql", None);
        let r = resolve_raw_sql_inner(
            &cache,
            Path::new("/in"),
            Path::new("/out"),
            &common,
            NodeType::Test,
            "models/t.sql",
        );
        assert_eq!(r.unwrap(), "SELECT 1");
    }

    #[test]
    fn test_falls_back_to_out_dir_original_path() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(out.join("tests")).unwrap();
        std::fs::write(out.join("tests/t.sql"), "SELECT FROM out").unwrap();

        let cache = BTreeMap::new();
        let common = make_common("tests/t.sql", "compiled/tests/t.sql", None);
        let r = resolve_raw_sql_inner(
            &cache,
            Path::new("/in"),
            &out,
            &common,
            NodeType::Test,
            "tests/t.sql",
        );
        assert_eq!(r.unwrap(), "SELECT FROM out");
    }

    #[test]
    fn test_skips_empty_files_and_falls_through() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let in_d = dir.path().join("in");
        std::fs::create_dir_all(out.join("tests")).unwrap();
        std::fs::create_dir_all(in_d.join("tests")).unwrap();
        // Empty file in out_dir, real file in in_dir → falls through to in_dir.
        std::fs::write(out.join("tests/t.sql"), "   \n").unwrap();
        std::fs::write(in_d.join("tests/t.sql"), "SELECT FROM in").unwrap();

        let cache = BTreeMap::new();
        let common = make_common("tests/t.sql", "tests/t.sql", None);
        let r = resolve_raw_sql_inner(&cache, &in_d, &out, &common, NodeType::Test, "k");
        assert_eq!(r.unwrap(), "SELECT FROM in");
    }

    #[test]
    fn test_falls_back_to_raw_code_when_disk_misses() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let common = make_common("tests/missing.sql", "tests/missing.sql", Some("SELECT raw_code"));
        let r = resolve_raw_sql_inner(
            &cache,
            &dir.path().join("nope-in"),
            &dir.path().join("nope-out"),
            &common,
            NodeType::Test,
            "k",
        );
        assert_eq!(r.unwrap(), "SELECT raw_code");
    }

    #[test]
    fn test_rejects_placeholder_raw_code() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let common = make_common("tests/missing.sql", "tests/missing.sql", Some("--placeholder--"));
        let err = resolve_raw_sql_inner(
            &cache,
            &dir.path().join("nope-in"),
            &dir.path().join("nope-out"),
            &common,
            NodeType::Test,
            "k",
        )
        .unwrap_err();
        assert_eq!(err.1.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_returns_error_when_no_source_available() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let common = make_common("tests/missing.sql", "tests/missing.sql", None);
        let err = resolve_raw_sql_inner(
            &cache,
            &dir.path().join("nope-in"),
            &dir.path().join("nope-out"),
            &common,
            NodeType::Test,
            "k",
        )
        .unwrap_err();
        assert_eq!(err.1.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn model_prefers_raw_code() {
        let cache = BTreeMap::new();
        let common = make_common("models/m.sql", "models/m.sql", Some("SELECT inline"));
        let r = resolve_raw_sql_inner(
            &cache,
            Path::new("/in"),
            Path::new("/out"),
            &common,
            NodeType::Model,
            "k",
        );
        assert_eq!(r.unwrap(), "SELECT inline");
    }

    #[test]
    fn model_skips_empty_raw_code_and_reads_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        let in_d = dir.path().join("in");
        std::fs::create_dir_all(in_d.join("models")).unwrap();
        std::fs::write(in_d.join("models/m.sql"), "SELECT FROM disk").unwrap();

        let cache = BTreeMap::new();
        let common = make_common("models/m.sql", "models/m.sql", Some(""));
        let r =
            resolve_raw_sql_inner(&cache, &in_d, Path::new("/out"), &common, NodeType::Model, "k");
        assert_eq!(r.unwrap(), "SELECT FROM disk");
    }

    #[test]
    fn model_skips_placeholder_raw_code_and_reads_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        let in_d = dir.path().join("in");
        std::fs::create_dir_all(in_d.join("models")).unwrap();
        std::fs::write(in_d.join("models/m.sql"), "SELECT FROM disk").unwrap();

        let cache = BTreeMap::new();
        let common = make_common("models/m.sql", "models/m.sql", Some("--placeholder--"));
        let r =
            resolve_raw_sql_inner(&cache, &in_d, Path::new("/out"), &common, NodeType::Model, "k");
        assert_eq!(r.unwrap(), "SELECT FROM disk");
    }

    #[test]
    fn model_returns_io_error_path_when_disk_misses() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BTreeMap::new();
        let common = make_common("models/missing.sql", "models/missing.sql", None);
        let err = resolve_raw_sql_inner(
            &cache,
            dir.path(),
            Path::new("/out"),
            &common,
            NodeType::Model,
            "k",
        )
        .unwrap_err();
        assert_eq!(err.0, dir.path().join("models/missing.sql"));
        assert_eq!(err.1.kind(), std::io::ErrorKind::NotFound);
    }
}
