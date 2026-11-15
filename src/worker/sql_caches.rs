use std::collections::BTreeMap;

use anyhow::Result;

/// Compiled, snapshot and generic-test SQL, each keyed by node unique id.
pub(super) type SqlCaches =
    (BTreeMap<String, String>, BTreeMap<String, String>, BTreeMap<String, String>);

/// Capture compiled SQL and snapshot raw SQL from the target directory into memory.
///
/// After the resolve step, dbt-fusion has written compiled SQL to `out_dir/compiled/`
/// and snapshot SQL to `out_dir/`. We read these into BTreeMaps keyed by the node's
/// unique id so activities can write them into ephemeral temp dirs.
///
/// The compiled layout nests under package name and varies by resource type, so
/// it comes from fusion's own `get_node_path_abs` rather than a reconstruction —
/// a path that only *looks* right yields an empty cache and no error.
pub(super) fn build_sql_caches(
    nodes: &dbt_schemas::schemas::Nodes,
    in_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> Result<SqlCaches> {
    use dbt_schemas::schemas::nodes::NodePathKind;
    use dbt_schemas::schemas::telemetry::NodeType;

    let mut compiled_sql_cache = BTreeMap::new();
    let mut snapshot_sql_cache = BTreeMap::new();
    let mut test_sql_cache = BTreeMap::new();

    // Keyed by unique id, not by `common.path`. A path is relative to the
    // project that declares it, so two packages with a `models/stg_orders.sql`
    // each write to the same key and the second silently replaces the first —
    // a node then runs SQL belonging to a different package's model of the
    // same name. The unique id is what already distinguishes them everywhere
    // else.
    for (unique_id, node) in nodes.iter() {
        let path = node.common().path.to_string_lossy().to_string();

        let compiled_path = node.get_node_path_abs(NodePathKind::Compiled, in_dir, out_dir);
        if let Some(sql) = read_cached_sql(&compiled_path, unique_id)? {
            compiled_sql_cache.insert(unique_id.clone(), sql);
        }

        // Snapshot raw SQL: out_dir/<path>
        if node.resource_type() == NodeType::Snapshot
            && let Some(sql) = read_cached_sql(&out_dir.join(&path), unique_id)?
        {
            snapshot_sql_cache.insert(unique_id.clone(), sql);
        }

        // Generic test generated SQL: out_dir/<path>
        if node.resource_type() == NodeType::Test
            && let Some(sql) = read_cached_sql(&out_dir.join(&path), unique_id)?
        {
            test_sql_cache.insert(unique_id.clone(), sql);
        }
    }

    Ok((compiled_sql_cache, snapshot_sql_cache, test_sql_cache))
}

/// Read one cached SQL file, or `None` when the node did not produce that file.
///
/// Not every node writes every artifact, so an absent file is ordinary. Any
/// other I/O failure is not: this runs once at startup and the resolve output
/// is deleted afterwards, so a file skipped here is gone for the worker's
/// lifetime, and the node fails much later with an error about missing SQL
/// rather than about the permission or I/O problem that actually caused it.
fn read_cached_sql(path: &std::path::Path, unique_id: &str) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(sql) => Ok(Some(sql)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::Error::new(e)
            .context(format!("reading cached SQL for {unique_id} from {}", path.display()))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- build_sql_caches ---

    use std::path::PathBuf;
    use std::sync::Arc;

    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::schemas::nodes::{
        CommonAttributes, DbtModel, DbtSnapshot, DbtTest, NodeBaseAttributes,
    };

    fn write(path: &std::path::Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    /// Fixtures carry a real package name and `original_file_path`: the
    /// compiled tree nests under both, so a fixture that leaves them blank
    /// would pass against a path built any number of wrong ways.
    fn common_attrs(unique_id: &str, name: &str, path: &str, ofp: &str) -> CommonAttributes {
        // The package is the second segment of a unique id, and it is what
        // separates two same-named nodes' compiled output on disk.
        let package = unique_id.split('.').nth(1).unwrap_or("shop").to_string();
        CommonAttributes {
            unique_id: unique_id.to_string(),
            name: name.to_string(),
            package_name: package,
            path: PathBuf::from(path).into(),
            original_file_path: PathBuf::from(ofp).into(),
            ..CommonAttributes::default()
        }
    }

    fn make_model(unique_id: &str, name: &str, path: &str) -> Arc<DbtModel> {
        Arc::new(DbtModel {
            __common_attr__: common_attrs(unique_id, name, path, path),
            ..DbtModel::default()
        })
    }

    fn make_snapshot(unique_id: &str, name: &str, path: &str) -> Arc<DbtSnapshot> {
        Arc::new(DbtSnapshot {
            __common_attr__: common_attrs(unique_id, name, path, path),
            __base_attr__: NodeBaseAttributes::default(),
            ..DbtSnapshot::default()
        })
    }

    /// A generic test's `original_file_path` is the YAML it was declared in,
    /// not its own generated SQL — the many-to-one case in the compiled tree.
    fn make_test(unique_id: &str, name: &str, path: &str) -> Arc<DbtTest> {
        Arc::new(DbtTest {
            __common_attr__: common_attrs(unique_id, name, path, "models/schema.yml"),
            ..DbtTest::default()
        })
    }

    /// Each compiled file is written where dbt-fusion itself would put it —
    /// nested under the package name, and under `original_file_path` for the
    /// many-to-one resource types. Reading from a flatter path finds nothing
    /// and fails silently, so these assertions are the only thing standing
    /// between a wrong layout and an empty cache.
    #[test]
    fn build_sql_caches_picks_up_compiled_sql_for_each_node() {
        let project = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let in_dir = project.path();
        let out = dir.path();

        let mut nodes = Nodes::default();
        nodes
            .models
            .insert("model.shop.m".to_string(), make_model("model.shop.m", "m", "models/m.sql"));
        write(&out.join("compiled/shop/models/m.sql"), "SELECT 1 -- compiled");

        // snapshot — both compiled SQL and raw snapshot file are checked.
        // Snapshots compile into a directory named after their source file.
        nodes.snapshots.insert(
            "snapshot.shop.s".to_string(),
            make_snapshot("snapshot.shop.s", "s", "snapshots/s.sql"),
        );
        write(&out.join("compiled/shop/snapshots/s.sql/s.sql"), "SELECT compiled_snap");
        write(&out.join("snapshots/s.sql"), "{% snapshot s %}...{% endsnapshot %}");

        // test — compiled SQL under the declaring YAML, plus the generated SQL
        nodes.tests.insert(
            "test.shop.t".to_string(),
            make_test("test.shop.t", "t", "generic_tests/t.sql"),
        );
        write(
            &out.join("compiled/shop/models/schema.yml/generic_tests/t.sql"),
            "SELECT compiled_test",
        );
        write(&out.join("generic_tests/t.sql"), "SELECT generated_test_sql");

        let (compiled, snapshots, tests) = build_sql_caches(&nodes, in_dir, out).unwrap();

        assert_eq!(compiled["model.shop.m"], "SELECT 1 -- compiled");
        assert_eq!(compiled["snapshot.shop.s"], "SELECT compiled_snap");
        assert_eq!(compiled["test.shop.t"], "SELECT compiled_test");

        // Only snapshot nodes contribute their raw file.
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots["snapshot.shop.s"], "{% snapshot s %}...{% endsnapshot %}");
        assert!(!snapshots.contains_key("model.shop.m"));

        // Test SQL cache is just for tests with on-disk generated SQL.
        assert_eq!(tests.len(), 1);
        assert_eq!(tests["test.shop.t"], "SELECT generated_test_sql");
        assert!(!tests.contains_key("model.shop.m"));
    }

    /// Two packages may each declare `models/stg_orders.sql`. Keyed by that
    /// path, the second node's SQL replaced the first and both then ran the
    /// same body — one of them a model it does not belong to.
    #[test]
    fn build_sql_caches_keeps_same_named_nodes_from_different_packages_apart() {
        let project = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path();

        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.shop.stg_orders".to_string(),
            make_model("model.shop.stg_orders", "stg_orders", "models/stg_orders.sql"),
        );
        nodes.models.insert(
            "model.vendor.stg_orders".to_string(),
            make_model("model.vendor.stg_orders", "stg_orders", "models/stg_orders.sql"),
        );
        write(&out.join("compiled/shop/models/stg_orders.sql"), "SELECT 'shop'");
        write(&out.join("compiled/vendor/models/stg_orders.sql"), "SELECT 'vendor'");

        let (compiled, _, _) = build_sql_caches(&nodes, project.path(), out).unwrap();

        assert_eq!(compiled["model.shop.stg_orders"], "SELECT 'shop'");
        assert_eq!(compiled["model.vendor.stg_orders"], "SELECT 'vendor'");
    }

    #[test]
    fn build_sql_caches_skips_missing_files() {
        // out_dir is empty — nothing on disk for any node. A file a node never
        // produced is ordinary, so the caches come back empty rather than
        // erroring; the workflow re-renders SQL when the cache misses. Any
        // other I/O failure is reported, since the resolve output is deleted
        // after this and a skipped file is gone for the worker's lifetime.
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path();

        let mut nodes = Nodes::default();
        nodes
            .models
            .insert("model.shop.m".to_string(), make_model("model.shop.m", "m", "models/m.sql"));

        let (compiled, snapshots, tests) = build_sql_caches(&nodes, dir.path(), out).unwrap();
        assert!(compiled.is_empty());
        assert!(snapshots.is_empty());
        assert!(tests.is_empty());
    }

    #[test]
    fn build_sql_caches_returns_empty_for_empty_node_set() {
        let dir = tempfile::tempdir().unwrap();
        let nodes = Nodes::default();
        let (compiled, snapshots, tests) =
            build_sql_caches(&nodes, dir.path(), dir.path()).unwrap();
        assert!(compiled.is_empty());
        assert!(snapshots.is_empty());
        assert!(tests.is_empty());
    }
}
