//! File-location selector methods: `path:` and `file:`.

use std::path::{Component, Path, PathBuf};

use super::glob;

/// Resolve `.` and `..` in a selector path, the way dbt normalises one before
/// matching.
///
/// Leading `..` survive on purpose. Node paths are always project-relative, so
/// a selector that climbs out of the project must go on to match nothing —
/// collapsing it would quietly re-point it at a directory inside the project.
pub fn clean(path: &Path) -> PathBuf {
    let mut kept: Vec<Component<'_>> = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match kept.last() {
                Some(Component::Normal(_)) => {
                    kept.pop();
                }
                // `/..` is the root; there is nothing above it to climb to.
                Some(Component::RootDir | Component::Prefix(_)) => {}
                _ => kept.push(component),
            },
            other => kept.push(other),
        }
    }
    if kept.is_empty() {
        return PathBuf::from(".");
    }
    kept.iter().collect()
}

/// Match a node's source file against a `path:` selector value.
///
/// Two shapes, exactly as dbt evaluates them:
///
/// * A pattern (`models/*/staging`, `models/**/*.sql`) is glob-matched against
///   the node's file *and each of its parent directories*, so a pattern naming
///   a directory selects everything beneath it. Here — and only here — the
///   node's patch file (its schema YAML) is matched too.
/// * A plain path selects the node when it is the node's file (or its patch
///   file) outright, or is a directory the node's file sits under. Comparison
///   is on whole components, so `models/staging` does not reach into
///   `models/staging_v2/`.
///
/// The asymmetry around `patch_path` is dbt's, not an oversight: a *directory*
/// selector never selects through a patch file, only an exact or glob one does.
pub fn match_path(pattern: &str, node_path: &Path, patch_path: Option<&Path>) -> bool {
    let cleaned = clean(Path::new(pattern));
    let pattern = cleaned.to_string_lossy();
    let pattern = pattern.as_ref();

    if glob::has_special_chars(pattern) {
        return node_path
            .ancestors()
            .any(|ancestor| glob::fnmatch(pattern, &ancestor.to_string_lossy()))
            || patch_path.is_some_and(|p| glob::fnmatch(pattern, &p.to_string_lossy()));
    }

    let selector = Path::new(pattern);
    if node_path == selector || patch_path.is_some_and(|p| p == selector) {
        return true;
    }
    node_path.starts_with(selector)
}

/// Match a node's file name against a `file:` selector value.
///
/// Matched against the file name and against its stem, so both `orders.sql` and
/// `orders` name the same node. Note that a bare value ending in `.sql`, `.py`
/// or `.csv` parses as `file:` even without the prefix.
pub fn match_file(pattern: &str, node_path: &Path) -> bool {
    let Some(file_name) = node_path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if glob::fnmatch(pattern, file_name) {
        return true;
    }
    Path::new(file_name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| glob::fnmatch(pattern, stem))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn matches(pattern: &str, path: &str) -> bool {
        match_path(pattern, Path::new(path), None)
    }

    #[test]
    fn path_matches_exact_file_and_containing_directories() {
        let node = "models/staging/orders/order_items.sql";
        assert!(matches("models/staging/orders/order_items.sql", node));
        assert!(matches("models/staging/orders", node));
        assert!(matches("models/staging", node));
        assert!(matches("models", node));

        assert!(!matches("models/staging/customers", node));
        assert!(!matches("models/core", node));
    }

    #[test]
    fn path_compares_whole_components() {
        let node = "models/staging/orders/order_items.sql";
        assert!(!matches("models/stag", node));
        assert!(!matches("models/staging/ord", node));
        assert!(!matches("model", node));
        assert!(!matches("models/staging_v2", node));
        assert!(!matches("models/staging2", node));

        // Directories whose names contain dots are still whole components.
        let dotted = "models/staging.v2/stg_customers_v2.sql";
        assert!(matches("models/staging.v2", dotted));
        assert!(!matches("models/staging.v20", dotted));
        assert!(!matches("models/staging.v", dotted));
    }

    #[test]
    fn path_does_not_match_a_file_stem() {
        let node = "models/staging/stg_orders.sql";
        assert!(matches("models/staging/stg_orders.sql", node));
        assert!(!matches("models/staging/stg_orders", node));
        assert!(!matches("models/staging/stg_orders.sql.bak", node));
    }

    #[test]
    fn path_wildcards_match_the_file_and_its_parents() {
        let node = "models/staging/orders/order_items.sql";
        assert!(matches("models/*/orders/*.sql", node));
        assert!(matches("models/**/*items.sql", node));

        assert!(matches(
            "models/group_a/*/subdir/leaf",
            "models/group_a/one/subdir/leaf/model_a.sql"
        ));
        assert!(!matches("models/group_a/*/subdir/leaf", "models/group_a/one/other/model_c.sql"));
    }

    /// dbt reaches a node through its patch file for exact and glob selectors
    /// but never for a directory selector.
    #[test]
    fn patch_path_participates_only_in_exact_and_glob_matches() {
        let node = Path::new("models/staging/orders/order_items.sql");
        let patch = Path::new("patches/staging/orders/order_items.sql");

        assert!(match_path("patches/staging/orders/order_items.sql", node, Some(patch)));
        assert!(match_path("patches/*/orders/*.sql", node, Some(patch)));
        assert!(match_path("patches/**/*items.sql", node, Some(patch)));

        assert!(!match_path("patches/staging/orders", node, Some(patch)));
        assert!(!match_path("patches/staging", node, Some(patch)));

        // The node's own file keeps working for every selector shape.
        assert!(match_path("models/staging/orders", node, Some(patch)));
    }

    #[test]
    fn path_normalises_dot_and_parent_components() {
        let node = "models/staging/foo.sql";
        assert!(matches("./models/staging/foo.sql", node));
        assert!(matches("models/./staging/foo.sql", node));
        assert!(matches("./models/./staging/./foo.sql", node));
        assert!(matches("./models/staging", node));
        assert!(matches("./models", node));

        assert!(matches("models/staging/../foo.sql", "models/foo.sql"));
        assert!(matches("models/staging/..", "models/foo.sql"));
        assert!(matches("models/staging/../../seeds/foo.csv", "seeds/foo.csv"));
    }

    /// A selector that climbs out of the project cannot name a project node.
    #[test]
    fn leading_parent_components_are_preserved() {
        assert!(!matches("../models/foo.sql", "models/foo.sql"));
        assert!(!matches("../../models/foo.sql", "models/foo.sql"));
        assert_eq!(clean(Path::new("../a")), PathBuf::from("../a"));
        assert_eq!(clean(Path::new(".")), PathBuf::from("."));
        assert_eq!(clean(Path::new("/../a")), PathBuf::from("/a"));
    }

    #[test]
    fn file_matches_name_and_stem() {
        let node = Path::new("models/staging/orders/order_items.sql");
        assert!(match_file("order_items.sql", node));
        assert!(match_file("order_items", node));
        assert!(match_file("order_*.sql", node));
        assert!(match_file("*_items.sql", node));

        assert!(!match_file("customer_orders.sql", node));
        assert!(!match_file("items.sql", node));
        assert!(!match_file("order_items.sql", Path::new("")));
    }
}
