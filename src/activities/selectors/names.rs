//! Name-shaped selector methods: the FQN machinery behind a bare selector, and
//! the `<package>.<name>` family every resource-specific method shares.

use dbt_schemas::schemas::nodes::{DbtSource, InternalDbtNodeAttributes};
use dbt_schemas::schemas::telemetry::NodeType;

use super::glob;

/// A `<name>` or `<package>.<name>` selector value, split once at validation.
///
/// An omitted package is `*`, so `exposure:weekly_report` matches the exposure
/// whatever package defines it.
#[derive(Debug, Clone, Copy)]
pub struct QualifiedName<'a> {
    package: &'a str,
    name: &'a str,
}

impl<'a> QualifiedName<'a> {
    /// Split a value, or report the same shape complaint dbt raises.
    ///
    /// `kind` names the resource in the message ("exposure", "metric", …) so a
    /// rejected selector reads the way dbt's own error does.
    pub fn parse(value: &'a str, kind: &str) -> Result<Self, String> {
        let parts: Vec<&str> = value.split('.').collect();
        match parts.as_slice() {
            [name] => Ok(Self { package: "*", name }),
            [package, name] => Ok(Self { package, name }),
            _ => Err(format!(
                "{kind}s must be written `<{kind}_name>` or `<package_name>.<{kind}_name>`"
            )),
        }
    }

    /// True when a node of the right resource type carries this package and name.
    pub fn matches(self, package_name: &str, name: &str) -> bool {
        glob::fnmatch(self.package, package_name) && glob::fnmatch(self.name, name)
    }

    /// True when `node` is of `kind` and carries this package and name.
    ///
    /// The resource-type gate is what keeps `metric:orders` from selecting a
    /// model that happens to share the metric's name.
    pub fn matches_node(self, node: &dyn InternalDbtNodeAttributes, kind: NodeType) -> bool {
        node.resource_type() == kind
            && self.matches(&node.common().package_name, &node.common().name)
    }
}

/// A `source:` selector value: `<table>`, `<source>.<table>` or
/// `<package>.<source>.<table>`, split once at validation.
#[derive(Debug, Clone, Copy)]
pub struct SourceName<'a> {
    package: &'a str,
    source: &'a str,
    table: &'a str,
}

impl<'a> SourceName<'a> {
    pub fn parse(value: &'a str) -> Result<Self, String> {
        let parts: Vec<&str> = value.split('.').collect();
        match parts.as_slice() {
            // A single element names the *source*, and every table under it —
            // not the table, which is the reading the two- and three-part forms
            // invite.
            [source] => Ok(Self {
                package: "*",
                source,
                table: "*",
            }),
            [source, table] => Ok(Self {
                package: "*",
                source,
                table,
            }),
            [package, source, table] => Ok(Self {
                package,
                source,
                table,
            }),
            _ => Err("sources must be written `<source_name>`, `<source_name>.<table_name>` or \
                 `<package_name>.<source_name>.<table_name>`"
                .to_string()),
        }
    }

    pub fn matches(self, package_name: &str, source_name: &str, table_name: &str) -> bool {
        glob::fnmatch(self.package, package_name)
            && glob::fnmatch(self.source, source_name)
            && glob::fnmatch(self.table, table_name)
    }

    /// True when `node` is a source table this value names.
    ///
    /// The source name lives on the source-specific attributes rather than on
    /// the common ones, so the node has to be downcast to be asked; a node that
    /// is not a source fails the downcast and is out.
    pub fn matches_node(self, node: &dyn InternalDbtNodeAttributes) -> bool {
        let Some(source) = node.as_any().downcast_ref::<DbtSource>() else {
            return false;
        };
        self.matches(
            &source.__common_attr__.package_name,
            &source.__source_attr__.source_name,
            &source.__common_attr__.name,
        )
    }
}

/// Match a node against a bare (`fqn:`) selector value.
///
/// The shortcuts come first and are what make `--select my_model` work: the
/// value is tried against the node's name, its unique id, and — for a generic
/// test whose name was truncated to fit a relation identifier — the name the
/// user actually wrote. Only then does the package-aware FQN walk run.
///
/// Sources are excluded outright. dbt reaches them through `source:` alone, so
/// a bare name can never quietly pick up a source table that shares a model's
/// name.
pub fn match_fqn(pattern: &str, node: &dyn InternalDbtNodeAttributes) -> bool {
    if node.resource_type() == NodeType::Source {
        return false;
    }
    let common = node.common();
    if glob::fnmatch(pattern, &common.name) || glob::fnmatch(pattern, &common.unique_id) {
        return true;
    }
    if node
        .original_name()
        .is_some_and(|original| glob::fnmatch(pattern, original))
    {
        return true;
    }
    node_is_match(pattern, &common.fqn, node.is_versioned())
}

/// Match a selector against an FQN, with and without its leading package
/// element, so `staging.stg_orders` works without naming the package.
pub fn node_is_match(selector: &str, fqn: &[String], is_versioned: bool) -> bool {
    is_selected_node(fqn, selector, is_versioned)
        || (fqn.len() > 1 && is_selected_node(&fqn[1..], selector, is_versioned))
}

/// Match a selector against one FQN spelling.
///
/// An FQN is `[package, ...directories, name]` (plus a trailing version element
/// for a versioned model). The selector matches a *leading run* of it, which is
/// what keeps `--select staging` from also selecting a `staging_v2` sibling
/// while still letting a directory name select everything under it.
fn is_selected_node(fqn: &[String], selector: &str, is_versioned: bool) -> bool {
    // An empty FQN carries nothing to match; cross-project refs can reach here
    // without one.
    let Some(leaf) = fqn.last() else {
        return false;
    };

    // A wildcard with no dots is a leaf-name pattern: `stg_*` means "any model
    // called stg_something", not "anything under a directory called stg_*".
    if !selector.contains('.') && glob::has_special_chars(selector) {
        return glob::Pattern::new(selector).is_ok_and(|pattern| pattern.matches(leaf));
    }

    if is_versioned {
        // A versioned model's FQN ends `[…, name, v2]`, so its name is the
        // second-to-last element and `name.v2` is the selector spelling.
        if fqn.len() >= 2 && fqn[fqn.len() - 2] == selector {
            return true;
        }
        let normalized = match selector.rsplit_once('.') {
            Some((stem, version)) => format!("{stem}_{version}"),
            None => selector.to_string(),
        };
        if fqn.len() >= 2 && format!("{}_{}", fqn[fqn.len() - 2], fqn[fqn.len() - 1]) == normalized
        {
            return true;
        }
    } else if leaf == selector {
        return true;
    }

    // Directory names may themselves contain dots, so both sides are flattened
    // on `.` before the run is compared element by element.
    let flat_fqn: Vec<&str> = fqn.iter().flat_map(|element| element.split('.')).collect();
    let selector_parts: Vec<&str> = selector.split('.').collect();
    if flat_fqn.len() < selector_parts.len() {
        return false;
    }

    let mut wildcard_at = None;
    for (index, part) in selector_parts.iter().enumerate() {
        if glob::has_special_chars(part) {
            wildcard_at = Some(index);
            break;
        }
        if flat_fqn[index] != *part {
            return false;
        }
    }

    // Everything from the first wildcard on is matched as one dotted string, so
    // a `*` there spans element boundaries.
    if let Some(index) = wildcard_at {
        let fqn_tail = flat_fqn[index..].join(".");
        let selector_tail = selector_parts[index..].join(".");
        return glob::Pattern::new(&selector_tail).is_ok_and(|pattern| pattern.matches(&fqn_tail));
    }

    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn fqn(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_string()).collect()
    }

    #[test]
    fn node_is_match_handles_names_paths_and_wildcards() {
        let plain = fqn(&["package", "path", "model_name"]);
        assert!(node_is_match("model_name", &plain, false));
        assert!(node_is_match("path.model_name", &plain, false));
        assert!(node_is_match("package.path.model_name", &plain, false));
        assert!(node_is_match("model_*", &plain, false));
        assert!(node_is_match("path", &plain, false), "directory prefix");
        assert!(!node_is_match("other_model", &plain, false));
        assert!(!node_is_match("mode", &plain, false));
    }

    #[test]
    fn node_is_match_handles_versioned_models() {
        let versioned = fqn(&["package", "path", "model_name", "v1"]);
        assert!(node_is_match("model_name", &versioned, true));
        assert!(node_is_match("model_name.v1", &versioned, true));
        assert!(node_is_match("model_name_v1", &versioned, true));
        assert!(!node_is_match("model_name.v2", &versioned, true));
    }

    #[test]
    fn empty_fqn_never_matches() {
        for selector in [
            "model_name",
            "model_*",
            "*",
            "pkg.model",
            "model_[0-9]*",
            "?odel_name",
        ] {
            assert!(!node_is_match(selector, &[], false), "{selector}");
        }
        assert!(!node_is_match("model.v1", &[], true));
    }

    #[test]
    fn wildcard_inside_a_dotted_selector_spans_elements() {
        let deep = fqn(&["pkg", "marts", "finance", "revenue"]);
        assert!(node_is_match("marts.*", &deep, false));
        assert!(node_is_match("marts.finance.rev*", &deep, false));
        assert!(!node_is_match("marts.marketing.*", &deep, false));
    }

    #[test]
    fn qualified_name_defaults_the_package_to_any() {
        let bare = QualifiedName::parse("weekly", "exposure").unwrap();
        assert!(bare.matches("shop", "weekly"));
        assert!(bare.matches("other", "weekly"));
        assert!(!bare.matches("shop", "monthly"));

        let scoped = QualifiedName::parse("shop.weekly", "exposure").unwrap();
        assert!(scoped.matches("shop", "weekly"));
        assert!(!scoped.matches("other", "weekly"));

        let wild = QualifiedName::parse("week*", "exposure").unwrap();
        assert!(wild.matches("shop", "weekly"));

        let err = QualifiedName::parse("a.b.c", "exposure").unwrap_err();
        assert!(err.contains("exposure"), "{err}");
    }

    #[test]
    fn source_name_splits_one_two_and_three_parts() {
        let source_only = SourceName::parse("raw").unwrap();
        assert!(source_only.matches("shop", "raw", "orders"));
        assert!(source_only.matches("shop", "raw", "customers"));
        assert!(!source_only.matches("shop", "other", "orders"));

        let table = SourceName::parse("raw.orders").unwrap();
        assert!(table.matches("shop", "raw", "orders"));
        assert!(!table.matches("shop", "raw", "customers"));

        let full = SourceName::parse("shop.raw.orders").unwrap();
        assert!(full.matches("shop", "raw", "orders"));
        assert!(!full.matches("other", "raw", "orders"));

        assert!(SourceName::parse("a.b.c.d").is_err());
    }
}
