//! Selector methods that ask what kind of node this is: `resource_type:`,
//! `test_type:`, `test_name:` and `version:`.

use std::cmp::Ordering;

use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_schemas::schemas::nodes::{DbtTest, InternalDbtNodeAttributes};
use dbt_schemas::schemas::serde::StringOrInteger;
use dbt_schemas::schemas::telemetry::NodeType;

use super::glob;

/// Match a node against a `resource_type:` selector value.
///
/// `relation` is dbt's alias for "produces something in the warehouse". dbt
/// implements it as "not a test and not a check", which also sweeps in
/// exposures and metrics; reproduced as-is so the same selector picks the same
/// nodes here as it does under dbt.
pub fn match_resource_type(pattern: &str, node: &dyn InternalDbtNodeAttributes) -> bool {
    if pattern == "relation" {
        return !matches!(node.resource_type(), NodeType::Test | NodeType::Check);
    }
    pattern == node.resource_type().as_static_ref()
}

/// The four `test_type:` values dbt defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestType {
    Unit,
    Data,
    Singular,
    Generic,
}

impl TestType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "unit" => Ok(Self::Unit),
            "data" => Ok(Self::Data),
            "singular" => Ok(Self::Singular),
            "generic" => Ok(Self::Generic),
            other => {
                Err(format!("test_type:{other} — must be one of unit, data, singular, generic"))
            }
        }
    }

    /// True when the node is a test of this kind.
    ///
    /// Generic tests are told apart from singular ones by where their generated
    /// SQL was written, which is the only signal on the node: dbt renders every
    /// generic test under a `generic_tests` directory and writes singular tests
    /// from the file the user authored.
    pub fn matches(self, node: &dyn InternalDbtNodeAttributes) -> bool {
        match self {
            Self::Unit => node.resource_type() == NodeType::UnitTest,
            Self::Data => node.resource_type() == NodeType::Test,
            Self::Singular | Self::Generic => {
                if node.resource_type() != NodeType::Test {
                    return false;
                }
                let path = node.common().original_file_path.to_string_lossy();
                let is_generic = path.contains(DBT_GENERIC_TESTS_DIR_NAME);
                is_generic == (self == Self::Generic)
            }
        }
    }
}

/// Match a node against a `test_name:` selector value.
///
/// A generic test is matched on the name of the test macro behind it
/// (`not_null`, `accepted_values`, …) rather than on the generated node name,
/// which is what makes `test_name:not_null` select every not-null test in the
/// project. Singular and unit tests carry no such metadata and match on their
/// own name.
pub fn match_test_name(pattern: &str, node: &dyn InternalDbtNodeAttributes) -> bool {
    match node.resource_type() {
        NodeType::Test => {
            let macro_name = node
                .as_any()
                .downcast_ref::<DbtTest>()
                .and_then(|test| test.__test_attr__.test_metadata.as_ref())
                .map(|metadata| metadata.name.as_str());
            glob::fnmatch(pattern, macro_name.unwrap_or(&node.common().name))
        }
        NodeType::UnitTest => glob::fnmatch(pattern, &node.common().name),
        _ => false,
    }
}

/// The four `version:` values dbt defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelector {
    Latest,
    Prerelease,
    Old,
    None,
}

impl VersionSelector {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "latest" => Ok(Self::Latest),
            "prerelease" => Ok(Self::Prerelease),
            "old" => Ok(Self::Old),
            "none" => Ok(Self::None),
            other => Err(format!("version:{other} — must be one of latest, prerelease, old, none")),
        }
    }

    /// True when a model's declared version stands in this relation to the
    /// model group's latest version.
    ///
    /// Only models carry versions, so every other resource type is out.
    pub fn matches(self, node: &dyn InternalDbtNodeAttributes) -> bool {
        if node.resource_type() != NodeType::Model {
            return false;
        }
        // Read the declared version rather than guessing at a `_v2` file-name
        // suffix: a versioned model may be named anything, and an unversioned
        // one may still end in `_v2`.
        if self == Self::None {
            return node.version().is_none();
        }
        let (Some(version), Some(latest)) = (node.version(), node.latest_version()) else {
            return false;
        };
        let order = numeric_order(&version, &latest);
        match self {
            // Compared through their rendered form: YAML may spell one side
            // `2` and the other `"2"`, and they still name the same version.
            Self::Latest => version.to_string() == latest.to_string(),
            Self::Prerelease => order == Some(Ordering::Greater),
            Self::Old => order == Some(Ordering::Less),
            // Answered above, before a latest version is required.
            Self::None => false,
        }
    }
}

/// Order two versions numerically, or `None` when either is not a number.
///
/// A non-numeric version simply does not participate in `prerelease`/`old`
/// rather than aborting the plan.
fn numeric_order(version: &StringOrInteger, latest: &StringOrInteger) -> Option<Ordering> {
    Some(as_i64(version)?.cmp(&as_i64(latest)?))
}

fn as_i64(value: &StringOrInteger) -> Option<i64> {
    match value {
        StringOrInteger::Integer(number) => Some(*number),
        StringOrInteger::String(text) => text.parse().ok(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use crate::activities::selectors::test_support::{
        generic_test_node, model_node, seed_node, singular_test_node, unit_test_node,
        versioned_model_node,
    };

    #[test]
    fn resource_type_matches_the_bare_type_name() {
        assert!(match_resource_type("model", &*model_node("model.shop.m", "m")));
        assert!(!match_resource_type("seed", &*model_node("model.shop.m", "m")));
        assert!(match_resource_type("seed", &*seed_node("seed.shop.s", "s")));
        assert!(match_resource_type("unit_test", &*unit_test_node("unit_test.shop.u", "u")));
    }

    #[test]
    fn resource_type_relation_covers_everything_that_lands_in_the_warehouse() {
        assert!(match_resource_type("relation", &*model_node("model.shop.m", "m")));
        assert!(match_resource_type("relation", &*seed_node("seed.shop.s", "s")));
        assert!(!match_resource_type("relation", &*singular_test_node("test.shop.t", "t")));
    }

    #[test]
    fn test_type_tells_unit_data_singular_and_generic_apart() {
        let generic = generic_test_node("test.shop.g", "g", "not_null");
        let singular = singular_test_node("test.shop.s", "s");
        let unit = unit_test_node("unit_test.shop.u", "u");
        let model = model_node("model.shop.m", "m");

        assert!(TestType::parse("generic").unwrap().matches(&*generic));
        assert!(!TestType::parse("generic").unwrap().matches(&*singular));
        assert!(TestType::parse("singular").unwrap().matches(&*singular));
        assert!(!TestType::parse("singular").unwrap().matches(&*generic));
        assert!(TestType::parse("data").unwrap().matches(&*generic));
        assert!(!TestType::parse("data").unwrap().matches(&*unit));
        assert!(TestType::parse("unit").unwrap().matches(&*unit));
        assert!(!TestType::parse("unit").unwrap().matches(&*model));

        let err = TestType::parse("integration").unwrap_err();
        assert!(err.contains("test_type:integration"), "{err}");
    }

    #[test]
    fn test_name_matches_the_generic_macro_behind_the_test() {
        let generic =
            generic_test_node("test.shop.not_null_orders_id", "not_null_orders_id", "not_null");
        assert!(match_test_name("not_null", &*generic));
        assert!(match_test_name("not_*", &*generic));
        assert!(!match_test_name("unique", &*generic));
        // The generated node name is not the match target for a generic test.
        assert!(!match_test_name("not_null_orders_id", &*generic));

        let singular = singular_test_node("test.shop.assert_totals", "assert_totals");
        assert!(match_test_name("assert_totals", &*singular));
        assert!(!match_test_name("not_null", &*singular));

        let unit = unit_test_node("unit_test.shop.u_orders", "u_orders");
        assert!(match_test_name("u_orders", &*unit));

        assert!(!match_test_name("anything", &*model_node("model.shop.m", "m")));
    }

    #[test]
    fn version_compares_the_declared_version_to_the_latest() {
        let old = versioned_model_node("model.shop.orders.v1", "orders", Some("1"), Some("2"));
        let latest = versioned_model_node("model.shop.orders.v2", "orders", Some("2"), Some("2"));
        let prerelease =
            versioned_model_node("model.shop.orders.v3", "orders", Some("3"), Some("2"));

        assert!(VersionSelector::parse("old").unwrap().matches(&*old));
        assert!(!VersionSelector::parse("old").unwrap().matches(&*latest));
        assert!(VersionSelector::parse("latest").unwrap().matches(&*latest));
        assert!(!VersionSelector::parse("latest").unwrap().matches(&*old));
        assert!(
            VersionSelector::parse("prerelease")
                .unwrap()
                .matches(&*prerelease)
        );
        assert!(
            !VersionSelector::parse("prerelease")
                .unwrap()
                .matches(&*latest)
        );

        let err = VersionSelector::parse("newest").unwrap_err();
        assert!(err.contains("version:newest"), "{err}");
    }

    #[test]
    fn version_none_selects_models_with_no_declared_version() {
        let plain = model_node("model.shop.orders_v2", "orders_v2");
        let versioned =
            versioned_model_node("model.shop.orders.v2", "orders", Some("2"), Some("2"));

        assert!(VersionSelector::parse("none").unwrap().matches(&*plain));
        assert!(!VersionSelector::parse("none").unwrap().matches(&*versioned));
        // Only models are versioned at all.
        assert!(
            !VersionSelector::parse("none")
                .unwrap()
                .matches(&*seed_node("seed.shop.s", "s"))
        );
    }

    #[test]
    fn a_non_numeric_version_orders_against_nothing() {
        let dated =
            versioned_model_node("model.shop.orders.vq1", "orders", Some("2024-01"), Some("2"));
        assert!(!VersionSelector::parse("old").unwrap().matches(&*dated));
        assert!(
            !VersionSelector::parse("prerelease")
                .unwrap()
                .matches(&*dated)
        );
        assert_eq!(
            numeric_order(
                &StringOrInteger::String("2024-01".to_string()),
                &StringOrInteger::Integer(2)
            ),
            None
        );
        assert_eq!(
            numeric_order(&StringOrInteger::String("1".to_string()), &StringOrInteger::Integer(2)),
            Some(Ordering::Less)
        );
    }
}
