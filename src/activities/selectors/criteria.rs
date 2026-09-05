//! The one place a selector method is turned into a decision about a node.
//!
//! Every criterion is *parsed* before it is matched. Parsing is the only gate:
//! a method or value this planner cannot evaluate has no variant to parse into,
//! so "is this supported?" and "does this node match?" are answers to the same
//! question and cannot drift apart. That matters because an unevaluable
//! criterion which slips through matches nothing, and matching nothing is
//! invisible in the two positions that count — inside a union it silently
//! under-selects, and in `--exclude` it silently excludes nothing.

use dbt_common::node_selector::{MethodName, SelectionCriteria};
use dbt_schemas::schemas::nodes::InternalDbtNodeAttributes;
use dbt_schemas::schemas::telemetry::NodeType;

use super::state::StateSelector;
use super::{config, glob, kinds, names, paths};

/// Everything outside the node itself that a criterion may need.
#[derive(Debug, Clone, Copy)]
pub struct MatchContext<'a> {
    /// State-comparison sets, absent when no previous manifest was loaded.
    pub state: Option<&'a StateSelector>,
    /// The root project name, which `package:this` resolves to.
    pub project_name: Option<&'a str>,
}

/// The `state:` sets this planner computes from a previous manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSet {
    New,
    Modified,
    Old,
    Unmodified,
}

/// The `modified.<sub>` refinements dbt defines.
///
/// All of them coarsen to the full modified set here: over-selecting rebuilds
/// more than asked, which is the safe direction for a CI build. They are still
/// spelled out so a typo is rejected rather than quietly selecting everything
/// modified under a name that means nothing.
const MODIFIED_SUBTYPES: [&str; 6] = [
    "body",
    "configs",
    "relation",
    "persisted_descriptions",
    "macros",
    "contract",
];

impl StateSet {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "new" => Ok(Self::New),
            "old" => Ok(Self::Old),
            "unmodified" => Ok(Self::Unmodified),
            "modified" => Ok(Self::Modified),
            _ => match value.strip_prefix("modified.") {
                Some(sub) if MODIFIED_SUBTYPES.contains(&sub) => Ok(Self::Modified),
                _ => Err(format!(
                    "state:{value} — must be one of new, modified[.<{}>], old, unmodified",
                    MODIFIED_SUBTYPES.join("|")
                )),
            },
        }
    }

    fn matches(self, unique_id: &str, state: &StateSelector) -> bool {
        match self {
            Self::New => state.new.contains(unique_id),
            Self::Modified => state.modified.contains(unique_id),
            Self::Old => state.existing.contains(unique_id),
            Self::Unmodified => {
                state.existing.contains(unique_id) && !state.modified.contains(unique_id)
            }
        }
    }
}

/// A selector criterion this planner knows how to evaluate.
#[derive(Debug, Clone)]
pub enum Criterion<'a> {
    Access(&'a str),
    Config(config::ConfigCriterion<'a>),
    Exposure(names::QualifiedName<'a>),
    File(&'a str),
    Function(names::QualifiedName<'a>),
    Fqn(&'a str),
    Group(&'a str),
    Metric(names::QualifiedName<'a>),
    Package(&'a str),
    Path(&'a str),
    ResourceType(&'a str),
    SavedQuery(names::QualifiedName<'a>),
    SemanticModel(names::QualifiedName<'a>),
    Source(names::SourceName<'a>),
    State(StateSet),
    Tag(&'a str),
    TestName(&'a str),
    TestType(kinds::TestType),
    UnitTest(names::QualifiedName<'a>),
    Version(kinds::VersionSelector),
}

impl<'a> Criterion<'a> {
    /// Read one criterion, or say why this planner cannot evaluate it.
    ///
    /// Two things are rejected here. A method whose answer needs data no worker
    /// holds when it plans a run — a previous run's artifacts, dbt's internal
    /// column lineage, a `selectors.yml` this worker does not read — and a
    /// value a supported method cannot read, which names no resource at all.
    /// Both would otherwise reach the matcher and quietly select nothing.
    pub fn parse(criteria: &'a SelectionCriteria) -> Result<Self, String> {
        let method = criteria.method;
        // A value that is not a scalar (a mapping or sequence written under a
        // `selectors.yml` definition) carries no string to match against.
        let Some(value) = criteria.value.as_str() else {
            return Err(format!("{method} (non-scalar value)"));
        };
        match method {
            MethodName::Access => Ok(Self::Access(value)),
            MethodName::Config => {
                Ok(Self::Config(config::ConfigCriterion::parse(&criteria.method_args, value)?))
            }
            MethodName::Exposure => {
                Ok(Self::Exposure(names::QualifiedName::parse(value, "exposure")?))
            }
            MethodName::File => Ok(Self::File(value)),
            MethodName::Function => {
                Ok(Self::Function(names::QualifiedName::parse(value, "function")?))
            }
            MethodName::Fqn => Ok(Self::Fqn(value)),
            MethodName::Group => Ok(Self::Group(value)),
            MethodName::Metric => Ok(Self::Metric(names::QualifiedName::parse(value, "metric")?)),
            MethodName::Package => Ok(Self::Package(value)),
            MethodName::Path => Ok(Self::Path(value)),
            MethodName::ResourceType => Ok(Self::ResourceType(value)),
            MethodName::SavedQuery => {
                Ok(Self::SavedQuery(names::QualifiedName::parse(value, "saved_query")?))
            }
            MethodName::SemanticModel => {
                Ok(Self::SemanticModel(names::QualifiedName::parse(value, "semantic_model")?))
            }
            MethodName::Source => Ok(Self::Source(names::SourceName::parse(value)?)),
            MethodName::State => Ok(Self::State(StateSet::parse(value)?)),
            MethodName::Tag => Ok(Self::Tag(value)),
            MethodName::TestName => Ok(Self::TestName(value)),
            MethodName::TestType => Ok(Self::TestType(kinds::TestType::parse(value)?)),
            MethodName::UnitTest => {
                Ok(Self::UnitTest(names::QualifiedName::parse(value, "unit_test")?))
            }
            MethodName::Version => Ok(Self::Version(kinds::VersionSelector::parse(value)?)),
            MethodName::Result => {
                Err(format!("{method} (needs run_results.json from a previous run)"))
            }
            MethodName::SourceStatus => {
                Err(format!("{method} (needs sources.json from a previous source freshness run)"))
            }
            MethodName::Column => {
                Err(format!("{method} (dbt-internal column lineage, not a run selector)"))
            }
            MethodName::Selector => Err(format!(
                "{method} (names a selectors.yml definition, which is not read — \
                 pass the expanded selector string instead)"
            )),
        }
    }

    /// True when this node satisfies the criterion, graph operators aside.
    pub fn matches(
        &self,
        unique_id: &str,
        node: &dyn InternalDbtNodeAttributes,
        ctx: MatchContext<'_>,
    ) -> bool {
        let common = node.common();
        match self {
            Self::Access(value) => node
                .get_access()
                .is_some_and(|access| access.to_string() == *value),
            // Matched against the node's serialized config rather than a typed
            // field, which is what makes every config key reachable.
            Self::Config(criterion) => criterion.matches(&node.serialized_config()),
            Self::Exposure(name) => name.matches_node(node, NodeType::Exposure),
            Self::File(value) => paths::match_file(value, &common.original_file_path),
            Self::Function(name) => name.matches_node(node, NodeType::Function),
            Self::Fqn(value) => names::match_fqn(value, node),
            Self::Group(value) => node.get_group().as_deref() == Some(*value),
            Self::Metric(name) => name.matches_node(node, NodeType::Metric),
            Self::Package(value) => {
                // `this` is dbt's alias for the root project's own package.
                let pattern = if *value == "this" {
                    ctx.project_name.unwrap_or(value)
                } else {
                    value
                };
                glob::fnmatch(pattern, &common.package_name)
            }
            Self::Path(value) => {
                paths::match_path(value, &common.original_file_path, common.patch_path.as_deref())
            }
            Self::ResourceType(value) => kinds::match_resource_type(value, node),
            Self::SavedQuery(name) => name.matches_node(node, NodeType::SavedQuery),
            Self::SemanticModel(name) => name.matches_node(node, NodeType::SemanticModel),
            Self::Source(name) => name.matches_node(node),
            // Without a previous manifest there are no state sets, so nothing
            // matches. The planner refuses a `state:` selector before it gets
            // this far, so this is not a silent empty selection.
            Self::State(set) => ctx.state.is_some_and(|state| set.matches(unique_id, state)),
            Self::Tag(value) => common.tags.iter().any(|tag| glob::fnmatch(value, tag)),
            Self::TestName(value) => kinds::match_test_name(value, node),
            Self::TestType(test_type) => test_type.matches(node),
            Self::UnitTest(name) => name.matches_node(node, NodeType::UnitTest),
            Self::Version(selector) => selector.matches(node),
        }
    }
}

/// Why a single criterion cannot be evaluated, or `None` when it can.
///
/// Defined in terms of [`Criterion::parse`] so that it answers exactly the
/// question the matcher asks — there is no second list of supported methods to
/// keep in step.
pub fn criterion_support_error(criteria: &SelectionCriteria) -> Option<String> {
    Criterion::parse(criteria).err()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use dbt_common::node_selector::parse_model_specifiers;
    use dbt_common::node_selector::{SelectExpression, SelectionValue};

    use crate::activities::selectors::test_support::{
        exposure_node, function_node, generic_test_node, metric_node, model_node,
        model_with_config, saved_query_node, semantic_model_node, source_node, tagged_model_node,
        unit_test_node,
    };

    /// Parse a selector string down to its single criterion.
    fn criteria(selector: &str) -> SelectionCriteria {
        let parsed = parse_model_specifiers(&[selector.to_string()])
            .unwrap_or_else(|e| panic!("{selector} should parse: {e}"));
        match parsed {
            SelectExpression::Atom(criteria) => criteria,
            other => panic!("{selector} parsed to {other:?}, expected a single atom"),
        }
    }

    /// True when `selector` selects `node`, with no state and no root project.
    fn selects(selector: &str, node: &dyn InternalDbtNodeAttributes) -> bool {
        let criteria = criteria(selector);
        let criterion = Criterion::parse(&criteria)
            .unwrap_or_else(|e| panic!("{selector} should be supported: {e}"));
        criterion.matches(
            &node.common().unique_id,
            node,
            MatchContext {
                state: None,
                project_name: None,
            },
        )
    }

    /// The message a rejected selector produces.
    fn rejection(selector: &str) -> String {
        let criteria = criteria(selector);
        criterion_support_error(&criteria)
            .unwrap_or_else(|| panic!("{selector} should have been rejected"))
    }

    #[test]
    fn access_matches_the_declared_access_level() {
        let public = model_node("model.shop.pub", "pub");
        assert!(selects("access:protected", &*public), "the schema default");
        assert!(!selects("access:public", &*public));
    }

    #[test]
    fn config_reaches_arbitrary_keys() {
        let node =
            model_with_config("model.shop.m", "m", "materialized: incremental\nschema: audit\n");
        assert!(selects("config.materialized:incremental", &*node));
        assert!(!selects("config.materialized:view", &*node));
        assert!(selects("config.schema:audit", &*node));
        assert!(!selects("config.schema:staging", &*node));
    }

    #[test]
    fn a_config_criterion_naming_no_key_is_rejected() {
        assert!(rejection("config:materialized").contains("config"));
    }

    #[test]
    fn resource_specific_methods_gate_on_the_resource_type() {
        let exposure = exposure_node("exposure.shop.weekly", "weekly");
        assert!(selects("exposure:weekly", &*exposure));
        assert!(!selects("exposure:monthly", &*exposure));
        assert!(!selects("exposure:weekly", &*model_node("model.shop.weekly", "weekly")));

        let metric = metric_node("metric.shop.revenue", "revenue");
        assert!(selects("metric:revenue", &*metric));
        assert!(!selects("metric:cost", &*metric));
        assert!(!selects("metric:revenue", &*exposure));

        let saved_query = saved_query_node("saved_query.shop.top", "top");
        assert!(selects("saved_query:top", &*saved_query));
        assert!(!selects("saved_query:bottom", &*saved_query));

        let semantic = semantic_model_node("semantic_model.shop.orders", "orders");
        assert!(selects("semantic_model:orders", &*semantic));
        assert!(!selects("semantic_model:customers", &*semantic));

        let function = function_node("function.shop.to_cents", "to_cents");
        assert!(selects("function:to_cents", &*function));
        assert!(!selects("function:to_dollars", &*function));

        let unit_test = unit_test_node("unit_test.shop.u_orders", "u_orders");
        assert!(selects("unit_test:u_orders", &*unit_test));
        assert!(!selects("unit_test:u_customers", &*unit_test));
    }

    #[test]
    fn a_qualified_name_may_carry_the_package() {
        let exposure = exposure_node("exposure.shop.weekly", "weekly");
        assert!(selects("exposure:shop.weekly", &*exposure));
        assert!(!selects("exposure:other.weekly", &*exposure));
        assert!(rejection("exposure:a.b.c").contains("exposure"));
    }

    #[test]
    fn source_matches_source_and_table() {
        let orders = source_node("source.shop.raw.orders", "orders", "raw");
        assert!(selects("source:raw", &*orders), "every table under a source");
        assert!(selects("source:raw.orders", &*orders));
        assert!(selects("source:shop.raw.orders", &*orders));
        assert!(!selects("source:raw.customers", &*orders));
        assert!(!selects("source:staging", &*orders));
        // A model never answers to `source:`, whatever it is called.
        assert!(!selects("source:raw", &*model_node("model.shop.raw", "raw")));
        assert!(rejection("source:a.b.c.d").contains("source"));
    }

    #[test]
    fn file_matches_the_file_name_and_its_stem() {
        let node = model_node("model.shop.orders", "orders");
        assert!(selects("file:orders.sql", &*node));
        assert!(selects("orders.sql", &*node), "a bare .sql value is a file selector");
        assert!(!selects("file:customers.sql", &*node));
    }

    #[test]
    fn group_matches_exactly() {
        let node = model_node("model.shop.m", "m");
        assert!(!selects("group:finance", &*node), "no group is set");
    }

    #[test]
    fn tag_accepts_wildcards() {
        let node = tagged_model_node("model.shop.m", "m", &["nightly", "tier_1"]);
        assert!(selects("tag:nightly", &*node));
        assert!(selects("tag:tier_*", &*node));
        assert!(!selects("tag:hourly", &*node));
    }

    #[test]
    fn package_this_resolves_to_the_root_project() {
        let node = model_node("model.shop.m", "m");
        let criteria = criteria("package:this");
        let criterion = Criterion::parse(&criteria).unwrap();
        let matches = |project: Option<&str>| {
            criterion.matches(
                "model.shop.m",
                &*node,
                MatchContext {
                    state: None,
                    project_name: project,
                },
            )
        };
        assert!(matches(Some("shop")));
        assert!(!matches(Some("marketing")));
    }

    #[test]
    fn test_name_and_test_type_reach_generic_tests() {
        let generic = generic_test_node("test.shop.nn_orders", "nn_orders", "not_null");
        assert!(selects("test_name:not_null", &*generic));
        assert!(!selects("test_name:unique", &*generic));
        assert!(selects("test_type:generic", &*generic));
        assert!(!selects("test_type:unit", &*generic));
    }

    #[test]
    fn state_sets_partition_the_previous_manifest() {
        let mut state = StateSelector::default();
        state.new.insert("model.shop.fresh".to_string());
        state.modified.insert("model.shop.fresh".to_string());
        state.modified.insert("model.shop.edited".to_string());
        state.existing.insert("model.shop.edited".to_string());
        state.existing.insert("model.shop.stable".to_string());

        let selects_state = |selector: &str, unique_id: &str| {
            let criteria = criteria(selector);
            Criterion::parse(&criteria).unwrap().matches(
                unique_id,
                &*model_node(unique_id, "n"),
                MatchContext {
                    state: Some(&state),
                    project_name: None,
                },
            )
        };

        assert!(selects_state("state:new", "model.shop.fresh"));
        assert!(!selects_state("state:new", "model.shop.edited"));
        assert!(selects_state("state:modified", "model.shop.edited"));
        assert!(!selects_state("state:modified", "model.shop.stable"));
        assert!(selects_state("state:old", "model.shop.edited"));
        assert!(!selects_state("state:old", "model.shop.fresh"));
        assert!(selects_state("state:unmodified", "model.shop.stable"));
        assert!(!selects_state("state:unmodified", "model.shop.edited"));
        assert!(!selects_state("state:unmodified", "model.shop.fresh"));
        // Every `modified.<sub>` coarsens to the full modified set.
        assert!(selects_state("state:modified.body", "model.shop.edited"));
    }

    #[test]
    fn a_misspelled_state_subselector_is_rejected() {
        let message = rejection("state:modified.bdoy");
        assert!(message.contains("state:modified.bdoy"), "{message}");
        assert!(message.contains("body"), "{message}");
    }

    /// The four methods that need data no worker holds at plan time.
    #[test]
    fn methods_needing_run_artifacts_are_rejected_by_name() {
        for (selector, method) in [
            ("result:success", "result"),
            ("source_status:fresher", "source_status"),
            ("column:model.shop.m.id", "column"),
            ("selector:nightly", "selector"),
        ] {
            let message = rejection(selector);
            assert!(message.contains(method), "{selector}: {message}");
        }
    }

    /// A `selectors.yml` definition can hold a mapping where a value belongs;
    /// there is no string to match, and no node may quietly match it.
    #[test]
    fn a_non_scalar_value_is_rejected() {
        let criteria = SelectionCriteria::new(
            MethodName::Tag,
            vec![],
            SelectionValue::Unsupported(Box::new(dbt_yaml::Value::null())),
            false,
            None,
            None,
            None,
            None,
        );
        let message = criterion_support_error(&criteria).expect("a non-scalar value is unusable");
        assert!(message.contains("tag"), "{message}");
        assert!(message.contains("non-scalar"), "{message}");
    }
}
