//! Which nodes a freshness plan may hold, and how a measured age is judged.
//!
//! Sources and models share one set of rules here because dbt models them
//! through `FreshnessNodeRef`, whose accessors already encode each type's
//! resolution order (a model reads `loaded_at_field` off its `freshness`
//! block, a source off the source config, where the parser has already folded
//! the nested key in). Following the trait rather than reaching into the attrs
//! keeps the `source-freshness` and `freshness` commands from drifting apart.

use chrono::{DateTime, Utc};
use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod, FreshnessRules};
use dbt_schemas::schemas::freshness_node::FreshnessNodeRef;

use crate::error::DbtTemporalError;
use crate::types::FreshnessOutcome;

use super::FreshnessVerdict;

/// Where a node's freshness is measured from.
///
/// dbt treats the empty string as "unset" for both keys — the parser writes
/// `""` into the sibling key when the nested one wins — and `loaded_at_query`
/// takes precedence when a node carries both.
pub enum LoadedAt<'a> {
    Field(&'a str),
    Query(&'a str),
}

pub fn loaded_at(node: &dyn FreshnessNodeRef) -> Option<LoadedAt<'_>> {
    let query = node.get_loaded_at_query();
    if !query.is_empty() {
        return Some(LoadedAt::Query(query));
    }
    let field = node.get_loaded_at_field();
    if !field.is_empty() {
        return Some(LoadedAt::Field(field));
    }
    None
}

/// Whether this node can be freshness-checked: it needs criteria with at least
/// one fully populated rule, plus a loaded_at field or query.
///
/// The loaded_at requirement is stricter than dbt, which falls back to a batch
/// `INFORMATION_SCHEMA` metadata query for nodes that name neither. That path
/// needs the adapter's metadata interface, which dbt-temporal does not drive —
/// so such nodes are excluded from the plan (and named in a warning) rather
/// than measured against relation metadata.
pub fn node_has_freshness_check(node: &dyn FreshnessNodeRef) -> bool {
    has_freshness_rule(&node.freshness_criteria()) && loaded_at(node).is_some()
}

/// At least one warn_after/error_after rule with both `count` and `period`.
///
/// An empty rule object (`error_after: {}`) is semantically absent — the same
/// rule dbt's own `FreshnessRules::validate` applies — and a half-filled rule
/// is rejected by [`validate_freshness_rules`] before it ever reaches here.
fn has_freshness_rule(criteria: &FreshnessDefinition) -> bool {
    [&criteria.warn_after, &criteria.error_after]
        .into_iter()
        .flatten()
        .any(|r| r.count.is_some() && r.period.is_some())
}

/// Reject a rule that names `count` without `period` (or the reverse).
///
/// dbt only *warns* about this at parse time so that `run`/`build` are not
/// aborted by an SLA they never consult, and re-raises it when the rule is
/// actually used. A freshness plan is exactly that moment: without this the
/// half-filled rule would read as "no rule", and the node would be dropped
/// from the plan as unmeasurable — an SLA that is never checked looks exactly
/// like an SLA that always passes.
pub fn validate_freshness_rules(node: &dyn FreshnessNodeRef) -> Result<(), DbtTemporalError> {
    let criteria = node.freshness_criteria();
    for (name, rule) in [
        ("warn_after", criteria.warn_after.as_ref()),
        ("error_after", criteria.error_after.as_ref()),
    ] {
        let Some(rule) = rule else { continue };
        if rule.count.is_none() && rule.period.is_none() {
            continue;
        }
        if rule.count.is_none() || rule.period.is_none() {
            return Err(DbtTemporalError::Configuration(format!(
                "{} {}: freshness {name} needs both count and period, got count: {:?}, \
                 period: {:?}",
                node.kind_label().to_lowercase(),
                node.common().unique_id,
                rule.count,
                rule.period,
            )));
        }
    }
    Ok(())
}

/// A node that declares an SLA we cannot measure: rules are set, but neither
/// `loaded_at_field` nor `loaded_at_query` is, so only dbt's relation-metadata
/// path could evaluate it.
///
/// Worth naming in a warning rather than dropping silently, for the same
/// reason [`validate_freshness_rules`] exists.
pub fn declares_unmeasurable_sla(node: &dyn FreshnessNodeRef) -> bool {
    has_freshness_rule(&node.freshness_criteria()) && loaded_at(node).is_none()
}

/// Compare the row age against warn_after/error_after.
pub fn evaluate(
    criteria: &FreshnessDefinition,
    resource_type: &str,
    max_loaded_at: DateTime<Utc>,
    snapshotted_at: DateTime<Utc>,
) -> FreshnessVerdict {
    let age_secs = (snapshotted_at - max_loaded_at).as_seconds_f64();

    let outcome = |status: &str| FreshnessOutcome {
        max_loaded_at: max_loaded_at.to_rfc3339(),
        snapshotted_at: snapshotted_at.to_rfc3339(),
        max_loaded_at_time_ago_in_s: age_secs,
        status: status.to_string(),
        resource_type: resource_type.to_string(),
        criteria: criteria.clone(),
    };

    // Thresholds are whole seconds (count × period). Widening through u32 keeps
    // the f64 comparison exact for every rule that fits, which is every rule
    // short of ~136 years. The two ways out of that range are opposites, so
    // they must not share a fallback: a negative count (dbt accepts one) means
    // nothing can be fresh enough, an oversized one that nothing can be stale.
    let exceeded =
        |limit: i64| u32::try_from(limit).map_or(limit < 0, |secs| age_secs > f64::from(secs));

    if let Some(error_secs) = threshold_secs(criteria.error_after.as_ref())
        && exceeded(error_secs)
    {
        return FreshnessVerdict::Stale {
            max_loaded_at: max_loaded_at.to_rfc3339(),
            age_secs,
            max_allowed_secs: error_secs,
        };
    }
    if let Some(warn_secs) = threshold_secs(criteria.warn_after.as_ref())
        && exceeded(warn_secs)
    {
        return FreshnessVerdict::Warning(outcome("warn"));
    }
    FreshnessVerdict::Fresh(outcome("pass"))
}

/// A rule's threshold in seconds; `None` for an absent or half-filled rule,
/// which has nothing to compare against.
///
/// `count` is whatever the YAML held, so the multiply saturates rather than
/// overflowing: nothing validates the magnitude, and a debug build would
/// otherwise panic inside the activity on a nonsense `count`.
fn threshold_secs(rule: Option<&FreshnessRules>) -> Option<i64> {
    let rule = rule?;
    let count = rule.count?;
    let period = rule.period.as_ref()?;
    Some(count.saturating_mul(match period {
        FreshnessPeriod::second => 1,
        FreshnessPeriod::minute => 60,
        FreshnessPeriod::hour => 60 * 60,
        FreshnessPeriod::day => 60 * 60 * 24,
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use super::super::test_util::{model_with, rules, source_with, ts};
    use dbt_schemas::schemas::common::ModelFreshnessRules;
    use dbt_schemas::schemas::nodes::{DbtModel, DbtSource};
    use dbt_schemas::schemas::properties::ModelFreshness;

    #[test]
    fn evaluate_passes_within_thresholds() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "source", ts("2026-06-12T11:30:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(
            matches!(verdict, FreshnessVerdict::Fresh(ref o)
                if o.status == "pass" && o.resource_type == "source"
                    && (o.max_loaded_at_time_ago_in_s - 1800.0).abs() < f64::EPSILON),
            "expected fresh: {verdict:?}"
        );
    }

    #[test]
    fn evaluate_warns_between_thresholds() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "model", ts("2026-06-12T06:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(ref o)
                if o.status == "warn" && o.resource_type == "model"));
    }

    #[test]
    fn evaluate_errors_past_error_after() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "model", ts("2026-06-01T12:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(
            matches!(verdict, FreshnessVerdict::Stale { ref max_loaded_at, age_secs, max_allowed_secs }
                if max_allowed_secs == 86_400
                    && age_secs > 86_400.0
                    && max_loaded_at.starts_with("2026-06-01")),
            "expected stale: {verdict:?}"
        );
    }

    #[test]
    fn evaluate_warn_only_never_errors() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::minute)),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "source", ts("2020-01-01T00:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(_)));
    }

    #[test]
    fn evaluate_ignores_empty_rules() {
        // `error_after: {}` deserializes to an empty rule — semantically
        // absent, must not panic in threshold_secs.
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(FreshnessRules::default()),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "source", ts("2020-01-01T00:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(_)));
    }

    #[test]
    fn evaluate_second_granularity_thresholds() {
        // `second`-period rules exercise the smallest FreshnessPeriod arm
        // (count * 1). 60s-old rows sit past a 30s warn but under a 90s error.
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(30, FreshnessPeriod::second)),
            error_after: Some(rules(90, FreshnessPeriod::second)),
            ..Default::default()
        };
        let verdict =
            evaluate(&criteria, "source", ts("2026-06-12T11:59:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(
            matches!(verdict, FreshnessVerdict::Warning(ref o) if o.status == "warn"),
            "expected warn: {verdict:?}"
        );
    }

    #[test]
    fn evaluate_thresholds_outside_u32_do_not_share_a_verdict() {
        // A negative count leaves nothing fresh enough; one past ~136 years
        // leaves nothing stale. Both fall out of the u32 comparison window and
        // must not collapse onto the same answer.
        let negative = FreshnessDefinition {
            error_after: Some(rules(-1, FreshnessPeriod::day)),
            ..Default::default()
        };
        assert!(matches!(
            evaluate(&negative, "source", ts("2026-06-12T12:00:00Z"), ts("2026-06-12T12:00:00Z")),
            FreshnessVerdict::Stale { .. }
        ));

        // Saturating rather than overflowing on a nonsense count.
        let astronomical = FreshnessDefinition {
            error_after: Some(rules(i64::MAX, FreshnessPeriod::day)),
            ..Default::default()
        };
        assert!(matches!(
            evaluate(
                &astronomical,
                "source",
                ts("1970-01-01T00:00:00Z"),
                ts("2026-06-12T12:00:00Z")
            ),
            FreshnessVerdict::Fresh(_)
        ));
    }

    #[test]
    fn threshold_secs_covers_every_period() {
        assert_eq!(threshold_secs(None), None);
        assert_eq!(threshold_secs(Some(&FreshnessRules::default())), None);
        assert_eq!(threshold_secs(Some(&rules(2, FreshnessPeriod::second))), Some(2));
        assert_eq!(threshold_secs(Some(&rules(2, FreshnessPeriod::minute))), Some(120));
        assert_eq!(threshold_secs(Some(&rules(2, FreshnessPeriod::hour))), Some(7200));
        assert_eq!(threshold_secs(Some(&rules(2, FreshnessPeriod::day))), Some(172_800));
    }

    #[test]
    fn source_needs_a_rule_and_a_loaded_at() {
        assert!(!node_has_freshness_check(&DbtSource::default()));

        // Criteria with a rule but no loaded_at anywhere: not checkable.
        let no_loaded_at = source_with(
            FreshnessDefinition {
                warn_after: Some(rules(1, FreshnessPeriod::hour)),
                ..Default::default()
            },
            None,
        );
        assert!(!node_has_freshness_check(&no_loaded_at));
        assert!(declares_unmeasurable_sla(&no_loaded_at));

        let checkable = source_with(
            FreshnessDefinition {
                warn_after: Some(rules(1, FreshnessPeriod::hour)),
                ..Default::default()
            },
            Some("loaded_at"),
        );
        assert!(node_has_freshness_check(&checkable));
        assert!(!declares_unmeasurable_sla(&checkable));

        // An empty rule object ({}) is semantically absent.
        let empty_rule = source_with(
            FreshnessDefinition {
                warn_after: Some(FreshnessRules::default()),
                ..Default::default()
            },
            Some("loaded_at"),
        );
        assert!(!node_has_freshness_check(&empty_rule));
        assert!(!declares_unmeasurable_sla(&empty_rule));

        // A `loaded_at_field: ""` sentinel means the field is unset.
        let blank_field = source_with(
            FreshnessDefinition {
                warn_after: Some(rules(1, FreshnessPeriod::hour)),
                ..Default::default()
            },
            Some(""),
        );
        assert!(!node_has_freshness_check(&blank_field));
    }

    #[test]
    fn model_needs_an_sla_and_a_loaded_at() {
        assert!(!node_has_freshness_check(&DbtModel::default()));

        // build_after is a scheduling rule, not an SLA — never checkable.
        let build_after_only = model_with(ModelFreshness {
            build_after: Some(ModelFreshnessRules {
                count: Some(1),
                period: Some(FreshnessPeriod::day),
                updates_on: None,
            }),
            loaded_at_field: Some("updated_at".to_string()),
            ..Default::default()
        });
        assert!(!node_has_freshness_check(&build_after_only));
        assert!(!declares_unmeasurable_sla(&build_after_only));

        // An SLA without a loaded_at would need the metadata path we don't drive.
        let sla_without_loaded_at = model_with(ModelFreshness {
            error_after: Some(rules(12, FreshnessPeriod::hour)),
            ..Default::default()
        });
        assert!(!node_has_freshness_check(&sla_without_loaded_at));
        assert!(declares_unmeasurable_sla(&sla_without_loaded_at));

        let checkable = model_with(ModelFreshness {
            error_after: Some(rules(12, FreshnessPeriod::hour)),
            loaded_at_field: Some("updated_at".to_string()),
            ..Default::default()
        });
        assert!(node_has_freshness_check(&checkable));

        // A loaded_at_query is an equally valid measurement source.
        let by_query = model_with(ModelFreshness {
            warn_after: Some(rules(1, FreshnessPeriod::day)),
            loaded_at_query: Some("select max(updated_at), now() from {{ this }}".to_string()),
            ..Default::default()
        });
        assert!(node_has_freshness_check(&by_query));
    }

    #[test]
    fn half_filled_rules_are_rejected_not_ignored() {
        let ok = source_with(
            FreshnessDefinition {
                error_after: Some(rules(1, FreshnessPeriod::day)),
                warn_after: Some(FreshnessRules::default()),
                ..Default::default()
            },
            Some("loaded_at"),
        );
        assert!(validate_freshness_rules(&ok).is_ok());

        let count_only = model_with(ModelFreshness {
            error_after: Some(FreshnessRules {
                count: Some(1),
                period: None,
            }),
            loaded_at_field: Some("updated_at".to_string()),
            ..Default::default()
        });
        let err = validate_freshness_rules(&count_only).unwrap_err();
        assert!(err.to_string().contains("error_after needs both"), "{err}");
        assert!(!err.is_retryable(), "a misconfigured rule must not retry");

        let period_only = source_with(
            FreshnessDefinition {
                warn_after: Some(FreshnessRules {
                    count: None,
                    period: Some(FreshnessPeriod::hour),
                }),
                ..Default::default()
            },
            Some("loaded_at"),
        );
        let err = validate_freshness_rules(&period_only).unwrap_err();
        assert!(err.to_string().contains("warn_after needs both"), "{err}");
    }
}
