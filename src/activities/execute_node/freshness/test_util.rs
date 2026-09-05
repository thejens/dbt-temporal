//! Node builders shared by the freshness unit tests.
//!
//! Lives outside `mod tests` so the criteria and macro-invocation tests build
//! their fixtures the same way; a source's freshness config and a model's
//! `freshness` block are assembled differently enough that duplicating the
//! construction invites the two test modules to drift.

#![allow(clippy::redundant_pub_crate)]

use chrono::{DateTime, Utc};
use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod, FreshnessRules};
use dbt_schemas::schemas::nodes::{DbtModel, DbtSource};
use dbt_schemas::schemas::properties::ModelFreshness;

pub(crate) fn rules(count: i64, period: FreshnessPeriod) -> FreshnessRules {
    FreshnessRules {
        count: Some(count),
        period: Some(period),
    }
}

#[allow(clippy::expect_used)]
pub(crate) fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("test timestamp must be RFC3339")
        .with_timezone(&Utc)
}

/// A source whose freshness config resolves the way the parser leaves it:
/// `loaded_at_field` folded onto the source attrs, criteria beside it.
pub(crate) fn source_with(
    freshness: FreshnessDefinition,
    loaded_at_field: Option<&str>,
) -> DbtSource {
    let mut source = DbtSource::default();
    source.__source_attr__.freshness = Some(freshness);
    source.__source_attr__.loaded_at_field = loaded_at_field.map(str::to_string);
    source
}

pub(crate) fn model_with(freshness: ModelFreshness) -> DbtModel {
    let mut model = DbtModel::default();
    model.__model_attr__.freshness = Some(freshness);
    model
}
