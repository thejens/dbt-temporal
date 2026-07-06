//! Source freshness checks (`source-freshness` command).
//!
//! Mirrors `dbt source freshness`: per source, run the bundled
//! `collect_freshness` macro (`select max(loaded_at_field), current_timestamp
//! from <source>`) — or `collect_freshness_custom_sql` for `loaded_at_query`
//! sources — then compare the age of the freshest row against the source's
//! `warn_after` / `error_after` rules.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessRules};
use dbt_schemas::schemas::nodes::DbtSource;

use crate::error::DbtTemporalError;
use crate::types::SourceFreshnessOutcome;

/// Freshness verdict before it's folded into the activity result.
#[derive(Debug)]
pub enum FreshnessVerdict {
    /// Within thresholds (or only informational): carry the outcome.
    Fresh(SourceFreshnessOutcome),
    /// warn_after exceeded but error_after not: succeed with a warning.
    Warning(SourceFreshnessOutcome),
    /// error_after exceeded.
    Stale {
        age_secs: f64,
        max_allowed_secs: i64,
    },
}

/// Whether this source can be freshness-checked: it needs criteria with at
/// least one populated rule, plus a loaded_at field or query. Sources without
/// these are excluded from `source-freshness` plans, matching dbt.
pub fn source_has_freshness_check(source: &DbtSource) -> bool {
    let attr = &source.__source_attr__;
    let Some(freshness) = &attr.freshness else {
        return false;
    };
    let has_rule = [&freshness.warn_after, &freshness.error_after]
        .into_iter()
        .flatten()
        .any(|r| r.count.is_some() && r.period.is_some());
    let has_loaded_at = freshness.loaded_at_field.is_some()
        || freshness.loaded_at_query.is_some()
        || attr.loaded_at_field.is_some()
        || attr.loaded_at_query.is_some();
    has_rule && has_loaded_at
}

/// Execute the freshness query for `source` through the Jinja macro layer and
/// evaluate the result against its criteria.
pub fn run_freshness_check(
    source: &DbtSource,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> Result<FreshnessVerdict, DbtTemporalError> {
    let unique_id = &source.__common_attr__.unique_id;
    let attr = &source.__source_attr__;
    let criteria = attr.freshness.as_ref().ok_or_else(|| {
        DbtTemporalError::Configuration(format!("source {unique_id} has no freshness criteria"))
    })?;

    // loaded_at on the freshness block wins over the source-level field,
    // matching dbt's resolution order.
    let loaded_at_field = criteria
        .loaded_at_field
        .as_deref()
        .or(attr.loaded_at_field.as_deref());
    let loaded_at_query = criteria
        .loaded_at_query
        .as_deref()
        .or(attr.loaded_at_query.as_deref());

    // The macros end in `return(load_result(...))` — the statement result is
    // consumed inside the macro, so re-reading it from the result store would
    // fail with "already loaded". Evaluate the macro as an expression and use
    // its return value instead.
    let mut ctx = node_context.clone();
    let expression = if let Some(field) = loaded_at_field {
        ctx.insert("__dbt_freshness_loaded_at__".to_owned(), minijinja::Value::from(field));
        ctx.insert(
            "__dbt_freshness_filter__".to_owned(),
            criteria
                .filter
                .as_deref()
                .map_or_else(|| minijinja::Value::from(()), minijinja::Value::from),
        );
        "collect_freshness(this, __dbt_freshness_loaded_at__, __dbt_freshness_filter__)"
    } else if let Some(query) = loaded_at_query {
        ctx.insert("__dbt_freshness_query__".to_owned(), minijinja::Value::from(query));
        "collect_freshness_custom_sql(this, __dbt_freshness_query__)"
    } else {
        return Err(DbtTemporalError::Configuration(format!(
            "source {unique_id} has freshness criteria but no loaded_at_field or \
             loaded_at_query — metadata-based freshness is not supported"
        )));
    };

    let result = jinja_env
        .compile_expression(expression)
        .map_err(|e| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "compiling freshness expression for {unique_id}: {e}"
            ))
        })?
        .eval(&ctx, &[])
        .map_err(|e| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "freshness query for {unique_id} failed: {e}"
            ))
        })?;

    let (max_loaded_at, snapshotted_at) = extract_timestamps(&result).map_err(|e| {
        DbtTemporalError::Adapter(anyhow::anyhow!("reading freshness result for {unique_id}: {e}"))
    })?;

    Ok(evaluate(criteria, max_loaded_at, snapshotted_at))
}

/// Compare the row age against warn_after/error_after.
fn evaluate(
    criteria: &FreshnessDefinition,
    max_loaded_at: DateTime<Utc>,
    snapshotted_at: DateTime<Utc>,
) -> FreshnessVerdict {
    let age_secs = (snapshotted_at - max_loaded_at).as_seconds_f64();

    let threshold = |rule: &Option<FreshnessRules>| -> Option<i64> {
        use dbt_schemas::schemas::common::FreshnessPeriod;
        let r = rule.as_ref()?;
        let count = r.count?;
        let period = r.period.as_ref()?;
        Some(
            count
                * match period {
                    FreshnessPeriod::second => 1,
                    FreshnessPeriod::minute => 60,
                    FreshnessPeriod::hour => 60 * 60,
                    FreshnessPeriod::day => 60 * 60 * 24,
                },
        )
    };

    let outcome = |status: &str| SourceFreshnessOutcome {
        max_loaded_at: max_loaded_at.to_rfc3339(),
        snapshotted_at: snapshotted_at.to_rfc3339(),
        max_loaded_at_time_ago_in_s: age_secs,
        status: status.to_string(),
        criteria: criteria.clone(),
    };

    // Thresholds are whole seconds (count × period); f64 holds them exactly
    // for any realistic rule, so the lossless u32-widened comparison applies.
    let exceeded = |limit: i64| age_secs > f64::from(u32::try_from(limit).unwrap_or(u32::MAX));

    if let Some(error_secs) = threshold(&criteria.error_after)
        && exceeded(error_secs)
    {
        return FreshnessVerdict::Stale {
            age_secs,
            max_allowed_secs: error_secs,
        };
    }
    if let Some(warn_secs) = threshold(&criteria.warn_after)
        && exceeded(warn_secs)
    {
        return FreshnessVerdict::Warning(outcome("warn"));
    }
    FreshnessVerdict::Fresh(outcome("pass"))
}

/// Pull `max_loaded_at` / `snapshotted_at` from the macro's returned result.
fn extract_timestamps(
    result: &minijinja::Value,
) -> Result<(DateTime<Utc>, DateTime<Utc>), anyhow::Error> {
    if result.is_none() || result.is_undefined() {
        anyhow::bail!("freshness query produced no result");
    }
    let table = result
        .get_attr("table")
        .map_err(|e| anyhow::anyhow!("freshness result table: {e}"))?;
    let rows = table
        .get_attr("rows")
        .map_err(|e| anyhow::anyhow!("freshness result rows: {e}"))?;
    let first_row = rows
        .get_item(&minijinja::Value::from(0))
        .map_err(|_| anyhow::anyhow!("freshness query returned no rows"))?;

    let cell = |name: &str| -> Result<minijinja::Value, anyhow::Error> {
        let v = first_row
            .get_item(&minijinja::Value::from(name))
            .map_err(|e| anyhow::anyhow!("column {name}: {e}"))?;
        if v.is_none() || v.is_undefined() {
            anyhow::bail!("freshness query returned NULL for {name} — is the source table empty?");
        }
        Ok(v)
    };

    let max_loaded_at = parse_timestamp(&cell("max_loaded_at")?.to_string())?;
    let snapshotted_at = parse_timestamp(&cell("snapshotted_at")?.to_string())?;
    Ok((max_loaded_at, snapshotted_at))
}

/// Parse warehouse timestamp strings. Naive timestamps (and bare dates) are
/// taken as UTC — both query columns come from the same warehouse clock, so a
/// consistent assumption keeps the age arithmetic correct.
fn parse_timestamp(raw: &str) -> Result<DateTime<Utc>, anyhow::Error> {
    let s = raw.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f %z", "%Y-%m-%dT%H:%M:%S%.f%z"] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Ok(dt.with_timezone(&Utc));
        }
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(naive.and_utc());
        }
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(midnight) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(midnight.and_utc());
    }
    anyhow::bail!("cannot parse timestamp {raw:?}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use dbt_schemas::schemas::common::FreshnessPeriod;

    fn rules(count: i64, period: FreshnessPeriod) -> FreshnessRules {
        FreshnessRules {
            count: Some(count),
            period: Some(period),
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn evaluate_passes_within_thresholds() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        };
        let verdict = evaluate(&criteria, ts("2026-06-12T11:30:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(
            matches!(verdict, FreshnessVerdict::Fresh(ref o)
                if o.status == "pass" && (o.max_loaded_at_time_ago_in_s - 1800.0).abs() < f64::EPSILON),
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
        let verdict = evaluate(&criteria, ts("2026-06-12T06:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(ref o) if o.status == "warn"));
    }

    #[test]
    fn evaluate_errors_past_error_after() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        };
        let verdict = evaluate(&criteria, ts("2026-06-01T12:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(
            matches!(verdict, FreshnessVerdict::Stale { age_secs, max_allowed_secs }
                if max_allowed_secs == 86_400 && age_secs > 86_400.0),
            "expected stale: {verdict:?}"
        );
    }

    #[test]
    fn evaluate_warn_only_never_errors() {
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::minute)),
            ..Default::default()
        };
        let verdict = evaluate(&criteria, ts("2020-01-01T00:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(_)));
    }

    #[test]
    fn evaluate_ignores_partial_rules() {
        // `error_after: {}` deserializes to an empty rule — semantically
        // absent, must not panic in to_seconds.
        let criteria = FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            error_after: Some(FreshnessRules::default()),
            ..Default::default()
        };
        let verdict = evaluate(&criteria, ts("2020-01-01T00:00:00Z"), ts("2026-06-12T12:00:00Z"));
        assert!(matches!(verdict, FreshnessVerdict::Warning(_)));
    }

    #[test]
    fn parse_timestamp_accepts_common_warehouse_formats() {
        for raw in [
            "2026-06-12T10:00:00+00:00",
            "2026-06-12T10:00:00Z",
            "2026-06-12 10:00:00.123 +0000",
            "2026-06-12T10:00:00.123456",
            "2026-06-12 10:00:00",
        ] {
            let dt = parse_timestamp(raw).unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(dt.date_naive().to_string(), "2026-06-12");
        }
        // Bare dates land at midnight UTC.
        assert_eq!(
            parse_timestamp("2026-06-12").unwrap().to_rfc3339(),
            "2026-06-12T00:00:00+00:00"
        );
        assert!(parse_timestamp("not a time").is_err());
    }

    fn empty_jinja_env() -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(minijinja::Environment::new())
    }

    #[test]
    fn run_freshness_check_requires_criteria() {
        use dbt_schemas::schemas::nodes::DbtSource;

        let source = DbtSource::default();
        let err = run_freshness_check(&source, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("no freshness criteria"));
    }

    #[test]
    fn run_freshness_check_requires_loaded_at() {
        use dbt_schemas::schemas::nodes::DbtSource;

        let mut source = DbtSource::default();
        let mut attr = source.__source_attr__.clone();
        attr.freshness = Some(FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            ..Default::default()
        });
        source.__source_attr__ = attr;
        let err = run_freshness_check(&source, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("no loaded_at_field"));
    }

    #[test]
    fn run_freshness_check_surfaces_query_errors() {
        use dbt_schemas::schemas::nodes::DbtSource;

        // An environment without the dbt macros: the collect_freshness call
        // fails at evaluation and must surface as a (retryable) adapter error.
        let mut source = DbtSource::default();
        let mut attr = source.__source_attr__.clone();
        attr.loaded_at_field = Some("loaded_at".to_string());
        attr.freshness = Some(FreshnessDefinition {
            error_after: Some(rules(1, FreshnessPeriod::day)),
            ..Default::default()
        });
        source.__source_attr__ = attr;
        let err = run_freshness_check(&source, &empty_jinja_env(), &BTreeMap::new()).unwrap_err();
        assert!(err.is_retryable(), "query failures must stay retryable: {err}");
    }

    #[test]
    fn extract_timestamps_reads_first_row() {
        let result = minijinja::Value::from_serialize(serde_json::json!({
            "table": {"rows": [{
                "max_loaded_at": "2026-06-12T10:00:00Z",
                "snapshotted_at": "2026-06-12T11:00:00Z",
            }]},
        }));
        let (max_loaded_at, snapshotted_at) = extract_timestamps(&result).unwrap();
        assert_eq!((snapshotted_at - max_loaded_at).num_seconds(), 3600);
    }

    #[test]
    fn extract_timestamps_rejects_missing_results() {
        assert!(extract_timestamps(&minijinja::Value::from(())).is_err());
        let no_rows = minijinja::Value::from_serialize(serde_json::json!({"table": {"rows": []}}));
        assert!(extract_timestamps(&no_rows).is_err());
        let null_cell = minijinja::Value::from_serialize(serde_json::json!({
            "table": {"rows": [{"max_loaded_at": null, "snapshotted_at": "2026-06-12T11:00:00Z"}]},
        }));
        let err = extract_timestamps(&null_cell).unwrap_err();
        assert!(err.to_string().contains("NULL"), "{err}");
    }

    #[test]
    fn source_freshness_check_requires_rule_and_loaded_at() {
        use dbt_schemas::schemas::nodes::DbtSource;

        let mut source = DbtSource::default();
        assert!(!source_has_freshness_check(&source));

        // Criteria with a rule but no loaded_at anywhere: not checkable.
        let mut src_attr = source.__source_attr__.clone();
        src_attr.freshness = Some(FreshnessDefinition {
            warn_after: Some(rules(1, FreshnessPeriod::hour)),
            ..Default::default()
        });
        source.__source_attr__ = src_attr;
        assert!(!source_has_freshness_check(&source));

        // Add a source-level loaded_at_field: checkable.
        let mut src_attr = source.__source_attr__.clone();
        src_attr.loaded_at_field = Some("loaded_at".to_string());
        source.__source_attr__ = src_attr;
        assert!(source_has_freshness_check(&source));

        // Rule present but empty ({}): not checkable.
        let mut src_attr = source.__source_attr__.clone();
        src_attr.freshness = Some(FreshnessDefinition {
            warn_after: Some(FreshnessRules::default()),
            ..Default::default()
        });
        source.__source_attr__ = src_attr;
        assert!(!source_has_freshness_check(&source));
    }
}
