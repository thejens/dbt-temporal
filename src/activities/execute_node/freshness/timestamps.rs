//! Reading the two timestamps a freshness query returns.
//!
//! The `collect_freshness` macros return an agate table with a single row of
//! `max_loaded_at` / `snapshotted_at`. Both come out of the Jinja layer as
//! rendered strings rather than typed timestamps, so the warehouse's own
//! formatting is what has to be parsed here.

use chrono::{DateTime, Utc};

/// Pull `max_loaded_at` / `snapshotted_at` from the macro's returned result.
pub fn extract_timestamps(
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
            anyhow::bail!("freshness query returned NULL for {name} — is the relation empty?");
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
}
