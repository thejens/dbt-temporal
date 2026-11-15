//! The `expect` and `given` fixtures of a unit test: where their rows come
//! from, and how they are rendered as SQL.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use dbt_schemas::schemas::common::{Formats, Rows};
use dbt_schemas::schemas::nodes::DbtUnitTest;
use dbt_yaml::Value as YmlValue;

use crate::error::DbtTemporalError;

/// Inject the `expect` fixture into the unit test's config mapping so the
/// bundled `unit` materialization can read `config.get('expected_rows')` /
/// `config.get('expected_sql')`. `expected_rows` is always set (empty for
/// SQL-format expectations) because the materialization evaluates
/// `expected_rows | length` unconditionally.
pub fn inject_expected_config(
    config: &mut YmlValue,
    unit: &DbtUnitTest,
    in_dir: &Path,
) -> Result<(), DbtTemporalError> {
    let expect = &unit.__unit_test_attr__.expect;
    let fixture = resolve_fixture(
        expect.rows.as_ref(),
        &expect.format,
        expect.fixture.as_deref(),
        in_dir,
        &unit.__common_attr__.unique_id,
    )?;

    let YmlValue::Mapping(map, _) = config else {
        return Err(DbtTemporalError::Configuration(format!(
            "unit test {} config did not serialise to a mapping",
            unit.__common_attr__.unique_id
        )));
    };

    match fixture {
        FixtureData::Rows(rows) => {
            let rows_yml = dbt_yaml::to_value(&rows).map_err(|e| {
                DbtTemporalError::Configuration(format!(
                    "serialising expected rows for {}: {e}",
                    unit.__common_attr__.unique_id
                ))
            })?;
            map.insert(YmlValue::string("expected_rows".to_string()), rows_yml);
        }
        FixtureData::Sql(sql) => {
            map.insert(
                YmlValue::string("expected_rows".to_string()),
                YmlValue::sequence(Vec::new()),
            );
            map.insert(YmlValue::string("expected_sql".to_string()), YmlValue::string(sql));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(super) enum FixtureData {
    Rows(Vec<BTreeMap<String, YmlValue>>),
    Sql(String),
}

/// Resolve a `given`/`expect` fixture to rows or raw SQL, reading fixture
/// files from disk when referenced by name.
pub(super) fn resolve_fixture(
    rows: Option<&Rows>,
    format: &Formats,
    fixture: Option<&str>,
    in_dir: &Path,
    unique_id: &str,
) -> Result<FixtureData, DbtTemporalError> {
    match format {
        Formats::Dict => match rows {
            Some(Rows::List(list)) => Ok(FixtureData::Rows(list.clone())),
            _ => Err(DbtTemporalError::Compilation(format!(
                "unit test {unique_id}: dict-format fixture must provide rows as a list of \
                 mappings"
            ))),
        },
        Formats::Csv => {
            let content = fixture_content(rows, fixture, "csv", in_dir, unique_id)?;
            parse_csv_rows(&content)
                .map(FixtureData::Rows)
                .map_err(|e| {
                    DbtTemporalError::Compilation(format!(
                        "unit test {unique_id}: parsing csv fixture: {e}"
                    ))
                })
        }
        Formats::Sql => {
            let content = fixture_content(rows, fixture, "sql", in_dir, unique_id)?;
            Ok(FixtureData::Sql(content))
        }
    }
}

/// Inline rows string, or the contents of a named fixture file. dbt resolves
/// fixture names to `tests/fixtures/<name>.<ext>`; a path-like fixture value
/// relative to the project root is also accepted.
fn fixture_content(
    rows: Option<&Rows>,
    fixture: Option<&str>,
    ext: &str,
    in_dir: &Path,
    unique_id: &str,
) -> Result<String, DbtTemporalError> {
    if let Some(Rows::String(s)) = rows {
        return Ok(s.clone());
    }
    let Some(name) = fixture else {
        return Err(DbtTemporalError::Compilation(format!(
            "unit test {unique_id}: {ext}-format fixture needs inline rows or a fixture file"
        )));
    };
    let direct = in_dir.join(name);
    let conventional = in_dir
        .join("tests")
        .join("fixtures")
        .join(format!("{name}.{ext}"));
    for candidate in [&direct, &conventional] {
        if candidate.is_file() {
            return std::fs::read_to_string(candidate).map_err(|e| {
                DbtTemporalError::Compilation(format!(
                    "unit test {unique_id}: reading fixture {}: {e}",
                    candidate.display()
                ))
            });
        }
    }
    Err(DbtTemporalError::Compilation(format!(
        "unit test {unique_id}: fixture {name:?} not found (looked at {} and {})",
        direct.display(),
        conventional.display()
    )))
}

/// Parse CSV fixture content into dict rows with dbt's type inference:
/// empty → null, then i64, f64, bool, else string.
fn parse_csv_rows(data: &str) -> Result<Vec<BTreeMap<String, YmlValue>>, anyhow::Error> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(data.as_bytes());
    let headers = reader
        .headers()
        .map_err(|e| anyhow::anyhow!("reading headers: {e}"))?
        .clone();
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|e| anyhow::anyhow!("reading record: {e}"))?;
        let mut row = BTreeMap::new();
        for (i, field) in record.iter().enumerate() {
            let value = if field.is_empty() {
                YmlValue::null()
            } else if let Ok(v) = field.parse::<i64>() {
                YmlValue::number(v.into())
            } else if let Ok(v) = field.parse::<f64>() {
                YmlValue::number(v.into())
            } else if field.eq_ignore_ascii_case("true") {
                YmlValue::bool(true)
            } else if field.eq_ignore_ascii_case("false") {
                YmlValue::bool(false)
            } else {
                YmlValue::string(field.to_string())
            };
            row.insert(headers[i].to_string(), value);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Render fixture rows to SQL through the bundled `get_fixture_sql` macro
/// with `this` shadowed to the mocked input's relation. The macro probes
/// `adapter.get_columns_in_relation(this)` for column types, so the input
/// relation must already exist in the warehouse — the same requirement
/// dbt-core imposes.
pub(super) fn render_fixture_rows_sql(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    input_base: &dbt_schemas::schemas::nodes::NodeBaseAttributes,
    rows: &[BTreeMap<String, YmlValue>],
    unique_id: &str,
) -> Result<String, DbtTemporalError> {
    // The relation is the mocked input's, so it renders in *its* adapter's
    // dialect — the one the relation actually lives on.
    let relation = dbt_adapter::relation::do_create_relation(
        input_base.adapter,
        input_base.database.clone(),
        input_base.schema.clone(),
        Some(input_base.alias.clone()),
        None,
        input_base.quoting,
    )
    .map_err(|e| {
        DbtTemporalError::Compilation(format!(
            "unit test {unique_id}: building relation for mocked input: {e}"
        ))
    })?;
    let relation_value =
        dbt_adapter::relation::RelationObject::new(Arc::from(relation)).into_value();

    let mut ctx = node_context.clone();
    ctx.insert("this".to_owned(), relation_value);
    // get_fixture_sql consults defer_relation when `this` is missing from the
    // warehouse; we don't thread defer state here, so pin it to none.
    ctx.insert("defer_relation".to_owned(), minijinja::Value::from(()));
    ctx.insert("__dbt_ut_fixture_rows__".to_owned(), minijinja::Value::from_serialize(rows));

    jinja_env
        .render_str("{{ get_fixture_sql(__dbt_ut_fixture_rows__, none) }}", &ctx, &[])
        .map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "unit test {unique_id}: rendering fixture rows: {e}"
            ))
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_rows_infers_types() {
        let rows = parse_csv_rows("id,amount,active,note\n1,2.5,true,\n2,3,FALSE,hello\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], YmlValue::number(1_i64.into()));
        assert_eq!(rows[0]["amount"], YmlValue::number(2.5_f64.into()));
        assert_eq!(rows[0]["active"], YmlValue::bool(true));
        assert_eq!(rows[0]["note"], YmlValue::null());
        assert_eq!(rows[1]["active"], YmlValue::bool(false));
        assert_eq!(rows[1]["note"], YmlValue::string("hello".to_string()));
    }

    fn unit_with_expect(rows: Option<Rows>, format: Formats, fixture: Option<&str>) -> DbtUnitTest {
        let mut unit = DbtUnitTest::default();
        unit.__common_attr__.unique_id = "unit_test.p.m.t".to_string();
        unit.__unit_test_attr__.expect = dbt_schemas::schemas::common::Expect {
            rows,
            format,
            fixture: fixture.map(ToString::to_string),
        };
        unit
    }

    fn empty_config() -> YmlValue {
        YmlValue::mapping(dbt_yaml::Mapping::new())
    }

    #[track_caller]
    fn as_mapping(config: &YmlValue) -> &dbt_yaml::Mapping {
        let YmlValue::Mapping(map, _) = config else {
            panic!("config should stay a mapping")
        };
        map
    }

    #[test]
    fn inject_expected_config_sets_rows_for_dict() {
        let dir = tempfile::tempdir().unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), YmlValue::number(1_i64.into()));
        let unit = unit_with_expect(Some(Rows::List(vec![row])), Formats::Dict, None);
        let mut config = empty_config();
        inject_expected_config(&mut config, &unit, dir.path()).unwrap();
        let map = as_mapping(&config);
        assert!(map.contains_key("expected_rows"));
        assert!(!map.contains_key("expected_sql"));
    }

    #[test]
    fn inject_expected_config_sets_sql_with_empty_rows_for_sql_format() {
        // expected_rows must always exist (empty) because the materialization
        // evaluates `expected_rows | length` unconditionally.
        let dir = tempfile::tempdir().unwrap();
        let unit =
            unit_with_expect(Some(Rows::String("select 1 as id".to_string())), Formats::Sql, None);
        let mut config = empty_config();
        inject_expected_config(&mut config, &unit, dir.path()).unwrap();
        let map = as_mapping(&config);
        assert_eq!(map.get("expected_sql").and_then(|v| v.as_str()), Some("select 1 as id"));
        assert!(
            map.get("expected_rows")
                .is_some_and(|v| v.as_sequence().is_some_and(Vec::is_empty))
        );
    }

    #[test]
    fn inject_expected_config_parses_csv_fixture_file() {
        let dir = tempfile::tempdir().unwrap();
        let fixtures = dir.path().join("tests/fixtures");
        std::fs::create_dir_all(&fixtures).unwrap();
        std::fs::write(
            fixtures.join("expected.csv"),
            "id,status
1,done
",
        )
        .unwrap();
        let unit = unit_with_expect(None, Formats::Csv, Some("expected"));
        let mut config = empty_config();
        inject_expected_config(&mut config, &unit, dir.path()).unwrap();
        let map = as_mapping(&config);
        let rows = map
            .get("expected_rows")
            .and_then(|v| v.as_sequence())
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn inject_expected_config_rejects_non_mapping_config() {
        let dir = tempfile::tempdir().unwrap();
        let unit = unit_with_expect(Some(Rows::List(vec![])), Formats::Dict, None);
        let mut config = YmlValue::null();
        let err = inject_expected_config(&mut config, &unit, dir.path()).unwrap_err();
        assert!(err.to_string().contains("mapping"));
    }

    #[test]
    fn fixture_content_prefers_inline_rows() {
        let dir = tempfile::tempdir().unwrap();
        let content = fixture_content(
            Some(&Rows::String("id\n1\n".to_string())),
            None,
            "csv",
            dir.path(),
            "unit_test.p.t",
        )
        .unwrap();
        assert_eq!(content, "id\n1\n");
    }

    #[test]
    fn fixture_content_reads_conventional_fixture_file() {
        let dir = tempfile::tempdir().unwrap();
        let fixtures = dir.path().join("tests/fixtures");
        std::fs::create_dir_all(&fixtures).unwrap();
        std::fs::write(fixtures.join("my_rows.csv"), "id\n42\n").unwrap();
        let content =
            fixture_content(None, Some("my_rows"), "csv", dir.path(), "unit_test.p.t").unwrap();
        assert_eq!(content, "id\n42\n");
    }

    #[test]
    fn fixture_content_errors_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let err =
            fixture_content(None, Some("nope"), "csv", dir.path(), "unit_test.p.t").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn resolve_fixture_dict_requires_list_rows() {
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_fixture(
            Some(&Rows::String("not,a,list".to_string())),
            &Formats::Dict,
            None,
            dir.path(),
            "unit_test.p.t",
        )
        .unwrap_err();
        assert!(err.to_string().contains("dict-format"));
    }

    #[test]
    fn resolve_fixture_csv_parse_errors_are_wrapped() {
        let dir = tempfile::tempdir().unwrap();
        // The csv crate rejects rows whose field count differs from the header.
        let err = resolve_fixture(
            Some(&Rows::String("id,status\n1,done,extra\n".to_string())),
            &Formats::Csv,
            None,
            dir.path(),
            "unit_test.p.t",
        )
        .unwrap_err();
        assert!(err.to_string().contains("parsing csv fixture"));
    }

    #[test]
    fn resolve_fixture_dict_rejects_fixture_files() {
        // dict rows live inline in the YAML; a fixture file makes no sense.
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_fixture(None, &Formats::Dict, Some("rows"), dir.path(), "unit_test.p.t")
            .unwrap_err();
        assert!(err.to_string().contains("dict-format"));
    }

    #[test]
    fn resolve_fixture_sql_passes_through() {
        let dir = tempfile::tempdir().unwrap();
        let result = resolve_fixture(
            Some(&Rows::String("select 1 as id".to_string())),
            &Formats::Sql,
            None,
            dir.path(),
            "unit_test.p.t",
        )
        .unwrap();
        assert!(
            matches!(result, FixtureData::Sql(ref sql) if sql == "select 1 as id"),
            "expected sql fixture: {result:?}"
        );
    }

    #[test]
    fn inject_expected_config_propagates_fixture_errors() {
        let dir = tempfile::tempdir().unwrap();
        let unit = unit_with_expect(None, Formats::Csv, Some("no_such_fixture"));
        let mut config = empty_config();
        let err = inject_expected_config(&mut config, &unit, dir.path()).unwrap_err();
        assert!(err.to_string().contains("no_such_fixture"));
    }
}
