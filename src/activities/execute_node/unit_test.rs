//! dbt unit test execution.
//!
//! A unit test runs the tested model's SQL against fixture inputs (`given`)
//! and compares the output to fixture expectations (`expect`). The flow
//! mirrors dbt-core's macro-based path: this module builds the model SQL with
//! each mocked input replaced by a fixture CTE (rendered through the bundled
//! `get_fixture_sql` macro), the `unit` materialization probes column types
//! and unions actual vs expected rows labelled by an `actual_or_expected`
//! column, and `extract_unit_test_outcome` compares the two partitions
//! order-insensitively (the union query carries no ORDER BY, so a positional
//! comparison would be flaky).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use dbt_adapter::load_store::ResultStore;
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::common::{Formats, Rows};
use dbt_schemas::schemas::nodes::DbtUnitTest;
use dbt_yaml::Value as YmlValue;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

/// Prefix dbt-fusion uses for ephemeral model CTE references in compiled SQL.
const EPHEMERAL_CTE_PREFIX: &str = "__dbt__cte__";

/// Maximum number of differing rows included in the failure diff message.
/// Keeps Temporal failure payloads bounded for tests over large fixtures.
const MAX_DIFF_LINES: usize = 20;

/// Outcome of comparing the executed unit test result table.
#[derive(Debug)]
pub struct UnitTestOutcome {
    pub passed: bool,
    /// Number of row instances that differ between actual and expected.
    pub failures: i64,
    /// Human-readable diff of the differing rows (bounded).
    pub diff: String,
    /// Total actual rows, for the success message.
    pub actual_rows: usize,
}

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

/// Build the unit test's input SQL: the tested model's compiled SQL with each
/// `given` input replaced by a fixture CTE.
#[allow(clippy::too_many_lines)]
pub fn build_unit_test_sql(
    state: &WorkerState,
    unit: &DbtUnitTest,
    nodes: &Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    in_dir: &Path,
    ephemeral_dir: &Path,
) -> Result<String, DbtTemporalError> {
    let unique_id = &unit.__common_attr__.unique_id;

    if let Some(overrides) = &unit.__unit_test_attr__.overrides
        && (overrides.macros.is_some() || overrides.vars.is_some() || overrides.env_vars.is_some())
    {
        return Err(DbtTemporalError::Compilation(format!(
            "unit test {unique_id} uses overrides (macros/vars/env_vars), which are not \
             supported yet — remove the overrides block or run this test via dbt directly"
        )));
    }

    // Resolve the tested model. The resolver records it directly; the first
    // depends_on entry is the documented fallback (dbt stores the tested
    // model as the unit test's first dependency).
    let model_id = unit
        .tested_node_unique_id
        .clone()
        .or_else(|| unit.__base_attr__.depends_on.nodes.first().cloned())
        .ok_or_else(|| {
            DbtTemporalError::Compilation(format!(
                "unit test {unique_id} has no tested model in depends_on"
            ))
        })?;
    let model = nodes.models.get(&model_id).ok_or_else(|| {
        DbtTemporalError::Compilation(format!(
            "unit test {unique_id}: tested model {model_id} not found in project"
        ))
    })?;

    // Render the tested model's raw SQL in this activity's run context so
    // ref()/source() resolve to relation names; the mocked inputs are then
    // string-replaced with fixture CTE names, mirroring dbt-fusion's
    // replace_subquery_refs_with_cte_names. Rendering in the unit test's
    // context is safe: ref/source resolution only depends on the package,
    // which the unit test shares with its model.
    let model_path = model.__common_attr__.path.to_string_lossy().to_string();
    let raw_sql = super::raw_sql::resolve_raw_sql(
        state,
        &model.__common_attr__,
        dbt_schemas::schemas::telemetry::NodeType::Model,
        &model_path,
    )
    .map_err(|(path, e)| {
        DbtTemporalError::Compilation(format!(
            "unit test {unique_id}: reading raw SQL of tested model {model_id} at {}: {e:#}",
            path.display()
        ))
    })?;
    let render_filename = in_dir.join(&model.__common_attr__.original_file_path);
    let mut model_sql = dbt_jinja_utils::utils::render_sql_with_listeners(
        &raw_sql,
        jinja_env,
        node_context,
        &[],
        &[],
        &render_filename,
    )
    .map_err(|e| {
        DbtTemporalError::Compilation(format!(
            "unit test {unique_id}: compiling SQL of tested model {model_id}: {e:#}"
        ))
    })?;

    let mut fixture_ctes: Vec<(String, String)> = Vec::new();
    for given in &unit.__unit_test_attr__.given {
        let input_raw: &str = &given.input;
        let input = parse_given_input(input_raw).map_err(|e| {
            DbtTemporalError::Compilation(format!(
                "unit test {unique_id}: cannot parse given input {input_raw:?}: {e}"
            ))
        })?;
        let input_id =
            resolve_given_input(&input, &model_id, &model.__common_attr__.package_name, nodes)
                .map_err(|e| {
                    DbtTemporalError::Compilation(format!(
                        "unit test {unique_id}: cannot resolve given input {input_raw:?}: {e}"
                    ))
                })?;
        let input_node = nodes.get_node(&input_id).ok_or_else(|| {
            DbtTemporalError::Compilation(format!(
                "unit test {unique_id}: resolved given input {input_id} not found"
            ))
        })?;
        let input_base = input_node.base();

        let cte_name = fixture_cte_name(&input_node.common().name);

        // Replacement token: ephemeral inputs appear in compiled SQL as
        // `__dbt__cte__<name>` references; everything else as the rendered
        // relation name.
        let replace_token = if input_base
            .materialized
            .to_string()
            .eq_ignore_ascii_case("ephemeral")
        {
            format!("{EPHEMERAL_CTE_PREFIX}{}", input_node.common().name)
        } else {
            input_base.relation_name.clone().ok_or_else(|| {
                DbtTemporalError::Compilation(format!(
                    "unit test {unique_id}: given input {input_id} has no relation name"
                ))
            })?
        };
        if !model_sql.contains(&replace_token) {
            return Err(DbtTemporalError::Compilation(format!(
                "unit test {unique_id}: given input {input_raw:?} (relation {replace_token}) \
                 does not appear in the compiled SQL of {model_id} — is the input actually \
                 referenced by the tested model?"
            )));
        }
        model_sql = model_sql.replace(&replace_token, &cte_name);

        let fixture = resolve_fixture(
            given.rows.as_ref(),
            &given.format,
            given.fixture.as_deref(),
            in_dir,
            unique_id,
        )?;
        let fixture_sql = match fixture {
            FixtureData::Sql(sql) => sql,
            FixtureData::Rows(rows) => render_fixture_rows_sql(
                state,
                jinja_env,
                node_context,
                input_base,
                &rows,
                unique_id,
            )?,
        };
        fixture_ctes.push((cte_name, fixture_sql));
    }

    // Inline any remaining (unmocked) ephemeral CTE references the standard
    // way; mocked ephemeral tokens were already replaced above.
    let model_sql = super::super::node_helpers::inject_ephemeral_ctes(
        &model_sql,
        &unit.__common_attr__.name,
        nodes,
        jinja_env,
        node_context,
        in_dir,
        ephemeral_dir,
    )?;

    Ok(prepend_fixture_ctes(&model_sql, &fixture_ctes))
}

/// Load the executed unit test result from the activity's result store and
/// compare actual vs expected partitions.
pub fn extract_unit_test_outcome(
    result_store: &ResultStore,
) -> Result<UnitTestOutcome, DbtTemporalError> {
    let load_fn = result_store.load_result();
    let result = load_fn(&[minijinja::Value::from("main")])
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("loading unit test result: {e}")))?;
    let table_val = result
        .get_attr("table")
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("unit test result table: {e}")))?;
    let table = table_val
        .downcast_object::<dbt_agate::AgateTable>()
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "unit test produced no result table — statement('main') did not run"
            ))
        })?;
    compare_actual_expected(table.original_record_batch().as_ref())
}

// --- fixtures ---

#[derive(Debug)]
enum FixtureData {
    Rows(Vec<BTreeMap<String, YmlValue>>),
    Sql(String),
}

/// Resolve a `given`/`expect` fixture to rows or raw SQL, reading fixture
/// files from disk when referenced by name.
fn resolve_fixture(
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
fn render_fixture_rows_sql(
    state: &WorkerState,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    input_base: &dbt_schemas::schemas::nodes::NodeBaseAttributes,
    rows: &[BTreeMap<String, YmlValue>],
    unique_id: &str,
) -> Result<String, DbtTemporalError> {
    let relation = dbt_adapter::relation::do_create_relation(
        state.resolver_state.adapter_type,
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

// --- given input parsing & resolution ---

#[derive(Debug, PartialEq, Eq)]
enum GivenInput {
    This,
    Ref {
        name: String,
        package: Option<String>,
    },
    Source {
        source: String,
        table: String,
    },
}

/// Parse a `given.input` expression: `this`, `ref('name')`,
/// `ref('package', 'name')`, or `source('source', 'table')`. Surrounding
/// Jinja braces are tolerated since fixtures in the wild use both forms.
fn parse_given_input(raw: &str) -> Result<GivenInput, anyhow::Error> {
    let mut s = raw.trim();
    if let Some(stripped) = s.strip_prefix("{{").and_then(|r| r.strip_suffix("}}")) {
        s = stripped.trim();
    }
    if s == "this" {
        return Ok(GivenInput::This);
    }
    let (func, args) = if let Some(rest) = s.strip_prefix("ref") {
        ("ref", rest)
    } else if let Some(rest) = s.strip_prefix("source") {
        ("source", rest)
    } else {
        anyhow::bail!("expected this, ref(...), or source(...)");
    };
    let args = args.trim();
    let inner = args
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| anyhow::anyhow!("expected {func}(...) call syntax"))?;
    let parts: Vec<String> = inner
        .split(',')
        .map(|p| {
            let p = p.trim();
            p.strip_prefix('\'')
                .and_then(|r| r.strip_suffix('\''))
                .or_else(|| p.strip_prefix('"').and_then(|r| r.strip_suffix('"')))
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("argument {p:?} is not a string literal"))
        })
        .collect::<Result<_, _>>()?;
    match (func, parts.as_slice()) {
        ("ref", [name]) => Ok(GivenInput::Ref {
            name: name.clone(),
            package: None,
        }),
        ("ref", [package, name]) => Ok(GivenInput::Ref {
            name: name.clone(),
            package: Some(package.clone()),
        }),
        ("source", [source, table]) => Ok(GivenInput::Source {
            source: source.clone(),
            table: table.clone(),
        }),
        _ => anyhow::bail!("unsupported argument count for {func}()"),
    }
}

/// Resolve a parsed given input to a node unique_id. Refs prefer the unit
/// test's own package, matching dbt's ref resolution order.
fn resolve_given_input(
    input: &GivenInput,
    model_id: &str,
    package_name: &str,
    nodes: &Nodes,
) -> Result<String, anyhow::Error> {
    match input {
        GivenInput::This => Ok(model_id.to_string()),
        GivenInput::Ref { name, package } => {
            let matches_pkg = |node_pkg: &str| package.as_deref().is_none_or(|p| p == node_pkg);
            let mut candidates: Vec<&String> = Vec::new();
            for (id, m) in &nodes.models {
                if m.__common_attr__.name == *name && matches_pkg(&m.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            for (id, s) in &nodes.seeds {
                if s.__common_attr__.name == *name && matches_pkg(&s.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            for (id, s) in &nodes.snapshots {
                if s.__common_attr__.name == *name && matches_pkg(&s.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            match candidates.len() {
                0 => anyhow::bail!("no model/seed/snapshot named {name:?}"),
                1 => Ok(candidates[0].clone()),
                _ => {
                    // Ambiguous across packages — prefer the unit test's own.
                    candidates
                        .iter()
                        .find(|id| {
                            nodes
                                .get_node(id)
                                .is_some_and(|n| n.common().package_name == package_name)
                        })
                        .map(|id| (*id).clone())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "ref {name:?} is ambiguous across packages — qualify it as \
                                 ref('<package>', '{name}')"
                            )
                        })
                }
            }
        }
        GivenInput::Source { source, table } => nodes
            .sources
            .iter()
            .find(|(_, s)| {
                s.__source_attr__.source_name == *source && s.__common_attr__.name == *table
            })
            .map(|(id, _)| id.clone())
            .ok_or_else(|| anyhow::anyhow!("no source {source:?}.{table:?}")),
    }
}

/// CTE name for a mocked input's fixture. Keeps only identifier-safe chars so
/// the name never needs quoting.
fn fixture_cte_name(node_name: &str) -> String {
    let sanitized: String = node_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("__dbt_ut_fixture__{sanitized}")
}

// --- SQL assembly ---

/// Prepend fixture CTEs to the model SQL, merging with an existing leading
/// WITH clause (including `WITH RECURSIVE`) instead of producing two WITHs.
fn prepend_fixture_ctes(model_sql: &str, ctes: &[(String, String)]) -> String {
    if ctes.is_empty() {
        return model_sql.to_string();
    }
    let cte_list = ctes
        .iter()
        .map(|(name, sql)| format!("{name} as (\n{sql}\n)"))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some((prefix, recursive, rest)) = split_leading_with(model_sql) {
        let kw = if recursive { "with recursive" } else { "with" };
        format!("{prefix}{kw} {cte_list}, {rest}")
    } else {
        format!("with {cte_list}\n{model_sql}")
    }
}

/// Detect a leading WITH keyword (skipping whitespace and SQL comments).
/// Returns (prefix up to the keyword, whether RECURSIVE follows, remainder
/// after the keyword(s)).
fn split_leading_with(sql: &str) -> Option<(&str, bool, &str)> {
    let mut idx = 0;
    let bytes = sql.as_bytes();
    loop {
        let rest = &sql[idx..];
        let trimmed = rest.trim_start();
        idx += rest.len() - trimmed.len();
        if trimmed.starts_with("--") {
            match sql[idx..].find('\n') {
                Some(nl) => idx += nl + 1,
                None => return None,
            }
        } else if trimmed.starts_with("/*") {
            match sql[idx..].find("*/") {
                Some(end) => idx += end + 2,
                None => return None,
            }
        } else {
            break;
        }
    }
    let rest = &sql[idx..];
    if rest.len() < 4 || !rest[..4].eq_ignore_ascii_case("with") {
        return None;
    }
    // Must be the WITH keyword, not an identifier prefix like `withdrawals`.
    if bytes
        .get(idx + 4)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return None;
    }
    let after_with = &sql[idx + 4..];
    let after_trimmed = after_with.trim_start();
    if after_trimmed.len() >= 9 && after_trimmed[..9].eq_ignore_ascii_case("recursive") {
        let next = after_trimmed.as_bytes().get(9);
        if !next.is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_') {
            let consumed = after_with.len() - after_trimmed.len() + 9;
            return Some((&sql[..idx], true, &sql[idx + 4 + consumed..]));
        }
    }
    Some((&sql[..idx], false, after_with))
}

// --- result comparison ---

/// Compare the actual vs expected partitions of the executed unit test
/// result as multisets of stringified rows. Order-insensitive by design.
fn compare_actual_expected(batch: &RecordBatch) -> Result<UnitTestOutcome, DbtTemporalError> {
    let schema = batch.schema();
    let label_idx = schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case("actual_or_expected"))
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "unit test result is missing the actual_or_expected column"
            ))
        })?;
    let labels = batch
        .column(label_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DbtTemporalError::Adapter(anyhow::anyhow!(
                "actual_or_expected column is not a string column"
            ))
        })?;

    let data_columns: Vec<usize> = (0..schema.fields().len())
        .filter(|i| *i != label_idx)
        .collect();
    let header = data_columns
        .iter()
        .map(|i| schema.field(*i).name().clone())
        .collect::<Vec<_>>()
        .join(" | ");

    let mut actual: BTreeMap<String, i64> = BTreeMap::new();
    let mut expected: BTreeMap<String, i64> = BTreeMap::new();
    let mut actual_rows = 0usize;
    for row in 0..batch.num_rows() {
        let key = data_columns
            .iter()
            .map(|col| format_cell(batch.column(*col).as_ref(), row))
            .collect::<Result<Vec<_>, _>>()?
            .join(" | ");
        match labels.value(row) {
            "actual" => {
                actual_rows += 1;
                *actual.entry(key).or_default() += 1;
            }
            "expected" => *expected.entry(key).or_default() += 1,
            other => {
                return Err(DbtTemporalError::Adapter(anyhow::anyhow!(
                    "unexpected actual_or_expected label {other:?}"
                )));
            }
        }
    }

    let mut failures = 0i64;
    let mut lines: Vec<String> = Vec::new();
    let all_keys: std::collections::BTreeSet<&String> =
        actual.keys().chain(expected.keys()).collect();
    for key in all_keys {
        let a = actual.get(key).copied().unwrap_or(0);
        let e = expected.get(key).copied().unwrap_or(0);
        if a == e {
            continue;
        }
        failures += (a - e).abs();
        if lines.len() < MAX_DIFF_LINES {
            if e > a {
                lines.push(format!("- expected (x{}): {key}", e - a));
            } else {
                lines.push(format!("+ actual   (x{}): {key}", a - e));
            }
        }
    }

    let passed = failures == 0;
    let diff = if passed {
        String::new()
    } else {
        let mut out = format!("columns: {header}\n");
        out.push_str(&lines.join("\n"));
        if lines.len() == MAX_DIFF_LINES {
            out.push_str("\n… (diff truncated)");
        }
        out
    };

    Ok(UnitTestOutcome {
        passed,
        failures,
        diff,
        actual_rows,
    })
}

/// Stringify one cell for multiset comparison and diff display.
fn format_cell(column: &dyn arrow_array::Array, row: usize) -> Result<String, DbtTemporalError> {
    if column.is_null(row) {
        return Ok("NULL".to_string());
    }
    arrow_cast::display::array_value_to_string(column, row)
        .map_err(|e| DbtTemporalError::Adapter(anyhow::anyhow!("formatting result cell: {e}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use arrow_array::{ArrayRef, Int64Array};

    // --- parse_given_input ---

    #[test]
    fn parse_given_input_handles_this_and_braces() {
        assert_eq!(parse_given_input("this").unwrap(), GivenInput::This);
        assert_eq!(parse_given_input("  {{ this }} ").unwrap(), GivenInput::This);
    }

    #[test]
    fn parse_given_input_handles_ref_forms() {
        assert_eq!(
            parse_given_input("ref('stg_orders')").unwrap(),
            GivenInput::Ref {
                name: "stg_orders".to_string(),
                package: None
            }
        );
        assert_eq!(
            parse_given_input("{{ ref(\"pkg\", \"stg_orders\") }}").unwrap(),
            GivenInput::Ref {
                name: "stg_orders".to_string(),
                package: Some("pkg".to_string())
            }
        );
    }

    #[test]
    fn parse_given_input_handles_source() {
        assert_eq!(
            parse_given_input("source('waffle_hut', 'payments')").unwrap(),
            GivenInput::Source {
                source: "waffle_hut".to_string(),
                table: "payments".to_string()
            }
        );
    }

    #[test]
    fn parse_given_input_rejects_garbage() {
        assert!(parse_given_input("var('x')").is_err());
        assert!(parse_given_input("ref(stg_orders)").is_err());
        assert!(parse_given_input("ref('a', 'b', 'c')").is_err());
        assert!(parse_given_input("source('only_one')").is_err());
    }

    // --- fixture_cte_name ---

    #[test]
    fn fixture_cte_name_sanitizes_to_identifier() {
        assert_eq!(fixture_cte_name("stg_orders"), "__dbt_ut_fixture__stg_orders");
        assert_eq!(fixture_cte_name("weird-name.v2"), "__dbt_ut_fixture__weird_name_v2");
    }

    // --- parse_csv_rows ---

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

    // --- split_leading_with / prepend_fixture_ctes ---

    #[test]
    fn split_leading_with_detects_plain_with() {
        let (prefix, recursive, rest) =
            split_leading_with("with a as (select 1) select 1").unwrap();
        assert_eq!(prefix, "");
        assert!(!recursive);
        assert_eq!(rest, " a as (select 1) select 1");
    }

    #[test]
    fn split_leading_with_skips_comments_and_detects_recursive() {
        let sql = "-- header comment\n/* block */ WITH RECURSIVE t as (select 1) select 1";
        let (prefix, recursive, rest) = split_leading_with(sql).unwrap();
        assert!(prefix.contains("header comment"));
        assert!(recursive);
        assert_eq!(rest.trim_start(), "t as (select 1) select 1");
    }

    #[test]
    fn split_leading_with_ignores_identifier_prefixes_and_plain_selects() {
        assert!(split_leading_with("withdrawals").is_none());
        assert!(split_leading_with("select 1").is_none());
        assert!(split_leading_with("-- only a comment").is_none());
    }

    #[test]
    fn prepend_fixture_ctes_wraps_plain_select() {
        let out = prepend_fixture_ctes(
            "select * from __dbt_ut_fixture__a",
            &[("__dbt_ut_fixture__a".to_string(), "select 1 as x".to_string())],
        );
        assert!(out.starts_with("with __dbt_ut_fixture__a as (\nselect 1 as x\n)\n"));
        assert!(out.ends_with("select * from __dbt_ut_fixture__a"));
    }

    #[test]
    fn prepend_fixture_ctes_merges_with_existing_with() {
        let out = prepend_fixture_ctes(
            "with base as (select 1) select * from base",
            &[("f".to_string(), "select 2".to_string())],
        );
        // Exactly one WITH keyword: the fixture CTE list joins the existing one.
        assert_eq!(out.to_lowercase().matches("with ").count(), 1);
        assert!(out.contains("f as (\nselect 2\n),  base as (select 1)"));
    }

    #[test]
    fn prepend_fixture_ctes_no_ctes_is_identity() {
        assert_eq!(prepend_fixture_ctes("select 1", &[]), "select 1");
    }

    #[test]
    fn split_leading_with_bails_on_unterminated_block_comment() {
        assert!(split_leading_with("/* never closed with a as ()").is_none());
    }

    #[test]
    fn split_leading_with_recursive_prefix_identifier_is_not_recursive() {
        // `recursivex` is an identifier (a CTE name), not the keyword.
        let (_, recursive, rest) = split_leading_with("with recursivex as (select 1)").unwrap();
        assert!(!recursive);
        assert_eq!(rest.trim_start(), "recursivex as (select 1)");
    }

    // --- compare_actual_expected ---

    fn batch(ids: &[i64], labels: &[&str]) -> RecordBatch {
        let id_col: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        let label_col: ArrayRef = Arc::new(StringArray::from(
            labels.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
        ));
        RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)]).unwrap()
    }

    #[test]
    fn compare_passes_when_multisets_match_regardless_of_order() {
        let b = batch(&[2, 1, 1, 2], &["actual", "actual", "expected", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed);
        assert_eq!(outcome.failures, 0);
        assert_eq!(outcome.actual_rows, 2);
    }

    #[test]
    fn compare_fails_on_value_mismatch_with_diff() {
        let b = batch(&[1, 3], &["actual", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, 2); // one extra actual + one missing expected
        assert!(outcome.diff.contains("+ actual"));
        assert!(outcome.diff.contains("- expected"));
        assert!(outcome.diff.contains("columns: id"));
    }

    #[test]
    fn compare_fails_on_duplicate_count_mismatch() {
        // Same value but different multiplicities must fail — a set-based
        // comparison would wrongly pass this.
        let b = batch(&[1, 1, 1], &["actual", "actual", "expected"]);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, 1);
    }

    #[test]
    fn compare_handles_null_cells_as_equal() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let label_col: ArrayRef =
            Arc::new(StringArray::from(vec!["actual".to_string(), "expected".to_string()]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)])
            .unwrap();
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(outcome.passed, "NULL must compare equal to NULL");
    }

    #[test]
    fn compare_truncates_long_diffs() {
        let n = i64::try_from(MAX_DIFF_LINES).expect("small constant") + 10;
        let ids: Vec<i64> = (0..n).collect();
        let labels: Vec<&str> = (0..n).map(|_| "actual").collect();
        let b = batch(&ids, &labels);
        let outcome = compare_actual_expected(&b).unwrap();
        assert!(!outcome.passed);
        assert_eq!(outcome.failures, n);
        assert!(outcome.diff.contains("diff truncated"));
    }

    #[test]
    fn compare_rejects_non_string_label_column() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let label_col: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col), ("actual_or_expected", label_col)])
            .unwrap();
        assert!(compare_actual_expected(&b).is_err());
    }

    #[test]
    fn compare_rejects_unknown_label_values() {
        let b = batch(&[1], &["maybe"]);
        assert!(compare_actual_expected(&b).is_err());
    }

    #[test]
    fn compare_errors_without_label_column() {
        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let b = RecordBatch::try_from_iter(vec![("id", id_col)]).unwrap();
        assert!(compare_actual_expected(&b).is_err());
    }

    // --- resolve_given_input ---

    fn nodes_with(
        models: &[(&str, &str, &str)],
        seeds: &[(&str, &str, &str)],
        sources: &[(&str, &str, &str)],
    ) -> Nodes {
        use dbt_schemas::schemas::nodes::{DbtModel, DbtSeed, DbtSource};

        let mut nodes = Nodes::default();
        for (uid, name, package) in models {
            let mut m = DbtModel::default();
            (*uid).clone_into(&mut m.__common_attr__.unique_id);
            (*name).clone_into(&mut m.__common_attr__.name);
            (*package).clone_into(&mut m.__common_attr__.package_name);
            nodes.models.insert((*uid).to_string(), Arc::new(m));
        }
        for (uid, name, package) in seeds {
            let mut s = DbtSeed::default();
            (*uid).clone_into(&mut s.__common_attr__.unique_id);
            (*name).clone_into(&mut s.__common_attr__.name);
            (*package).clone_into(&mut s.__common_attr__.package_name);
            nodes.seeds.insert((*uid).to_string(), Arc::new(s));
        }
        for (uid, source_name, table) in sources {
            let mut s = DbtSource::default();
            (*uid).clone_into(&mut s.__common_attr__.unique_id);
            (*table).clone_into(&mut s.__common_attr__.name);
            (*source_name).clone_into(&mut s.__source_attr__.source_name);
            nodes.sources.insert((*uid).to_string(), Arc::new(s));
        }
        nodes
    }

    #[test]
    fn resolve_given_input_this_returns_tested_model() {
        let nodes = Nodes::default();
        let id = resolve_given_input(&GivenInput::This, "model.p.m", "p", &nodes).unwrap();
        assert_eq!(id, "model.p.m");
    }

    #[test]
    fn resolve_given_input_finds_models_and_seeds_by_name() {
        let nodes = nodes_with(
            &[("model.p.stg_orders", "stg_orders", "p")],
            &[("seed.p.countries", "countries", "p")],
            &[],
        );
        let by_ref = |name: &str| GivenInput::Ref {
            name: name.to_string(),
            package: None,
        };
        assert_eq!(
            resolve_given_input(&by_ref("stg_orders"), "model.p.m", "p", &nodes).unwrap(),
            "model.p.stg_orders"
        );
        assert_eq!(
            resolve_given_input(&by_ref("countries"), "model.p.m", "p", &nodes).unwrap(),
            "seed.p.countries"
        );
        assert!(resolve_given_input(&by_ref("nope"), "model.p.m", "p", &nodes).is_err());
    }

    #[test]
    fn resolve_given_input_respects_package_qualifier() {
        let nodes = nodes_with(
            &[
                ("model.a.shared", "shared", "a"),
                ("model.b.shared", "shared", "b"),
            ],
            &[],
            &[],
        );
        let qualified = GivenInput::Ref {
            name: "shared".to_string(),
            package: Some("b".to_string()),
        };
        assert_eq!(
            resolve_given_input(&qualified, "model.a.m", "a", &nodes).unwrap(),
            "model.b.shared"
        );
        // Unqualified + ambiguous: the unit test's own package wins.
        let bare = GivenInput::Ref {
            name: "shared".to_string(),
            package: None,
        };
        assert_eq!(resolve_given_input(&bare, "model.a.m", "a", &nodes).unwrap(), "model.a.shared");
    }

    #[test]
    fn resolve_given_input_finds_sources() {
        let nodes = nodes_with(&[], &[], &[("source.p.raw.orders", "raw", "orders")]);
        let input = GivenInput::Source {
            source: "raw".to_string(),
            table: "orders".to_string(),
        };
        assert_eq!(
            resolve_given_input(&input, "model.p.m", "p", &nodes).unwrap(),
            "source.p.raw.orders"
        );
        let missing = GivenInput::Source {
            source: "raw".to_string(),
            table: "nope".to_string(),
        };
        assert!(resolve_given_input(&missing, "model.p.m", "p", &nodes).is_err());
    }

    #[test]
    fn resolve_given_input_ambiguous_foreign_packages_errors() {
        // Two foreign candidates, none in the unit test's package: refuse to
        // guess and demand qualification.
        let nodes = nodes_with(
            &[
                ("model.a.shared", "shared", "a"),
                ("model.b.shared", "shared", "b"),
            ],
            &[],
            &[],
        );
        let bare = GivenInput::Ref {
            name: "shared".to_string(),
            package: None,
        };
        let err = resolve_given_input(&bare, "model.c.m", "c", &nodes).unwrap_err();
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn resolve_given_input_finds_snapshots() {
        use dbt_schemas::schemas::nodes::DbtSnapshot;

        let mut nodes = Nodes::default();
        let mut snap = DbtSnapshot::default();
        snap.__common_attr__.unique_id = "snapshot.p.history".to_string();
        snap.__common_attr__.name = "history".to_string();
        snap.__common_attr__.package_name = "p".to_string();
        nodes
            .snapshots
            .insert("snapshot.p.history".to_string(), Arc::new(snap));
        let input = GivenInput::Ref {
            name: "history".to_string(),
            package: None,
        };
        assert_eq!(
            resolve_given_input(&input, "model.p.m", "p", &nodes).unwrap(),
            "snapshot.p.history"
        );
    }

    // --- inject_expected_config ---

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

    // --- fixture_content / resolve_fixture ---

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
