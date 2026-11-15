//! Assembling a unit test's input SQL: the tested model with each mocked
//! input replaced by a fixture CTE.

use std::collections::BTreeMap;

use dbt_common::constants::DBT_CTE_PREFIX;
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::nodes::DbtUnitTest;

use crate::error::DbtTemporalError;
use crate::worker_state::WorkerState;

use super::fixtures::{FixtureData, render_fixture_rows_sql, resolve_fixture};
use super::given::{parse_given_input, resolve_given_input};

/// Build the unit test's input SQL: the tested model's compiled SQL with each
/// `given` input replaced by a fixture CTE.
#[allow(clippy::too_many_lines)]
pub fn build_unit_test_sql(
    state: &WorkerState,
    unit: &DbtUnitTest,
    nodes: &Nodes,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
    ephemeral: crate::activities::node_helpers::EphemeralDirs<'_>,
) -> Result<String, DbtTemporalError> {
    let in_dir = ephemeral.in_dir;
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
    let raw_sql = super::super::raw_sql::resolve_raw_sql(
        state,
        &model.__common_attr__,
        dbt_schemas::schemas::telemetry::NodeType::Model,
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
        None,
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
            format!("{DBT_CTE_PREFIX}{}", input_node.common().name)
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
            FixtureData::Rows(rows) => {
                render_fixture_rows_sql(jinja_env, node_context, input_base, &rows, unique_id)?
            }
        };
        fixture_ctes.push((cte_name, fixture_sql));
    }

    // Inline any remaining (unmocked) ephemeral CTE references the standard
    // way; mocked ephemeral tokens were already replaced above.
    let model_sql = crate::activities::node_helpers::inject_ephemeral_ctes(
        &model_sql,
        &unit.__common_attr__.name,
        // The SQL being inlined is the tested *model's*, so its ephemeral
        // references resolve against the model's dependencies, not the unit
        // test's.
        &model.__base_attr__.depends_on.nodes,
        nodes,
        jinja_env,
        ephemeral,
    )?;

    Ok(prepend_fixture_ctes(&model_sql, &fixture_ctes))
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
            let nl = sql[idx..].find('\n')?;
            idx += nl + 1;
        } else if trimmed.starts_with("/*") {
            let end = sql[idx..].find("*/")?;
            idx += end + 2;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn fixture_cte_name_sanitizes_to_identifier() {
        assert_eq!(fixture_cte_name("stg_orders"), "__dbt_ut_fixture__stg_orders");
        assert_eq!(fixture_cte_name("weird-name.v2"), "__dbt_ut_fixture__weird_name_v2");
    }

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
}
