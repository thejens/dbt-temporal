use super::*;

/// `(old_schema, identifier) -> new_schema`, with the database untouched.
fn schemas(entries: &[(&str, &str, &str)]) -> RelationRewrite {
    let mut r = RelationRewrite::default();
    for (old_schema, identifier, new_schema) in entries {
        r.insert(
            old_schema,
            identifier,
            RelationMove {
                old_database: "warehouse".to_owned(),
                new_database: "warehouse".to_owned(),
                new_schema: (*new_schema).to_owned(),
            },
        );
    }
    r
}

/// The standard-literal dialect the generic tests run under.
fn rewrite(sql: &str, r: &RelationRewrite) -> String {
    rewrite_on(AdapterType::Postgres, sql, r)
}

fn rewrite_on(adapter: AdapterType, sql: &str, r: &RelationRewrite) -> String {
    rewrite_relations(sql.to_string(), r, adapter)
}

/// Dialects where a backslash is an ordinary character in `'…'`.
const STANDARD: [AdapterType; 4] = [
    AdapterType::Postgres,
    AdapterType::DuckDB,
    AdapterType::Trino,
    AdapterType::LakeCompute,
];
/// Dialects where a backslash escapes inside `'…'`.
const BACKSLASH: [AdapterType; 6] = [
    AdapterType::Bigquery,
    AdapterType::Snowflake,
    AdapterType::Databricks,
    AdapterType::Spark,
    AdapterType::Redshift,
    AdapterType::ClickHouse,
];

fn every_tested_dialect() -> Vec<AdapterType> {
    STANDARD.iter().chain(&BACKSLASH).copied().collect()
}

#[test]
fn rewrites_a_double_quoted_schema() {
    let r = schemas(&[("dbt_dev", "orders", "dbt_tenant1")]);
    assert_eq!(
        rewrite(r#"SELECT * FROM "dbt_dev"."orders""#, &r),
        r#"SELECT * FROM "dbt_tenant1"."orders""#
    );
}

#[test]
fn rewrites_a_backtick_quoted_schema_leaving_the_project_alone() {
    let r = schemas(&[("dbt_dev", "orders", "dbt_tenant1")]);
    assert_eq!(
        rewrite("SELECT * FROM `my-project`.`dbt_dev`.`orders`", &r),
        "SELECT * FROM `my-project`.`dbt_tenant1`.`orders`"
    );
}

#[test]
fn rewrites_every_relation_in_the_statement() {
    let r = schemas(&[
        ("dev_analytics", "a", "t1_analytics"),
        ("dev_raw", "b", "t1_raw"),
    ]);
    assert_eq!(
        rewrite(r#"SELECT * FROM "dev_analytics"."a" JOIN "dev_raw"."b" ON TRUE"#, &r),
        r#"SELECT * FROM "t1_analytics"."a" JOIN "t1_raw"."b" ON TRUE"#
    );
}

#[test]
fn empty_rewrite_returns_the_input() {
    let sql = r#"SELECT * FROM "dbt_dev"."orders""#;
    assert_eq!(rewrite(sql, &RelationRewrite::default()), sql);
}

/// Two models that share a startup schema may resolve to different ones.
/// A schema-to-schema map could only pick one of these.
#[test]
fn two_relations_in_one_schema_can_move_apart() {
    let r = schemas(&[
        ("dev", "orders", "tenant_a"),
        ("dev", "customers", "tenant_b"),
    ]);
    assert_eq!(
        rewrite(r#""dev"."orders" JOIN "dev"."customers""#, &r),
        r#""tenant_a"."orders" JOIN "tenant_b"."customers""#
    );
}

/// A source shares the models' schema in almost every dev project, and its
/// table is one dbt did not create — so it is absent from the map and must
/// come through untouched.
#[test]
fn a_relation_absent_from_the_map_is_left_alone() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(
        rewrite(r#""dev"."orders" JOIN "dev"."events""#, &r),
        r#""tenant1"."orders" JOIN "dev"."events""#
    );
}

/// The whole point of the position rule. A search-and-replace changed the
/// data this query returns.
#[test]
fn leaves_string_literals_alone() {
    let r = schemas(&[("dev", "t", "tenant1")]);
    assert_eq!(
        rewrite(r#"SELECT '"dev"."t"' AS label FROM "dev"."t""#, &r),
        r#"SELECT '"dev"."t"' AS label FROM "tenant1"."t""#
    );
}

#[test]
fn leaves_escaped_quotes_inside_literals_alone() {
    let r = schemas(&[("dev", "t", "tenant1")]);
    // The doubled quote keeps the literal open past the first apostrophe.
    assert_eq!(
        rewrite(r#"SELECT 'it''s "dev"."t"' FROM "dev"."t""#, &r),
        r#"SELECT 'it''s "dev"."t"' FROM "tenant1"."t""#
    );
}

#[test]
fn leaves_comments_alone() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(
        rewrite(
            "-- from dev.orders\n/* dev.orders /* dev.orders */ */ SELECT * FROM dev.orders",
            &r
        ),
        "-- from dev.orders\n/* dev.orders /* dev.orders */ */ SELECT * FROM tenant1.orders"
    );
}

#[test]
fn leaves_dollar_quoted_bodies_alone() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(
        rewrite("SELECT $tag$ dev.orders $tag$ FROM dev.orders", &r),
        "SELECT $tag$ dev.orders $tag$ FROM tenant1.orders"
    );
}

/// A bare name is a column, an alias, or a CTE — never a qualifier.
#[test]
fn leaves_unqualified_names_alone() {
    let r = schemas(&[("raw", "orders", "workflow_42")]);
    assert_eq!(
        rewrite(r#"WITH raw AS (SELECT 1) SELECT "raw" FROM raw"#, &r),
        r#"WITH raw AS (SELECT 1) SELECT "raw" FROM raw"#
    );
}

/// Sequential `String::replace` calls turned both source schemas into `c`.
#[test]
fn chained_mappings_do_not_cascade() {
    let r = schemas(&[("a", "t", "b"), ("b", "t", "c")]);
    assert_eq!(rewrite(r#""a"."t", "b"."t""#, &r), r#""b"."t", "c"."t""#);
}

#[test]
fn rewrites_unquoted_relations() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(rewrite("SELECT * FROM dev.orders", &r), "SELECT * FROM tenant1.orders");
}

#[test]
fn rewrites_a_mixed_quoting_chain() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(
        rewrite(r#"SELECT * FROM "dev".orders"#, &r),
        r#"SELECT * FROM "tenant1".orders"#
    );
}

#[test]
fn rewrites_the_database_and_schema_of_a_three_part_name() {
    let mut r = RelationRewrite::default();
    r.insert(
        "dev",
        "orders",
        RelationMove {
            old_database: "warehouse".to_owned(),
            new_database: "wh_tenant1".to_owned(),
            new_schema: "tenant1".to_owned(),
        },
    );
    assert_eq!(
        rewrite(r#"SELECT * FROM "warehouse"."dev"."orders""#, &r),
        r#"SELECT * FROM "wh_tenant1"."tenant1"."orders""#
    );
}

/// A relation qualified by a database this run does not own — a BigQuery
/// project it only reads from — keeps it, while its dataset still moves.
#[test]
fn leaves_a_database_this_run_does_not_own_alone() {
    let mut r = RelationRewrite::default();
    r.insert(
        "dev",
        "orders",
        RelationMove {
            old_database: "warehouse".to_owned(),
            new_database: "wh_tenant1".to_owned(),
            new_schema: "tenant1".to_owned(),
        },
    );
    assert_eq!(
        rewrite("SELECT * FROM `other-project`.`dev`.`orders`", &r),
        "SELECT * FROM `other-project`.`tenant1`.`orders`"
    );
}

/// `schema.table.column` must read as a schema-qualified relation with a
/// column suffix, not as a database-qualified one.
#[test]
fn a_qualified_column_reference_still_rewrites_its_schema() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(rewrite(r#""dev"."orders"."id""#, &r), r#""tenant1"."orders"."id""#);
}

#[test]
fn tolerates_whitespace_around_the_dots() {
    let r = schemas(&[("dev", "orders", "tenant1")]);
    assert_eq!(rewrite(r#""dev" . "orders""#, &r), r#""tenant1" . "orders""#);
}

#[test]
fn an_unterminated_quote_rewrites_nothing_after_it() {
    let r = schemas(&[("dev", "t", "tenant1")]);
    let sql = r#"SELECT * FROM "dev"."t" WHERE x = "dev"."t"#;
    assert_eq!(rewrite(sql, &r), r#"SELECT * FROM "tenant1"."t" WHERE x = "dev"."t"#);
}

#[test]
fn handles_non_ascii_identifiers() {
    let r = schemas(&[("café", "orders", "tenant1")]);
    assert_eq!(rewrite("SELECT * FROM café.orders", &r), "SELECT * FROM tenant1.orders");
}

#[test]
fn leaves_numeric_literals_alone() {
    let r = schemas(&[("1", "5", "2")]);
    assert_eq!(rewrite("SELECT 1.5", &r), "SELECT 1.5");
}

/// Run `(sql, expected)` pairs against `dev.orders -> t1.orders` on each dialect.
fn check(adapters: &[AdapterType], cases: &[(&str, &str)]) {
    let r = schemas(&[("dev", "orders", "t1")]);
    for adapter in adapters {
        for (sql, expected) in cases {
            assert_eq!(&rewrite_on(*adapter, sql, &r), expected, "{adapter:?}: {sql}");
        }
    }
}

#[test]
fn a_lone_backslash_literal_ends_on_standard_dialects() {
    check(
        &STANDARD,
        &[
            (r"select '\' as sep, * from dev.orders", r"select '\' as sep, * from t1.orders"),
            // The literal after it must still be skipped, not rewritten.
            (
                r"select '\', 'dev.orders' from dev.orders",
                r"select '\', 'dev.orders' from t1.orders",
            ),
        ],
    );
}

#[test]
fn a_backslash_quote_does_not_end_the_literal_on_backslash_dialects() {
    check(
        &BACKSLASH,
        &[
            (
                r"select 'it\'s dev.orders' from dev.orders",
                r"select 'it\'s dev.orders' from t1.orders",
            ),
            (r"select '\\' from dev.orders", r"select '\\' from t1.orders"),
        ],
    );
}

#[test]
fn escape_strings_honour_backslashes_where_plain_literals_do_not() {
    check(
        &[AdapterType::Postgres, AdapterType::DuckDB],
        &[
            (r"select E'\'' , * from dev.orders", r"select E'\'' , * from t1.orders"),
            (
                r"select e'a\'dev.orders' from dev.orders",
                r"select e'a\'dev.orders' from t1.orders",
            ),
            // `E` closing a longer identifier is not an escape-string prefix.
            (r"select name'\' from dev.orders", r"select name'\' from t1.orders"),
            (
                r"select E'\\', 'dev.orders' from dev.orders",
                r"select E'\\', 'dev.orders' from t1.orders",
            ),
        ],
    );
}

#[test]
fn escape_prefix_is_inert_where_the_dialect_has_no_escape_strings() {
    // Trino has no `E'…'`, so `E` is an identifier and `'\'` a plain literal.
    check(
        &[AdapterType::Trino],
        &[(r"select E'\', * from dev.orders", r"select E'\', * from t1.orders")],
    );
}

#[test]
fn doubled_quotes_escape_on_every_dialect() {
    check(
        &every_tested_dialect(),
        &[
            (
                "select 'it''s dev.orders' from dev.orders",
                "select 'it''s dev.orders' from t1.orders",
            ),
            ("select '' , * from dev.orders", "select '' , * from t1.orders"),
        ],
    );
}

#[test]
fn relation_names_inside_literals_are_never_rewritten() {
    check(
        &every_tested_dialect(),
        &[(
            "select 'dev.orders', 'x dev.orders y' from dev.orders",
            "select 'dev.orders', 'x dev.orders y' from t1.orders",
        )],
    );
}

#[test]
fn unterminated_literals_do_not_panic_or_rewrite_what_follows() {
    check(
        &every_tested_dialect(),
        &[
            ("select 'dev.orders", "select 'dev.orders"),
            // A trailing backslash would advance past the end of input.
            (r"select 'dev.orders\", r"select 'dev.orders\"),
            (r"select E'dev.orders\", r"select E'dev.orders\"),
            ("select e", "select e"),
            ("select E'", "select E'"),
            ("select '''dev.orders", "select '''dev.orders"),
        ],
    );
}

#[test]
fn non_ascii_text_in_literals_and_quoted_identifiers_is_safe() {
    let r = schemas(&[("dev", "orders", "t1"), ("scheme_é", "tbl_ü", "t2")]);
    for adapter in every_tested_dialect() {
        assert_eq!(
            rewrite_on(adapter, "select 'é日本 dev.orders' from dev.orders", &r),
            "select 'é日本 dev.orders' from t1.orders",
            "{adapter:?}"
        );
        assert_eq!(
            rewrite_on(adapter, "select `x` from `scheme_é`.`tbl_ü`", &r),
            "select `x` from `t2`.`tbl_ü`",
            "{adapter:?}"
        );
    }
    // A backslash directly before a multi-byte character.
    assert_eq!(
        rewrite_on(AdapterType::Bigquery, r"select 'a\é', * from dev.orders", &r),
        r"select 'a\é', * from t1.orders"
    );
    assert_eq!(
        rewrite_on(AdapterType::Postgres, r#"select E'\é', * from "scheme_é"."tbl_ü""#, &r),
        r#"select E'\é', * from "t2"."tbl_ü""#
    );
}

#[test]
fn double_quotes_delimit_strings_only_where_identifiers_use_backticks() {
    let r = schemas(&[("dev", "orders", "t1")]);
    for adapter in [
        AdapterType::Bigquery,
        AdapterType::Databricks,
        AdapterType::Spark,
    ] {
        assert_eq!(
            rewrite_on(adapter, r#"select "dev.orders", "a\"b dev.orders" from dev.orders"#, &r),
            r#"select "dev.orders", "a\"b dev.orders" from t1.orders"#,
            "{adapter:?}"
        );
    }
    // Elsewhere `"dev"."orders"` is a relation.
    for adapter in [AdapterType::Postgres, AdapterType::Snowflake] {
        assert_eq!(
            rewrite_on(adapter, r#"select * from "dev"."orders""#, &r),
            r#"select * from "t1"."orders""#,
            "{adapter:?}"
        );
    }
}

#[test]
fn bigquery_triple_quoted_literals_may_contain_bare_quotes() {
    let r = schemas(&[("dev", "orders", "t1")]);
    for sql in [
        "select '''it's dev.orders''' from dev.orders",
        "select \"\"\"say \"dev.orders\" \"\"\" from dev.orders",
        r"select '''a\''' dev.orders''' from dev.orders",
        "select r'''dev.orders''' from dev.orders",
    ] {
        assert_eq!(
            rewrite_on(AdapterType::Bigquery, sql, &r),
            sql.replace("from dev.orders", "from t1.orders"),
            "{sql}"
        );
    }
}

#[test]
fn bigquery_raw_and_empty_strings() {
    let r = schemas(&[("dev", "orders", "t1")]);
    for sql in [
        r"select r'\d+ dev.orders', R'\' ' from dev.orders",
        "select '', \"\" from dev.orders",
        "select rb'\\\\', b'x' from dev.orders",
    ] {
        assert_eq!(
            rewrite_on(AdapterType::Bigquery, sql, &r),
            sql.replace("from dev.orders", "from t1.orders"),
            "{sql}"
        );
    }
}
