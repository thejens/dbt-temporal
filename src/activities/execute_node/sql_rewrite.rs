//! Rewrite schema and database names in compiled SQL, in relation positions only.
//!
//! When a workflow overrides the profile's schema or database, the SQL dbt
//! compiled at worker startup still names the startup relations. Recompiling
//! is not an option — the resolved manifest is built once per worker — so the
//! compiled text is rewritten before it runs.
//!
//! The rewrite is deliberately narrow. A plain search-and-replace over the SQL
//! corrupts data: `select '"dev"'` returns different text once `dev` is
//! remapped, a column aliased `"dev"` is renamed, and applying `a -> b` then
//! `b -> c` in sequence collapses both source schemas onto `c`. So this
//! module lexes the statement well enough to know what it is looking at, and
//! rewrites in one pass:
//!
//! - String literals and comments are skipped entirely. Which literal forms
//!   exist depends on the warehouse (see [`LiteralRules`]): whether a
//!   backslash escapes inside `'…'`, `E'…'` escape strings, double-quoted
//!   strings and triple-quoted strings. `$tag$…$tag$` bodies are skipped on
//!   every dialect.
//! - An identifier is only a rewrite candidate when it is a leading component
//!   of a dotted chain — `x` in `x.y`, never the trailing `y`, and never a
//!   bare `x`. Column aliases and keywords therefore never match.
//! - Each site is rewritten at most once, from the original text, so chained
//!   mappings cannot cascade.
//!
//! This is a lexer, not a parser: it cannot tell `schema.table` from
//! `alias.column` when an alias shadows a schema name. That ambiguity is the
//! reason the real fix is to resolve relations from per-run metadata before
//! rendering rather than to patch text afterwards.

use std::collections::BTreeMap;

use dbt_adapter::AdapterType;

/// Where each of the run's relations moved, indexed the way compiled SQL
/// names them: by the schema and identifier the startup manifest resolved.
///
/// Indexing by relation rather than by schema name is what lets two nodes that
/// share a startup schema resolve to different ones — which a project's own
/// `generate_schema_name` is free to do — and what keeps a source sharing that
/// schema from being dragged along with the models.
#[derive(Debug, Default, Clone)]
pub struct RelationRewrite {
    by_schema: BTreeMap<String, BTreeMap<String, RelationMove>>,
}

/// The destination of one relation, plus the database it started in — needed
/// to tell a `db.schema.name` whose database this run also moves from one
/// whose database it leaves alone.
#[derive(Debug, Clone)]
pub struct RelationMove {
    pub old_database: String,
    pub new_database: String,
    pub new_schema: String,
}

impl RelationRewrite {
    pub fn insert(&mut self, old_schema: &str, old_identifier: &str, moved: RelationMove) {
        self.by_schema
            .entry(old_schema.to_owned())
            .or_default()
            .insert(old_identifier.to_owned(), moved);
    }

    pub fn is_empty(&self) -> bool {
        self.by_schema.is_empty()
    }

    fn get(&self, schema: &str, identifier: &str) -> Option<&RelationMove> {
        self.by_schema.get(schema)?.get(identifier)
    }
}

/// One component of a dotted identifier chain: the byte range of the bare
/// name, excluding any quoting.
#[derive(Clone, Copy)]
struct Component {
    start: usize,
    end: usize,
}

/// A dotted identifier chain. A relation is at most three components, so the
/// rest is counted rather than kept.
struct Chain {
    first: Component,
    second: Option<Component>,
    third: Option<Component>,
}

/// What a double-quoted token is on one warehouse.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DoubleQuote {
    /// A quoted identifier, as in PostgreSQL and Snowflake.
    Identifier,
    /// A string literal; identifiers are backtick-quoted instead.
    String,
}

/// How string literals are written on one warehouse. Misjudging any of these
/// inverts the lexer's notion of inside/outside a literal for the rest of the
/// statement: relations get skipped, or text inside literals gets rewritten.
#[derive(Clone, Copy)]
struct LiteralRules {
    /// A backslash inside a plain `'…'` literal escapes the next character.
    /// Where it does not (standard-conforming strings), `'\'` is a complete
    /// one-character literal.
    backslash_escapes: bool,
    /// `E'…'` opens an escape string whose backslashes escape, even though a
    /// plain literal's do not.
    escape_strings: bool,
    /// What `"…"` is.
    double_quote: DoubleQuote,
    /// `'''…'''` and `"""…"""` are literals that may contain bare quotes.
    triple_quoted: bool,
}

impl LiteralRules {
    /// Exhaustive on purpose: a new adapter must be classified here rather than
    /// silently inheriting another warehouse's literal syntax.
    const fn for_adapter(adapter: AdapterType) -> Self {
        // Standard SQL: a backslash is an ordinary character in `'…'`.
        const STANDARD: LiteralRules = LiteralRules {
            backslash_escapes: false,
            escape_strings: false,
            double_quote: DoubleQuote::Identifier,
            triple_quoted: false,
        };
        // Backslash escapes in `'…'`; `"…"` stays an identifier.
        const BACKSLASH: LiteralRules = LiteralRules {
            backslash_escapes: true,
            ..STANDARD
        };
        match adapter {
            // PostgreSQL (standard_conforming_strings defaults on) and DuckDB
            // take backslashes literally in plain literals and honour them in
            // `E'…'`. dbt lexes LakeCompute with DuckDB's dialect.
            AdapterType::Postgres | AdapterType::DuckDB | AdapterType::LakeCompute => Self {
                escape_strings: true,
                ..STANDARD
            },
            // Redshift forked PostgreSQL 8.0, before standard-conforming
            // strings existed: plain literals process backslash escapes, which
            // is why LIKE's default escape is written as two backslashes.
            // Snowflake and ClickHouse document backslash escapes as well.
            AdapterType::Redshift | AdapterType::Snowflake | AdapterType::ClickHouse => BACKSLASH,
            // `"…"` is a string literal and identifiers are backtick-quoted.
            AdapterType::Databricks | AdapterType::Spark => Self {
                double_quote: DoubleQuote::String,
                ..BACKSLASH
            },
            AdapterType::Bigquery => Self {
                double_quote: DoubleQuote::String,
                triple_quoted: true,
                ..BACKSLASH
            },
            // ANSI literals: Trino and its derivatives, T-SQL, Oracle, Exasol
            // and DataFusion. Dremio (Calcite) and Salesforce Data Cloud
            // (PostgreSQL-style SQL) are expected to follow the standard too.
            AdapterType::Trino
            | AdapterType::Starburst
            | AdapterType::Athena
            | AdapterType::Fabric
            | AdapterType::Oracle
            | AdapterType::Exasol
            | AdapterType::Datafusion
            | AdapterType::Dremio
            | AdapterType::Salesforce => STANDARD,
        }
    }
}

pub fn rewrite_relations(sql: String, rewrite: &RelationRewrite, adapter: AdapterType) -> String {
    if rewrite.is_empty() || sql.is_empty() {
        return sql;
    }
    let rules = LiteralRules::for_adapter(adapter);

    // (byte range of the name, replacement). Collected in source order over a
    // single pass, then applied together — a replacement is never re-scanned.
    let mut edits: Vec<(usize, usize, &str)> = Vec::new();

    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'\'' => {
                i = skip_string_literal(bytes, i, rules.backslash_escapes, rules.triple_quoted);
            }
            b'"' if rules.double_quote == DoubleQuote::String => {
                i = skip_string_literal(bytes, i, rules.backslash_escapes, rules.triple_quoted);
            }
            b'$' => match skip_dollar_quoted(bytes, i) {
                Some(next) => i = next,
                // Not a dollar-quote opener; `$` cannot start an identifier we
                // care about, so step over it.
                None => i += 1,
            },
            // `E'…'` must be recognised before the `E` is read as an identifier.
            // An `E` that ends a longer identifier (`name'…'`) is not a prefix.
            b'e' | b'E'
                if rules.escape_strings
                    && bytes.get(i + 1) == Some(&b'\'')
                    && (i == 0 || !is_ident_continue(bytes[i - 1])) =>
            {
                i = skip_string_literal(bytes, i + 1, true, false);
            }
            _ => {
                let Some((chain, next)) = scan_chain(bytes, i) else {
                    // An unterminated quote means the rest of the statement
                    // cannot be classified, so stop rather than guess.
                    if matches!(bytes[i], b'"' | b'`') {
                        break;
                    }
                    i += 1;
                    continue;
                };
                plan_edits(&sql, &chain, rewrite, &mut edits);
                i = next;
            }
        }
    }

    if edits.is_empty() {
        return sql;
    }

    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    for (start, end, replacement) in edits {
        out.push_str(&sql[cursor..start]);
        out.push_str(replacement);
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Decide which components of one dotted chain to rewrite.
///
/// dbt renders a relation as `database.schema.identifier` or
/// `schema.identifier`, and a column reference can extend either. Looking the
/// trailing pair up as a relation first, then the leading pair, tells those
/// apart without a parser: `db.schema.tbl` matches on (schema, tbl), while
/// `schema.tbl.col` misses on (tbl, col) and matches on (schema, tbl).
fn plan_edits<'a>(
    sql: &str,
    chain: &Chain,
    rewrite: &'a RelationRewrite,
    edits: &mut Vec<(usize, usize, &'a str)>,
) {
    // The trailing component is the object itself and is never a qualifier.
    let Some(second) = chain.second else {
        return;
    };
    let name = |c: Component| &sql[c.start..c.end];

    if let Some(third) = chain.third
        && let Some(moved) = rewrite.get(name(second), name(third))
    {
        // Only this run's own database is remapped. A relation qualified by
        // some other database keeps it — a BigQuery project the workflow does
        // not own, say — while its dataset still moves.
        if name(chain.first) == moved.old_database && moved.new_database != moved.old_database {
            edits.push((chain.first.start, chain.first.end, &moved.new_database));
        }
        if moved.new_schema != name(second) {
            edits.push((second.start, second.end, &moved.new_schema));
        }
        return;
    }

    if let Some(moved) = rewrite.get(name(chain.first), name(second))
        && moved.new_schema != name(chain.first)
    {
        edits.push((chain.first.start, chain.first.end, &moved.new_schema));
    }
}

/// Read a dotted identifier chain starting at `start`, if one is there.
///
/// Returns the components and the offset just past the chain. Quoting styles
/// may be mixed within a chain because that is what the adapters emit — a
/// BigQuery relation is `` `p`.`d`.`t` `` while its column suffix is bare.
fn scan_chain(bytes: &[u8], start: usize) -> Option<(Chain, usize)> {
    let (first, mut i) = scan_component(bytes, start)?;
    let mut chain = Chain {
        first,
        second: None,
        third: None,
    };
    let mut len = 1usize;
    loop {
        let after_name = i;
        let mut j = skip_whitespace(bytes, i);
        if bytes.get(j) != Some(&b'.') {
            return Some((chain, after_name));
        }
        j = skip_whitespace(bytes, j + 1);
        let Some((next, after)) = scan_component(bytes, j) else {
            return Some((chain, after_name));
        };
        match len {
            1 => chain.second = Some(next),
            2 => chain.third = Some(next),
            // A fourth component and beyond cannot be part of the relation:
            // `db.schema.table.column` already ends the relation at `table`.
            _ => {}
        }
        len += 1;
        i = after;
    }
}

/// Read one identifier — `"quoted"`, `` `quoted` ``, or bare — at `start`.
fn scan_component(bytes: &[u8], start: usize) -> Option<(Component, usize)> {
    match bytes.get(start)? {
        quote @ (b'"' | b'`') => {
            let close = *quote;
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == close {
                    // A doubled quote is an escaped quote, not the end.
                    if bytes.get(i + 1) == Some(&close) {
                        i += 2;
                        continue;
                    }
                    return Some((
                        Component {
                            start: start + 1,
                            end: i,
                        },
                        i + 1,
                    ));
                }
                i += 1;
            }
            // Unterminated quote: nothing after it can be trusted.
            None
        }
        c if is_ident_start(*c) => {
            let mut i = start + 1;
            while i < bytes.len() && is_ident_continue(bytes[i]) {
                i += 1;
            }
            Some((Component { start, end: i }, i))
        }
        _ => None,
    }
}

const fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || !c.is_ascii()
}

const fn is_ident_continue(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || !c.is_ascii()
}

const fn skip_whitespace(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

const fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

/// Block comments nest in PostgreSQL and Snowflake, so count depth.
fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    let mut depth = 1usize;
    while i < bytes.len() {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    i
}

/// Skip the literal whose opening quote is at `start`, returning the offset
/// just past it, or `bytes.len()` when it is unterminated.
///
/// The delimiter is whichever quote byte sits at `start`. A doubled quote is an
/// escaped quote in every dialect. A backslash skips the following byte only
/// when `backslash` says this literal honours them; callers decide that per
/// literal because `E'…'` differs from a plain literal on the same dialect.
/// Only ASCII bytes are ever matched, so skipping the byte after a backslash
/// may land inside a multi-byte character but never on a delimiter, and every
/// returned offset follows an ASCII quote or is the end of input.
fn skip_string_literal(bytes: &[u8], start: usize, backslash: bool, triple_quoted: bool) -> usize {
    let quote = bytes[start];
    // `'''` opens a triple-quoted literal; `''` alone is an empty string.
    let triple = triple_quoted
        && bytes.get(start + 1) == Some(&quote)
        && bytes.get(start + 2) == Some(&quote);
    let mut i = start + if triple { 3 } else { 1 };
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if backslash => i += 2,
            c if c == quote && triple => {
                if bytes.get(i + 1) == Some(&quote) && bytes.get(i + 2) == Some(&quote) {
                    return i + 3;
                }
                i += 1;
            }
            c if c == quote && bytes.get(i + 1) == Some(&quote) => i += 2,
            c if c == quote => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Skip a `$tag$…$tag$` literal, returning `None` when `start` does not open
/// one — a bare `$` is legal elsewhere.
fn skip_dollar_quoted(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'$' {
        if !is_ident_continue(bytes[i]) {
            return None;
        }
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let tag = &bytes[start..=i];
    let mut j = i + 1;
    while j + tag.len() <= bytes.len() {
        if &bytes[j..j + tag.len()] == tag {
            return Some(j + tag.len());
        }
        j += 1;
    }
    Some(bytes.len())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
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
                rewrite_on(
                    adapter,
                    r#"select "dev.orders", "a\"b dev.orders" from dev.orders"#,
                    &r
                ),
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
}
