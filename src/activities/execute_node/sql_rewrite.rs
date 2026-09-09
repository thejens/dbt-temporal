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
//! - String literals, quoted-literal dialects (`E'…'`, `$tag$…$tag$`) and
//!   comments are skipped entirely.
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

/// The names to swap, split by which position they may legally appear in.
///
/// Keeping schemas and databases apart matters: a project whose database and
/// one of its schemas share a name would otherwise have each rewritten with
/// the other's replacement.
#[derive(Debug, Default, Clone)]
pub struct RelationRewrite {
    pub schemas: BTreeMap<String, String>,
    pub databases: BTreeMap<String, String>,
}

impl RelationRewrite {
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty() && self.databases.is_empty()
    }
}

/// One component of a dotted identifier chain: the byte range of the bare
/// name, excluding any quoting.
#[derive(Clone, Copy)]
struct Component {
    start: usize,
    end: usize,
}

/// A dotted identifier chain. Only the two leading components can ever be
/// qualifiers, so the rest is counted rather than kept.
struct Chain {
    first: Component,
    second: Option<Component>,
    len: usize,
}

pub fn rewrite_relations(sql: String, rewrite: &RelationRewrite) -> String {
    if rewrite.is_empty() || sql.is_empty() {
        return sql;
    }

    // (byte range of the name, replacement). Collected in source order over a
    // single pass, then applied together — a replacement is never re-scanned.
    let mut edits: Vec<(usize, usize, &str)> = Vec::new();

    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'\'' => i = skip_single_quoted(bytes, i),
            b'$' => match skip_dollar_quoted(bytes, i) {
                Some(next) => i = next,
                // Not a dollar-quote opener; `$` cannot start an identifier we
                // care about, so step over it.
                None => i += 1,
            },
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
/// `db.schema.name` and `schema.name` are both compiled by dbt, and a column
/// reference can extend either (`schema.table.column`). Matching against the
/// maps rather than against the chain length resolves that: a leading
/// component that names a known database is a database, one that names a known
/// schema is a schema, and a chain that starts with neither may still carry the
/// schema in second position because its database is not being remapped.
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

    if chain.len >= 3
        && let Some(new_db) = rewrite.databases.get(name(chain.first))
    {
        edits.push((chain.first.start, chain.first.end, new_db));
        if let Some(new_schema) = rewrite.schemas.get(name(second)) {
            edits.push((second.start, second.end, new_schema));
        }
        return;
    }
    if let Some(new_schema) = rewrite.schemas.get(name(chain.first)) {
        edits.push((chain.first.start, chain.first.end, new_schema));
        return;
    }
    if chain.len >= 3
        && let Some(new_schema) = rewrite.schemas.get(name(second))
    {
        edits.push((second.start, second.end, new_schema));
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
        len: 1,
    };
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
        if chain.len == 1 {
            chain.second = Some(next);
        }
        chain.len += 1;
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

/// Skip a `'…'` literal. A doubled quote escapes; so does a backslash, which
/// PostgreSQL only honours for `E'…'` but which no adapter emits inside a
/// relation name — treating it as an escape everywhere only ever skips more
/// text, never less.
fn skip_single_quoted(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
            b'\'' => return i + 1,
            _ => i += 1,
        }
    }
    i
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

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
            .collect()
    }

    fn schemas(pairs: &[(&str, &str)]) -> RelationRewrite {
        RelationRewrite {
            schemas: map(pairs),
            databases: BTreeMap::new(),
        }
    }

    fn rewrite(sql: &str, r: &RelationRewrite) -> String {
        rewrite_relations(sql.to_string(), r)
    }

    #[test]
    fn rewrites_a_double_quoted_schema() {
        let r = schemas(&[("dbt_dev", "dbt_tenant1")]);
        assert_eq!(
            rewrite(r#"SELECT * FROM "dbt_dev"."orders""#, &r),
            r#"SELECT * FROM "dbt_tenant1"."orders""#
        );
    }

    #[test]
    fn rewrites_a_backtick_quoted_schema_leaving_the_project_alone() {
        let r = schemas(&[("dbt_dev", "dbt_tenant1")]);
        assert_eq!(
            rewrite("SELECT * FROM `my-project`.`dbt_dev`.`orders`", &r),
            "SELECT * FROM `my-project`.`dbt_tenant1`.`orders`"
        );
    }

    #[test]
    fn rewrites_every_schema_in_the_statement() {
        let r = schemas(&[("dev_analytics", "t1_analytics"), ("dev_raw", "t1_raw")]);
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

    /// The whole point of the position rule. A search-and-replace changed the
    /// data this query returns.
    #[test]
    fn leaves_string_literals_alone() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(
            rewrite(r#"SELECT '"dev"' AS label, 'dev.x' AS b FROM "dev"."t""#, &r),
            r#"SELECT '"dev"' AS label, 'dev.x' AS b FROM "tenant1"."t""#
        );
    }

    #[test]
    fn leaves_escaped_quotes_inside_literals_alone() {
        let r = schemas(&[("dev", "tenant1")]);
        // The doubled quote keeps the literal open past the first apostrophe.
        assert_eq!(
            rewrite(r#"SELECT 'it''s "dev".x' FROM "dev"."t""#, &r),
            r#"SELECT 'it''s "dev".x' FROM "tenant1"."t""#
        );
    }

    #[test]
    fn leaves_comments_alone() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(
            rewrite("-- from dev.orders\n/* dev.x /* dev.y */ */ SELECT * FROM dev.orders", &r),
            "-- from dev.orders\n/* dev.x /* dev.y */ */ SELECT * FROM tenant1.orders"
        );
    }

    #[test]
    fn leaves_dollar_quoted_bodies_alone() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(
            rewrite("SELECT $tag$ dev.orders $tag$ FROM dev.orders", &r),
            "SELECT $tag$ dev.orders $tag$ FROM tenant1.orders"
        );
    }

    /// A bare name is a column, an alias, or a CTE — never a qualifier.
    #[test]
    fn leaves_unqualified_names_alone() {
        let r = schemas(&[("raw", "workflow_42")]);
        assert_eq!(
            rewrite(r#"WITH raw AS (SELECT 1) SELECT "raw" FROM raw"#, &r),
            r#"WITH raw AS (SELECT 1) SELECT "raw" FROM raw"#
        );
    }

    /// Sequential `String::replace` calls turned both source schemas into `c`.
    #[test]
    fn chained_mappings_do_not_cascade() {
        let r = schemas(&[("a", "b"), ("b", "c")]);
        assert_eq!(rewrite(r#""a"."t", "b"."t""#, &r), r#""b"."t", "c"."t""#);
    }

    #[test]
    fn rewrites_unquoted_relations() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(rewrite("SELECT * FROM dev.orders", &r), "SELECT * FROM tenant1.orders");
    }

    #[test]
    fn rewrites_a_mixed_quoting_chain() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(
            rewrite(r#"SELECT * FROM "dev".orders"#, &r),
            r#"SELECT * FROM "tenant1".orders"#
        );
    }

    #[test]
    fn rewrites_the_database_and_schema_of_a_three_part_name() {
        let r = RelationRewrite {
            schemas: map(&[("dev", "tenant1")]),
            databases: map(&[("warehouse", "wh_tenant1")]),
        };
        assert_eq!(
            rewrite(r#"SELECT * FROM "warehouse"."dev"."orders""#, &r),
            r#"SELECT * FROM "wh_tenant1"."tenant1"."orders""#
        );
    }

    /// BigQuery projects are not remapped, but the dataset behind them is.
    #[test]
    fn rewrites_the_schema_when_the_database_is_not_remapped() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(
            rewrite("SELECT * FROM `my-project`.`dev`.`orders`", &r),
            "SELECT * FROM `my-project`.`tenant1`.`orders`"
        );
    }

    /// `schema.table.column` reads as a schema, not as a database, because the
    /// name matches the schema map and no database map entry claims it.
    #[test]
    fn a_qualified_column_reference_still_rewrites_its_schema() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(rewrite(r#""dev"."orders"."id""#, &r), r#""tenant1"."orders"."id""#);
    }

    /// A database and a schema sharing a name used to be rewritten with
    /// whichever mapping the merged map happened to hold.
    #[test]
    fn a_shared_name_takes_the_mapping_for_its_own_position() {
        let r = RelationRewrite {
            schemas: map(&[("shared", "schema_new")]),
            databases: map(&[("shared", "db_new")]),
        };
        assert_eq!(rewrite(r#""shared"."shared"."t""#, &r), r#""db_new"."schema_new"."t""#);
    }

    #[test]
    fn tolerates_whitespace_around_the_dots() {
        let r = schemas(&[("dev", "tenant1")]);
        assert_eq!(rewrite(r#""dev" . "orders""#, &r), r#""tenant1" . "orders""#);
    }

    #[test]
    fn an_unterminated_quote_rewrites_nothing_after_it() {
        let r = schemas(&[("dev", "tenant1")]);
        let sql = r#"SELECT * FROM "dev"."t" WHERE x = "dev.t"#;
        assert_eq!(rewrite(sql, &r), r#"SELECT * FROM "tenant1"."t" WHERE x = "dev.t"#);
    }

    #[test]
    fn handles_non_ascii_identifiers() {
        let r = schemas(&[("café", "tenant1")]);
        assert_eq!(rewrite("SELECT * FROM café.orders", &r), "SELECT * FROM tenant1.orders");
    }

    #[test]
    fn leaves_numeric_literals_alone() {
        let r = schemas(&[("1", "2")]);
        assert_eq!(rewrite("SELECT 1.5, 1.orders", &r), "SELECT 1.5, 1.orders");
    }
}
