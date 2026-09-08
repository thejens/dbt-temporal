//! Shell-style pattern matching for selector values.
//!
//! dbt runs nearly every selector value through `glob::Pattern::matches` (the
//! `fnmatch` helper in dbt-scheduler's `node_selector`), so what a selector
//! matches is defined by that crate's rules and not by an intuitive reading of
//! `*`: a `*` spans path separators, `?` is exactly one character, `[a-z]` and
//! `[!a-z]` are character classes, and `**` is legal only as a whole path
//! component.
//!
//! So this uses the same crate. dbt-scheduler itself is not a dependency of
//! this worker — it brings a scheduler we do not want — but `glob` is, and it
//! is the part that actually decides the answer. Reimplementing it here meant
//! owning tokenization, character classes, recursive wildcards and their
//! backtracking, and keeping all of it in step with a crate we could simply
//! call.
//!
//! A value with no special characters is compared verbatim, so ordinary names
//! never acquire pattern meaning and never pay for compilation.

pub use glob::Pattern;

/// Characters that turn a value into a pattern. Anything else is a literal.
const SPECIAL_CHARS: [char; 4] = ['*', '?', '[', ']'];

/// True when `value` is a pattern rather than a literal to compare verbatim.
pub fn has_special_chars(value: &str) -> bool {
    value.contains(SPECIAL_CHARS)
}

/// dbt's `fnmatch`: glob-match when the pattern has special characters,
/// verbatim string equality otherwise.
///
/// A pattern that fails to compile matches nothing, as it does in dbt (which
/// prints the failure and carries on). Callers must not read a `false` here as
/// "no such node".
pub fn fnmatch(pattern: &str, text: &str) -> bool {
    if has_special_chars(pattern) {
        Pattern::new(pattern).is_ok_and(|p| p.matches(text))
    } else {
        pattern == text
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn literal_values_are_compared_verbatim() {
        assert!(fnmatch("stg_orders", "stg_orders"));
        assert!(!fnmatch("stg_orders", "stg_orders_v2"));
        assert!(!fnmatch("stg_orders", "STG_ORDERS"), "matching is case sensitive");
    }

    #[test]
    fn star_matches_any_run_including_separators() {
        assert!(fnmatch("stg_*", "stg_orders"));
        assert!(fnmatch("*_orders", "stg_orders"));
        assert!(fnmatch("stg_*_v2", "stg_orders_v2"));
        assert!(!fnmatch("stg_*_v2", "stg_orders_v3"));
        assert!(fnmatch("*", "anything/at/all"));
        // Unlike a shell glob over real paths, `*` is not stopped by `/`.
        assert!(fnmatch("models/*.sql", "models/staging/orders.sql"));
    }

    #[test]
    fn question_mark_matches_exactly_one_character() {
        assert!(fnmatch("?rders", "orders"));
        assert!(!fnmatch("?rders", "rders"));
        assert!(!fnmatch("?rders", "xxrders"));
    }

    #[test]
    fn character_classes_and_negation() {
        assert!(fnmatch("model_[0-9]", "model_7"));
        assert!(!fnmatch("model_[0-9]", "model_x"));
        assert!(fnmatch("model_[!0-9]", "model_x"));
        assert!(!fnmatch("model_[!0-9]", "model_7"));
        assert!(fnmatch("model_[abc]", "model_b"));
        // A `]` immediately after `[` or `[!` is a class member, not the close.
        assert!(fnmatch("x[]]", "x]"));
        assert!(fnmatch("x[!]]", "xa"));
        assert!(!fnmatch("x[!]]", "x]"));
    }

    /// An unterminated class does not compile, and dbt's own `fnmatch` reports
    /// the failure and returns false — so the value matches nothing rather than
    /// silently becoming a literal bracket. The reimplementation this module
    /// replaced treated it as a literal, which selected a node dbt would not.
    #[test]
    fn an_unterminated_class_matches_nothing() {
        assert!(!fnmatch("models/[draft", "models/[draft"));
        assert!(!fnmatch("a[", "a["));
    }

    #[test]
    fn recursive_wildcard_spans_whole_components_and_may_be_empty() {
        assert!(fnmatch("a/**/b", "a/b"), "`**/` collapses to nothing");
        assert!(fnmatch("a/**/b", "a/x/b"));
        assert!(fnmatch("a/**/b", "a/x/y/b"));
        assert!(!fnmatch("a/**/b", "a/xb"));
        assert!(fnmatch("**/b", "x/y/b"));
        assert!(fnmatch("models/**/*items.sql", "models/staging/orders/order_items.sql"));
    }

    #[test]
    fn misplaced_recursive_wildcard_is_a_compile_error() {
        assert!(Pattern::new("a**/b").is_err());
        assert!(Pattern::new("a/**b").is_err());
        assert!(Pattern::new("a/***/b").is_err());
        // A pattern that will not compile matches nothing rather than panicking.
        assert!(!fnmatch("a**/b", "a/b"));
    }

    #[test]
    fn anchors_hold_at_both_ends() {
        assert!(!fnmatch("stg_*", "x_stg_orders"));
        assert!(!fnmatch("*_orders", "stg_orders_v2"));
        assert!(fnmatch("*", ""));
        assert!(fnmatch("**", "anything"));
    }
}
