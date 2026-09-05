//! Shell-style pattern matching, reproducing the `glob` crate's `Pattern`.
//!
//! dbt runs nearly every selector value through `glob::Pattern::matches` (the
//! `fnmatch` helper in dbt-scheduler's `node_selector`), so what a selector
//! matches is defined by that crate's rules and not by an intuitive reading of
//! `*`: a `*` spans path separators, `?` is exactly one character, `[a-z]` and
//! `[!a-z]` are character classes, and `**` is legal only as a whole path
//! component. dbt-scheduler is not a dependency of this worker, so the rules
//! are reproduced here rather than approximated — a looser or stricter matcher
//! would select a different node set than dbt for the same selector string,
//! which is exactly the silent divergence the selector module exists to avoid.
//!
//! A value with no special characters is compared verbatim, so ordinary names
//! never acquire pattern meaning and never pay for compilation.

use std::path::is_separator;

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

/// One element of a compiled pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Char(char),
    /// `?`
    AnyChar,
    /// `*` — any run of characters, path separators included.
    AnySequence,
    /// `**` — any run of whole path components.
    AnyRecursiveSequence,
    /// `[abc]`
    AnyWithin(Vec<CharSpecifier>),
    /// `[!abc]`
    AnyExcept(Vec<CharSpecifier>),
}

/// One member of a character class: a literal or an inclusive range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CharSpecifier {
    Single(char),
    Range(char, char),
}

/// Why one attempt at matching the remaining tokens failed.
///
/// The distinction is what bounds backtracking: once the text is exhausted, no
/// wider split of an earlier `*` can succeed, so the whole attempt stops rather
/// than re-trying every remaining split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchResult {
    Match,
    SubPatternDoesntMatch,
    EntirePatternDoesntMatch,
}

/// A compiled glob pattern.
#[derive(Debug, Clone)]
pub struct Pattern {
    tokens: Vec<Token>,
}

impl Pattern {
    /// Compile `pattern`, or report why it is not a valid glob.
    pub fn new(pattern: &str) -> Result<Self, String> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut tokens = Vec::new();
        let mut i = 0;

        while i < chars.len() {
            match chars[i] {
                '?' => {
                    tokens.push(Token::AnyChar);
                    i += 1;
                }
                '*' => {
                    let start = i;
                    while i < chars.len() && chars[i] == '*' {
                        i += 1;
                    }
                    match i - start {
                        1 => tokens.push(Token::AnySequence),
                        2 => {
                            // `**` carries its recursive meaning only as a whole
                            // path component; dbt rejects every other placement
                            // rather than reading it as a literal pair of stars.
                            let opens_component = start == 0 || chars[start - 1] == '/';
                            let closes_component = i == chars.len() || chars[i] == '/';
                            if !opens_component || !closes_component {
                                return Err(format!(
                                    "`{pattern}`: recursive wildcards (`**`) must form a \
                                     whole path component"
                                ));
                            }
                            // Swallow the separator so `a/**/b` still matches `a/b`.
                            if chars.get(i) == Some(&'/') {
                                i += 1;
                            }
                            if tokens.last() != Some(&Token::AnyRecursiveSequence) {
                                tokens.push(Token::AnyRecursiveSequence);
                            }
                        }
                        _ => {
                            return Err(format!(
                                "`{pattern}`: wildcards are either regular `*` or recursive `**`"
                            ));
                        }
                    }
                }
                '[' => {
                    let consumed = parse_class(&chars, i, &mut tokens);
                    i += consumed;
                }
                c => {
                    tokens.push(Token::Char(c));
                    i += 1;
                }
            }
        }

        Ok(Self { tokens })
    }

    /// True when the whole of `text` is described by the pattern.
    pub fn matches(&self, text: &str) -> bool {
        self.matches_from(true, text.chars(), 0) == MatchResult::Match
    }

    /// Match `text` against the tokens from `offset` on.
    ///
    /// `follows_separator` tracks whether the previous character was a path
    /// separator; `**` may only resume matching on such a boundary, which is
    /// what makes it "whole components" rather than a second `*`.
    fn matches_from(
        &self,
        mut follows_separator: bool,
        mut text: std::str::Chars<'_>,
        offset: usize,
    ) -> MatchResult {
        for (index, token) in self.tokens[offset..].iter().enumerate() {
            let rest = offset + index + 1;
            match token {
                Token::AnySequence | Token::AnyRecursiveSequence => {
                    // Try the zero-width match, then widen one character at a
                    // time. Exhausting `text` here leaves the loop below with an
                    // empty iterator, which the literal arm turns into
                    // `EntirePatternDoesntMatch` on the next token.
                    match self.matches_from(follows_separator, text.clone(), rest) {
                        MatchResult::SubPatternDoesntMatch => {}
                        result => return result,
                    }
                    while let Some(c) = text.next() {
                        follows_separator = is_separator(c);
                        if *token == Token::AnyRecursiveSequence && !follows_separator {
                            continue;
                        }
                        match self.matches_from(follows_separator, text.clone(), rest) {
                            MatchResult::SubPatternDoesntMatch => {}
                            result => return result,
                        }
                    }
                }
                _ => {
                    let Some(c) = text.next() else {
                        return MatchResult::EntirePatternDoesntMatch;
                    };
                    let matched = match token {
                        Token::AnyChar => true,
                        Token::AnyWithin(specifiers) => in_specifiers(specifiers, c),
                        Token::AnyExcept(specifiers) => !in_specifiers(specifiers, c),
                        Token::Char(expected) => c == *expected,
                        Token::AnySequence | Token::AnyRecursiveSequence => false,
                    };
                    if !matched {
                        return MatchResult::SubPatternDoesntMatch;
                    }
                    follows_separator = is_separator(c);
                }
            }
        }

        if text.next().is_none() {
            MatchResult::Match
        } else {
            MatchResult::SubPatternDoesntMatch
        }
    }
}

/// Parse a `[...]` class starting at `start`, pushing one token, and report how
/// many characters were consumed.
///
/// An unterminated `[` is a literal bracket rather than an error — the same
/// forgiving reading dbt's matcher applies, so `path:models/[draft` still names
/// a directory that happens to contain a bracket.
fn parse_class(chars: &[char], start: usize, tokens: &mut Vec<Token>) -> usize {
    // `[!` negates; the character right after it is always a class member, so
    // the closing bracket is searched for one position further along. That is
    // what lets `[!]]` and `[]]` name a literal `]`.
    let (members_start, search_start, negated) =
        if start + 4 <= chars.len() && chars[start + 1] == '!' {
            (start + 2, start + 3, true)
        } else if start + 3 <= chars.len() && chars[start + 1] != '!' {
            (start + 1, start + 2, false)
        } else {
            tokens.push(Token::Char('['));
            return 1;
        };

    let Some(offset) = chars[search_start..].iter().position(|c| *c == ']') else {
        tokens.push(Token::Char('['));
        return 1;
    };
    let members = parse_char_specifiers(&chars[members_start..search_start + offset]);
    tokens.push(if negated {
        Token::AnyExcept(members)
    } else {
        Token::AnyWithin(members)
    });
    search_start + offset + 1 - start
}

/// Split the inside of a character class into literals and `a-z` ranges.
fn parse_char_specifiers(chars: &[char]) -> Vec<CharSpecifier> {
    let mut specifiers = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if i + 3 <= chars.len() && chars[i + 1] == '-' {
            specifiers.push(CharSpecifier::Range(chars[i], chars[i + 2]));
            i += 3;
        } else {
            specifiers.push(CharSpecifier::Single(chars[i]));
            i += 1;
        }
    }
    specifiers
}

fn in_specifiers(specifiers: &[CharSpecifier], c: char) -> bool {
    specifiers.iter().any(|specifier| match *specifier {
        CharSpecifier::Single(single) => c == single,
        CharSpecifier::Range(start, end) => start <= c && c <= end,
    })
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

    #[test]
    fn unterminated_class_is_a_literal_bracket() {
        assert!(fnmatch("models/[draft", "models/[draft"));
        assert!(fnmatch("a[", "a["));
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
