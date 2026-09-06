//! The `config.<key>:` selector method.

use dbt_yaml::Value as YmlValue;

use super::glob;

/// A `config` criterion: the keys to walk into, and the value to match there.
///
/// dbt spells the same thing two ways — `config.materialized:table` (dot
/// notation, keys in `method_args`) and `config:materialized:table` (the form a
/// `selectors.yml` definition writes, key and value in one string). Both land
/// here as the same keys/pattern pair.
#[derive(Debug, Clone)]
pub struct ConfigCriterion<'a> {
    keys: Vec<&'a str>,
    pattern: &'a str,
}

impl<'a> ConfigCriterion<'a> {
    /// Build from a criterion's `method_args` and value.
    ///
    /// A `config` criterion with neither dot-notation keys nor a `key:value`
    /// body names no key at all and can never match a node, so it is rejected
    /// rather than left to return an empty set.
    pub fn parse(method_args: &'a [String], value: &'a str) -> Result<Self, String> {
        if method_args.is_empty() {
            let Some((key, pattern)) = value.split_once(':') else {
                return Err(
                    "config needs a key — write `config.<key>:<value>` or `config:<key>:<value>`"
                        .to_string(),
                );
            };
            return Ok(Self {
                keys: vec![key],
                pattern,
            });
        }
        Ok(Self {
            keys: method_args.iter().map(String::as_str).collect(),
            pattern: value,
        })
    }

    /// True when the node's rendered config holds a matching value at the key path.
    ///
    /// Matching is against the *serialized* config rather than any typed field
    /// on the node, which is what makes every key reachable and what makes the
    /// node's own spelling of a value authoritative.
    pub fn matches(&self, config: &YmlValue) -> bool {
        self.matches_from(config, 0)
    }

    fn matches_from(&self, config: &YmlValue, depth: usize) -> bool {
        let Some(key) = self.keys.get(depth) else {
            return false;
        };
        let YmlValue::Mapping(mapping, ..) = config else {
            return false;
        };
        let Some(value) = mapping.get(YmlValue::string((*key).to_string())) else {
            return false;
        };
        match value {
            YmlValue::String(text, ..) => glob::fnmatch(self.pattern, text),
            // Booleans and numbers are compared through their rendered form, so
            // `config.enabled:true` and `config.threads:4` both work. Only the
            // boolean comparison ignores case — that is dbt's asymmetry.
            YmlValue::Bool(flag, ..) => self.pattern.eq_ignore_ascii_case(&flag.to_string()),
            YmlValue::Number(number, ..) => number.to_string() == self.pattern,
            // A list config (`tags`, `cluster_by`, a compound `unique_key`)
            // matches when any element does.
            YmlValue::Sequence(items, ..) => items
                .iter()
                .filter_map(YmlValue::as_str)
                .any(|item| glob::fnmatch(self.pattern, item)),
            // A nested mapping consumes the next key in the path.
            YmlValue::Mapping(..) => self.matches_from(value, depth + 1),
            _ => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn config() -> YmlValue {
        dbt_yaml::from_str(
            "materialized: incremental\n\
             schema: audit\n\
             enabled: true\n\
             threads: 4\n\
             cluster_by:\n  - geo_country\n  - event_day\n\
             unique_key:\n  - column_a\n  - column_b\n\
             meta:\n  owner: finance\n",
        )
        .unwrap()
    }

    fn criterion<'a>(args: &'a [String], value: &'a str) -> ConfigCriterion<'a> {
        ConfigCriterion::parse(args, value).unwrap()
    }

    #[test]
    fn dot_notation_matches_scalar_values() {
        let args = vec!["materialized".to_string()];
        assert!(criterion(&args, "incremental").matches(&config()));
        assert!(!criterion(&args, "view").matches(&config()));

        let args = vec!["schema".to_string()];
        assert!(criterion(&args, "audit").matches(&config()));
        assert!(!criterion(&args, "staging").matches(&config()));
    }

    #[test]
    fn booleans_and_numbers_match_through_their_rendered_form() {
        let args = vec!["enabled".to_string()];
        assert!(criterion(&args, "true").matches(&config()));
        assert!(criterion(&args, "TRUE").matches(&config()));
        assert!(!criterion(&args, "false").matches(&config()));

        let args = vec!["threads".to_string()];
        assert!(criterion(&args, "4").matches(&config()));
        assert!(!criterion(&args, "8").matches(&config()));
    }

    #[test]
    fn list_configs_match_on_any_element() {
        let args = vec!["cluster_by".to_string()];
        assert!(criterion(&args, "geo_country").matches(&config()));
        assert!(criterion(&args, "event_day").matches(&config()));
        assert!(criterion(&args, "geo_*").matches(&config()));
        assert!(!criterion(&args, "user_id").matches(&config()));

        let args = vec!["unique_key".to_string()];
        assert!(criterion(&args, "column_a").matches(&config()));
    }

    #[test]
    fn nested_keys_walk_into_mappings() {
        let args = vec!["meta".to_string(), "owner".to_string()];
        assert!(criterion(&args, "finance").matches(&config()));
        assert!(!criterion(&args, "marketing").matches(&config()));

        // A mapping with no further key to consume matches nothing.
        let args = vec!["meta".to_string()];
        assert!(!criterion(&args, "finance").matches(&config()));
    }

    #[test]
    fn absent_keys_and_non_mappings_match_nothing() {
        let args = vec!["not_a_key".to_string()];
        assert!(!criterion(&args, "value").matches(&config()));
        assert!(!criterion(&args, "value").matches(&YmlValue::string("scalar".to_string())));
    }

    #[test]
    fn colon_form_carries_the_key_in_the_value() {
        assert!(criterion(&[], "materialized:incremental").matches(&config()));
        assert!(criterion(&[], "schema:audit").matches(&config()));
        assert!(!criterion(&[], "materialized:view").matches(&config()));
    }

    #[test]
    fn a_config_criterion_with_no_key_is_rejected() {
        let err = ConfigCriterion::parse(&[], "materialized").unwrap_err();
        assert!(err.contains("config"), "{err}");
    }
}
