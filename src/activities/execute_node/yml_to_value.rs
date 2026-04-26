//! Convert `dbt_yaml::Value` into native `minijinja::Value`.
//!
//! `minijinja::Value::from_serialize()` produces an opaque object that doesn't
//! support iteration or `**kwargs` splatting in templates. Generic test kwargs
//! (`accepted_values values=[...]`, `greater_than threshold=0`, etc.) need
//! native list/map types, which is what these helpers produce.

use std::collections::BTreeMap;

use dbt_jinja_utils::jinja_environment::JinjaEnv;

#[allow(clippy::option_if_let_else)]
pub fn yml_value_to_minijinja(v: &dbt_yaml::Value) -> minijinja::Value {
    match v {
        dbt_yaml::Value::Null(_) => minijinja::Value::from(()),
        dbt_yaml::Value::Bool(b, _) => minijinja::Value::from(*b),
        dbt_yaml::Value::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(u) = n.as_u64() {
                minijinja::Value::from(u)
            } else if let Some(f) = n.as_f64() {
                minijinja::Value::from(f)
            } else {
                minijinja::Value::from(n.to_string())
            }
        }
        dbt_yaml::Value::String(s, _) => minijinja::Value::from(s.clone()),
        dbt_yaml::Value::Sequence(seq, _) => {
            let items: Vec<minijinja::Value> = seq.iter().map(yml_value_to_minijinja).collect();
            minijinja::Value::from(items)
        }
        dbt_yaml::Value::Mapping(map, _) => {
            let items: BTreeMap<String, minijinja::Value> = map
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        dbt_yaml::Value::String(s, _) => s.clone(),
                        other => format!("{other:?}"),
                    };
                    (key, yml_value_to_minijinja(v))
                })
                .collect();
            minijinja::Value::from(items)
        }
        dbt_yaml::Value::Tagged(tagged, _) => yml_value_to_minijinja(&tagged.value),
    }
}

/// Generic test kwargs can include expression strings like
/// `"{{ get_where_subquery(ref('my_model')) }}"`. Strip the wrapping `{{ ... }}`
/// and evaluate so `**_dbt_generic_test_kwargs` behaves like dbt-fusion's
/// executor path. Falls back to the literal value on any error.
pub fn yml_value_to_minijinja_with_jinja(
    v: &dbt_yaml::Value,
    jinja_env: &JinjaEnv,
    node_context: &BTreeMap<String, minijinja::Value>,
) -> minijinja::Value {
    let base = yml_value_to_minijinja(v);
    let Some(raw) = base.as_str() else {
        return base;
    };

    let Some(expr) = raw
        .trim()
        .strip_prefix("{{")
        .and_then(|s| s.strip_suffix("}}"))
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return base;
    };

    jinja_env
        .env
        .compile_expression(expr)
        .and_then(|compiled| compiled.eval(node_context, &[]))
        .unwrap_or(base)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn parse(s: &str) -> dbt_yaml::Value {
        dbt_yaml::from_str(s).expect("parse yaml")
    }

    #[test]
    fn null_maps_to_unit() {
        let v = yml_value_to_minijinja(&parse("null"));
        assert!(v.is_undefined() || v.is_none());
    }

    #[test]
    fn bool_maps_to_bool() {
        assert_eq!(yml_value_to_minijinja(&parse("true")).to_string(), "True");
        assert_eq!(yml_value_to_minijinja(&parse("false")).to_string(), "False");
    }

    #[test]
    fn integer_maps_to_i64() {
        let v = yml_value_to_minijinja(&parse("42"));
        assert_eq!(v.to_string(), "42");
    }

    #[test]
    fn negative_integer_maps_to_i64() {
        let v = yml_value_to_minijinja(&parse("-7"));
        assert_eq!(v.to_string(), "-7");
    }

    #[test]
    fn large_unsigned_maps_to_u64() {
        // 2^63 doesn't fit in i64 but does in u64.
        let v = yml_value_to_minijinja(&parse("9223372036854775808"));
        assert_eq!(v.to_string(), "9223372036854775808");
    }

    #[test]
    fn float_maps_to_f64() {
        let v = yml_value_to_minijinja(&parse("3.14"));
        assert_eq!(v.to_string(), "3.14");
    }

    #[test]
    fn string_maps_to_string() {
        let v = yml_value_to_minijinja(&parse("\"hi\""));
        assert_eq!(v.as_str(), Some("hi"));
    }

    #[test]
    fn sequence_maps_to_list() {
        let v = yml_value_to_minijinja(&parse("[1, 2, 3]"));
        let items: Vec<i64> = v
            .try_iter()
            .unwrap()
            .map(|x| x.to_string().parse().unwrap())
            .collect();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn mapping_maps_to_object() {
        let v = yml_value_to_minijinja(&parse("{a: 1, b: 2}"));
        assert_eq!(v.get_attr("a").unwrap().to_string(), "1");
        assert_eq!(v.get_attr("b").unwrap().to_string(), "2");
    }

    #[test]
    fn nested_structure_round_trips() {
        let v = yml_value_to_minijinja(&parse("{values: [10, 20, null]}"));
        let values = v.get_attr("values").unwrap();
        let items: Vec<String> = values.try_iter().unwrap().map(|x| x.to_string()).collect();
        assert_eq!(items, vec!["10", "20", "None"]);
    }

    #[test]
    fn non_string_mapping_key_uses_debug_format() {
        // Numeric keys are valid YAML and get debug-formatted into a string.
        let v = yml_value_to_minijinja(&parse("{1: \"one\"}"));
        // Debug format of Number includes the number; just check the value resolves.
        let keys: Vec<String> = v.try_iter().unwrap().map(|k| k.to_string()).collect();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains('1'));
    }

    fn empty_jinja_env() -> JinjaEnv {
        JinjaEnv::new(minijinja::Environment::new())
    }

    #[test]
    fn with_jinja_passes_through_non_string() {
        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        let v = yml_value_to_minijinja_with_jinja(&parse("42"), &env, &ctx);
        assert_eq!(v.to_string(), "42");
    }

    #[test]
    fn with_jinja_passes_through_plain_string() {
        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        let v = yml_value_to_minijinja_with_jinja(&parse("\"plain\""), &env, &ctx);
        assert_eq!(v.as_str(), Some("plain"));
    }

    #[test]
    fn with_jinja_evaluates_braced_expression() {
        let env = empty_jinja_env();
        let mut ctx = BTreeMap::new();
        ctx.insert("x".to_string(), minijinja::Value::from(7));
        let v = yml_value_to_minijinja_with_jinja(&parse("\"{{ x + 1 }}\""), &env, &ctx);
        assert_eq!(v.to_string(), "8");
    }

    #[test]
    fn with_jinja_falls_back_on_compile_error() {
        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        // Unknown reference fails at eval; we should get the raw string back.
        let v = yml_value_to_minijinja_with_jinja(
            &parse("\"{{ missing_var.missing_attr }}\""),
            &env,
            &ctx,
        );
        assert_eq!(v.as_str(), Some("{{ missing_var.missing_attr }}"));
    }

    #[test]
    fn with_jinja_ignores_empty_braces() {
        let env = empty_jinja_env();
        let ctx = BTreeMap::new();
        // "{{ }}" trims to empty — treated as a plain string.
        let v = yml_value_to_minijinja_with_jinja(&parse("\"{{ }}\""), &env, &ctx);
        assert_eq!(v.as_str(), Some("{{ }}"));
    }
}
