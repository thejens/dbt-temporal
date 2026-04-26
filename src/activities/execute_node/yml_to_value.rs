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
