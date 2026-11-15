//! Bridging per-workflow values into the Jinja environment.

use std::collections::BTreeMap;

use anyhow::Context as _;

/// Patch the `target` (and `env`) Jinja global with per-workflow schema/database.
///
/// The startup `target` global has the profile schema/database from worker init.
/// When per-workflow env overrides change the profile (e.g. different schema via env_var()),
/// all Jinja macros that access `target.schema` / `target.database` must see the new values.
/// This includes `generate_schema_name`, `generate_database_name`, materialization templates,
/// and any custom macros.
///
/// Every failure here is an error rather than a silent return. This runs only
/// when a workflow resolved a different profile than the worker started on, so
/// giving up leaves `target` describing the *startup* warehouse while the
/// adapter is connected to another one — every `target.schema` in the project
/// then names a schema the run is not writing to.
pub(in crate::activities) fn patch_target_global(
    jinja_env: &mut dbt_jinja_utils::jinja_environment::JinjaEnv,
    schema: &str,
    database: Option<&str>,
    target_name: Option<&str>,
) -> Result<(), anyhow::Error> {
    // Extract current target as JSON, modify fields, re-inject as a native BTreeMap Value.
    let target_json = jinja_env
        .render_str("{{ target | tojson }}", BTreeMap::<String, minijinja::Value>::new(), &[])
        .map_err(|e| anyhow::anyhow!("reading the current Jinja target: {e}"))?;

    let json_val: serde_json::Value = serde_json::from_str(&target_json)
        .with_context(|| format!("parsing the current Jinja target: {target_json}"))?;
    let obj = json_val
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("the Jinja target is not an object: {json_val}"))?;

    let mut new_target: BTreeMap<String, minijinja::Value> = obj
        .iter()
        .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
        .collect();

    new_target.insert("schema".to_string(), minijinja::Value::from(schema));
    // A target that declares no database keeps the one the adapter derived —
    // DuckDB names it after the file, and overwriting that with an empty
    // string puts `""` into every relation the project renders.
    if let Some(database) = database {
        new_target.insert("database".to_string(), minijinja::Value::from(database));
    }
    if let Some(name) = target_name {
        new_target.insert("name".to_string(), minijinja::Value::from(name));
        new_target.insert("target_name".to_string(), minijinja::Value::from(name));
    }

    let val = minijinja::Value::from(new_target);
    jinja_env.env.add_global("target", val.clone());
    // In dbt, `env` is an alias for `target`.
    jinja_env.env.add_global("env", val);
    Ok(())
}

/// Convert a `serde_json::Value` to a native `minijinja::Value`.
///
/// Produces native minijinja types (Vec, BTreeMap) that support iteration,
/// attribute access, and `.get()` in Jinja templates — unlike `from_serialize`
/// which creates "plain objects" with limited Jinja interop.
#[allow(clippy::option_if_let_else)]
pub(in crate::activities) fn json_to_minijinja(v: &serde_json::Value) -> minijinja::Value {
    match v {
        serde_json::Value::Null => minijinja::Value::from(()),
        serde_json::Value::Bool(b) => minijinja::Value::from(*b),
        serde_json::Value::Number(n) => {
            // `u64` before `f64`: a JSON integer above `i64::MAX` is exact as
            // an unsigned, and lossy as a float. Warehouse ids and epoch
            // nanoseconds land in that range, and a var silently rounded to
            // the nearest representable double changes what the model selects.
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
        serde_json::Value::String(s) => minijinja::Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            minijinja::Value::from(arr.iter().map(json_to_minijinja).collect::<Vec<_>>())
        }
        serde_json::Value::Object(obj) => {
            let map: BTreeMap<String, minijinja::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
                .collect();
            minijinja::Value::from(map)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn json_to_minijinja_null_maps_to_unit() {
        let v = json_to_minijinja(&serde_json::Value::Null);
        assert!(v.is_none() || v.is_undefined());
    }

    #[test]
    fn json_to_minijinja_primitives() {
        assert_eq!(json_to_minijinja(&serde_json::json!(true)).to_string(), "True");
        assert_eq!(json_to_minijinja(&serde_json::json!(42)).to_string(), "42");
        assert_eq!(json_to_minijinja(&serde_json::json!(-3)).to_string(), "-3");
        assert_eq!(json_to_minijinja(&serde_json::json!(2.5)).to_string(), "2.5");
        assert_eq!(json_to_minijinja(&serde_json::json!("hi")).as_str(), Some("hi"));
    }

    #[test]
    fn json_to_minijinja_array_yields_list() {
        let v = json_to_minijinja(&serde_json::json!([1, 2, "x"]));
        let items: Vec<String> = v.try_iter().unwrap().map(|i| i.to_string()).collect();
        assert_eq!(items, vec!["1", "2", "x"]);
    }

    #[test]
    fn json_to_minijinja_object_supports_attribute_access() {
        let v = json_to_minijinja(&serde_json::json!({"a": 1, "b": "two"}));
        assert_eq!(v.get_attr("a").unwrap().to_string(), "1");
        assert_eq!(v.get_attr("b").unwrap().as_str(), Some("two"));
    }

    #[test]
    fn json_to_minijinja_large_unsigned_keeps_every_digit() {
        // Above i64::MAX the value has to go through the u64 branch: as an f64
        // it would round to the nearest double and come back as a different
        // number, which is how a var reaches SQL as the wrong id.
        let v = json_to_minijinja(&serde_json::json!(u64::MAX));
        assert_eq!(v.to_string(), u64::MAX.to_string());

        let id = 9_007_199_254_740_993_u64; // 2^53 + 1 — not representable as f64
        let v = json_to_minijinja(&serde_json::json!(id));
        assert_eq!(v.to_string(), id.to_string());
    }

    // --- patch_target_global ---

    fn make_jinja_env_with_target(
        initial: &serde_json::Value,
    ) -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        let mut env = minijinja::Environment::new();
        env.add_global("target", json_to_minijinja(initial));
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(env)
    }

    #[test]
    fn patch_target_global_overrides_schema_and_database() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "old_schema",
            "database": "old_db",
            "name": "dev",
        }));
        patch_target_global(&mut env, "new_schema", Some("new_db"), None)
            .expect("patching the target succeeds");

        let rendered = env
            .render_str(
                "{{ target.schema }}|{{ target.database }}|{{ target.name }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "new_schema|new_db|dev");
    }

    #[test]
    fn patch_target_global_overrides_name_when_provided() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "s",
            "database": "d",
            "name": "old",
        }));
        patch_target_global(&mut env, "s", Some("d"), Some("prod"))
            .expect("patching the target succeeds");

        let rendered = env
            .render_str(
                "{{ target.name }}|{{ target.target_name }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "prod|prod");
    }

    #[test]
    fn patch_target_global_aliases_env_to_target() {
        let mut env = make_jinja_env_with_target(&serde_json::json!({
            "schema": "s",
            "database": "d",
        }));
        patch_target_global(&mut env, "s2", Some("d2"), None)
            .expect("patching the target succeeds");

        let rendered = env
            .render_str(
                "{{ env.schema }}|{{ env.database }}",
                BTreeMap::<String, minijinja::Value>::new(),
                &[],
            )
            .unwrap();
        assert_eq!(rendered, "s2|d2");
    }

    /// A `target` that is not an object cannot be patched. Returning quietly
    /// left it describing the startup warehouse while the adapter was connected
    /// to another one, so every `target.schema` in the project named a schema
    /// the run was not writing to.
    #[test]
    fn patch_target_global_reports_a_target_it_cannot_patch() {
        let mut env = minijinja::Environment::new();
        env.add_global("target", minijinja::Value::from("not_an_object"));
        let mut jenv = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);

        let err = patch_target_global(&mut jenv, "s", None, None)
            .expect_err("a non-object target must be reported");
        assert!(err.to_string().contains("not an object"), "should say what was wrong: {err}");
    }
}
