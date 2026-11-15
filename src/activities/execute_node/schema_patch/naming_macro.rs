//! Calling the project's `generate_schema_name` / `generate_database_name`
//! with the arguments dbt gives them when it parses the project.
//!
//! dbt (`dbt_jinja_utils::utils::generate_component_name`) calls the macro with
//! two positional arguments: the node's `config.<component>` value — `none`
//! when the node sets none — and the whole node serialized with `none` fields
//! kept, its `path` relative to the resource directory. A macro that branches
//! on any node attribute therefore sees the same node here as at parse time.

use std::collections::BTreeMap;
use std::path::Path;

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::telemetry::NodeType;

use crate::error::DbtTemporalError;
use crate::worker::relation_cache::NamingMacro;
use crate::worker_state::WorkerState;

/// Call the project's `generate_schema_name` / `generate_database_name` with
/// this node.
///
/// A macro defined by a project is a *template*, `<package>.<macro>`, not a
/// global of the Jinja environment. Rendering `{{ generate_schema_name(…) }}`
/// as a plain expression therefore resolved dbt's built-in and returned
/// `target.schema` no matter what the project had written — a project's
/// override was accepted at startup and then never actually run. The macro is
/// looked up and invoked the same way a materialization is.
pub(super) fn render_naming_macro(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    template_name: &str,
    macro_name: &str,
    custom_name: Option<&str>,
    node: &minijinja::Value,
) -> Result<String, anyhow::Error> {
    let template = jinja_env
        .env
        .get_template(template_name)
        .map_err(|e| anyhow::anyhow!("template {template_name} not found: {e}"))?;
    let state = template
        .eval_to_state(BTreeMap::<String, minijinja::Value>::new(), &[])
        .map_err(|e| anyhow::anyhow!("evaluating {template_name}: {e}"))?;
    let func = state
        .lookup(macro_name, &[])
        .ok_or_else(|| anyhow::anyhow!("macro {macro_name} not found in {template_name}"))?;

    let custom = custom_name.map_or_else(|| minijinja::Value::from(()), minijinja::Value::from);
    let out = func
        .call(&state, &[custom, node.clone()], &[])
        .map_err(|e| anyhow::anyhow!("calling {macro_name}: {e}"))?;
    Ok(out.as_str().unwrap_or_default().trim().to_string())
}

/// The template defining a naming macro, looked up once per worker.
///
/// The error names the macro and surfaces as a compilation failure at the
/// call site, as a failed render does.
pub(super) fn naming_template<'a>(
    state: &'a WorkerState,
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    which: NamingMacro,
) -> Result<&'a str, DbtTemporalError> {
    let macro_name = match which {
        NamingMacro::Schema => "generate_schema_name",
        NamingMacro::Database => "generate_database_name",
    };
    state
        .naming
        .template(which, || naming_macro_template(jinja_env, &state.project_name, macro_name))
        .ok_or_else(|| DbtTemporalError::Compilation(format!("no template defines {macro_name}")))
}

/// The template that defines a naming macro, preferring the root project's own
/// — which is dbt's precedence: a project's override beats a package's.
fn naming_macro_template(
    jinja_env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
    project_name: &str,
    macro_name: &str,
) -> Option<String> {
    let preferred = format!("{project_name}.{macro_name}");
    if jinja_env.env.get_template(&preferred).is_ok() {
        return Some(preferred);
    }
    crate::activities::node_helpers::find_materialization_template(jinja_env, macro_name)
}

/// The `custom_schema_name` / `custom_database_name` argument dbt passes: the
/// node's resolved `config` value for `key` (`"schema"` or `"database"`), which
/// is already rendered. `None` means the node set none.
pub(super) fn custom_name(node: &dyn InternalDbtNodeAttributes, key: &str) -> Option<String> {
    node.serialized_config()
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// The `node` a naming macro is handed: the node as dbt serializes it for the
/// same call at parse time.
///
/// Macros in the wild read far more than the name — `node.config.materialized`,
/// `node.tags`, `node.fqn[0]`, `node.resource_type`, `node.path`. An attribute
/// that is missing reads as "not set" rather than as an error, so a stand-in
/// with fewer fields quietly sends a node to the wrong schema.
///
/// `schema` and `database` are what the node held when dbt called the macro:
/// the profile's target values, not the result of the macro. Under a
/// per-workflow override they are the override's values, as in a fresh parse.
pub(super) fn naming_macro_node(
    node: &dyn InternalDbtNodeAttributes,
    target_schema: &str,
    target_database: &str,
) -> minijinja::Value {
    let mut serialized = node.serialize_keep_none();
    if let dbt_yaml::Value::Mapping(map, _) = &mut serialized {
        // dbt hands macros the path below the resource directory
        // ("staging/m.sql", not "models/staging/m.sql"), as dbt-core does.
        let dir = match node.resource_type() {
            NodeType::Model => "models",
            NodeType::Snapshot => "snapshots",
            NodeType::Seed => "seeds",
            NodeType::Analysis => "analyses",
            _ => "",
        };
        let path_key = dbt_yaml::Value::string("path".to_string());
        if !dir.is_empty()
            && let Some(path) = map.get(&path_key).and_then(|v| v.as_str())
            && let Ok(below) = Path::new(path).strip_prefix(dir)
        {
            let below = below.to_string_lossy().into_owned();
            map.insert(path_key, dbt_yaml::Value::string(below));
        }
        map.insert("schema".into(), dbt_yaml::Value::string(target_schema.to_string()));
        map.insert("database".into(), dbt_yaml::Value::string(target_database.to_string()));
    }
    minijinja::Value::from_serialize(serialized)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::worker::relation_cache::{NodeRelation, RunRelations};

    fn node_named(name: &str) -> minijinja::Value {
        let mut map = BTreeMap::<String, minijinja::Value>::new();
        map.insert("name".to_owned(), minijinja::Value::from(name));
        minijinja::Value::from(map)
    }

    /// A project macro is a template named `<package>.<macro>`, which is why
    /// the renderer looks one up rather than evaluating a bare expression.
    fn env_with_macro(
        template_name: &str,
        body: &str,
    ) -> dbt_jinja_utils::jinja_environment::JinjaEnv {
        let mut env = minijinja::Environment::new();
        env.add_template_owned(template_name.to_string(), body.to_string(), None)
            .expect("add template");
        dbt_jinja_utils::jinja_environment::JinjaEnv::new(env)
    }

    /// What the activity does on a cold worker: find the template, then render.
    fn render_via_lookup(
        env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
        macro_name: &str,
        custom_name: Option<&str>,
        node: &minijinja::Value,
    ) -> Result<String, anyhow::Error> {
        let template = naming_macro_template(env, "spike", macro_name)
            .ok_or_else(|| anyhow::anyhow!("no template defines {macro_name}"))?;
        render_naming_macro(env, &template, macro_name, custom_name, node)
    }

    const SCHEMA_MACRO: &str = "{% macro generate_schema_name(custom, node) %}\
        {%- if custom is none -%}default_{{ node.name }}\
        {%- else -%}custom_{{ custom }}_{{ node.name }}{%- endif -%}\
    {% endmacro %}";

    #[test]
    fn render_naming_macro_passes_none_when_the_node_sets_no_override() {
        let env = env_with_macro("spike.generate_schema_name", SCHEMA_MACRO);
        let result =
            render_via_lookup(&env, "generate_schema_name", None, &node_named("my_model")).unwrap();
        assert_eq!(result, "default_my_model");
    }

    #[test]
    fn render_naming_macro_passes_the_custom_name_through() {
        let env = env_with_macro("spike.generate_schema_name", SCHEMA_MACRO);
        let result = render_via_lookup(
            &env,
            "generate_schema_name",
            Some("marketing"),
            &node_named("my_model"),
        )
        .unwrap();
        assert_eq!(result, "custom_marketing_my_model");
    }

    /// A project can override the database macro alone, so it is dispatched by
    /// name rather than assumed to be the schema one.
    #[test]
    fn render_naming_macro_calls_the_macro_it_is_given() {
        let env = env_with_macro(
            "spike.generate_database_name",
            "{% macro generate_database_name(custom, node) %}db_{{ node.name }}{% endmacro %}",
        );
        let result =
            render_via_lookup(&env, "generate_database_name", None, &node_named("my_model"))
                .unwrap();
        assert_eq!(result, "db_my_model");
    }

    /// dbt's precedence: the root project's override wins over a package's.
    #[test]
    fn render_naming_macro_prefers_the_root_projects_own() {
        let mut env = minijinja::Environment::new();
        env.add_template_owned(
            "some_package.generate_schema_name".to_string(),
            "{% macro generate_schema_name(custom, node) %}package{% endmacro %}".to_string(),
            None,
        )
        .expect("add template");
        env.add_template_owned(
            "spike.generate_schema_name".to_string(),
            "{% macro generate_schema_name(custom, node) %}root{% endmacro %}".to_string(),
            None,
        )
        .expect("add template");
        let env = dbt_jinja_utils::jinja_environment::JinjaEnv::new(env);
        let result =
            render_via_lookup(&env, "generate_schema_name", None, &node_named("m")).unwrap();
        assert_eq!(result, "root");
    }

    /// A package's macro is still used when the root project defines none —
    /// which is how a project inherits one from a dependency.
    #[test]
    fn render_naming_macro_falls_back_to_a_package() {
        let env = env_with_macro(
            "some_package.generate_schema_name",
            "{% macro generate_schema_name(custom, node) %}package_{{ node.name }}{% endmacro %}",
        );
        let result =
            render_via_lookup(&env, "generate_schema_name", None, &node_named("m")).unwrap();
        assert_eq!(result, "package_m");
    }

    #[test]
    fn render_naming_macro_reports_a_macro_it_cannot_find() {
        let env = env_with_macro("spike.other", "{% macro other() %}x{% endmacro %}");
        let err = render_via_lookup(&env, "generate_schema_name", None, &node_named("m"))
            .expect_err("a missing macro must be reported");
        assert!(err.to_string().contains("generate_schema_name"), "should name the macro: {err}");
    }

    /// A custom name carrying an apostrophe reaches the macro intact — it is
    /// passed as a value, not spliced into a template string.
    #[test]
    fn render_naming_macro_passes_a_quoted_custom_name_through() {
        let env = env_with_macro("spike.generate_schema_name", SCHEMA_MACRO);
        let result =
            render_via_lookup(&env, "generate_schema_name", Some("it's"), &node_named("m"))
                .unwrap();
        assert_eq!(result, "custom_it's_m");
    }

    // --- per-run memoization ---

    /// The resolution the activity runs per node under custom macros: schema
    /// from the macro, database left at the node's own.
    fn macro_relation(
        env: &dbt_jinja_utils::jinja_environment::JinjaEnv,
        name: &str,
        custom: Option<&str>,
    ) -> Result<NodeRelation, anyhow::Error> {
        Ok(NodeRelation {
            database: "warehouse".to_string(),
            schema: render_via_lookup(env, "generate_schema_name", custom, &node_named(name))?,
        })
    }

    const NODES: [(&str, Option<&str>); 4] = [
        ("customers", None),
        ("orders", Some("marketing")),
        ("payments", Some("finance")),
        ("stg_customers", None),
    ];

    /// Memoizing must not change any answer, whatever order or how often nodes
    /// are asked for.
    #[test]
    fn memoized_relations_equal_the_uncached_ones() {
        let env = env_with_macro("spike.generate_schema_name", SCHEMA_MACRO);
        let run = RunRelations::default();

        // Three passes, the later ones in a different order, as overlapping
        // ancestor sets of different nodes ask for them.
        for pass in [
            NODES.to_vec(),
            NODES.iter().rev().copied().collect(),
            NODES.to_vec(),
        ] {
            for (name, custom) in pass {
                let memoized = run
                    .resolve(name, || macro_relation(&env, name, custom))
                    .unwrap();
                let uncached = macro_relation(&env, name, custom).unwrap();
                assert_eq!(memoized, uncached, "{name}");
            }
        }
    }

    #[test]
    fn a_node_already_resolved_in_the_run_is_not_rendered_again() {
        let env = env_with_macro("spike.generate_schema_name", SCHEMA_MACRO);
        let run = RunRelations::default();
        let renders = std::cell::Cell::new(0);
        let lookup = |name: &str, custom: Option<&str>| {
            run.resolve(name, || {
                renders.set(renders.get() + 1);
                macro_relation(&env, name, custom)
            })
            .unwrap()
        };

        for (name, custom) in NODES {
            lookup(name, custom);
        }
        assert_eq!(renders.get(), NODES.len());
        for (name, custom) in NODES {
            lookup(name, custom);
        }
        assert_eq!(renders.get(), NODES.len(), "the second lookups rendered nothing");
    }
}
