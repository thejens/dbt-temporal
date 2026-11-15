//! Fixtures shared by the tests of the helpers in this module.

#[allow(clippy::expect_used)]
pub(super) fn jinja_env_with_templates(
    templates: &[(&str, &str)],
) -> dbt_jinja_utils::jinja_environment::JinjaEnv {
    let mut env = minijinja::Environment::new();
    for (name, body) in templates {
        env.add_template_owned(name.to_string(), body.to_string(), None)
            .expect("add template");
    }
    dbt_jinja_utils::jinja_environment::JinjaEnv::new(env)
}
