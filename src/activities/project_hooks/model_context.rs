//! The `model` placeholder project-level hooks render against.

/// Create a `model` placeholder for project-level hook rendering.
///
/// Returns an object where any attribute access returns an empty string, matching
/// dbt-core's behaviour: when `model` is not in scope, macros that read `model.schema`,
/// `model.name`, etc. get empty strings rather than undefined errors.
pub(super) fn empty_model_context() -> minijinja::Value {
    use std::sync::Arc;

    #[derive(Debug)]
    struct EmptyModel;

    impl minijinja::value::Object for EmptyModel {
        fn get_value(self: &Arc<Self>, _key: &minijinja::Value) -> Option<minijinja::Value> {
            Some(minijinja::Value::from(""))
        }
    }

    minijinja::Value::from_object(EmptyModel)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// `empty_model_context` returns an object where any attribute access yields `""`.
    /// This lets on-run-start/on-run-end macros use `model.schema`, `model.name`, etc.
    /// without erroring — matching dbt-core's behaviour where `model` is an empty context
    /// object rather than absent from the rendering context.
    #[test]
    fn empty_model_context_allows_model_attr_in_hook_templates() -> anyhow::Result<()> {
        let env = minijinja::Environment::new();
        let mut ctx: BTreeMap<String, minijinja::Value> = BTreeMap::new();
        ctx.insert("model".to_owned(), empty_model_context());

        // `model.schema` should return empty string, making `+` concatenation work.
        let rendered = env
            .template_from_str(r#"{{ "s=" + model.schema }}"#)?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "s=", "model.schema should render as empty string");

        // Any attribute access returns empty string.
        let rendered = env
            .template_from_str(r"{{ model.name }}")?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "", "model.name should render as empty string");

        // `model is defined` is true — the placeholder is a real value.
        let rendered = env
            .template_from_str(r"{% if model is defined %}yes{% else %}no{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "yes", "model should be defined");

        Ok(())
    }
}
