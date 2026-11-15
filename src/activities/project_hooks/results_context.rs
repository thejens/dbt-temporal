//! The `results` list an `on-run-end` hook sees.

use std::collections::BTreeMap;

use crate::activities::node_helpers::json_to_minijinja;
use crate::activities::store_artifacts::dbt_status;
use crate::types::NodeExecutionResult;

/// Convert the workflow's `Vec<NodeExecutionResult>` into the `results` list an
/// `on-run-end` hook sees.
///
/// Each item has the attributes of dbt's run result:
/// - `status`, in the words `run_results.json` uses (`success`, `error`,
///   `skipped`, `pass`, `fail`, `warn`, ...)
/// - `unique_id`, `execution_time`, `message`, `failures`, `thread_id`,
///   `compiled_code`
/// - `timing`, a list of `name` / `started_at` / `completed_at`
/// - `adapter_response`, the warehouse-specific response fields
/// - `node`, the resolved node as dbt serializes it (`name`, `resource_type`,
///   `config`, ...)
///
/// The node is looked up in the worker's resolver state at render time rather
/// than carried through Temporal workflow history.
pub(super) fn build_results_context(
    results: &[NodeExecutionResult],
    nodes: &dbt_schemas::schemas::Nodes,
) -> minijinja::Value {
    let items: Vec<minijinja::Value> = results
        .iter()
        .map(|r| {
            let mut map = BTreeMap::<String, minijinja::Value>::new();

            map.insert("unique_id".to_owned(), minijinja::Value::from(r.unique_id.clone()));
            map.insert("status".to_owned(), minijinja::Value::from(dbt_status(r).as_str()));
            map.insert("execution_time".to_owned(), minijinja::Value::from(r.execution_time));
            map.insert("message".to_owned(), optional(r.message.as_deref()));
            map.insert("failures".to_owned(), optional(r.failures));
            map.insert("compiled_code".to_owned(), optional(r.compiled_code.as_deref()));
            // The artifact writer names the thread the same way; see its comment.
            map.insert("thread_id".to_owned(), minijinja::Value::from("main"));
            map.insert(
                "timing".to_owned(),
                minijinja::Value::from(r.timing.iter().map(timing_value).collect::<Vec<_>>()),
            );

            // adapter_response as a flat map (rows_affected, message, code, query_id...).
            let resp: BTreeMap<String, minijinja::Value> = r
                .adapter_response
                .iter()
                .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
                .collect();
            map.insert("adapter_response".to_owned(), minijinja::Value::from(resp));

            map.insert("node".to_owned(), node_value(&r.unique_id, nodes));

            minijinja::Value::from(map)
        })
        .collect();
    minijinja::Value::from(items)
}

/// `none` for an absent value, as dbt's result objects hold.
fn optional<T: Into<minijinja::Value>>(value: Option<T>) -> minijinja::Value {
    value.map_or_else(|| minijinja::Value::from(()), Into::into)
}

/// One timing entry. The instants are naive datetimes, as dbt hands them, so
/// a hook can call `strftime` on them; an instant that does not parse is `none`.
fn timing_value(timing: &crate::types::TimingEntry) -> minijinja::Value {
    use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;

    let instant = |text: &str| {
        text.parse::<chrono::DateTime<chrono::Utc>>().map_or_else(
            |_| minijinja::Value::from(()),
            |at| minijinja::Value::from_object(PyDateTime::new_naive(at.naive_utc())),
        )
    };
    let mut entry = BTreeMap::<String, minijinja::Value>::new();
    entry.insert("name".to_owned(), minijinja::Value::from(timing.name.clone()));
    entry.insert("started_at".to_owned(), instant(&timing.started_at));
    entry.insert("completed_at".to_owned(), instant(&timing.completed_at));
    minijinja::Value::from(entry)
}

/// The `node` of a result: the resolved node as dbt serializes it, so
/// `node.config`, `node.tags` and the rest read as they do in dbt.
///
/// A result whose node is not in the resolver (a node of another project, or a
/// run planned by an older worker) carries only its `unique_id`.
fn node_value(unique_id: &str, nodes: &dbt_schemas::schemas::Nodes) -> minijinja::Value {
    let Some(node) = nodes.get_node(unique_id) else {
        let mut minimal = BTreeMap::<String, minijinja::Value>::new();
        minimal.insert("unique_id".to_owned(), minijinja::Value::from(unique_id.to_owned()));
        return minijinja::Value::from(minimal);
    };
    let mut serialized = node.serialize_keep_none();
    if let dbt_yaml::Value::Mapping(map, _) = &mut serialized {
        // dbt's serialization spells the type as the node's own variant name;
        // macros compare it with the lower-case words ("model", "seed", ...).
        let resource_type = node.resource_type().as_str_name().to_ascii_lowercase();
        let resource_type = resource_type
            .strip_prefix("node_type_")
            .unwrap_or(&resource_type)
            .to_string();
        map.insert("resource_type".into(), dbt_yaml::Value::string(resource_type));
    }
    minijinja::Value::from_serialize(serialized)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::types::NodeStatus;

    use super::super::test_support::mk_result;

    #[test]
    fn results_context_renders_via_jinja() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = vec![
            mk_result("model.pkg.customers", NodeStatus::Success, None),
            mk_result("test.pkg.unique_customers", NodeStatus::Error, Some(2)),
        ];

        let value = build_results_context(&results, &nodes);

        // Exercise the value through Jinja the way real macros do.
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        // Length check — `results | length` is the canonical "node count" pattern.
        let len = env
            .template_from_str("{{ results | length }}")?
            .render(&ctx, &[])?;
        assert_eq!(len, "2");

        // Field access on the first item.
        let first_status = env
            .template_from_str("{{ results[0].status }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_status, "success");

        let first_unique_id = env
            .template_from_str("{{ results[0].unique_id }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_unique_id, "model.pkg.customers");

        // node.unique_id — matches the bigquery example macro pattern.
        let first_node_id = env
            .template_from_str("{{ results[0].node.unique_id }}")?
            .render(&ctx, &[])?;
        assert_eq!(first_node_id, "model.pkg.customers");

        // failures on the failing test.
        let second_failures = env
            .template_from_str("{{ results[1].failures }}")?
            .render(&ctx, &[])?;
        assert_eq!(second_failures, "2");

        // Iteration with status filtering — the actual log_run_results pattern.
        // An assertion that errors reports dbt's word for it, `fail`.
        let summary = env
            .template_from_str(
                r"{% set ns = namespace(pass=0, error=0) %}\
{% for r in results %}\
{% if r.status == 'success' %}{% set ns.pass = ns.pass + 1 %}{% endif %}\
{% if r.status == 'fail' %}{% set ns.error = ns.error + 1 %}{% endif %}\
{% endfor %}\
pass={{ ns.pass }} error={{ ns.error }}",
            )?
            .render(&ctx, &[])?;
        assert!(summary.contains("pass=1"), "summary should report 1 pass: {summary}");
        assert!(summary.contains("error=1"), "summary should report 1 error: {summary}");

        Ok(())
    }

    #[test]
    fn empty_results_renders_as_empty_list() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let value = build_results_context(&[], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let len = env
            .template_from_str("{{ results | length }}")?
            .render(&ctx, &[])?;
        assert_eq!(len, "0");

        // `results is defined` should be true even for empty list.
        let defined = env
            .template_from_str("{% if results is defined %}yes{% else %}no{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(defined, "yes");

        Ok(())
    }

    #[test]
    fn results_with_no_message_renders_none() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.pkg.foo", NodeStatus::Skipped, None);
        r.message = None;
        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        // None message renders as 'none' in minijinja's lenient mode.
        let result = env
            .template_from_str("{% if results[0].message %}has-msg{% else %}no-msg{% endif %}")?
            .render(&ctx, &[])?;
        assert_eq!(result, "no-msg");
        Ok(())
    }

    #[test]
    fn node_lookup_enriches_with_name_and_resource_type() -> anyhow::Result<()> {
        // When the node IS in the resolver, build_results_context fills in
        // name + package_name + resource_type (lower-cased, stripped of the
        // proto prefix).
        use std::sync::Arc;

        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        let common = CommonAttributes {
            unique_id: "model.shop.orders".to_string(),
            name: "orders".to_string(),
            package_name: "shop".to_string(),
            ..CommonAttributes::default()
        };
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: common,
                ..DbtModel::default()
            }),
        );

        let result = mk_result("model.shop.orders", NodeStatus::Success, None);
        let value = build_results_context(&[result], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let name = env
            .template_from_str("{{ results[0].node.name }}")?
            .render(&ctx, &[])?;
        assert_eq!(name, "orders");

        let pkg = env
            .template_from_str("{{ results[0].node.package_name }}")?
            .render(&ctx, &[])?;
        assert_eq!(pkg, "shop");

        let rt = env
            .template_from_str("{{ results[0].node.resource_type }}")?
            .render(&ctx, &[])?;
        assert_eq!(rt, "model");

        Ok(())
    }

    fn render(template: &str, results: minijinja::Value) -> Result<String, minijinja::Error> {
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), results);
        env.template_from_str(template)?.render(&ctx, &[])
    }

    /// The words are `run_results.json`'s, not this worker's own.
    #[test]
    fn statuses_use_dbt_words() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = [
            mk_result("model.p.ok", NodeStatus::Success, None),
            mk_result("model.p.bad", NodeStatus::Error, None),
            mk_result("model.p.later", NodeStatus::Skipped, None),
            mk_result("test.p.holds", NodeStatus::Success, None),
            mk_result("test.p.breaks", NodeStatus::Error, Some(3)),
            mk_result("test.p.noisy", NodeStatus::Warn, Some(1)),
        ];

        let rendered = render(
            "{% for r in results %}{{ r.status }},{% endfor %}",
            build_results_context(&results, &nodes),
        )?;

        assert_eq!(rendered, "success,error,skipped,pass,fail,warn,");
        Ok(())
    }

    #[test]
    fn a_freshness_result_reports_its_grade() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut stale = mk_result("source.p.raw.orders", NodeStatus::Error, None);
        stale.freshness = Some(crate::types::FreshnessOutcome {
            max_loaded_at: "2026-06-12T10:00:00+00:00".into(),
            snapshotted_at: "2026-06-12T11:00:00+00:00".into(),
            max_loaded_at_time_ago_in_s: 3600.0,
            status: crate::types::FreshnessGrade::Warn,
            resource_type: "source".into(),
            criteria: dbt_schemas::schemas::common::FreshnessDefinition::default(),
        });

        let rendered = render("{{ results[0].status }}", build_results_context(&[stale], &nodes))?;

        assert_eq!(rendered, "warn");
        Ok(())
    }

    #[test]
    fn the_node_is_the_resolved_node_with_its_config() -> anyhow::Result<()> {
        use dbt_schemas::schemas::nodes::{CommonAttributes, DbtModel};

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes.models.insert(
            "model.shop.orders".to_string(),
            Arc::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: "model.shop.orders".to_string(),
                    name: "orders".to_string(),
                    ..CommonAttributes::default()
                },
                ..DbtModel::default()
            }),
        );

        let rendered = render(
            "{{ results[0].node.name }}|{{ results[0].node.resource_type }}|\
             {{ results[0].node.config is defined }}",
            build_results_context(
                &[mk_result("model.shop.orders", NodeStatus::Success, None)],
                &nodes,
            ),
        )?;

        assert_eq!(rendered, "orders|model|True");
        Ok(())
    }

    #[test]
    fn timing_instants_support_strftime() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.p.m", NodeStatus::Success, None);
        r.timing = vec![crate::types::TimingEntry {
            name: "execute".to_string(),
            started_at: "2026-03-04T05:06:07Z".to_string(),
            completed_at: "not a timestamp".to_string(),
        }];

        let rendered = render(
            "{% set t = results[0].timing[0] %}{{ t.name }}|\
             {{ t.started_at.strftime('%Y-%m-%d %H:%M') }}|{{ t.completed_at is none }}",
            build_results_context(&[r], &nodes),
        )?;

        assert_eq!(rendered, "execute|2026-03-04 05:06|True");
        Ok(())
    }

    #[test]
    fn adapter_response_carried_through_as_jinja_map() -> anyhow::Result<()> {
        use std::collections::BTreeMap;

        let nodes = dbt_schemas::schemas::Nodes::default();
        let mut r = mk_result("model.pkg.x", NodeStatus::Success, None);
        let mut resp = BTreeMap::new();
        resp.insert("rows_affected".to_string(), serde_json::json!(42));
        resp.insert("message".to_string(), serde_json::json!("CREATE TABLE"));
        r.adapter_response = resp;

        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);

        let rendered = env
            .template_from_str(
                "{{ results[0].adapter_response.rows_affected }}|\
                 {{ results[0].adapter_response.message }}",
            )?
            .render(&ctx, &[])?;
        assert_eq!(rendered, "42|CREATE TABLE");
        Ok(())
    }

    #[test]
    fn results_with_failures_renders_count() -> anyhow::Result<()> {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let r = mk_result("test.pkg.t", NodeStatus::Error, Some(7));
        let value = build_results_context(&[r], &nodes);

        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("results".to_string(), value);
        let s = env
            .template_from_str("{{ results[0].failures }}")?
            .render(&ctx, &[])?;
        assert_eq!(s, "7");
        Ok(())
    }
}
