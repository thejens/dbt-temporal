//! The `schemas` and `database_schemas` an `on-run-end` hook sees.

use std::collections::BTreeSet;

use crate::types::{NodeExecutionResult, NodeStatus};

/// The `(database, schema)` pairs of the relations the run built, which is what
/// dbt hands `on-run-end` as `database_schemas` (and, without the database, as
/// `schemas`).
///
/// Models, seeds and snapshots that succeeded, as in dbt. The schema is the
/// one the node wrote to when it recorded it, because a run that overrides the
/// target or the profile's env builds elsewhere than the worker's startup
/// metadata says.
pub(super) fn built_schemas(
    results: &[NodeExecutionResult],
    nodes: &dbt_schemas::schemas::Nodes,
) -> BTreeSet<(String, String)> {
    results
        .iter()
        .filter(|r| r.status == NodeStatus::Success)
        .filter(|r| {
            ["model.", "seed.", "snapshot."]
                .iter()
                .any(|p| r.unique_id.starts_with(p))
        })
        .filter_map(|r| {
            if let Some((database, schema, _)) = crate::activities::catalog::executed_relation(r) {
                return Some((database, schema));
            }
            let base = nodes.get_node(&r.unique_id)?.base();
            Some((base.database.clone(), base.schema.clone()))
        })
        .collect()
}

/// `schemas`: the distinct schema names, in order.
pub(super) fn schemas_context(built: &BTreeSet<(String, String)>) -> minijinja::Value {
    let schemas: BTreeSet<&str> = built.iter().map(|(_, schema)| schema.as_str()).collect();
    minijinja::Value::from_iter(schemas)
}

/// `database_schemas`: `[database, schema]` pairs.
pub(super) fn database_schemas_context(built: &BTreeSet<(String, String)>) -> minijinja::Value {
    minijinja::Value::from_serialize(built)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    use super::super::test_support::mk_result;

    fn built(unique_id: &str, status: NodeStatus, relation: Option<&str>) -> NodeExecutionResult {
        let mut r = mk_result(unique_id, status, None);
        r.relation_name = relation.map(ToString::to_string);
        r
    }

    #[test]
    fn schemas_are_those_of_the_relations_the_run_built() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let results = [
            built("model.p.a", NodeStatus::Success, Some("db.analytics.a")),
            built("seed.p.b", NodeStatus::Success, Some("db.analytics.b")),
            built("snapshot.p.c", NodeStatus::Success, Some("db.history.c")),
            // Not built: failed, an assertion, or a node with nowhere recorded.
            built("model.p.failed", NodeStatus::Error, Some("db.broken.failed")),
            built("test.p.t", NodeStatus::Success, Some("db.tests.t")),
            built("model.p.unrecorded", NodeStatus::Success, None),
        ];

        let set = built_schemas(&results, &nodes);

        assert_eq!(
            set.iter().cloned().collect::<Vec<_>>(),
            [
                ("db".to_string(), "analytics".to_string()),
                ("db".to_string(), "history".to_string()),
            ]
        );
    }

    #[test]
    fn schemas_and_database_schemas_render_as_dbt_hands_them() -> anyhow::Result<()> {
        let set: BTreeSet<(String, String)> = [("db", "a"), ("db", "b"), ("other", "a")]
            .into_iter()
            .map(|(d, s)| (d.to_string(), s.to_string()))
            .collect();
        let env = minijinja::Environment::new();
        let mut ctx = BTreeMap::new();
        ctx.insert("schemas".to_string(), schemas_context(&set));
        ctx.insert("database_schemas".to_string(), database_schemas_context(&set));

        let rendered = env
            .template_from_str(
                "{% for s in schemas %}{{ s }},{% endfor %}|\
                 {% for d, s in database_schemas %}{{ d }}.{{ s }},{% endfor %}",
            )?
            .render(&ctx, &[])?;

        assert_eq!(rendered, "a,b,|db.a,db.b,other.a,");
        Ok(())
    }
}
