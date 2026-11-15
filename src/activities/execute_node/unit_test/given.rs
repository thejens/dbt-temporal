//! Resolving the model a unit test's `given` input names.

use dbt_schemas::schemas::Nodes;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum GivenInput {
    This,
    Ref {
        name: String,
        package: Option<String>,
    },
    Source {
        source: String,
        table: String,
    },
}

/// Parse a `given.input` expression: `this`, `ref('name')`,
/// `ref('package', 'name')`, or `source('source', 'table')`. Surrounding
/// Jinja braces are tolerated since fixtures in the wild use both forms.
pub(super) fn parse_given_input(raw: &str) -> Result<GivenInput, anyhow::Error> {
    let mut s = raw.trim();
    if let Some(stripped) = s.strip_prefix("{{").and_then(|r| r.strip_suffix("}}")) {
        s = stripped.trim();
    }
    if s == "this" {
        return Ok(GivenInput::This);
    }
    let (func, args) = if let Some(rest) = s.strip_prefix("ref") {
        ("ref", rest)
    } else if let Some(rest) = s.strip_prefix("source") {
        ("source", rest)
    } else {
        anyhow::bail!("expected this, ref(...), or source(...)");
    };
    let args = args.trim();
    let inner = args
        .strip_prefix('(')
        .and_then(|r| r.strip_suffix(')'))
        .ok_or_else(|| anyhow::anyhow!("expected {func}(...) call syntax"))?;
    let parts: Vec<String> = inner
        .split(',')
        .map(|p| {
            let p = p.trim();
            p.strip_prefix('\'')
                .and_then(|r| r.strip_suffix('\''))
                .or_else(|| p.strip_prefix('"').and_then(|r| r.strip_suffix('"')))
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("argument {p:?} is not a string literal"))
        })
        .collect::<Result<_, _>>()?;
    match (func, parts.as_slice()) {
        ("ref", [name]) => Ok(GivenInput::Ref {
            name: name.clone(),
            package: None,
        }),
        ("ref", [package, name]) => Ok(GivenInput::Ref {
            name: name.clone(),
            package: Some(package.clone()),
        }),
        ("source", [source, table]) => Ok(GivenInput::Source {
            source: source.clone(),
            table: table.clone(),
        }),
        _ => anyhow::bail!("unsupported argument count for {func}()"),
    }
}

/// Resolve a parsed given input to a node unique_id. Refs prefer the unit
/// test's own package, matching dbt's ref resolution order.
pub(super) fn resolve_given_input(
    input: &GivenInput,
    model_id: &str,
    package_name: &str,
    nodes: &Nodes,
) -> Result<String, anyhow::Error> {
    match input {
        GivenInput::This => Ok(model_id.to_string()),
        GivenInput::Ref { name, package } => {
            let matches_pkg = |node_pkg: &str| package.as_deref().is_none_or(|p| p == node_pkg);
            let mut candidates: Vec<&String> = Vec::new();
            for (id, m) in &nodes.models {
                if m.__common_attr__.name == *name && matches_pkg(&m.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            for (id, s) in &nodes.seeds {
                if s.__common_attr__.name == *name && matches_pkg(&s.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            for (id, s) in &nodes.snapshots {
                if s.__common_attr__.name == *name && matches_pkg(&s.__common_attr__.package_name) {
                    candidates.push(id);
                }
            }
            match candidates.len() {
                0 => anyhow::bail!("no model/seed/snapshot named {name:?}"),
                1 => Ok(candidates[0].clone()),
                _ => {
                    // Ambiguous across packages — prefer the unit test's own.
                    candidates
                        .iter()
                        .find(|id| {
                            nodes
                                .get_node(id)
                                .is_some_and(|n| n.common().package_name == package_name)
                        })
                        .map(|id| (*id).clone())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "ref {name:?} is ambiguous across packages — qualify it as \
                                 ref('<package>', '{name}')"
                            )
                        })
                }
            }
        }
        GivenInput::Source { source, table } => nodes
            .sources
            .iter()
            .find(|(_, s)| {
                s.__source_attr__.source_name == *source && s.__common_attr__.name == *table
            })
            .map(|(id, _)| id.clone())
            .ok_or_else(|| anyhow::anyhow!("no source {source:?}.{table:?}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn parse_given_input_handles_this_and_braces() {
        assert_eq!(parse_given_input("this").unwrap(), GivenInput::This);
        assert_eq!(parse_given_input("  {{ this }} ").unwrap(), GivenInput::This);
    }

    #[test]
    fn parse_given_input_handles_ref_forms() {
        assert_eq!(
            parse_given_input("ref('stg_orders')").unwrap(),
            GivenInput::Ref {
                name: "stg_orders".to_string(),
                package: None
            }
        );
        assert_eq!(
            parse_given_input("{{ ref(\"pkg\", \"stg_orders\") }}").unwrap(),
            GivenInput::Ref {
                name: "stg_orders".to_string(),
                package: Some("pkg".to_string())
            }
        );
    }

    #[test]
    fn parse_given_input_handles_source() {
        assert_eq!(
            parse_given_input("source('waffle_hut', 'payments')").unwrap(),
            GivenInput::Source {
                source: "waffle_hut".to_string(),
                table: "payments".to_string()
            }
        );
    }

    #[test]
    fn parse_given_input_rejects_garbage() {
        assert!(parse_given_input("var('x')").is_err());
        assert!(parse_given_input("ref(stg_orders)").is_err());
        assert!(parse_given_input("ref('a', 'b', 'c')").is_err());
        assert!(parse_given_input("source('only_one')").is_err());
    }

    fn nodes_with(
        models: &[(&str, &str, &str)],
        seeds: &[(&str, &str, &str)],
        sources: &[(&str, &str, &str)],
    ) -> Nodes {
        use dbt_schemas::schemas::nodes::{DbtModel, DbtSeed, DbtSource};

        let mut nodes = Nodes::default();
        for (uid, name, package) in models {
            let mut m = DbtModel::default();
            (*uid).clone_into(&mut m.__common_attr__.unique_id);
            (*name).clone_into(&mut m.__common_attr__.name);
            (*package).clone_into(&mut m.__common_attr__.package_name);
            nodes.models.insert((*uid).to_string(), Arc::new(m));
        }
        for (uid, name, package) in seeds {
            let mut s = DbtSeed::default();
            (*uid).clone_into(&mut s.__common_attr__.unique_id);
            (*name).clone_into(&mut s.__common_attr__.name);
            (*package).clone_into(&mut s.__common_attr__.package_name);
            nodes.seeds.insert((*uid).to_string(), Arc::new(s));
        }
        for (uid, source_name, table) in sources {
            let mut s = DbtSource::default();
            (*uid).clone_into(&mut s.__common_attr__.unique_id);
            (*table).clone_into(&mut s.__common_attr__.name);
            (*source_name).clone_into(&mut s.__source_attr__.source_name);
            nodes.sources.insert((*uid).to_string(), Arc::new(s));
        }
        nodes
    }

    #[test]
    fn resolve_given_input_this_returns_tested_model() {
        let nodes = Nodes::default();
        let id = resolve_given_input(&GivenInput::This, "model.p.m", "p", &nodes).unwrap();
        assert_eq!(id, "model.p.m");
    }

    #[test]
    fn resolve_given_input_finds_models_and_seeds_by_name() {
        let nodes = nodes_with(
            &[("model.p.stg_orders", "stg_orders", "p")],
            &[("seed.p.countries", "countries", "p")],
            &[],
        );
        let by_ref = |name: &str| GivenInput::Ref {
            name: name.to_string(),
            package: None,
        };
        assert_eq!(
            resolve_given_input(&by_ref("stg_orders"), "model.p.m", "p", &nodes).unwrap(),
            "model.p.stg_orders"
        );
        assert_eq!(
            resolve_given_input(&by_ref("countries"), "model.p.m", "p", &nodes).unwrap(),
            "seed.p.countries"
        );
        assert!(resolve_given_input(&by_ref("nope"), "model.p.m", "p", &nodes).is_err());
    }

    #[test]
    fn resolve_given_input_respects_package_qualifier() {
        let nodes = nodes_with(
            &[
                ("model.a.shared", "shared", "a"),
                ("model.b.shared", "shared", "b"),
            ],
            &[],
            &[],
        );
        let qualified = GivenInput::Ref {
            name: "shared".to_string(),
            package: Some("b".to_string()),
        };
        assert_eq!(
            resolve_given_input(&qualified, "model.a.m", "a", &nodes).unwrap(),
            "model.b.shared"
        );
        // Unqualified + ambiguous: the unit test's own package wins.
        let bare = GivenInput::Ref {
            name: "shared".to_string(),
            package: None,
        };
        assert_eq!(resolve_given_input(&bare, "model.a.m", "a", &nodes).unwrap(), "model.a.shared");
    }

    #[test]
    fn resolve_given_input_finds_sources() {
        let nodes = nodes_with(&[], &[], &[("source.p.raw.orders", "raw", "orders")]);
        let input = GivenInput::Source {
            source: "raw".to_string(),
            table: "orders".to_string(),
        };
        assert_eq!(
            resolve_given_input(&input, "model.p.m", "p", &nodes).unwrap(),
            "source.p.raw.orders"
        );
        let missing = GivenInput::Source {
            source: "raw".to_string(),
            table: "nope".to_string(),
        };
        assert!(resolve_given_input(&missing, "model.p.m", "p", &nodes).is_err());
    }

    #[test]
    fn resolve_given_input_ambiguous_foreign_packages_errors() {
        // Two foreign candidates, none in the unit test's package: refuse to
        // guess and demand qualification.
        let nodes = nodes_with(
            &[
                ("model.a.shared", "shared", "a"),
                ("model.b.shared", "shared", "b"),
            ],
            &[],
            &[],
        );
        let bare = GivenInput::Ref {
            name: "shared".to_string(),
            package: None,
        };
        let err = resolve_given_input(&bare, "model.c.m", "c", &nodes).unwrap_err();
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn resolve_given_input_finds_snapshots() {
        use dbt_schemas::schemas::nodes::DbtSnapshot;

        let mut nodes = Nodes::default();
        let mut snap = DbtSnapshot::default();
        snap.__common_attr__.unique_id = "snapshot.p.history".to_string();
        snap.__common_attr__.name = "history".to_string();
        snap.__common_attr__.package_name = "p".to_string();
        nodes
            .snapshots
            .insert("snapshot.p.history".to_string(), Arc::new(snap));
        let input = GivenInput::Ref {
            name: "history".to_string(),
            package: None,
        };
        assert_eq!(
            resolve_given_input(&input, "model.p.m", "p", &nodes).unwrap(),
            "snapshot.p.history"
        );
    }
}
