//! The adapter engines a run can execute on: one per adapter type the active
//! target declares.
//!
//! A dbt profile target declares one or more adapters. Every node carries the
//! adapter it runs on in `NodeBaseAttributes::adapter` — its `+adapter`
//! selection when it made one, and the target's default otherwise — so routing
//! a node is a lookup by that type, never a decision this crate makes.

use std::sync::Arc;

use dbt_adapter::{AdapterEngine, AdapterType};

use crate::error::DbtTemporalError;

/// One engine per adapter type the active target declares.
///
/// Engines are built eagerly: an `AdbcEngine` opens no connection until its
/// first query, so constructing one the run never selects costs nothing but a
/// struct. They are held in declaration order, which is what diagnostics list.
/// The common single-adapter target holds exactly one entry.
pub struct AdapterEngines {
    /// Declaration order, as the profile wrote it. Never empty, no duplicate
    /// types. A `Vec` rather than a map because a target declares a handful of
    /// adapters at most — a linear scan beats hashing, and `AdapterType` is not
    /// `Ord`, so the repo's `BTreeMap` convention has nothing to offer here.
    engines: Vec<(AdapterType, Arc<dyn AdapterEngine>)>,
    /// The type unannotated work runs on.
    default_type: AdapterType,
    /// The same `Arc` as the `default_type` entry of [`Self::engines`], held
    /// directly so [`Self::default_engine`] is infallible by construction
    /// rather than by convention.
    default_engine: Arc<dyn AdapterEngine>,
}

/// Projects only what a reader can act on: which adapters exist and which is the
/// default. The engines themselves have no useful `Debug`.
impl std::fmt::Debug for AdapterEngines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterEngines")
            .field("declared", &self.declared_names())
            .field("default", &self.default_type.as_ref())
            .finish_non_exhaustive()
    }
}

impl AdapterEngines {
    /// Build from `engines` in declaration order, with `default_type` naming the
    /// one unannotated work runs on.
    ///
    /// Rejects an empty set, a duplicate type, or a default that is not among
    /// the declared ones. All three mean profile resolution produced something
    /// incoherent, and saying so at construction beats failing later on a lookup
    /// that would read like a user error.
    pub fn new(
        engines: Vec<(AdapterType, Arc<dyn AdapterEngine>)>,
        default_type: AdapterType,
    ) -> anyhow::Result<Self> {
        if let Some(duplicate) = engines
            .iter()
            .enumerate()
            .find(|(index, (ty, _))| engines[..*index].iter().any(|(seen, _)| seen == ty))
            .map(|(_, (ty, _))| *ty)
        {
            anyhow::bail!("the active target declares adapter '{duplicate}' more than once");
        }
        let declared: Vec<&str> = engines.iter().map(|(ty, _)| ty.as_ref()).collect();
        let default_engine = engines
            .iter()
            .find(|(ty, _)| *ty == default_type)
            .map(|(_, engine)| Arc::clone(engine))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the target's default adapter '{default_type}' is not among the adapters it \
                     declares ({})",
                    declared.join(", ")
                )
            })?;
        Ok(Self {
            engines,
            default_type,
            default_engine,
        })
    }

    /// The engine `adapter_type` executes on.
    ///
    /// `context` names whoever is asking — a node's unique id, a hook phase.
    /// It is required rather than optional because an undeclared adapter is only
    /// actionable when the message says who asked for it.
    ///
    /// There is deliberately no fallback. A node naming an adapter the target
    /// does not declare has no credentials to run against, and quietly using the
    /// default would write it to the wrong warehouse. The error is
    /// `Configuration`, hence non-retryable: no number of attempts makes an
    /// undeclared adapter appear.
    pub fn get(
        &self,
        adapter_type: AdapterType,
        context: &str,
    ) -> Result<Arc<dyn AdapterEngine>, DbtTemporalError> {
        self.engines
            .iter()
            .find(|(ty, _)| *ty == adapter_type)
            .map(|(_, engine)| Arc::clone(engine))
            .ok_or_else(|| {
                DbtTemporalError::Configuration(format!(
                    "{context} selects adapter '{adapter_type}', which the active target does not \
                     declare (it declares {}); add it to the target in profiles.yml or drop the \
                     `+adapter` selection",
                    self.declared_names().join(", ")
                ))
            })
    }

    /// The engine unannotated work runs on.
    pub fn default_engine(&self) -> Arc<dyn AdapterEngine> {
        Arc::clone(&self.default_engine)
    }

    /// The type unannotated work runs on.
    pub const fn default_type(&self) -> AdapterType {
        self.default_type
    }

    /// Every type the target declares, in declaration order.
    pub fn declared(&self) -> impl Iterator<Item = AdapterType> + '_ {
        self.engines.iter().map(|(ty, _)| *ty)
    }

    /// Decorate every engine, preserving declaration order and the default.
    ///
    /// The seam a test harness wraps the real engines through (fault injection,
    /// tracing). Wrapping has to reach all of them, or a node routed to a
    /// non-default adapter would slip past the decorator.
    #[must_use]
    pub fn map_engines(
        &self,
        wrap: impl Fn(&Arc<dyn AdapterEngine>) -> Arc<dyn AdapterEngine>,
    ) -> Self {
        let engines: Vec<(AdapterType, Arc<dyn AdapterEngine>)> = self
            .engines
            .iter()
            .map(|(ty, engine)| (*ty, wrap(engine)))
            .collect();
        // Re-find rather than reuse the old handle: the default must be the
        // *wrapped* engine, or the decorator would be bypassed on exactly the
        // adapter most work runs on. The fallback is unreachable — the default
        // type is always among the entries — and wraps anyway, so no path here
        // can hand back a bare engine.
        let default_engine = engines
            .iter()
            .find(|(ty, _)| *ty == self.default_type)
            .map_or_else(|| wrap(&self.default_engine), |(_, engine)| Arc::clone(engine));
        Self {
            engines,
            default_type: self.default_type,
            default_engine,
        }
    }

    fn declared_names(&self) -> Vec<&str> {
        self.engines.iter().map(|(ty, _)| ty.as_ref()).collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::worker::adapter::{AdapterSettings, build_adapter_engine};
    use dbt_schemas::schemas::common::ResolvedQuoting;
    use dbt_schemas::schemas::profiles::{DbConfig, DuckDbConfig, PostgresDbConfig};

    /// Real engines rather than mocks: `AdbcEngine::new` opens no connection, so
    /// building one is pure construction, and the real type keeps the
    /// `adapter_type()` the routing assertions read honest.
    fn duckdb_engine() -> Arc<dyn AdapterEngine> {
        let config = DbConfig::DuckDB(Box::new(DuckDbConfig {
            path: Some(":memory:".to_string()),
            schema: Some("main".to_string()),
            ..Default::default()
        }));
        build_adapter_engine(&config, ResolvedQuoting::default(), &AdapterSettings::default(), None)
            .unwrap()
    }

    fn postgres_engine() -> Arc<dyn AdapterEngine> {
        let config = DbConfig::Postgres(Box::new(PostgresDbConfig {
            host: Some("localhost".to_string()),
            database: Some("postgres".to_string()),
            schema: Some("public".to_string()),
            ..Default::default()
        }));
        build_adapter_engine(&config, ResolvedQuoting::default(), &AdapterSettings::default(), None)
            .unwrap()
    }

    fn pair() -> AdapterEngines {
        AdapterEngines::new(
            vec![
                (AdapterType::Postgres, postgres_engine()),
                (AdapterType::DuckDB, duckdb_engine()),
            ],
            AdapterType::Postgres,
        )
        .unwrap()
    }

    /// The single-adapter target every existing project has.
    fn lone_duckdb() -> AdapterEngines {
        AdapterEngines::new(vec![(AdapterType::DuckDB, duckdb_engine())], AdapterType::DuckDB)
            .unwrap()
    }

    #[test]
    fn a_lone_adapter_is_also_the_default() {
        let engines = lone_duckdb();
        assert_eq!(engines.default_type(), AdapterType::DuckDB);
        assert_eq!(engines.declared().collect::<Vec<_>>(), vec![AdapterType::DuckDB]);
        assert_eq!(engines.default_engine().adapter_type(), AdapterType::DuckDB);
    }

    #[test]
    fn declaration_order_is_preserved() {
        assert_eq!(
            pair().declared().collect::<Vec<_>>(),
            vec![AdapterType::Postgres, AdapterType::DuckDB]
        );
    }

    #[test]
    fn a_declared_adapter_routes_to_its_own_engine() {
        let engines = pair();
        assert_eq!(
            engines
                .get(AdapterType::DuckDB, "model.p.m")
                .unwrap()
                .adapter_type(),
            AdapterType::DuckDB
        );
        assert_eq!(
            engines
                .get(AdapterType::Postgres, "model.p.m")
                .unwrap()
                .adapter_type(),
            AdapterType::Postgres
        );
    }

    /// The default is the target's marked one, not whichever was declared first.
    #[test]
    fn the_default_is_the_marked_type_not_the_first_declared() {
        let engines = AdapterEngines::new(
            vec![
                (AdapterType::Postgres, postgres_engine()),
                (AdapterType::DuckDB, duckdb_engine()),
            ],
            AdapterType::DuckDB,
        )
        .unwrap();
        assert_eq!(engines.default_type(), AdapterType::DuckDB);
        assert_eq!(engines.default_engine().adapter_type(), AdapterType::DuckDB);
    }

    /// The whole point of the strict lookup: no silent fallback to the default,
    /// and a message naming both the asker and the adapter it asked for.
    #[test]
    fn an_undeclared_adapter_is_a_non_retryable_error_naming_the_node() {
        let engines = lone_duckdb();
        // `let else` rather than `expect_err`: the Ok side is an engine handle,
        // which has no `Debug` to unwrap through.
        let Err(err) = engines.get(AdapterType::Snowflake, "model.spike.on_snowflake") else {
            panic!("snowflake is not declared");
        };
        assert!(!err.is_retryable(), "{err}");
        let msg = err.to_string();
        assert!(msg.contains("model.spike.on_snowflake"), "{msg}");
        assert!(msg.contains("snowflake"), "{msg}");
        assert!(msg.contains("duckdb"), "{msg}");
    }

    #[test]
    fn an_empty_set_is_rejected() {
        let err = AdapterEngines::new(vec![], AdapterType::DuckDB).expect_err("empty");
        assert!(
            err.to_string()
                .contains("not among the adapters it declares"),
            "{err}"
        );
    }

    #[test]
    fn a_duplicate_adapter_type_is_rejected() {
        let err = AdapterEngines::new(
            vec![
                (AdapterType::DuckDB, duckdb_engine()),
                (AdapterType::DuckDB, duckdb_engine()),
            ],
            AdapterType::DuckDB,
        )
        .expect_err("duplicate duckdb");
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    #[test]
    fn a_default_outside_the_declared_set_is_rejected() {
        let err = AdapterEngines::new(
            vec![(AdapterType::DuckDB, duckdb_engine())],
            AdapterType::Postgres,
        )
        .expect_err("postgres is not declared");
        assert!(
            err.to_string()
                .contains("not among the adapters it declares"),
            "{err}"
        );
    }

    #[test]
    fn map_engines_wraps_every_adapter_and_keeps_the_default() {
        // Identity wrap: what matters is that the shape survives, since a
        // decorator that dropped an entry would silently unroute a node.
        let wrapped = pair().map_engines(Arc::clone);
        assert_eq!(
            wrapped.declared().collect::<Vec<_>>(),
            vec![AdapterType::Postgres, AdapterType::DuckDB]
        );
        assert_eq!(wrapped.default_type(), AdapterType::Postgres);
        assert_eq!(wrapped.default_engine().adapter_type(), AdapterType::Postgres);
    }

    #[test]
    fn debug_names_the_declared_adapters_and_the_default() {
        let rendered = format!("{:?}", pair());
        assert!(rendered.contains("postgres"), "{rendered}");
        assert!(rendered.contains("duckdb"), "{rendered}");
        assert!(rendered.contains("default"), "{rendered}");
    }
}
