//! Handing the run's relations to the catalog builder and storing the result.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;

use crate::artifact_store::ArtifactStore;
use crate::types::StoreArtifactsInput;

use super::super::DbtActivities;

/// Build catalog.json for the run's project and store it.
pub(super) async fn generate_and_store_catalog(
    activities: &DbtActivities,
    input: &StoreArtifactsInput,
    store: &dyn ArtifactStore,
    cancellation: &dbt_common::cancellation::CancellationToken,
) -> Result<String, anyhow::Error> {
    let state = Arc::clone(
        activities
            .registry
            .get(input.project.as_deref())
            .context("resolving project for catalog generation")?,
    );
    // The catalog queries the warehouse, so it is built on the dbt pool, which
    // needs owned inputs. Copying the results costs one allocation per run.
    let node_results = input.node_results.clone();
    let invocation_id = input.invocation_id.clone();
    let env = input.env.clone();
    let target = input.target.clone();
    let token = cancellation.clone();
    let catalog_json = crate::dbt_pool::run(move || {
        // Catalog generation queries the warehouse through the adapter, which
        // emits dbt telemetry spans; those need an `Invocation` root above them.
        crate::activities::node_telemetry::invocation_span(&invocation_id, "dbt docs generate")
            .in_scope(|| {
                crate::activities::catalog::build_catalog_json(
                    &state,
                    &node_results,
                    &invocation_id,
                    &crate::activities::render_env::RenderOverrides {
                        cancellation: &token,
                        env: &env,
                        target: target.as_deref(),
                        // Neither reaches a `get_columns_in_relation` call.
                        vars: &BTreeMap::new(),
                        full_refresh: false,
                        invocation_id: &invocation_id,
                        run_started_at: None,
                    },
                )
            })
    })
    .await??;
    store
        .store(&input.invocation_id, "catalog.json", catalog_json.into_bytes().into())
        .await
        .context("storing catalog.json")
}
