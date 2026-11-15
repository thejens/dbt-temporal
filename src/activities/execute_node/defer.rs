//! The deferred node set (`--defer`), loaded from a previous run's manifest.

use std::sync::Arc;

use anyhow::Context;

use crate::error::DbtTemporalError;

use super::super::DbtActivities;

/// Load the deferred node set from a previous run's manifest.
///
/// `--defer`'s purpose: unbuilt upstream `ref()`s resolve against the relations
/// that manifest describes instead of failing.
pub(super) async fn load_defer_nodes(
    activities: &DbtActivities,
    state: &crate::worker_state::WorkerState,
    manifest_ref: Option<&str>,
) -> Result<Option<Arc<dbt_schemas::schemas::Nodes>>, anyhow::Error> {
    let Some(manifest_ref) = manifest_ref else {
        return Ok(None);
    };
    // A manifest at a given reference is the run that produced it — immutable,
    // and the same for every node of this run, so the first node loads it and
    // the rest wait for that load.
    let nodes = state
        .defer_manifests
        .get_or_load(manifest_ref, || fetch_defer_nodes(activities, state, manifest_ref))
        .await?;
    Ok(Some(nodes))
}

/// Download and decode the manifest at `manifest_ref`.
async fn fetch_defer_nodes(
    activities: &DbtActivities,
    state: &crate::worker_state::WorkerState,
    manifest_ref: &str,
) -> Result<Arc<dbt_schemas::schemas::Nodes>, anyhow::Error> {
    let store = activities.artifact_store.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "defer_manifest_ref requires artifact storage to be configured \
             (set ARTIFACT_STORE and WRITE_ARTIFACTS)"
        )
    })?;
    let bytes = store.retrieve(manifest_ref).await.map_err(|e| {
        DbtTemporalError::ArtifactStore(
            e.context(format!("loading defer manifest from {manifest_ref}")),
        )
    })?;
    // Manifests must parse through dbt's own serde path (JSON → YmlValue →
    // typed): node config structs serialize warehouse-specific keys flattened,
    // which the plain serde_json deserializer rejects ("missing field
    // __warehouse_specific_config__").
    let manifest_str = std::str::from_utf8(&bytes).context("defer manifest is not UTF-8")?;
    let manifest: dbt_schemas::schemas::manifest::DbtManifest =
        dbt_schemas::schemas::serde::typed_struct_from_json_str(manifest_str, None)
            .map_err(|e| anyhow::anyhow!("parsing defer manifest JSON: {e}"))?;
    // The project's own quoting, not a fixed guess. These relations are
    // rendered into this run's SQL by this run's adapter, so its rules are what
    // decide whether each part is quoted; fixed values would be wrong for any
    // project whose quoting differs from them.
    let resolved = state.resolver_state.root_project_quoting;
    let quoting = dbt_schemas::schemas::common::DbtQuoting {
        database: Some(resolved.database),
        schema: Some(resolved.schema),
        identifier: Some(resolved.identifier),
        snowflake_ignore_case: None,
    };
    Ok(Arc::new(dbt_schemas::schemas::manifest::nodes_from_dbt_manifest(
        manifest, quoting,
    )))
}
