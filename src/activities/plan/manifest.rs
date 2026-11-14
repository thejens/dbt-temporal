//! The run manifest, serialized once per worker and project.
//!
//! A manifest is large (every node, macro and doc of the project) and costs
//! far more to build and serialize than to upload, yet its nodes are a pure
//! function of the resolved project, which cannot change while the worker
//! lives. Only the `metadata` entry varies from run to run, so the body is
//! cached and each run splices a freshly serialized `metadata` in front of it.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dbt_schemas::schemas::manifest::{DbtManifest, ManifestMetadata};
use tokio::sync::OnceCell;

/// A project's manifest with the per-run `metadata` entry factored out.
#[derive(Debug)]
pub struct ManifestTemplate {
    /// Metadata as the project resolves it; the per-run fields are overwritten
    /// by [`Self::render`].
    metadata: ManifestMetadata,
    /// The serialized manifest after its `metadata` entry: starts at the comma
    /// that follows it and runs through the closing brace.
    body: String,
}

impl ManifestTemplate {
    fn from_manifest(mut manifest: DbtManifest) -> Result<Self, anyhow::Error> {
        let metadata = std::mem::take(&mut manifest.metadata);
        // `metadata` is the manifest's first entry, so with it emptied the
        // document is `{"metadata":<empty>` followed by the body. Anchoring on
        // the serialized empty value, rather than on a hard-coded position,
        // makes a change in the manifest's field order an error instead of a
        // silently corrupt document.
        let prefix = format!("{{\"metadata\":{}", serde_json::to_string(&manifest.metadata)?);
        let document = serde_json::to_string(&manifest)?;
        let body = document
            .strip_prefix(&prefix)
            .filter(|rest| rest.starts_with(','))
            .ok_or_else(|| {
                anyhow::anyhow!("manifest does not serialize with `metadata` as its first entry")
            })?
            .to_owned();
        Ok(Self { metadata, body })
    }

    /// The manifest document for one run.
    ///
    /// The run supplies everything that differs between runs: when the
    /// document was generated, which invocation it belongs to, and the
    /// `DBT_ENV_CUSTOM_ENV_*` variables visible to the process at that moment.
    pub fn render(
        &self,
        invocation_id: &str,
        generated_at: DateTime<Utc>,
        env: BTreeMap<String, String>,
    ) -> Result<String, anyhow::Error> {
        const OPENING: &str = "{\"metadata\":";

        let mut metadata = self.metadata.clone();
        metadata.__base__.generated_at = generated_at;
        metadata.__base__.invocation_id = Some(invocation_id.to_string());
        metadata.__base__.env = env;
        let metadata = serde_json::to_string(&metadata)?;

        let mut document = String::with_capacity(OPENING.len() + metadata.len() + self.body.len());
        document.push_str(OPENING);
        document.push_str(&metadata);
        document.push_str(&self.body);
        Ok(document)
    }
}

/// Holds a project's [`ManifestTemplate`], built by the first run that needs it.
#[derive(Debug, Default)]
pub struct ManifestCache {
    template: OnceCell<ManifestTemplate>,
}

impl ManifestCache {
    /// The cached template, building it with `build` if no run has yet.
    ///
    /// Concurrent first calls wait for a single build rather than each doing
    /// the work, and a failed build is not cached, so the next run retries.
    pub async fn get_or_build(
        &self,
        build: impl FnOnce() -> DbtManifest,
    ) -> Result<&ManifestTemplate, anyhow::Error> {
        self.template
            .get_or_try_init(|| async { ManifestTemplate::from_manifest(build()) })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A manifest with enough populated entries that a splice error in any
    /// position would show.
    fn sample_manifest() -> DbtManifest {
        let mut manifest = DbtManifest::default();
        manifest.metadata.project_name = "shop".to_string();
        manifest.metadata.adapter_type = "duckdb".to_string();
        manifest.metadata.__base__.dbt_version = "2.0.11".to_string();
        manifest.child_map.insert(
            "model.shop.a".to_string(),
            vec!["model.shop.b".to_string(), "test.shop.t".to_string()],
        );
        manifest
            .parent_map
            .insert("model.shop.b".to_string(), vec!["model.shop.a".to_string()]);
        manifest
            .group_map
            .insert("finance".to_string(), vec!["model.shop.a".to_string()]);
        manifest
            .disabled
            .insert("model.shop.off".to_string(), Vec::new());
        manifest
    }

    /// What the document would be if the whole manifest were serialized for
    /// this run, with nothing cached.
    fn direct(
        invocation_id: &str,
        generated_at: DateTime<Utc>,
        env: &BTreeMap<String, String>,
    ) -> Result<String, anyhow::Error> {
        let mut manifest = sample_manifest();
        manifest.metadata.__base__.generated_at = generated_at;
        manifest.metadata.__base__.invocation_id = Some(invocation_id.to_string());
        manifest.metadata.__base__.env = env.clone();
        Ok(serde_json::to_string(&manifest)?)
    }

    fn instant(secs: i64) -> Result<DateTime<Utc>, anyhow::Error> {
        DateTime::from_timestamp(secs, 0).ok_or_else(|| anyhow::anyhow!("{secs} is out of range"))
    }

    #[test]
    fn rendered_document_is_byte_identical_to_a_direct_serialization_for_each_run()
    -> Result<(), anyhow::Error> {
        let template = ManifestTemplate::from_manifest(sample_manifest())?;
        let env_one = BTreeMap::new();
        let env_two = BTreeMap::from([("TEAM".to_string(), "growth".to_string())]);

        let first = template.render("inv-1", instant(1_700_000_000)?, env_one.clone())?;
        let second = template.render("inv-2", instant(1_800_000_000)?, env_two.clone())?;

        assert_eq!(first, direct("inv-1", instant(1_700_000_000)?, &env_one)?);
        assert_eq!(second, direct("inv-2", instant(1_800_000_000)?, &env_two)?);
        // The shared body must not leak one run's metadata into another's.
        assert!(!second.contains("inv-1"));
        assert!(second.contains("growth"));
        Ok(())
    }

    #[test]
    fn rendered_document_is_valid_json_with_the_run_metadata() -> Result<(), anyhow::Error> {
        let template = ManifestTemplate::from_manifest(sample_manifest())?;
        let document = template.render("inv-9", instant(1_700_000_000)?, BTreeMap::new())?;
        let parsed: serde_json::Value = serde_json::from_str(&document)?;
        assert_eq!(parsed["metadata"]["invocation_id"], "inv-9");
        assert_eq!(parsed["metadata"]["project_name"], "shop");
        assert_eq!(parsed["child_map"]["model.shop.a"][0], "model.shop.b");
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_first_runs_build_the_template_once() -> Result<(), anyhow::Error> {
        let cache = ManifestCache::default();
        let builds = AtomicUsize::new(0);
        let build = || {
            builds.fetch_add(1, Ordering::SeqCst);
            sample_manifest()
        };

        let (a, b, c) = tokio::join!(
            cache.get_or_build(build),
            cache.get_or_build(build),
            cache.get_or_build(build),
        );
        a?;
        b?;
        c?;
        cache.get_or_build(build).await?;

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
