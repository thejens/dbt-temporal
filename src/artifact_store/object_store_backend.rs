use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};

use super::ArtifactStore;

/// Parse an object store URL (`gs://bucket/prefix`, `s3://bucket/prefix`) and return
/// the backend + prefix. Shared by both artifact store and model store.
pub fn parse_object_store_url(url: &str) -> Result<(Box<dyn ObjectStore>, String)> {
    let parsed = url::Url::parse(url).context("parsing object store URL")?;
    let bucket = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("no bucket in object store URL: {url}"))?;
    let prefix = parsed.path().trim_start_matches('/').to_string();

    let store: Box<dyn ObjectStore> = match parsed.scheme() {
        "gs" | "gcs" => build_gcs_store(bucket)?,
        "s3" => build_s3_store(bucket)?,
        other => anyhow::bail!("unsupported object store URL scheme: {other}"),
    };
    Ok((store, prefix))
}

#[cfg(feature = "gcs")]
fn build_gcs_store(bucket: &str) -> Result<Box<dyn ObjectStore>> {
    let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .context("building GCS object store")?;
    Ok(Box::new(store))
}

#[cfg(not(feature = "gcs"))]
fn build_gcs_store(_bucket: &str) -> Result<Box<dyn ObjectStore>> {
    anyhow::bail!("GCS requires the 'gcs' feature flag")
}

#[cfg(feature = "aws")]
fn build_s3_store(bucket: &str) -> Result<Box<dyn ObjectStore>> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .context("building S3 object store")?;
    Ok(Box::new(store))
}

#[cfg(not(feature = "aws"))]
fn build_s3_store(_bucket: &str) -> Result<Box<dyn ObjectStore>> {
    anyhow::bail!("S3 requires the 'aws' feature flag")
}

/// Stores artifacts in any object_store backend (GCS, S3/Minio, etc.).
pub struct ObjectStoreArtifactStore {
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl std::fmt::Debug for ObjectStoreArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreArtifactStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreArtifactStore {
    /// Build from a URL like `gs://bucket/prefix` or `s3://bucket/prefix`.
    ///
    /// Credentials come from the environment:
    /// - GCS: application default credentials
    /// - S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, etc.
    pub fn from_url(url: &str) -> Result<Self> {
        let (store, prefix) = parse_object_store_url(url)?;
        Ok(Self { store, prefix })
    }

    /// Build from an explicit object_store instance and prefix (for testing).
    #[allow(dead_code)]
    pub fn new(store: Box<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Resolve an artifact reference to an object path inside the store.
    ///
    /// `retrieve` is reached with values that arrive on workflow input
    /// (`defer_manifest_ref`, `state_manifest_ref`, `resume_from.state_ref`),
    /// so a reference that may name any key turns "can start a workflow" into
    /// "can read anything in the bucket the worker's credentials reach". An
    /// empty prefix means the whole bucket *is* the store, and matches
    /// everything by design.
    fn resolve_for_read(&self, reference: &str) -> Result<Path> {
        let path = Path::from(reference);
        let prefix = Path::from(self.prefix.as_str());
        anyhow::ensure!(
            path.prefix_matches(&prefix),
            "artifact reference resolves outside the artifact store: {reference}"
        );
        Ok(path)
    }
}

#[async_trait]
impl ArtifactStore for ObjectStoreArtifactStore {
    async fn store(&self, invocation_id: &str, filename: &str, content: Bytes) -> Result<String> {
        let path = Path::from(self.prefix.as_str())
            .join(invocation_id)
            .join(filename);
        self.store
            .put(&path, content.into())
            .await
            .with_context(|| format!("writing artifact: {path}"))?;
        Ok(path.to_string())
    }

    async fn retrieve(&self, path: &str) -> Result<Bytes> {
        let path = self.resolve_for_read(path)?;
        let result = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("reading artifact: {path}"))?;
        Ok(result.bytes().await?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn in_memory(prefix: &str) -> ObjectStoreArtifactStore {
        ObjectStoreArtifactStore::new(Box::new(InMemory::new()), prefix.to_string())
    }

    #[cfg(feature = "aws")]
    #[test]
    fn an_s3_url_splits_into_bucket_and_prefix() -> Result<()> {
        let (_, prefix) = parse_object_store_url("s3://artifacts/dbt/runs")?;
        assert_eq!(prefix, "dbt/runs");
        Ok(())
    }

    #[cfg(feature = "aws")]
    #[test]
    fn a_bucket_url_has_an_empty_prefix() -> Result<()> {
        let (_, prefix) = parse_object_store_url("s3://artifacts")?;
        assert_eq!(prefix, "");
        Ok(())
    }

    /// A worker built without the backend's feature must say which flag is
    /// missing, not fail as though the URL were malformed.
    #[cfg(not(feature = "gcs"))]
    #[test]
    fn a_gcs_url_without_the_gcs_feature_names_the_flag() {
        let err = parse_object_store_url("gs://artifacts/dbt").expect_err("gcs is not compiled in");
        assert!(format!("{err:#}").contains("'gcs' feature"), "unexpected error: {err:#}");
    }

    #[cfg(not(feature = "aws"))]
    #[test]
    fn an_s3_url_without_the_aws_feature_names_the_flag() {
        let err = parse_object_store_url("s3://artifacts/dbt").expect_err("aws is not compiled in");
        assert!(format!("{err:#}").contains("'aws' feature"), "unexpected error: {err:#}");
    }

    #[test]
    fn an_unsupported_scheme_is_refused() {
        let err = parse_object_store_url("ftp://artifacts/dbt").expect_err("ftp is not a backend");
        assert!(
            format!("{err:#}").contains("unsupported object store URL scheme: ftp"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn a_url_without_a_bucket_is_refused() {
        let err = parse_object_store_url("s3:/dbt/runs").expect_err("no bucket was named");
        assert!(format!("{err:#}").contains("no bucket"), "unexpected error: {err:#}");
    }

    #[test]
    fn a_path_that_is_not_a_url_is_refused() {
        let err = parse_object_store_url("artifacts/dbt").expect_err("not a URL");
        assert!(
            format!("{err:#}").contains("parsing object store URL"),
            "unexpected error: {err:#}"
        );
    }

    /// Backend credentials reach the store's client; the Debug form is what
    /// lands in logs, so it carries the prefix and nothing else.
    #[test]
    fn debug_shows_the_prefix_but_not_the_backend() {
        let rendered = format!("{:?}", in_memory("dbt-artifacts"));
        assert!(rendered.contains("dbt-artifacts"), "{rendered}");
        assert!(!rendered.contains("InMemory"), "{rendered}");
    }

    #[tokio::test]
    async fn a_stored_artifact_reads_back_from_the_returned_reference() -> Result<()> {
        let store = in_memory("dbt-artifacts");
        let reference = store
            .store("inv-123", "run_results.json", Bytes::from_static(b"{\"results\":[]}"))
            .await?;

        assert_eq!(reference, "dbt-artifacts/inv-123/run_results.json");
        assert_eq!(store.retrieve(&reference).await?, Bytes::from_static(b"{\"results\":[]}"));
        Ok(())
    }

    /// `retrieve` is reached with references that arrive on workflow input, so
    /// a key outside the prefix must be refused before the bucket is read.
    #[tokio::test]
    async fn retrieve_refuses_a_key_outside_the_prefix() {
        let err = in_memory("dbt-artifacts")
            .retrieve("secrets/credentials.json")
            .await
            .expect_err("a key outside the prefix must be refused");
        assert!(
            format!("{err:#}").contains("outside the artifact store"),
            "unexpected error: {err:#}"
        );
    }

    /// The prefix is matched segment by segment: a sibling whose name merely
    /// starts with the prefix is a different directory, not inside the store.
    #[tokio::test]
    async fn retrieve_refuses_a_sibling_that_shares_the_prefix_as_a_string() {
        let err = in_memory("dbt-artifacts")
            .retrieve("dbt-artifacts-other/inv-123/manifest.json")
            .await
            .expect_err("a sibling prefix must be refused");
        assert!(
            format!("{err:#}").contains("outside the artifact store"),
            "unexpected error: {err:#}"
        );
    }

    /// An empty prefix makes the whole bucket the store, so every key is in it.
    #[tokio::test]
    async fn an_empty_prefix_reads_any_key() -> Result<()> {
        let store = in_memory("");
        let reference = store
            .store("inv-123", "manifest.json", Bytes::from_static(b"{}"))
            .await?;

        assert_eq!(reference, "inv-123/manifest.json");
        assert_eq!(store.retrieve(&reference).await?, Bytes::from_static(b"{}"));
        Ok(())
    }

    #[tokio::test]
    async fn retrieve_of_a_missing_key_names_it() {
        let err = in_memory("dbt-artifacts")
            .retrieve("dbt-artifacts/inv-123/missing.json")
            .await
            .expect_err("nothing was stored there");
        assert!(
            format!("{err:#}").contains("reading artifact: dbt-artifacts/inv-123/missing.json"),
            "unexpected error: {err:#}"
        );
    }
}
