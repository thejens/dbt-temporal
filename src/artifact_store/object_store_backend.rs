use anyhow::{Context, Result};
use async_trait::async_trait;
use object_store::ObjectStore;
use object_store::path::Path;

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
}

#[async_trait]
impl ArtifactStore for ObjectStoreArtifactStore {
    async fn store(&self, invocation_id: &str, filename: &str, content: &[u8]) -> Result<String> {
        let key = format!("{}/{invocation_id}/{filename}", self.prefix);
        let path = Path::from(key.as_str());
        self.store
            .put(&path, content.to_vec().into())
            .await
            .with_context(|| format!("writing artifact: {key}"))?;
        Ok(key)
    }

    async fn retrieve(&self, path: &str) -> Result<Vec<u8>> {
        let path = Path::from(path);
        let result = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("reading artifact: {path}"))?;
        Ok(result.bytes().await?.to_vec())
    }
}
