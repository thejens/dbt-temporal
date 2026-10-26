//! Integration test: ObjectStoreArtifactStore against RustFS (S3-compatible).
//!
//! Requires the `aws` feature flag:
//!   cargo test --features aws --test artifact_store_s3

#![cfg(feature = "aws")]

use anyhow::{Context, Result};
use bytes::Bytes;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use dbt_temporal::artifact_store::{ArtifactStore, ObjectStoreArtifactStore};

const BUCKET: &str = "test-artifacts";
const PREFIX: &str = "dbt-artifacts";

/// RustFS rather than MinIO: MinIO stopped publishing community images, and
/// the tag `testcontainers-modules` defaults to no longer resolves. Pinned to
/// a release so the S3 behaviour under test only changes when we bump it.
const S3_IMAGE: &str = "rustfs/rustfs";
const S3_TAG: &str = "1.0.4";
const S3_PORT: u16 = 9000;
const ACCESS_KEY: &str = "artifact-test";
const SECRET_KEY: &str = "artifact-test-secret";

/// Start RustFS, create the test bucket, and return the ObjectStoreArtifactStore.
async fn setup() -> Result<(ContainerAsync<GenericImage>, ObjectStoreArtifactStore)> {
    // RustFS logs to a file, not stdout, so readiness comes from its health
    // endpoint: the S3 API answers once `/health/ready` does.
    let container = GenericImage::new(S3_IMAGE, S3_TAG)
        .with_exposed_port(S3_PORT.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/health/ready")
                .with_port(S3_PORT.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .start()
        .await
        .context("starting rustfs container")?;

    let host = container.get_host().await.context("get rustfs host")?;
    let port = container
        .get_host_port_ipv4(S3_PORT)
        .await
        .context("get rustfs port")?;
    let endpoint = format!("http://{host}:{port}");

    // Create the bucket via the AWS SDK (path-style: the bucket is not a DNS name).
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .credentials_provider(aws_credential_types::Credentials::new(
            ACCESS_KEY, SECRET_KEY, None, None, "test",
        ))
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    s3_client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .context("creating test bucket")?;

    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_endpoint(&endpoint)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()
        .context("building S3 object store for rustfs")?;

    let artifact_store = ObjectStoreArtifactStore::new(Box::new(store), PREFIX.to_string());

    Ok((container, artifact_store))
}

#[tokio::test]
async fn test_store_and_retrieve() -> Result<()> {
    let (_container, store) = setup().await?;

    let content = Bytes::from_static(b"hello world from dbt-temporal");
    let path = store.store("inv-001", "test.json", content.clone()).await?;

    assert_eq!(path, "dbt-artifacts/inv-001/test.json");

    let retrieved = store.retrieve(&path).await?;
    assert_eq!(retrieved, content);

    Ok(())
}

#[tokio::test]
async fn test_store_large_payload() -> Result<()> {
    let (_container, store) = setup().await?;

    // 1 MB payload.
    let content = Bytes::from((0u8..=255).cycle().take(1_000_000).collect::<Vec<u8>>());
    let path = store
        .store("inv-002", "manifest.json", content.clone())
        .await?;

    let retrieved = store.retrieve(&path).await?;
    assert_eq!(retrieved.len(), content.len());
    assert_eq!(retrieved, content);

    Ok(())
}

#[tokio::test]
async fn test_store_multiple_files_same_invocation() -> Result<()> {
    let (_container, store) = setup().await?;

    let manifest = Bytes::from_static(br#"{"metadata":{},"nodes":{}}"#);
    let run_results = Bytes::from_static(br#"{"results":[]}"#);

    let p1 = store
        .store("inv-003", "manifest.json", manifest.clone())
        .await?;
    let p2 = store
        .store("inv-003", "run_results.json", run_results.clone())
        .await?;

    assert_eq!(p1, "dbt-artifacts/inv-003/manifest.json");
    assert_eq!(p2, "dbt-artifacts/inv-003/run_results.json");

    assert_eq!(store.retrieve(&p1).await?, manifest);
    assert_eq!(store.retrieve(&p2).await?, run_results);

    Ok(())
}

#[tokio::test]
async fn test_retrieve_nonexistent_returns_error() -> Result<()> {
    let (_container, store) = setup().await?;

    let result = store.retrieve("dbt-artifacts/no-such/file.json").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_overwrite_existing_file() -> Result<()> {
    let (_container, store) = setup().await?;

    let path = store
        .store("inv-004", "data.json", Bytes::from_static(b"v1"))
        .await?;
    assert_eq!(store.retrieve(&path).await?, Bytes::from_static(b"v1"));

    let path2 = store
        .store("inv-004", "data.json", Bytes::from_static(b"v2"))
        .await?;
    assert_eq!(path, path2);
    assert_eq!(store.retrieve(&path).await?, Bytes::from_static(b"v2"));

    Ok(())
}
