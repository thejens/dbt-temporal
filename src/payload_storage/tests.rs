use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::{BoxFuture, ready};
use temporalio_common::data_converters::{
    DataConverter, ExternalStorage, StorageDriverLimiter, StorageDriverPermit,
};
use temporalio_common::external_storage::{
    StoreTarget, retrieve_external_payloads, store_external_payloads,
};
use temporalio_common::protos::temporal::api::common::v1::Payloads;

use super::*;
use crate::artifact_store::LocalArtifactStore;

/// A limiter that records how many permits were requested.
#[derive(Default)]
struct CountingLimiter {
    acquired: AtomicUsize,
}

impl<Item: Sync> StorageDriverLimiter<Item> for CountingLimiter {
    fn acquire<'a>(
        &'a self,
        _item: &'a Item,
    ) -> BoxFuture<'a, Result<StorageDriverPermit, StorageDriverError>> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(Ok(StorageDriverPermit::new(Box::new(())))))
    }
}

fn payload(data: &[u8]) -> Payload {
    Payload {
        metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
        data: data.to_vec(),
        external_payloads: Vec::new(),
    }
}

fn driver_in(dir: &tempfile::TempDir) -> (ArtifactStoreDriver, Arc<dyn ArtifactStore>) {
    let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(dir.path().to_path_buf()));
    (ArtifactStoreDriver::new(Arc::clone(&store)), store)
}

fn store_ctx(limiter: &Arc<CountingLimiter>) -> StorageDriverStoreContext {
    StorageDriverStoreContext::builder()
        .limiter(Arc::clone(limiter) as Arc<_>)
        .build()
}

fn retrieve_ctx(limiter: &Arc<CountingLimiter>) -> StorageDriverRetrieveContext {
    StorageDriverRetrieveContext::builder()
        .limiter(Arc::clone(limiter) as Arc<_>)
        .build()
}

fn claim_path(claim: &StorageDriverClaim) -> &str {
    &claim.claim_data[CLAIM_PATH]
}

fn claim_to(path: &str) -> StorageDriverClaim {
    claim_for(path.to_owned())
}

#[tokio::test]
async fn stored_payloads_come_back_unchanged_and_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());
    let payloads = vec![payload(b"first"), payload(b"second"), payload(b"third")];

    let claims = driver
        .store(&store_ctx(&limiter), payloads.clone())
        .await
        .unwrap();
    let retrieved = driver
        .retrieve(&retrieve_ctx(&limiter), claims)
        .await
        .unwrap();

    assert_eq!(retrieved, payloads);
    // One permit per request, in each direction.
    assert_eq!(limiter.acquired.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn identical_payloads_share_one_object() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());

    let claims = driver
        .store(&store_ctx(&limiter), vec![payload(b"same"), payload(b"same")])
        .await
        .unwrap();

    assert_eq!(claim_path(&claims[0]), claim_path(&claims[1]));
    let objects = std::fs::read_dir(dir.path().join(KEY_DIR)).unwrap().count();
    assert_eq!(objects, 1);
}

#[tokio::test]
async fn objects_live_under_the_payload_directory_only() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, store) = driver_in(&dir);
    store
        .store("run-1", "manifest.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    let limiter = Arc::new(CountingLimiter::default());

    let claims = driver
        .store(&store_ctx(&limiter), vec![payload(b"x")])
        .await
        .unwrap();

    let stored = std::path::Path::new(claim_path(&claims[0]));
    assert_eq!(stored.parent().unwrap(), dir.path().join(KEY_DIR));
    let mut top_level: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    top_level.sort();
    assert_eq!(top_level, vec![KEY_DIR.to_owned(), "run-1".to_owned()]);
}

#[tokio::test]
async fn missing_object_is_reported_as_permanent() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());
    let absent = format!("{}/{KEY_DIR}/{}", dir.path().display(), "0".repeat(64));

    let err = driver
        .retrieve(&retrieve_ctx(&limiter), vec![claim_to(&absent)])
        .await
        .unwrap_err()
        .into_source();

    assert!(is_permanent(&err), "{err:#}");
    assert!(format!("{err:#}").contains("retrying will not help"), "{err:#}");
}

#[tokio::test]
async fn a_transient_store_failure_is_not_marked_permanent() {
    let err = describe_failure("storing external payload", anyhow!("connection reset"));
    assert!(!is_permanent(&err));
    assert_eq!(format!("{err:#}"), "storing external payload: connection reset");
}

#[tokio::test]
async fn claims_outside_the_payload_directory_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, store) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());
    // A real artifact that a hand-written reference might try to read.
    let manifest = store
        .store("run-1", "manifest.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    let digest = "a".repeat(64);
    let root = dir.path().display();

    let refused = [
        manifest,
        format!("{root}/{KEY_DIR}/../run-1/manifest.json"),
        format!("{root}/{KEY_DIR}/./{digest}"),
        format!("{root}/other/{digest}"),
        format!("{root}/{KEY_DIR}/{}", "A".repeat(64)),
        format!("{root}/{KEY_DIR}/{}", "a".repeat(63)),
        format!("{root}/{KEY_DIR}/nested/{digest}"),
        digest.clone(),
        "/etc/passwd".to_owned(),
    ];
    for path in refused {
        let err = driver
            .retrieve(&retrieve_ctx(&limiter), vec![claim_to(&path)])
            .await
            .unwrap_err()
            .into_source();
        let message = format!("{err:#}");
        assert!(
            message.contains("payload directory") || message.contains("not normalised"),
            "{path}: {message}"
        );
    }
}

#[tokio::test]
async fn a_claim_without_a_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());

    let err = driver
        .retrieve(&retrieve_ctx(&limiter), vec![StorageDriverClaim::new(HashMap::new())])
        .await
        .unwrap_err()
        .into_source();

    assert!(format!("{err:#}").contains("no `path` field"), "{err:#}");
}

#[tokio::test]
async fn an_object_that_no_longer_matches_its_digest_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());
    let claims = driver
        .store(&store_ctx(&limiter), vec![payload(b"original")])
        .await
        .unwrap();
    std::fs::write(claim_path(&claims[0]), b"tampered").unwrap();

    let err = driver
        .retrieve(&retrieve_ctx(&limiter), claims)
        .await
        .unwrap_err()
        .into_source();

    assert!(format!("{err:#}").contains("corrupt"), "{err:#}");
}

#[tokio::test]
async fn an_object_that_is_not_a_payload_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, store) = driver_in(&dir);
    let limiter = Arc::new(CountingLimiter::default());
    // Valid digest for its bytes, but the bytes are not a protobuf payload.
    let garbage = Bytes::from_static(&[0xff, 0xff, 0xff]);
    let path = store
        .store(KEY_DIR, &digest_hex(&garbage), garbage)
        .await
        .unwrap();

    let err = driver
        .retrieve(&retrieve_ctx(&limiter), vec![claim_to(&path)])
        .await
        .unwrap_err()
        .into_source();

    assert!(format!("{err:#}").contains("decoding external payload"), "{err:#}");
}

#[tokio::test]
async fn probe_succeeds_on_a_writable_store_and_fails_on_a_blocked_one() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    driver.probe().await.unwrap();

    // A file where the payload directory should be makes every write fail.
    let blocked = tempfile::tempdir().unwrap();
    std::fs::write(blocked.path().join(KEY_DIR), b"not a directory").unwrap();
    let (driver, _) = driver_in(&blocked);
    assert!(driver.probe().await.is_err());
}

/// The SDK's own store and retrieve passes with a tiny threshold: the large
/// payload is replaced by a reference, the small one stays inline, and the
/// round trip restores the original.
#[tokio::test]
async fn the_sdk_offloads_only_payloads_above_the_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let (driver, _) = driver_in(&dir);
    let converter = DataConverter::default().with_external_storage(
        ExternalStorage::new(Arc::new(driver))
            .unwrap()
            .with_payload_size_threshold(64),
    );
    let small = payload(b"tiny");
    let large = payload(&[b'x'; 500]);
    let mut message = Payloads {
        payloads: vec![small.clone(), large.clone()],
    };
    let target = StoreTarget::workflow("default", "wf", "run", "dbt_run");

    let metrics = store_external_payloads(&mut message, &converter, Some(target))
        .await
        .unwrap()
        .expect("one payload offloaded");

    assert_eq!(message.payloads[0], small);
    assert_eq!(
        message.payloads[1].metadata["messageType"],
        b"temporal.api.sdk.v1.ExternalStorageReference".to_vec()
    );
    assert!(message.payloads[1].data.len() < large.data.len());
    assert_eq!(metrics.into_proto().driver_names, vec![DRIVER_NAME.to_owned()]);

    retrieve_external_payloads(&mut message, &converter)
        .await
        .unwrap();
    assert_eq!(message.payloads, vec![small, large]);
}
