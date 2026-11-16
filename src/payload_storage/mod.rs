//! External storage for large Temporal payloads, backed by the artifact store.
//!
//! The Temporal SDK can swap a payload above a size threshold for a small
//! reference and keep the bytes in a [`StorageDriver`]. This driver keeps them
//! in the same [`ArtifactStore`] that holds run artifacts, so an operator
//! needs no second bucket, client or set of credentials.
//!
//! Each payload is stored as its protobuf encoding under
//! `<store>/_external_payloads/<sha256>`. Addressing by content makes a store
//! idempotent, which matters because the SDK stores again whenever it replays a
//! workflow, and means two identical payloads share one object.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::future::{BoxFuture, try_join_all};
use prost::Message;
use sha2::{Digest, Sha256};
use temporalio_common::data_converters::{
    StorageDriver, StorageDriverClaim, StorageDriverError, StorageDriverRetrieveContext,
    StorageDriverStoreContext,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;

use crate::artifact_store::{ArtifactStore, is_permanent};

/// Written into workflow history next to every stored payload and used to route
/// retrieval back here, so renaming it strands everything already stored.
pub const DRIVER_NAME: &str = "dbt-temporal-artifact-store";

/// Identifies this implementation, as opposed to this instance.
const DRIVER_TYPE: &str = "dbt-temporal.artifactstore";

/// The directory every payload is written to. It sits beside the per-invocation
/// directories that hold run artifacts, and a leading underscore keeps it from
/// being taken for an invocation id.
const KEY_DIR: &str = "_external_payloads";

/// The claim field holding the path the artifact store returned for the object.
const CLAIM_PATH: &str = "path";

/// Length of a hex-encoded SHA-256 digest.
const DIGEST_HEX_LEN: usize = 64;

/// Stores Temporal payloads in an [`ArtifactStore`].
pub struct ArtifactStoreDriver {
    store: Arc<dyn ArtifactStore>,
}

impl std::fmt::Debug for ArtifactStoreDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactStoreDriver")
            .finish_non_exhaustive()
    }
}

impl ArtifactStoreDriver {
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self { store }
    }

    /// Store and read back one object, so a store that cannot serve payloads
    /// (read-only, wrong credentials, unreachable) fails at start-up rather
    /// than on the first workflow task that carries a large payload.
    pub async fn probe(&self) -> Result<()> {
        let payload = Payload::default();
        let path = store_one(&self.store, &payload).await?;
        let read_back = retrieve_one(&self.store, &claim_for(path)).await?;
        if read_back != payload {
            bail!("external payload store returned different bytes than were written");
        }
        Ok(())
    }
}

impl StorageDriver for ArtifactStoreDriver {
    fn name(&self) -> &str {
        DRIVER_NAME
    }

    fn driver_type(&self) -> &str {
        DRIVER_TYPE
    }

    fn store(
        &self,
        context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
        let store = Arc::clone(&self.store);
        let limiter = Arc::clone(&context.limiter);
        Box::pin(async move {
            let claims = try_join_all(payloads.into_iter().map(|payload| {
                let store = Arc::clone(&store);
                let limiter = Arc::clone(&limiter);
                async move {
                    // One permit per request, held until the request ends: the
                    // SDK bounds concurrency only through drivers that ask.
                    let _permit = limiter.acquire(&payload).await?;
                    let path = store_one(&store, &payload).await?;
                    Ok::<_, StorageDriverError>(claim_for(path))
                }
            }))
            .await?;
            Ok(claims)
        })
    }

    fn retrieve(
        &self,
        context: &StorageDriverRetrieveContext,
        claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, StorageDriverError>> {
        let store = Arc::clone(&self.store);
        let limiter = Arc::clone(&context.limiter);
        Box::pin(async move {
            let payloads = try_join_all(claims.into_iter().map(|claim| {
                let store = Arc::clone(&store);
                let limiter = Arc::clone(&limiter);
                async move {
                    let _permit = limiter.acquire(&claim).await?;
                    Ok::<_, StorageDriverError>(retrieve_one(&store, &claim).await?)
                }
            }))
            .await?;
            Ok(payloads)
        })
    }
}

fn claim_for(path: String) -> StorageDriverClaim {
    StorageDriverClaim::new(HashMap::from([(CLAIM_PATH.to_owned(), path)]))
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

async fn store_one(store: &Arc<dyn ArtifactStore>, payload: &Payload) -> Result<String> {
    let bytes = Bytes::from(payload.encode_to_vec());
    let digest = digest_hex(&bytes);
    store
        .store(KEY_DIR, &digest, bytes)
        .await
        .map_err(|err| describe_failure("storing external payload", err))
}

async fn retrieve_one(
    store: &Arc<dyn ArtifactStore>,
    claim: &StorageDriverClaim,
) -> Result<Payload> {
    let path = claim
        .claim_data
        .get(CLAIM_PATH)
        .ok_or_else(|| anyhow!("external payload claim has no `{CLAIM_PATH}` field"))?;
    let digest = claimed_digest(path)?;
    let bytes = store
        .retrieve(path)
        .await
        .map_err(|err| describe_failure("retrieving external payload", err))?;
    // The path is the content's digest, so a mismatch means the object was
    // overwritten or truncated; handing that to a workflow would corrupt it.
    if digest_hex(&bytes) != digest {
        bail!("external payload {path} does not match its digest; the stored object is corrupt");
    }
    Payload::decode(bytes).with_context(|| format!("decoding external payload {path}"))
}

/// The digest a claimed path names, after checking the path can only be an
/// object this driver wrote.
///
/// Claims come out of workflow history, and anyone who can start a workflow can
/// put a hand-written reference in its input. Without this check that reference
/// could name any object the artifact store holds, such as another run's
/// manifest. The artifact store confines a path to the store as a whole; this
/// confines it to the payload directory.
fn claimed_digest(path: &str) -> Result<&str> {
    if path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        bail!("external payload path is not normalised: {path}");
    }
    let mut segments = path.rsplit('/');
    let name = segments.next().unwrap_or_default();
    let dir = segments.next().unwrap_or_default();
    let is_digest = name.len() == DIGEST_HEX_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if dir != KEY_DIR || !is_digest {
        bail!("external payload path is outside the payload directory: {path}");
    }
    Ok(name)
}

/// Add what the SDK cannot know: it treats every driver failure alike, but a
/// permanent one (missing object, refused credentials) fails the workflow task
/// on every retry until someone intervenes, so say so in the error.
fn describe_failure(action: &str, err: anyhow::Error) -> anyhow::Error {
    if is_permanent(&err) {
        err.context(format!("{action} (permanent failure, retrying will not help)"))
    } else {
        err.context(action.to_owned())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
