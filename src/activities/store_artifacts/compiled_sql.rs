//! Reading the compiled SQL the nodes spilled to the store back for
//! `run_results.json`.

use std::collections::BTreeMap;

use futures::StreamExt;

use crate::artifact_store::ArtifactStore;

/// How many compiled-SQL objects are fetched at once.
///
/// A run can have thousands of nodes, each with its own small object, so one
/// round trip at a time makes this step scale with node count times store
/// latency. Unbounded fan-out would instead open a connection per node against
/// the store and its rate limits; 16 hides the latency without that.
const COMPILED_SQL_FETCH_CONCURRENCY: usize = 16;

/// Fetch each node's compiled SQL back from the store, keyed by unique id.
///
/// A node that failed to store its SQL, or one from a run without artifact
/// storage, simply has none here and keeps whatever it carried inline. A
/// failure to read one is logged rather than fatal: `run_results.json` without
/// one node's compiled SQL is worth far more than no artifact at all, and this
/// runs after every node has already finished.
///
/// Fetches overlap, but `buffered` yields in input order, so the map is built
/// exactly as a sequential walk would build it.
pub(super) async fn load_compiled_sql(
    store: &dyn ArtifactStore,
    results: &[crate::types::NodeExecutionResult],
) -> BTreeMap<String, String> {
    // Futures are built eagerly (they do nothing until polled) and handed to
    // `buffered` as plain items; a closure over borrowed items inside the
    // stream combinators defeats the `Send` inference the activity macro needs.
    let pending: Vec<_> = results
        .iter()
        .filter_map(|result| {
            let reference = result.compiled_code_ref.as_deref()?;
            Some(async move {
                (result.unique_id.as_str(), fetch_compiled_sql(store, result, reference).await)
            })
        })
        .collect();
    let fetched: Vec<(&str, Option<String>)> = futures::stream::iter(pending)
        .buffered(COMPILED_SQL_FETCH_CONCURRENCY)
        .collect()
        .await;

    fetched
        .into_iter()
        .filter_map(|(unique_id, sql)| Some((unique_id.to_owned(), sql?)))
        .collect()
}

/// Read one node's spilled SQL, logging and returning `None` if it cannot be had.
async fn fetch_compiled_sql(
    store: &dyn ArtifactStore,
    result: &crate::types::NodeExecutionResult,
    reference: &str,
) -> Option<String> {
    match store.retrieve(reference).await {
        Ok(bytes) => match String::from_utf8(bytes.to_vec()) {
            Ok(sql) => Some(sql),
            Err(e) => {
                tracing::warn!(
                    node = %result.unique_id,
                    error = %e,
                    "compiled SQL is not UTF-8; omitting it from run_results.json"
                );
                None
            }
        },
        Err(e) => {
            tracing::warn!(
                node = %result.unique_id,
                reference,
                error = %format!("{e:#}"),
                "could not read back compiled SQL; omitting it from run_results.json"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::artifact_store::LocalArtifactStore;
    use crate::types::{NodeExecutionResult, NodeStatus, StoreArtifactsInput};

    use super::super::run_results::build_run_results_json;
    use super::super::test_support::{freshness_input, sample_result};

    /// Compiled SQL crosses Temporal once, to the store, and comes back only
    /// here — the one artifact that reports it. The workflow accumulator, the
    /// checkpoint and the hook payloads carry a reference instead.
    #[tokio::test]
    async fn run_results_reads_compiled_sql_back_from_the_store() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = LocalArtifactStore::new(dir.path().to_path_buf());
        let reference = store
            .store(
                "inv-9",
                &crate::activities::execute_node::compiled_sql_artifact_name("model.p.m"),
                Bytes::from_static(b"select 1 as id"),
            )
            .await?;

        let mut spilled = sample_result("model.p.m", NodeStatus::Success, 1.0);
        spilled.compiled_code = None;
        spilled.compiled_code_ref = Some(reference);

        let compiled = load_compiled_sql(&store, std::slice::from_ref(&spilled)).await;
        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            node_results: vec![spilled],
            ..freshness_input("build", vec![])
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&build_run_results_json(&input, &compiled)?)?;
        let row = &parsed["results"][0];
        assert_eq!(row["compiled_code"], "select 1 as id", "{parsed}");
        assert_eq!(row["compiled"], true, "{parsed}");
        Ok(())
    }

    /// A reference that cannot be read costs that node's SQL, not the whole
    /// artifact — every node has already finished by the time this runs.
    #[tokio::test]
    async fn an_unreadable_reference_does_not_lose_the_artifact() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = LocalArtifactStore::new(dir.path().to_path_buf());

        let mut spilled = sample_result("model.p.m", NodeStatus::Success, 1.0);
        spilled.compiled_code = None;
        spilled.compiled_code_ref = Some("inv-9/compiled/nothing-here.sql".to_string());

        let compiled = load_compiled_sql(&store, std::slice::from_ref(&spilled)).await;
        assert!(compiled.is_empty(), "nothing was read");

        let input = StoreArtifactsInput {
            prior_segments: Vec::new(),
            node_results: vec![spilled],
            ..freshness_input("build", vec![])
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&build_run_results_json(&input, &compiled)?)?;
        assert_eq!(parsed["results"][0]["unique_id"], "model.p.m", "the row survives: {parsed}");
        Ok(())
    }

    /// Answers every read with its own path after a pause, recording how many
    /// reads were in flight at once.
    struct SlowEchoStore {
        in_flight: std::sync::atomic::AtomicUsize,
        max_in_flight: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ArtifactStore for SlowEchoStore {
        async fn store(&self, _: &str, _: &str, _: Bytes) -> anyhow::Result<String> {
            anyhow::bail!("read-only")
        }

        async fn retrieve(&self, path: &str) -> anyhow::Result<Bytes> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.max_in_flight.fetch_max(now, SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, SeqCst);
            Ok(Bytes::from(format!("select '{path}'")))
        }
    }

    #[tokio::test]
    async fn compiled_sql_reads_overlap_within_the_bound_and_land_on_their_nodes() {
        use std::sync::atomic::Ordering::SeqCst;
        let store = SlowEchoStore {
            in_flight: 0.into(),
            max_in_flight: 0.into(),
        };
        let node_count = COMPILED_SQL_FETCH_CONCURRENCY * 3;
        let results: Vec<NodeExecutionResult> = (0..node_count)
            .map(|i| {
                let mut r = sample_result(&format!("model.p.m{i}"), NodeStatus::Success, 1.0);
                // Every third node carries its SQL inline and has nothing to fetch.
                if i % 3 != 0 {
                    r.compiled_code_ref = Some(format!("ref-{i}"));
                }
                r
            })
            .collect();

        let compiled = load_compiled_sql(&store, &results).await;

        let max = store.max_in_flight.load(SeqCst);
        assert!(max > 1, "reads ran one at a time");
        assert!(max <= COMPILED_SQL_FETCH_CONCURRENCY, "{max} reads in flight exceeds the bound");
        assert_eq!(compiled.len(), (0..node_count).filter(|i| i % 3 != 0).count());
        for i in (0..node_count).filter(|i| i % 3 != 0) {
            assert_eq!(
                compiled.get(&format!("model.p.m{i}")).map(String::as_str),
                Some(format!("select 'ref-{i}'").as_str()),
            );
        }
    }
}
