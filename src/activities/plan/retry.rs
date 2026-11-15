//! Narrowing a plan to the nodes a previous run left unfinished.

use tracing::warn;

use super::super::artifact_status::ArtifactStatus;

/// Node ids a retry re-runs, parsed from a previous `run_results.json`: the
/// nodes `dbt retry` would pick (see [`ArtifactStatus::is_retryable`]).
///
/// Reads the status as a plain string, so both this worker's artifacts and the
/// dbt CLI's parse. A status word this worker does not know is retried: it
/// cannot be shown to be an outcome that finished well, and re-running a
/// node whose materialization is idempotent costs less than silently leaving
/// a failed one behind.
pub(in crate::activities) fn parse_retry_node_ids(
    bytes: &[u8],
) -> Result<std::collections::BTreeSet<String>, anyhow::Error> {
    #[derive(serde::Deserialize)]
    struct RunResults {
        results: Vec<ResultEntry>,
    }
    #[derive(serde::Deserialize)]
    struct ResultEntry {
        unique_id: String,
        status: String,
    }

    let parsed: RunResults =
        serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!("parsing run_results: {e}"))?;
    Ok(parsed
        .results
        .into_iter()
        .filter(|r| {
            let known = ArtifactStatus::parse(&r.status);
            if known.is_none() {
                warn!(
                    unique_id = %r.unique_id,
                    status = %r.status,
                    "retry_from: unrecognised status in previous run_results, retrying the node"
                );
            }
            known.is_none_or(ArtifactStatus::is_retryable)
        })
        .map(|r| r.unique_id)
        .collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Written the way the dbt CLI writes it, with its vocabulary.
    #[test]
    fn parse_retry_node_ids_reads_dbt_written_results() -> anyhow::Result<()> {
        let json = serde_json::json!({
            "metadata": {"invocation_id": "x", "dbt_version": "1.9.0"},
            "elapsed_time": 3.2,
            "args": {"which": "build"},
            "results": [
                {"unique_id": "model.p.ok", "status": "success", "execution_time": 1.0},
                {"unique_id": "model.p.bad", "status": "error", "execution_time": 0.5},
                {"unique_id": "model.p.blocked", "status": "skipped", "execution_time": 0.0},
                {"unique_id": "model.p.batched", "status": "partial success"},
                {"unique_id": "model.p.noop", "status": "no-op"},
                {"unique_id": "model.p.reused", "status": "reused"},
                {"unique_id": "test.p.passes", "status": "pass", "failures": 0},
                {"unique_id": "test.p.fails", "status": "fail", "failures": 4},
                {"unique_id": "test.p.warns", "status": "warn", "failures": 1},
                {"unique_id": "source.p.s.t", "status": "runtime error"},
            ],
        });
        let ids = parse_retry_node_ids(serde_json::to_vec(&json)?.as_slice())?;
        assert_eq!(
            ids.into_iter().collect::<Vec<_>>(),
            [
                "model.p.bad",
                "model.p.blocked",
                "source.p.s.t",
                "test.p.fails"
            ]
        );
        Ok(())
    }

    #[test]
    fn parse_retry_node_ids_retries_an_unrecognised_status() -> anyhow::Result<()> {
        let json = serde_json::json!({
            "results": [
                {"unique_id": "model.p.ok", "status": "success"},
                {"unique_id": "model.p.odd", "status": "exploded"},
            ],
        });
        let ids = parse_retry_node_ids(serde_json::to_vec(&json)?.as_slice())?;
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), ["model.p.odd"]);
        Ok(())
    }

    #[test]
    fn parse_retry_node_ids_rejects_garbage() {
        assert!(parse_retry_node_ids(b"not json").is_err());
        assert!(parse_retry_node_ids(b"{}").is_err());
    }
}
