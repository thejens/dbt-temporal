//! Workflow determinism regression net.
//!
//! Replays committed histories from real `dbt_run` executions against the
//! current workflow code. A replay failure means the workflow would have made
//! a different sequence of commands than the recorded run did — which, for a
//! run already in flight when the worker is upgraded, is a non-deterministic
//! failure in production rather than a test failure here.
//!
//! This is the only test that covers that class of bug. The determinism test
//! in `src/workflow/mod.rs` is a source scan (no clocks, no randomness, no
//! env/filesystem access); it cannot see a change in the *order* or *shape* of
//! the commands the workflow emits, which is what replay checks.
//!
//! Needs neither Docker nor a Temporal server: the replayer drives the
//! workflow entirely from recorded history.
//!
//! Fixtures live in `tests/fixtures/histories/` as gzipped JSON, and are
//! produced by running the `waffle_hut` suite with
//! `DBT_TEMPORAL_RECORD_HISTORIES=1`. Their value comes from having been
//! recorded by an older build, so regenerate them only when a deliberate
//! workflow change makes the recorded shape genuinely unreachable — never to
//! make a red test go green.

use std::path::PathBuf;

use anyhow::{Context, Result};
use temporalio_client::WorkflowHistory;
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};

use dbt_temporal::workflow::DbtRunWorkflow;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/histories")
}

/// Load every committed history fixture, newest-shape-last for stable ordering.
fn load_fixtures() -> Result<Vec<(String, WorkflowHistory)>> {
    let dir = fixture_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|e| e.path().extension().is_some_and(|x| x == "gz"))
        .collect();
    entries.sort_by_key(std::fs::DirEntry::path);

    entries
        .into_iter()
        .map(|entry| {
            let path = entry.path();
            let compressed = std::fs::read(&path)
                .with_context(|| format!("reading fixture {}", path.display()))?;
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(
                &mut flate2::read::GzDecoder::new(compressed.as_slice()),
                &mut bytes,
            )
            .with_context(|| format!("decompressing fixture {}", path.display()))?;
            let history = WorkflowHistory::from_json(&bytes)
                .with_context(|| format!("decoding fixture {}", path.display()))?;
            let name = path
                .file_name()
                .map_or_else(|| path.display().to_string(), |n| n.to_string_lossy().into());
            Ok((name, history))
        })
        .collect()
}

fn replayer() -> Result<WorkflowReplayer> {
    // Only the workflow is registered: activity results come from the recorded
    // history, so the replayer never invokes activity code.
    let options = WorkflowReplayerOptions::new()
        .register_workflow::<DbtRunWorkflow>()
        .map_err(|e| anyhow::anyhow!("registering DbtRunWorkflow for replay: {e}"))?
        .build();
    WorkflowReplayer::new(options).map_err(|e| anyhow::anyhow!("building replayer: {e}"))
}

#[tokio::test(flavor = "current_thread")]
async fn committed_histories_replay_against_current_workflow() -> Result<()> {
    let fixtures = load_fixtures()?;
    assert!(
        !fixtures.is_empty(),
        "no history fixtures in {} — regenerate with \
         DBT_TEMPORAL_RECORD_HISTORIES=1 (see this file's module docs). An empty \
         fixture set would make this test vacuously pass and silently retire the \
         only determinism net we have.",
        fixture_dir().display()
    );

    let replayer = replayer()?;
    for (name, history) in fixtures {
        let events = history.events().len();
        replayer.replay_workflow(history).await.with_context(|| {
            format!("replaying {name} ({events} events) against current workflow code")
        })?;
    }
    Ok(())
}
