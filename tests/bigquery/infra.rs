//! Shared test infrastructure: Temporal dev server + BigQuery config from env vars.
//!
//! Cleanup strategy:
//!   1. Each test run uses a unique dataset (`dbt_temporal_test_<short_uuid>`).
//!   2. The dataset is pre-created with a 1-hour default table expiration (safety net).
//!   3. A ref-counted `TestGuard` drops the dataset via `bq rm` when the last test finishes.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, UntypedWorkflow,
    WorkflowGetResultOptions, WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::data_converters::RawValue;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_sdk_core::ephemeral_server::TemporalDevServerConfig;

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use dbt_temporal::config::DbtTemporalConfig;
use dbt_temporal::telemetry_compat::DbtTelemetryCompatLayer;
use dbt_temporal::types::{DbtRunInput, DbtRunOutput, NodeStatus, NodeStatusTree};

/// Initialize tracing with the dbt-fusion telemetry compatibility layer.
pub fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(DbtTelemetryCompatLayer)
        .try_init();
}

// ---------- Shared test infrastructure ----------

pub struct SharedInfra {
    pub temporal_addr: String,
    pub gcp_project: String,
    pub dataset: String,
    _temporal_server: temporalio_sdk_core::ephemeral_server::EphemeralServer,
}

/// Return shared Temporal infrastructure, starting it on first call.
///
/// Also pre-creates the BigQuery dataset with a 1-hour default table expiration.
pub fn shared_infra() -> &'static SharedInfra {
    use std::sync::OnceLock;
    static INFRA: OnceLock<SharedInfra> = OnceLock::new();
    INFRA.get_or_init(|| {
        let result = std::thread::spawn(|| -> Result<SharedInfra> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("building init runtime")?;
            rt.block_on(async {
                tracing::info!("=== Starting shared BigQuery test infrastructure ===");

                let gcp_project = std::env::var("GOOGLE_CLOUD_PROJECT")
                    .context("GOOGLE_CLOUD_PROJECT env var is required for BigQuery tests")?;
                let dataset = std::env::var("BIGQUERY_TEST_DATASET").unwrap_or_else(|_| {
                    format!(
                        "dbt_temporal_test_{}",
                        &uuid::Uuid::new_v4().to_string()[..8]
                    )
                });

                // Pre-create dataset with 1-hour table expiration as safety net.
                tracing::info!("Creating dataset {gcp_project}:{dataset} (1h table TTL)");
                let mk_status = std::process::Command::new("bq")
                    .args([
                        "mk",
                        "--dataset",
                        "--default_table_expiration",
                        "3600",
                        &format!("{gcp_project}:{dataset}"),
                    ])
                    .output();
                match mk_status {
                    Ok(out) if out.status.success() => {
                        tracing::info!("Dataset created");
                    }
                    Ok(out) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        if stderr.contains("already exists") {
                            tracing::info!("Dataset already exists, reusing");
                        } else {
                            tracing::warn!("bq mk returned {}: {stderr}", out.status);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("bq not available ({e}), dataset will be created by dbt (no auto-expiration safety net)");
                    }
                }

                let config = TemporalDevServerConfig::builder()
                    .exe(temporalio_sdk_core::ephemeral_server::default_cached_download())
                    .namespace("default")
                    .extra_args(vec![
                        "--search-attribute".to_string(),
                        "DbtProject=Keyword".to_string(),
                        "--search-attribute".to_string(),
                        "DbtCommand=Keyword".to_string(),
                        "--search-attribute".to_string(),
                        "DbtTarget=Keyword".to_string(),
                        "--search-attribute".to_string(),
                        "env=Keyword".to_string(),
                    ])
                    .build();
                let temporal_server = config
                    .start_server()
                    .await
                    .context("starting shared temporal server")?;
                let temporal_addr = temporal_server.target.clone();
                tracing::info!("Shared Temporal: {temporal_addr}");

                Ok(SharedInfra {
                    temporal_addr,
                    gcp_project,
                    dataset,
                    _temporal_server: temporal_server,
                })
            })
        })
        .join();
        match result {
            Ok(Ok(infra)) => infra,
            Ok(Err(e)) => panic!("infrastructure init failed: {e:#}"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    })
}

// ---------- Dataset cleanup ----------

/// Number of tests currently holding a guard. When this reaches zero the dataset is dropped.
static ACTIVE_TESTS: AtomicUsize = AtomicUsize::new(0);

/// RAII guard that drops the BigQuery dataset when the last test finishes.
///
/// Each test should hold one of these (via `test_guard()`). On `Drop`, if this
/// is the last active guard, we run `bq rm -r -f` to nuke the dataset.
/// Even on test panic the guard is dropped during unwinding, so cleanup still runs.
pub struct TestGuard {
    gcp_project: String,
    dataset: String,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        if ACTIVE_TESTS.fetch_sub(1, Ordering::SeqCst) == 1 {
            tracing::info!(
                "Last test finished — dropping dataset {}:{}",
                self.gcp_project,
                self.dataset
            );
            let status = std::process::Command::new("bq")
                .args([
                    "rm",
                    "-r",
                    "-f",
                    &format!("{}:{}", self.gcp_project, self.dataset),
                ])
                .output();
            match status {
                Ok(out) if out.status.success() => {
                    tracing::info!("Dataset dropped successfully");
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    tracing::warn!(
                        "bq rm exited with {}: {stderr} (dataset may need manual cleanup or will expire via TTL)",
                        out.status
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to run bq rm: {e} (dataset will expire via 1h TTL)");
                }
            }
        }
    }
}

/// Create a test guard that ref-counts active tests and drops the dataset on last finish.
pub fn test_guard(infra: &SharedInfra) -> TestGuard {
    ACTIVE_TESTS.fetch_add(1, Ordering::SeqCst);
    TestGuard {
        gcp_project: infra.gcp_project.clone(),
        dataset: infra.dataset.clone(),
    }
}

// ---------- Config helpers ----------

fn bigquery_project_path() -> PathBuf {
    PathBuf::from(format!("{}/examples/bigquery", env!("CARGO_MANIFEST_DIR")))
}

/// Build a `DbtTemporalConfig` for the BigQuery example project.
///
/// Writes a profiles.yml with credentials from the shared infra to a temp directory.
pub fn bigquery_test_config(
    temporal_address: &str,
    gcp_project: &str,
    dataset: &str,
) -> Result<DbtTemporalConfig> {
    bigquery_test_config_with_project_dir(
        temporal_address,
        gcp_project,
        dataset,
        &bigquery_project_path(),
    )
}

/// Build a `DbtTemporalConfig` for a custom BigQuery project directory.
pub fn bigquery_test_config_with_project_dir(
    temporal_address: &str,
    gcp_project: &str,
    dataset: &str,
    project_dir: &std::path::Path,
) -> Result<DbtTemporalConfig> {
    let profiles_dir =
        std::env::temp_dir().join(format!("dbtt-bq-profiles-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&profiles_dir).context("creating profiles dir")?;

    let profiles_yml = format!(
        r#"bigquery_example:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: "{gcp_project}"
      dataset: "{dataset}"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 1
"#
    );
    std::fs::write(profiles_dir.join("profiles.yml"), &profiles_yml)
        .context("writing profiles.yml")?;

    let artifact_dir =
        std::env::temp_dir().join(format!("dbtt-bq-artifacts-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&artifact_dir).ok();

    Ok(DbtTemporalConfig {
        temporal_address: format!("http://{temporal_address}"),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("bq-test-{}", uuid::Uuid::new_v4()),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![project_dir.to_string_lossy().to_string()],
        dbt_profiles_dir: Some(profiles_dir.to_string_lossy().to_string()),
        dbt_target: None,

        health_file: None,
        health_port: None,
        write_artifacts: false,
        artifact_store: artifact_dir.to_string_lossy().to_string(),
        search_attributes: std::collections::BTreeMap::default(),
        write_run_log: false,
        worker_tuning: dbt_temporal::config::WorkerTuningConfig::Fixed {
            max_concurrent_workflow_tasks: 200,
            max_concurrent_activities: 100,
            max_concurrent_local_activities: 100,
        },
        sticky_queue_timeout_secs: 10,
        nonsticky_to_sticky_poll_ratio: 0.2,
        max_worker_activities_per_second: None,
        max_task_queue_activities_per_second: None,
        graceful_shutdown_secs: None,
        max_cached_workflows: 1000,
    })
}

// ---------- Fixture helpers ----------

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Copy the BigQuery example project to a temp directory for isolated modification.
pub fn copy_bigquery_fixture() -> Result<PathBuf> {
    let src = bigquery_project_path();
    let dst = std::env::temp_dir().join(format!("dbtt-bq-fixture-{}", uuid::Uuid::new_v4()));
    copy_dir_recursive(&src, &dst).context("copying BigQuery fixture")?;
    // Remove target/ to avoid stale compilation artifacts.
    std::fs::remove_dir_all(dst.join("target")).ok();
    Ok(dst)
}

// ---------- Workflow helpers ----------

pub fn make_input(
    command: &str,
    select: Option<&str>,
    exclude: Option<&str>,
    fail_fast: bool,
) -> DbtRunInput {
    DbtRunInput {
        project: None,
        command: command.to_string(),
        select: select.map(String::from),
        exclude: exclude.map(String::from),
        vars: std::collections::BTreeMap::default(),
        target: None,
        full_refresh: false,
        fail_fast,
        hooks: None,
        env: std::collections::BTreeMap::default(),
    }
}

pub async fn connect_client(temporal_addr: &str) -> Result<Client> {
    let url = url::Url::parse(&format!("http://{temporal_addr}"))
        .context("parsing temporal address as URL")?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal server")?;
    Client::new(connection, ClientOptions::new("default").build())
        .map_err(|e| anyhow::anyhow!("creating Temporal client: {e}"))
}

pub struct WorkflowRun {
    pub output: DbtRunOutput,
    #[allow(dead_code)]
    pub workflow_id: String,
    #[allow(dead_code)]
    pub run_id: String,
}

pub async fn run_dbt_workflow(
    client: &Client,
    task_queue: &str,
    input: DbtRunInput,
) -> Result<WorkflowRun> {
    let workflow_id = format!("bq-test-{}", uuid::Uuid::new_v4());
    let handle = client
        .start_workflow(
            UntypedWorkflow::new("dbt_run"),
            RawValue::new(vec![input.as_json_payload()?]),
            WorkflowStartOptions::new(task_queue, &workflow_id).build(),
        )
        .await
        .context("starting workflow")?;

    let run_id = handle.run_id().unwrap_or_default().to_string();
    let raw_value = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("waiting for workflow result")?;

    let payload = raw_value
        .payloads
        .first()
        .ok_or_else(|| anyhow::anyhow!("workflow should return a payload"))?;
    let output: DbtRunOutput =
        serde_json::from_slice(&payload.data).context("deserializing DbtRunOutput")?;
    Ok(WorkflowRun {
        output,
        workflow_id,
        run_id,
    })
}

/// Run a workflow that is expected to fail (e.g. because a test node errors).
///
/// Returns the `NodeStatusTree` from the workflow memo, which records which
/// nodes succeeded, errored, or were skipped.
pub async fn run_dbt_workflow_expect_failure(
    client: &mut Client,
    task_queue: &str,
    input: DbtRunInput,
) -> Result<NodeStatusTree> {
    let workflow_id = format!("bq-test-{}", uuid::Uuid::new_v4());
    let handle = client
        .start_workflow(
            UntypedWorkflow::new("dbt_run"),
            RawValue::new(vec![input.as_json_payload()?]),
            WorkflowStartOptions::new(task_queue, &workflow_id).build(),
        )
        .await
        .context("starting workflow")?;

    let run_id = handle.run_id().unwrap_or_default().to_string();
    let result = handle.get_result(WorkflowGetResultOptions::default()).await;

    match &result {
        Err(temporalio_client::errors::WorkflowGetResultError::Failed(_)) => {}
        Ok(_) => anyhow::bail!("expected workflow to fail, but it succeeded"),
        Err(other) => anyhow::bail!("expected workflow to fail, got: {other:?}"),
    }

    // Extract node_status from the workflow memo via describe.
    let desc = describe_workflow(client, &workflow_id, &run_id).await?;
    let memo = desc
        .workflow_execution_info
        .as_ref()
        .and_then(|i| i.memo.as_ref())
        .ok_or_else(|| anyhow::anyhow!("workflow has no memo"))?;

    let status_payload = memo
        .fields
        .get("node_status")
        .ok_or_else(|| anyhow::anyhow!("node_status memo not found"))?;
    let node_status: NodeStatusTree =
        serde_json::from_slice(&status_payload.data).context("deserializing NodeStatusTree")?;

    Ok(node_status)
}

pub async fn describe_workflow(
    client: &mut impl WorkflowService,
    workflow_id: &str,
    run_id: &str,
) -> Result<temporalio_common::protos::temporal::api::workflowservice::v1::DescribeWorkflowExecutionResponse>
{
    use temporalio_client::tonic::IntoRequest;
    use temporalio_common::protos::temporal::api::{
        common::v1::WorkflowExecution as WfExec,
        workflowservice::v1::DescribeWorkflowExecutionRequest,
    };

    let resp = client
        .describe_workflow_execution(
            DescribeWorkflowExecutionRequest {
                namespace: "default".to_owned(),
                execution: Some(WfExec {
                    workflow_id: workflow_id.to_owned(),
                    run_id: run_id.to_owned(),
                }),
            }
            .into_request(),
        )
        .await
        .context("describe_workflow_execution")?;

    Ok(resp.into_inner())
}

pub fn print_results(output: &DbtRunOutput) {
    tracing::info!(
        "  success={} elapsed={:.2}s nodes={}",
        output.success,
        output.elapsed_time,
        output.node_results.len()
    );
    for r in &output.node_results {
        let msg = r.message.as_deref().unwrap_or("");
        tracing::info!("    {} -> {} ({:.2}s) {msg}", r.unique_id, r.status, r.execution_time);
    }
}

pub fn print_node_status(tree: &NodeStatusTree) {
    for (uid, status) in &tree.nodes {
        tracing::info!("    {uid} -> {status:?}");
    }
}

pub fn nodes_with_status(tree: &NodeStatusTree, status: NodeStatus) -> Vec<&str> {
    tree.nodes
        .iter()
        .filter(|(_, s)| **s == status)
        .map(|(uid, _)| uid.as_str())
        .collect()
}
