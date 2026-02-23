//! Shared test infrastructure: Postgres testcontainer, Temporal dev server,
//! config builders, and workflow execution helpers.

use std::path::{Path, PathBuf};

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
use dbt_temporal::types::{DbtRunInput, DbtRunOutput};

/// Initialize tracing with the dbt-fusion telemetry compatibility layer.
///
/// Without `DbtTelemetryCompatLayer`, dbt-fusion's adapter code crashes (SIGSEGV)
/// when it tries to access `TelemetryAttributes` from span extensions.
pub fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(DbtTelemetryCompatLayer)
        .try_init();
}

// ---------- Seed data ----------

/// SQL to set up the raw schema and seed data in Postgres.
const SEED_SQL: &str = r"
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    id integer PRIMARY KEY,
    first_name text NOT NULL,
    last_name text NOT NULL
);

INSERT INTO raw.customers (id, first_name, last_name) VALUES
    (1, 'Michael', 'P.'),
    (2, 'Shawn', 'M.'),
    (3, 'Kathleen', 'P.'),
    (4, 'Jimmy', 'C.'),
    (5, 'Katherine', 'R.')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS raw.orders (
    id integer PRIMARY KEY,
    user_id integer NOT NULL,
    order_date date NOT NULL,
    status text NOT NULL
);

INSERT INTO raw.orders (id, user_id, order_date, status) VALUES
    (1, 1, '2018-01-01', 'returned'),
    (2, 3, '2018-01-02', 'completed'),
    (3, 4, '2018-01-04', 'completed'),
    (4, 1, '2018-01-05', 'completed'),
    (5, 2, '2018-01-05', 'completed')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS raw.payments (
    id integer PRIMARY KEY,
    order_id integer NOT NULL,
    payment_method text NOT NULL,
    amount integer NOT NULL
);

INSERT INTO raw.payments (id, order_id, payment_method, amount) VALUES
    (1, 1, 'credit_card', 1000),
    (2, 2, 'credit_card', 2000),
    (3, 3, 'coupon', 100),
    (4, 4, 'credit_card', 2500),
    (5, 5, 'bank_transfer', 1700)
ON CONFLICT DO NOTHING;
";

// ---------- Shared test infrastructure ----------
//
// All tests share a single Temporal dev server and a single Postgres
// testcontainer. Each test gets a unique task queue for isolation.

pub struct SharedInfra {
    pub temporal_addr: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    _temporal_server: temporalio_sdk_core::ephemeral_server::EphemeralServer,
    _pg_container: testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
}

/// Return shared infrastructure, starting Temporal + Postgres on first call.
///
/// Initialization runs in a dedicated thread with its own tokio runtime to avoid
/// nesting `block_on` inside the test's `#[tokio::test]` runtime.
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
                tracing::info!("=== Starting shared test infrastructure ===");

                // Start Postgres testcontainer
                use testcontainers::runners::AsyncRunner;
                use testcontainers_modules::postgres::Postgres;

                let pg_container = Postgres::default()
                    .with_db_name("waffle_hut")
                    .with_user("test")
                    .with_password("test")
                    .start()
                    .await
                    .context("starting postgres testcontainer")?;

                let pg_host = pg_container
                    .get_host()
                    .await
                    .context("getting postgres host")?
                    .to_string();
                let pg_port = pg_container
                    .get_host_port_ipv4(5432)
                    .await
                    .context("getting postgres port")?;

                tracing::info!("Postgres: {pg_host}:{pg_port}");

                // Seed the database
                let output = std::process::Command::new("psql")
                    .arg("-h")
                    .arg(&pg_host)
                    .arg("-p")
                    .arg(pg_port.to_string())
                    .arg("-U")
                    .arg("test")
                    .arg("-d")
                    .arg("waffle_hut")
                    .arg("-c")
                    .arg(SEED_SQL)
                    .env("PGPASSWORD", "test")
                    .output()
                    .context("running psql seed (is `psql` installed? brew install libpq)")?;
                assert!(
                    output.status.success(),
                    "psql seed failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );

                // Start Temporal dev server
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
                    pg_host,
                    pg_port,
                    pg_user: "test".to_string(),
                    pg_password: "test".to_string(),
                    pg_database: "waffle_hut".to_string(),
                    _temporal_server: temporal_server,
                    _pg_container: pg_container,
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

// ---------- Config & fixture helpers ----------

/// Create a unique Postgres schema for test isolation.
fn create_test_schema(infra: &SharedInfra) -> String {
    let schema = format!("t_{}", uuid::Uuid::new_v4().simple());
    let output = std::process::Command::new("psql")
        .arg("-h")
        .arg(&infra.pg_host)
        .arg("-p")
        .arg(infra.pg_port.to_string())
        .arg("-U")
        .arg(&infra.pg_user)
        .arg("-d")
        .arg(&infra.pg_database)
        .arg("-c")
        .arg(format!("CREATE SCHEMA \"{schema}\""))
        .env("PGPASSWORD", &infra.pg_password)
        .output()
        .expect("psql should be available");
    assert!(
        output.status.success(),
        "CREATE SCHEMA failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    schema
}

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR")))
}

/// Build a `DbtTemporalConfig` pointing at the given fixture directory.
///
/// Writes profiles.yml with real Postgres connection params from the testcontainer.
pub fn test_config(infra: &SharedInfra, fixture_dir: &Path) -> Result<DbtTemporalConfig> {
    let schema = create_test_schema(infra);
    let profiles_dir = std::env::temp_dir().join(format!("dbtt-profiles-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&profiles_dir).context("creating profiles dir")?;

    let profiles_yml = format!(
        r#"waffle_hut:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{}"
      port: {}
      user: "{}"
      password: "{}"
      dbname: "{}"
      schema: "{schema}"
      threads: 1
"#,
        infra.pg_host, infra.pg_port, infra.pg_user, infra.pg_password, infra.pg_database,
    );
    std::fs::write(profiles_dir.join("profiles.yml"), profiles_yml)
        .context("writing profiles.yml")?;

    let artifact_dir =
        std::env::temp_dir().join(format!("dbtt-artifacts-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&artifact_dir).ok();

    Ok(DbtTemporalConfig {
        temporal_address: format!("http://{}", infra.temporal_addr),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("test-{}", uuid::Uuid::new_v4()),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![fixture_dir.to_string_lossy().to_string()],
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
            max_concurrent_activities: 4,
            max_concurrent_local_activities: 4,
        },
        sticky_queue_timeout_secs: 10,
        nonsticky_to_sticky_poll_ratio: 0.2,
        max_worker_activities_per_second: None,
        max_task_queue_activities_per_second: None,
        graceful_shutdown_secs: None,
        max_cached_workflows: 1000,
    })
}

/// Build a `DbtTemporalConfig` whose profiles.yml uses `env_var()` with defaults
/// pointing at the testcontainer.
pub fn test_config_env_var_profile(
    infra: &SharedInfra,
    fixture_dir: &Path,
) -> Result<DbtTemporalConfig> {
    let schema = create_test_schema(infra);
    let profiles_dir = std::env::temp_dir().join(format!("dbtt-profiles-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&profiles_dir).context("creating profiles dir")?;

    let profiles_yml = format!(
        r#"waffle_hut:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{{{ env_var('DB_HOST', '{}') }}}}"
      port: "{{{{ env_var('DB_PORT', '{}') }}}}"
      user: "{{{{ env_var('DB_USER', '{}') }}}}"
      password: "{{{{ env_var('DB_PASSWORD', '{}') }}}}"
      dbname: "{{{{ env_var('DB_NAME', '{}') }}}}"
      schema: "{{{{ env_var('DB_SCHEMA', '{schema}') }}}}"
      threads: 1
"#,
        infra.pg_host, infra.pg_port, infra.pg_user, infra.pg_password, infra.pg_database,
    );
    std::fs::write(profiles_dir.join("profiles.yml"), profiles_yml)
        .context("writing profiles.yml")?;

    let artifact_dir =
        std::env::temp_dir().join(format!("dbtt-artifacts-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&artifact_dir).ok();

    Ok(DbtTemporalConfig {
        temporal_address: format!("http://{}", infra.temporal_addr),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("test-{}", uuid::Uuid::new_v4()),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: vec![fixture_dir.to_string_lossy().to_string()],
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
            max_concurrent_activities: 4,
            max_concurrent_local_activities: 4,
        },
        sticky_queue_timeout_secs: 10,
        nonsticky_to_sticky_poll_ratio: 0.2,
        max_worker_activities_per_second: None,
        max_task_queue_activities_per_second: None,
        graceful_shutdown_secs: None,
        max_cached_workflows: 1000,
    })
}

/// Build a `DbtTemporalConfig` that loads multiple project directories.
pub fn test_config_multi_project(
    infra: &SharedInfra,
    fixture_dirs: &[&Path],
    profile_names: &[&str],
) -> Result<DbtTemporalConfig> {
    let schema = create_test_schema(infra);
    let profiles_dir = std::env::temp_dir().join(format!("dbtt-profiles-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&profiles_dir).context("creating profiles dir")?;

    let mut profiles_yml = String::new();
    for profile_name in profile_names {
        profiles_yml.push_str(&format!(
            r#"{profile_name}:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{}"
      port: {}
      user: "{}"
      password: "{}"
      dbname: "{}"
      schema: "{schema}"
      threads: 1
"#,
            infra.pg_host, infra.pg_port, infra.pg_user, infra.pg_password, infra.pg_database,
        ));
    }
    std::fs::write(profiles_dir.join("profiles.yml"), profiles_yml)
        .context("writing profiles.yml")?;

    let artifact_dir =
        std::env::temp_dir().join(format!("dbtt-artifacts-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&artifact_dir).ok();

    Ok(DbtTemporalConfig {
        temporal_address: format!("http://{}", infra.temporal_addr),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("test-{}", uuid::Uuid::new_v4()),
        temporal_api_key: None,
        temporal_tls_cert: None,
        temporal_tls_key: None,
        dbt_project_dirs: fixture_dirs
            .iter()
            .map(|d| d.to_string_lossy().to_string())
            .collect(),
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
            max_concurrent_activities: 4,
            max_concurrent_local_activities: 4,
        },
        sticky_queue_timeout_secs: 10,
        nonsticky_to_sticky_poll_ratio: 0.2,
        max_worker_activities_per_second: None,
        max_task_queue_activities_per_second: None,
        graceful_shutdown_secs: None,
        max_cached_workflows: 1000,
    })
}

pub fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target_path)?;
        } else {
            std::fs::copy(entry.path(), &target_path)?;
        }
    }
    Ok(())
}

/// Copy a fixture to a temp directory for isolated modification.
pub fn copy_fixture(name: &str) -> Result<PathBuf> {
    let src = fixture_path(name);
    let dst = std::env::temp_dir().join(format!("dbtt-fixture-{}", uuid::Uuid::new_v4()));
    copy_dir_recursive(&src, &dst).context("copying fixture")?;
    Ok(dst)
}

// ---------- Client helpers ----------

/// Create a Temporal Client connected to the given address.
pub async fn connect_client(temporal_addr: &str) -> Result<Client> {
    let url = url::Url::parse(&format!("http://{temporal_addr}"))
        .context("parsing temporal address as URL")?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal server")?;
    Client::new(connection, ClientOptions::new("default").build())
        .map_err(|e| anyhow::anyhow!("creating Temporal client: {e}"))
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

pub fn make_input_with_env(
    command: &str,
    project: Option<&str>,
    env: std::collections::BTreeMap<String, String>,
) -> DbtRunInput {
    DbtRunInput {
        project: project.map(String::from),
        command: command.to_string(),
        select: None,
        exclude: None,
        vars: std::collections::BTreeMap::default(),
        target: None,
        full_refresh: false,
        fail_fast: true,
        hooks: None,
        env,
    }
}

/// Build env overrides that point at the shared Postgres testcontainer.
pub fn pg_env(infra: &SharedInfra) -> std::collections::BTreeMap<String, String> {
    let mut env = std::collections::BTreeMap::new();
    env.insert("DB_HOST".to_string(), infra.pg_host.clone());
    env.insert("DB_PORT".to_string(), infra.pg_port.to_string());
    env.insert("DB_USER".to_string(), infra.pg_user.clone());
    env.insert("DB_PASSWORD".to_string(), infra.pg_password.clone());
    env.insert("DB_NAME".to_string(), infra.pg_database.clone());
    env
}

pub struct WorkflowRun {
    pub output: DbtRunOutput,
    pub workflow_id: String,
    pub run_id: String,
}

pub async fn run_dbt_workflow(
    client: &Client,
    task_queue: &str,
    input: DbtRunInput,
) -> Result<WorkflowRun> {
    let workflow_id = format!("test-{}", uuid::Uuid::new_v4());
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

/// Describe a completed workflow and return its memo and search attributes.
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

/// Extract a JSON-encoded string value from a Temporal Payload.
pub fn payload_json_string(
    payload: &temporalio_common::protos::temporal::api::common::v1::Payload,
) -> Option<String> {
    serde_json::from_slice::<String>(&payload.data).ok()
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

/// Run a workflow that is expected to fail and extract node status from the memo.
pub async fn run_dbt_workflow_expect_failure(
    client: &mut Client,
    task_queue: &str,
    input: DbtRunInput,
) -> Result<dbt_temporal::types::NodeStatusTree> {
    let workflow_id = format!("test-{}", uuid::Uuid::new_v4());
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

    let desc = describe_workflow(client, &workflow_id, &run_id).await?;
    let memo = desc
        .workflow_execution_info
        .as_ref()
        .and_then(|info| info.memo.as_ref())
        .context("no memo on workflow")?;

    let node_status_payload = memo
        .fields
        .get("node_status")
        .context("no node_status in memo")?;

    let node_status: dbt_temporal::types::NodeStatusTree =
        serde_json::from_slice(&node_status_payload.data)
            .context("deserializing node_status from memo")?;

    Ok(node_status)
}
