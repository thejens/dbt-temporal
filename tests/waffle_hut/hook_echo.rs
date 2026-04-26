//! Minimal echo workflow used to drive `dbt_temporal::hooks::execute_hooks`
//! end-to-end in lifecycle-hook integration tests.
//!
//! `HookEchoWorkflow` accepts an arbitrary JSON value as input and either
//! returns it verbatim or fails based on a sentinel field:
//!
//! - `{"error": "msg"}` → workflow terminates with `Err(...)`.
//! - any other value → workflow returns that value verbatim.
//!
//! The execute_hooks loop reads the returned value as the hook's completion
//! payload, so by setting `HookConfig.input` to e.g. `{"skip": true,
//! "reason": "test"}`, a test can drive every branch of
//! `process_pre_run_completion` / `record_hook_error` without mocking
//! `WorkflowContext`.

#![allow(unreachable_pub, missing_debug_implementations)]

use anyhow::Result;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{Worker, WorkerOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};

#[workflow]
pub struct HookEchoWorkflow {
    input: serde_json::Value,
}

#[workflow_methods]
impl HookEchoWorkflow {
    #[init]
    #[allow(clippy::missing_const_for_fn)] // #[init] requires non-const.
    fn new(_ctx: &WorkflowContextView, input: serde_json::Value) -> Self {
        Self { input }
    }

    #[run(name = "hook_echo")]
    #[allow(
        clippy::needless_pass_by_ref_mut, // #[run] macro requires &mut self.
        clippy::future_not_send,          // WorkflowContext uses Rc internally.
        clippy::unused_async              // #[run] macro requires async fn.
    )]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<serde_json::Value> {
        let input = ctx.state(|s| s.input.clone());
        if let Some(msg) = input.get("error").and_then(|v| v.as_str()) {
            return Err(temporalio_sdk::WorkflowTermination::failed(anyhow::anyhow!(
                "hook echo error: {msg}"
            )));
        }
        Ok(input)
    }
}

/// Build a Temporal worker with `HookEchoWorkflow` registered on the given
/// task queue. Uses its own `CoreRuntime` (multiple `new_assume_tokio` calls
/// are safe — they share the ambient tokio runtime).
pub async fn build_hook_worker(temporal_addr: &str, task_queue: &str) -> Result<Worker> {
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .map_err(|e| anyhow::anyhow!("building runtime options: {e}"))?,
    )?;

    let url = url::Url::parse(&format!("http://{temporal_addr}"))?;
    let connection = Connection::connect(ConnectionOptions::new(url).build()).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())
        .map_err(|e| anyhow::anyhow!("creating Temporal client: {e}"))?;

    let opts = WorkerOptions::new(task_queue)
        .register_workflow::<HookEchoWorkflow>()
        .build();

    Worker::new(&runtime, client, opts).map_err(|e| anyhow::anyhow!("creating worker: {e}"))
}
