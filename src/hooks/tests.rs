use crate::hooks::*;
use crate::types::HookConfig;

fn make_hook(workflow_type: &str, timeout_secs: Option<u64>, fire_and_forget: bool) -> HookConfig {
    HookConfig {
        workflow_type: workflow_type.to_string(),
        task_queue: "q".to_string(),
        timeout_secs,
        on_error: HookErrorMode::default(),
        input: None,
        fire_and_forget,
    }
}

fn empty_pre_run_payload() -> HookPayload {
    HookPayload {
        event: HookEvent::PreRun,
        invocation_id: "inv".to_string(),
        input: crate::types::DbtRunInput {
            project: None,
            indirect_selection: None,
            resource_types: Vec::new(),
            exclude_resource_types: Vec::new(),
            resume_from: None,
            command: "build".to_string(),
            select: None,
            exclude: None,
            vars: std::collections::BTreeMap::new(),
            target: None,
            full_refresh: false,
            fail_fast: false,
            hooks: None,
            env: std::collections::BTreeMap::new(),
            defer_manifest_ref: None,
            event_time_start: None,
            event_time_end: None,
            retry_from: None,
            state_manifest_ref: None,
        },
        plan: None,
        output: None,
    }
}

mod child_workflows;
mod completion;
mod config;
