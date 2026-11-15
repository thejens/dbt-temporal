//! Fixtures shared by the hook-context tests.

use std::collections::BTreeMap;

use crate::types::{NodeExecutionResult, NodeStatus};

pub(super) fn mk_result(
    unique_id: &str,
    status: NodeStatus,
    failures: Option<i64>,
) -> NodeExecutionResult {
    NodeExecutionResult {
        unique_id: unique_id.to_string(),
        status,
        execution_time: 1.5,
        message: Some(format!("{status:?} message")),
        adapter_response: BTreeMap::new(),
        compiled_code: None,
        timing: vec![],
        failures,
        freshness: None,
        compiled_code_ref: None,
        relation_name: None,
    }
}
