//! The terminal `DbtStatus` search-attribute upsert as a run ends.

use std::collections::BTreeMap;

use temporalio_common::search_attributes::SearchAttributeUpdate;
use temporalio_sdk::{WorkflowContext, WorkflowTermination};

use crate::types::{ExecutionPlan, RunOutcome};

use super::super::DbtRunWorkflow;
use super::super::search_attributes::build_search_attribute_updates;

/// Build the single-entry `DbtStatus` update for the terminal upsert, or
/// `None` when `DbtStatus` isn't in the plan's (registration-filtered)
/// attributes — upserting an unregistered attribute would fail the workflow
/// task. Pure, so the gating and update shape are unit-testable without a
/// `WorkflowContext`.
fn terminal_status_updates(
    plan: &ExecutionPlan,
    status: RunOutcome,
) -> Option<Result<Vec<SearchAttributeUpdate>, WorkflowTermination>> {
    if !plan.search_attributes.contains_key("DbtStatus") {
        return None;
    }
    Some(build_search_attribute_updates(&BTreeMap::from([(
        "DbtStatus".to_string(),
        status.as_str().to_string(),
    )])))
}

/// Re-upsert the `DbtStatus` search attribute to a terminal outcome
/// (`passed` / `failed` / `skipped` / `cancelled`) as the run ends, so the
/// workflow list can be filtered by outcome. The initial `running` value is
/// set at plan time.
///
/// `plan` is `None` for a run that ended before it was planned; it never set
/// `running`, so there is nothing to replace.
pub fn upsert_terminal_status(
    ctx: &WorkflowContext<DbtRunWorkflow>,
    plan: Option<&ExecutionPlan>,
    status: RunOutcome,
) -> Result<(), WorkflowTermination> {
    if let Some(updates) = plan.and_then(|plan| terminal_status_updates(plan, status)) {
        ctx.upsert_search_attributes(updates?);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::test_support::empty_plan;
    use super::*;

    #[test]
    fn terminal_status_updates_none_when_dbtstatus_not_registered() {
        // empty_plan has no search attributes → DbtStatus is unregistered, so
        // the terminal upsert must be skipped (upserting it would fail the task).
        let plan = empty_plan();
        assert!(terminal_status_updates(&plan, RunOutcome::Passed).is_none());
    }

    #[test]
    fn terminal_status_updates_builds_dbtstatus_when_registered() {
        use temporalio_common::search_attributes::{SearchAttributeKey, SearchAttributes};

        let mut plan = empty_plan();
        plan.search_attributes
            .insert("DbtStatus".to_string(), "running".to_string());
        let updates = terminal_status_updates(&plan, RunOutcome::Failed)
            .expect("registered → Some")
            .expect("update builds");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].name(), "DbtStatus");

        // Round-trip through the typed collection to confirm the terminal
        // value replaces the "running" one written at plan time.
        let applied = SearchAttributes::new(updates);
        let key: SearchAttributeKey<String> = SearchAttributeKey::keyword("DbtStatus");
        assert_eq!(applied.get(&key).as_deref(), Some("failed"));
    }
}
