//! Cancellation, and resuming from a checkpoint.

use std::collections::BTreeSet;

use super::fixtures::*;
use crate::types::NodeStatus;
use crate::workflow::scheduler::{Scheduler, SchedulerError, SkipReason, Step};

#[test]
fn cancelling_closes_out_running_ready_and_waiting_nodes() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &["a"]), ("d", &["c"])]))?;
    s.advance(1, false);
    s.on_complete("a", NodeStatus::Success)?;
    s.advance(1, false); // starts b; c stays ready

    assert_eq!(s.cancel_unfinished(), vec!["b", "c", "d"]);
    assert!(s.is_finished());
    assert_eq!(s.terminal_status("a"), Some(NodeStatus::Success));
    assert_eq!(s.terminal_status("b"), Some(NodeStatus::Cancelled));
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    assert_eq!(s.cancel_unfinished(), Vec::<String>::new(), "idempotent");
    Ok(())
}

#[test]
fn resuming_skips_finished_nodes_and_honours_carried_failures() -> Result<(), SchedulerError> {
    let by_id = specs(&[
        ("done", &[]),
        ("bad", &[]),
        ("after_done", &["done"]),
        ("after_bad", &["bad"]),
        ("free", &[]),
    ]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("done".to_string(), NodeStatus::Success);
    tree.nodes.insert("bad".to_string(), NodeStatus::Error);
    let failed: BTreeSet<String> = ["bad".to_string(), "gone".to_string()].into();
    let mut s = Scheduler::new(&plan, &tree, &failed, true)?;

    assert!(s.had_failure());
    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["free", "after_done"]);
    assert_eq!(skips(&steps), vec![("after_bad", SkipReason::UpstreamFailed)]);
    assert!(s.failed_nodes().contains("gone"), "ids outside the plan are handed on");
    assert_eq!(s.counts().success, 1);
    assert_eq!(s.counts().error, 1);
    Ok(())
}

#[test]
fn resuming_with_fail_fast_and_a_carried_failure_skips_the_rest() -> Result<(), SchedulerError> {
    let by_id = specs(&[("bad", &[]), ("next", &["bad"]), ("other", &[])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("bad".to_string(), NodeStatus::Error);
    let mut s = Scheduler::new(&plan, &tree, &["bad".to_string()].into(), true)?;

    let steps = s.advance(WIDE, true);
    assert_eq!(starts(&steps), Vec::<&str>::new());
    assert_eq!(skips(&steps).len(), 2);
    assert!(s.is_finished());
    Ok(())
}

/// A checkpoint records `Running` for nothing, but a stale one must not be
/// trusted as finished.
#[test]
fn a_running_status_in_a_checkpoint_is_rerun() -> Result<(), SchedulerError> {
    let by_id = specs(&[("a", &[])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("a".to_string(), NodeStatus::Running);
    let mut s = Scheduler::new(&plan, &tree, &BTreeSet::new(), false)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    Ok(())
}

#[test]
fn a_fully_finished_resume_is_finished_immediately() -> Result<(), SchedulerError> {
    let by_id = specs(&[("a", &[]), ("b", &["a"])]);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    for status in tree.nodes.values_mut() {
        *status = NodeStatus::Success;
    }
    let s = Scheduler::new(&plan, &tree, &BTreeSet::new(), false)?;
    assert!(s.is_finished());
    assert_eq!(s.completed_levels(), 2);
    Ok(())
}
