//! Skips, `on_error` and fail-fast.

use std::collections::BTreeSet;

use dbt_schemas::schemas::common::OnError;

use super::fixtures::*;
use crate::types::NodeStatus;
use crate::workflow::scheduler::{Scheduler, SchedulerError, SkipReason, Step};

#[test]
fn a_failure_skips_exactly_the_transitive_dependents() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("bad", &[]),
        ("ok", &[]),
        ("child", &["bad"]),
        ("grandchild", &["child"]),
        ("sibling", &["ok"]),
        ("mixed", &["ok", "grandchild"]),
    ]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("ok", NodeStatus::Success)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["sibling"], "unrelated work still runs");
    assert_eq!(
        skips(&steps),
        vec![
            ("child", SkipReason::UpstreamFailed),
            ("grandchild", SkipReason::UpstreamFailed),
            ("mixed", SkipReason::UpstreamFailed),
        ]
    );
    assert!(s.had_failure());
    assert_eq!(
        s.failed_nodes(),
        ["bad", "child", "grandchild", "mixed"]
            .map(String::from)
            .into()
    );
    s.on_complete("sibling", NodeStatus::Success)?;
    assert!(s.is_finished());
    Ok(())
}

/// A node that waits on one failed and one still-running dependency is skipped
/// once the running one ends, and not started in between.
#[test]
fn a_blocked_node_waits_for_all_dependencies_before_skipping() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("slow", &[]), ("join", &["bad", "slow"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    s.on_complete("slow", NodeStatus::Success)?;
    assert_eq!(skips(&s.advance(WIDE, false)), vec![("join", SkipReason::UpstreamFailed)]);
    Ok(())
}

#[test]
fn on_error_continue_lets_dependents_run() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[
        ("soft", &[]),
        ("child", &["soft"]),
        ("hard", &[]),
        ("hchild", &["hard"]),
    ]);
    by_id.entry("soft".to_string()).or_default().on_error = Some(OnError::Continue);
    let mut s = scheduler(&by_id)?;
    s.advance(WIDE, false);
    s.on_complete("soft", NodeStatus::Error)?;
    s.on_complete("hard", NodeStatus::Error)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(starts(&steps), vec!["child"]);
    assert_eq!(skips(&steps), vec![("hchild", SkipReason::UpstreamFailed)]);
    assert!(s.had_failure(), "a tolerated failure still counts for fail-fast");
    Ok(())
}

/// A model with `on_error: continue` that is itself skipped for an upstream
/// failure does not exempt its dependents: the failure upstream marks every
/// descendant, however many of them tolerate their own errors.
#[test]
fn a_skipped_continue_node_still_blocks_its_dependents() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[
        ("bad", &[]),
        ("soft", &["bad"]),
        ("after", &["soft"]),
        ("after_after", &["after"]),
        ("unrelated", &[]),
        ("unrelated_child", &["unrelated"]),
    ]);
    by_id.entry("soft".to_string()).or_default().on_error = Some(OnError::Continue);
    by_id.entry("after".to_string()).or_default().on_error = Some(OnError::Continue);
    let mut s = scheduler(&by_id)?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("unrelated", NodeStatus::Success)?;

    let steps = s.advance(WIDE, false);
    assert_eq!(
        skips(&steps),
        vec![
            ("soft", SkipReason::UpstreamFailed),
            ("after", SkipReason::UpstreamFailed),
            ("after_after", SkipReason::UpstreamFailed),
        ]
    );
    assert_eq!(starts(&steps), vec!["unrelated_child"]);
    s.on_complete("unrelated_child", NodeStatus::Success)?;
    assert!(s.is_finished());
    Ok(())
}

/// The same chain resumed from a checkpoint: the carried statuses say which
/// failed nodes were skipped, which is all the rule needs.
#[test]
fn a_carried_skipped_continue_node_still_blocks_its_dependents() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("bad", &[]), ("soft", &["bad"]), ("after", &["soft"])]);
    by_id.entry("soft".to_string()).or_default().on_error = Some(OnError::Continue);
    let plan = build_plan(&by_id);
    let mut tree = pending_tree(&plan);
    tree.nodes.insert("bad".to_string(), NodeStatus::Error);
    tree.nodes.insert("soft".to_string(), NodeStatus::Skipped);
    let failed: BTreeSet<String> = ["bad", "soft"].map(String::from).into();
    let mut s = Scheduler::new(&plan, &tree, &failed, true)?;

    assert_eq!(skips(&s.advance(WIDE, false)), vec![("after", SkipReason::UpstreamFailed)]);
    Ok(())
}

#[test]
fn warn_and_activity_skipped_nodes_do_not_block_dependents() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("w", &[]), ("sk", &[]), ("after", &["w", "sk"])]))?;
    s.advance(WIDE, false);
    s.on_complete("w", NodeStatus::Warn)?;
    s.on_complete("sk", NodeStatus::Skipped)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["after"]);
    assert!(!s.had_failure());
    Ok(())
}

#[test]
fn fail_fast_skips_every_unstarted_node_and_lets_running_ones_finish() -> Result<(), SchedulerError>
{
    let mut s = scheduler(&specs(&[
        ("bad", &[]),
        ("running", &[]),
        ("child", &["bad"]),
        ("unrelated", &["running"]),
        ("late", &["unrelated"]),
    ]))?;
    s.advance(2, true);
    s.on_complete("bad", NodeStatus::Error)?;

    let steps = s.advance(WIDE, true);
    assert_eq!(starts(&steps), Vec::<&str>::new(), "nothing starts after a failure");
    assert_eq!(
        skips(&steps),
        vec![
            ("child", SkipReason::FailFast),
            ("unrelated", SkipReason::FailFast),
            ("late", SkipReason::FailFast),
        ]
    );
    assert_eq!(s.terminal_status("running"), None, "an in-flight node is left to finish");
    s.on_complete("running", NodeStatus::Success)?;
    assert!(s.is_finished());
    assert_eq!(s.failed_nodes(), ["bad".to_string()].into(), "fail-fast skips are not failures");
    Ok(())
}

#[test]
fn fail_fast_off_keeps_scheduling_past_a_failure() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("other", &[]), ("next", &["other"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("other", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["next"]);
    Ok(())
}

/// `set_fail_fast` can flip the setting mid-run; the next decision honours it.
#[test]
fn enabling_fail_fast_after_a_failure_takes_effect_at_the_next_advance()
-> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("bad", &[]), ("hold", &[]), ("next", &["hold"])]))?;
    s.advance(WIDE, false);
    s.on_complete("bad", NodeStatus::Error)?;
    s.on_complete("hold", NodeStatus::Success)?;
    assert_eq!(skips(&s.advance(WIDE, true)), vec![("next", SkipReason::FailFast)]);
    Ok(())
}
