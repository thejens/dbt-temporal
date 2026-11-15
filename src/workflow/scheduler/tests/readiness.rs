//! Which nodes become ready, in what order, and under what cap.

use std::collections::{BTreeMap, BTreeSet};

use super::fixtures::*;
use crate::types::NodeStatus;
use crate::workflow::scheduler::{Scheduler, SchedulerError, SkipReason, Step};

#[test]
fn a_chain_starts_one_node_at_a_time() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]))?;

    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new(), "b waits for a");
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["b"]);
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["c"]);
    assert!(!s.is_finished());
    s.on_complete("c", NodeStatus::Success)?;
    assert!(s.is_finished());
    Ok(())
}

#[test]
fn a_diamond_joins_only_after_both_branches() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]))?;

    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["b", "c"]);
    s.on_complete("c", NodeStatus::Success)?;
    assert_eq!(s.advance(WIDE, false), Vec::<Step>::new(), "d still needs b");
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["d"]);
    Ok(())
}

#[test]
fn fan_out_starts_every_dependent_and_fan_in_waits_for_all() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("root", &[]),
        ("m1", &["root"]),
        ("m2", &["root"]),
        ("m3", &["root"]),
        ("sink", &["m1", "m2", "m3"]),
    ]))?;
    s.advance(WIDE, false);
    s.on_complete("root", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["m1", "m2", "m3"]);
    for m in ["m3", "m1"] {
        s.on_complete(m, NodeStatus::Success)?;
        assert_eq!(s.advance(WIDE, false), Vec::<Step>::new());
    }
    s.on_complete("m2", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["sink"]);
    Ok(())
}

/// The point of the scheduler: `fast_child` depends only on `fast`, so a slow
/// node in the same level must not hold it back.
#[test]
fn a_slow_node_does_not_block_unrelated_ready_nodes() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[
        ("slow", &[]),
        ("fast", &[]),
        ("slow_child", &["slow"]),
        ("fast_child", &["fast"]),
    ]))?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["fast", "slow"]);

    s.on_complete("fast", NodeStatus::Success)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["fast_child"]);
    assert_eq!(s.terminal_status("slow"), None, "slow is still running");
    Ok(())
}

#[test]
fn the_cap_is_respected_and_refilled_on_each_completion() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &[]), ("d", &[]), ("e", &[])]))?;
    let cap = 2;
    let mut running: Vec<String> = Vec::new();

    let first = s.advance(cap - running.len(), false);
    running.extend(starts(&first).iter().map(ToString::to_string));
    assert_eq!(running, vec!["a", "b"]);
    assert_eq!(s.advance(cap - running.len(), false), Vec::<Step>::new(), "window is full");

    for expected in ["c", "d", "e"] {
        let done = running.remove(0);
        s.on_complete(&done, NodeStatus::Success)?;
        let next = s.advance(cap - running.len(), false);
        assert_eq!(starts(&next), vec![expected], "one slot freed starts exactly one node");
        running.push(expected.to_string());
        assert!(running.len() <= cap);
    }
    Ok(())
}

#[test]
fn zero_free_slots_start_nothing_but_still_skip() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"]), ("c", &[])]))?;
    assert_eq!(s.advance(0, false), Vec::<Step>::new());
    s.advance(1, false);
    s.on_complete("a", NodeStatus::Error)?;
    assert_eq!(
        skips(&s.advance(0, false)),
        vec![("b", SkipReason::UpstreamFailed)],
        "a skip occupies no activity slot"
    );
    Ok(())
}

#[test]
fn ready_nodes_start_by_priority_then_plan_position() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("a", &[]), ("b", &[]), ("c", &[]), ("d", &[])]);
    by_id.entry("c".to_string()).or_default().priority = Some(1);
    by_id.entry("d".to_string()).or_default().priority = Some(3);
    by_id.entry("b".to_string()).or_default().priority = Some(3);
    let mut s = scheduler(&by_id)?;

    // c (1) first; b and d tie at 3 and fall back to plan order; a has no key.
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["c", "b", "d", "a"]);
    Ok(())
}

#[test]
fn a_priority_cap_picks_the_most_urgent_ready_nodes() -> Result<(), SchedulerError> {
    let mut by_id = specs(&[("a", &[]), ("b", &[]), ("c", &[])]);
    by_id.entry("c".to_string()).or_default().priority = Some(1);
    let mut s = scheduler(&by_id)?;
    assert_eq!(starts(&s.advance(1, false)), vec!["c"]);
    Ok(())
}

#[test]
fn an_empty_plan_is_finished() -> Result<(), SchedulerError> {
    let s = scheduler(&BTreeMap::new())?;
    assert!(s.is_finished());
    Ok(())
}

#[test]
fn edges_to_ids_outside_the_plan_are_ignored() -> Result<(), SchedulerError> {
    let mut plan = build_plan(&specs(&[("a", &["not_in_plan"])]));
    // The edge points outside the selection, so only `a` is in the levels.
    plan.levels = vec![vec!["a".to_string()]];
    let mut s = Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false)?;
    assert_eq!(starts(&s.advance(WIDE, false)), vec!["a"]);
    Ok(())
}

#[test]
fn a_dependency_cycle_is_rejected() {
    let by_id = specs(&[("a", &["b"]), ("b", &["a"]), ("c", &[])]);
    let mut plan = build_plan(&by_id);
    // The level computation rejects cycles; a plan whose edges disagree with
    // its levels is the case the scheduler has to catch itself.
    plan.levels = vec![vec!["a".to_string(), "b".to_string(), "c".to_string()]];
    let result = Scheduler::new(&plan, &pending_tree(&plan), &BTreeSet::new(), false);
    assert_eq!(result.err(), Some(SchedulerError::Cycle(2)));
}

#[test]
fn completing_a_node_that_is_not_running_is_an_error() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &["a"])]))?;
    assert_eq!(
        s.on_complete("a", NodeStatus::Success),
        Err(SchedulerError::NotRunning("a".to_string())),
        "ready but not started"
    );
    assert_eq!(
        s.on_complete("nope", NodeStatus::Success),
        Err(SchedulerError::NotRunning("nope".to_string()))
    );
    s.advance(WIDE, false);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(
        s.on_complete("a", NodeStatus::Success),
        Err(SchedulerError::NotRunning("a".to_string())),
        "a node completes once"
    );
    Ok(())
}

#[test]
fn completed_levels_counts_leading_finished_levels() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("slow", &[]), ("b", &["a"]), ("c", &["b"])]))?;
    assert_eq!(s.completed_levels(), 0);
    s.advance(WIDE, false);
    s.on_complete("a", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 0, "level 0 still has `slow`");
    s.on_complete("slow", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 1);
    s.advance(WIDE, false);
    s.on_complete("b", NodeStatus::Success)?;
    assert_eq!(s.completed_levels(), 2);
    Ok(())
}

#[test]
fn counts_track_running_and_terminal_states() -> Result<(), SchedulerError> {
    let mut s = scheduler(&specs(&[("a", &[]), ("b", &[]), ("c", &["a"]), ("d", &["b"])]))?;
    s.advance(WIDE, false);
    assert_eq!(s.counts().running, 2);
    s.on_complete("a", NodeStatus::Warn)?;
    s.on_complete("b", NodeStatus::Error)?;
    s.advance(WIDE, false);
    let counts = s.counts();
    assert_eq!((counts.running, counts.warn, counts.error, counts.skipped), (1, 1, 1, 1));
    Ok(())
}
