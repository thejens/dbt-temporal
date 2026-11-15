use super::{MAX_ACTIVITIES_IN_FLIGHT, should_continue_as_new, take_startable};
use std::collections::VecDeque;

/// A wide level must not put one command per node into a single workflow
/// task: worker concurrency bounds what executes, not what is scheduled,
/// and the server has its own limits on incomplete activities.
#[test]
fn a_level_wider_than_the_window_starts_only_what_fits() {
    let mut pending: VecDeque<usize> = (0..1_200).collect();

    let first = take_startable(&mut pending, 0);
    assert_eq!(first.len(), MAX_ACTIVITIES_IN_FLIGHT);
    assert_eq!(first[0], 0, "the level's own order is preserved");
    assert_eq!(pending.len(), 1_200 - MAX_ACTIVITIES_IN_FLIGHT);
}

#[test]
fn a_full_window_starts_nothing_and_one_completion_starts_one() {
    let mut pending: VecDeque<usize> = (0..10).collect();

    assert_eq!(take_startable(&mut pending, MAX_ACTIVITIES_IN_FLIGHT), Vec::<usize>::new());
    assert_eq!(pending.len(), 10, "nothing was taken");

    let next = take_startable(&mut pending, MAX_ACTIVITIES_IN_FLIGHT - 1);
    assert_eq!(next, vec![0], "one slot freed starts exactly one node");
}

/// A level narrower than the window is started in one go, in the level's own
/// order, so replay sees the same commands in the same sequence.
#[test]
fn a_narrow_level_is_started_in_one_go() {
    let mut pending: VecDeque<usize> = (0..7).collect();
    assert_eq!(take_startable(&mut pending, 0), (0..7).collect::<Vec<_>>());
    assert!(pending.is_empty());
}

// --- continue-as-new policy ---

#[test]
fn continues_only_when_the_server_suggests_it() {
    assert!(should_continue_as_new(3, true, true));
    assert!(
        !should_continue_as_new(3, true, false),
        "never continue while history is comfortable"
    );
}

/// Without an artifact store there is nowhere to spill state, so the run
/// pushes on and accepts the history growth rather than failing.
#[test]
fn never_continues_without_somewhere_to_spill_state() {
    assert!(!should_continue_as_new(3, false, true));
}

/// A run already over the threshold before executing anything would
/// otherwise continue forever without making progress. This holds for a
/// *resumed* segment too, which starts at a non-zero absolute level — the
/// count is per-segment for exactly that reason.
#[test]
fn never_continues_before_the_first_level_completes() {
    assert!(!should_continue_as_new(0, true, true));
    assert!(should_continue_as_new(1, true, true));
}
