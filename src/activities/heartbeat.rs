//! Periodic heartbeat ticker for long-running activities.
//!
//! Used as a `tokio::select!` arm alongside the actual activity work and the
//! cancellation future. The Temporal server interprets a missed heartbeat
//! (relative to the workflow-side `heartbeat_timeout`) as a dead worker and
//! reschedules the activity on a healthy one. The ticker also keeps the
//! "Last Heartbeat" field current in the Temporal UI so an in-progress
//! activity reads as alive rather than stalled.

use std::convert::Infallible;
use std::time::Duration;

use temporalio_sdk::activities::ActivityContext;
use tokio::time::MissedTickBehavior;

/// Default heartbeat interval. The workflow-side `heartbeat_timeout` should be
/// at least ~2x this so a single missed tick does not trip a false timeout.
pub const INTERVAL: Duration = Duration::from_secs(30);

/// Records empty heartbeats forever at `INTERVAL`. Never returns — intended
/// to be one branch of a `tokio::select!` whose other branches drive the real
/// activity work and cancellation.
///
/// The first tick fires immediately, so an activity that begins inside this
/// loop emits a heartbeat right away (replacing any explicit "starting"
/// heartbeat the caller would otherwise need).
///
/// `MissedTickBehavior::Skip` collapses bursts: if the executor was blocked
/// for several intervals, we emit one heartbeat on resume instead of catching
/// up — the server only cares that *some* heartbeat arrived recently.
pub async fn heartbeat_loop(ctx: &ActivityContext) -> Infallible {
    let mut ticker = tokio::time::interval(INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        ctx.record_heartbeat(vec![]);
    }
}
