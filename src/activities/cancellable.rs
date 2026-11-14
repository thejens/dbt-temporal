//! Run blocking warehouse work to completion while heartbeating, stopping it
//! cooperatively on cancellation.
//!
//! Activity work runs on the dbt pool's blocking threads. Dropping the future
//! that awaits such a thread does not stop it: the SQL already sent keeps
//! running against the warehouse with nothing waiting for it. So on
//! cancellation the work is told to stop through its cancellation token and is
//! then *joined*, never dropped.

use std::convert::Infallible;
use std::future::Future;

use dbt_common::cancellation::CancellationTokenSource;
use temporalio_sdk::activities::ActivityContext;

use super::heartbeat;

/// Drive `work` to completion, signalling `source` once `cancel_requested`
/// resolves, while polling `heartbeat` the whole time.
///
/// Returns the work's output and whether cancellation was requested before it
/// finished. After the request the work is still polled until it returns, so a
/// caller observes the warehouse work actually stopped.
///
/// `work` borrows a token from `source`; the caller keeps the source alive for
/// the call because the adapter engine holds only a `Weak` reference to it.
///
/// All three futures are pinned once, outside the loop: recreating the
/// heartbeat per iteration would restart its interval, and `cancel_requested`
/// must not be polled again after it completes (guarded by `cancelled`).
pub async fn run_cancellable<W, C, H>(
    work: W,
    cancel_requested: C,
    heartbeat: H,
    source: &CancellationTokenSource,
) -> (W::Output, bool)
where
    W: Future,
    C: Future<Output = ()>,
    H: Future<Output = Infallible>,
{
    tokio::pin!(work, cancel_requested, heartbeat);

    let mut cancelled = false;
    let output = loop {
        tokio::select! {
            output = &mut work => break output,
            () = &mut cancel_requested, if !cancelled => {
                cancelled = true;
                source.cancel();
            }
            // Never resolves — keeps the UI's last-heartbeat fresh and lets the
            // server's heartbeat_timeout reschedule on a fresh worker if this
            // one dies. The work runs on the dbt pool, so this task is free to
            // keep heartbeating, including while cancelled work winds down.
            never = &mut heartbeat => match never {},
        }
    };
    (output, cancelled)
}

/// [`run_cancellable`] bound to a Temporal activity.
///
/// Cancellation is the activity's and heartbeats go to its context. `on_cancel`
/// runs once, when the cancellation arrives, so the caller can log with its own
/// identifiers.
pub async fn run_in_activity<W: Future>(
    ctx: &ActivityContext,
    source: &CancellationTokenSource,
    on_cancel: impl FnOnce(),
    work: W,
) -> (W::Output, bool) {
    let cancel_requested = async {
        ctx.cancelled().await;
        on_cancel();
    };
    run_cancellable(work, cancel_requested, heartbeat::heartbeat_loop(ctx), source).await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use dbt_common::cancellation::CancellationToken;

    use super::*;

    /// Work that finishes only once its token is cancelled, like a query
    /// parked in the warehouse until told to stop. The token is taken before
    /// the work is first polled, as in production, where the activity hands the
    /// token to the work up front: a token minted after the source was
    /// cancelled would never observe it.
    async fn wait_for_cancel(token: CancellationToken, finished: &AtomicBool) -> &'static str {
        while !token.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // The helper must still be polling us here, not have dropped the future.
        tokio::time::sleep(Duration::from_millis(20)).await;
        finished.store(true, Ordering::SeqCst);
        "stopped"
    }

    #[tokio::test]
    async fn cancellation_signals_the_token_and_joins_the_work() {
        let source = CancellationTokenSource::new();
        let finished = AtomicBool::new(false);
        let cancel_after = async { tokio::time::sleep(Duration::from_millis(20)).await };

        let (output, cancelled) = run_cancellable(
            wait_for_cancel(source.token(), &finished),
            cancel_after,
            std::future::pending(),
            &source,
        )
        .await;

        assert!(cancelled);
        assert_eq!(output, "stopped");
        assert!(finished.load(Ordering::SeqCst), "work must run to its own end");
    }

    #[tokio::test]
    async fn work_finishing_first_reports_not_cancelled() {
        let source = CancellationTokenSource::new();

        let (output, cancelled) =
            run_cancellable(async { 7 }, std::future::pending(), std::future::pending(), &source)
                .await;

        assert_eq!(output, 7);
        assert!(!cancelled);
        assert!(!source.token().is_cancelled());
    }

    #[tokio::test]
    async fn heartbeat_keeps_running_while_cancelled_work_winds_down() {
        let source = CancellationTokenSource::new();
        let finished = AtomicBool::new(false);
        let beats = std::sync::atomic::AtomicU32::new(0);
        let heartbeat = async {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                beats.fetch_add(1, Ordering::SeqCst);
            }
        };

        let (_, cancelled) = run_cancellable(
            wait_for_cancel(source.token(), &finished),
            async {},
            heartbeat,
            &source,
        )
        .await;

        assert!(cancelled);
        assert!(beats.load(Ordering::SeqCst) >= 2, "heartbeat stalled during wind-down");
    }
}
