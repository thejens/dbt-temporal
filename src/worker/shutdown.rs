//! Graceful shutdown on `SIGTERM` / `SIGINT`.
//!
//! In a container the worker is PID 1, and the kernel does not apply a
//! signal's default action to PID 1: without a handler, the `SIGTERM` an
//! orchestrator sends on scale-down or rollout is silently dropped, and the
//! process is `SIGKILL`ed when the grace period runs out — mid-activity, with
//! `WORKER_GRACEFUL_SHUTDOWN_SECS` never consulted. Handling the signal turns
//! it into the SDK's own shutdown: stop polling, let in-flight work drain.

use std::future::Future;

use anyhow::{Context, Result, anyhow};
use futures::{Stream, StreamExt};
use temporalio_sdk::Worker;
use tracing::{info, warn};

/// Run `worker` until it stops on its own or a signal shuts it down.
///
/// On the first signal the SDK stops polling, so no new activity is taken,
/// while those already running continue. They finish on their own unless
/// `WORKER_GRACEFUL_SHUTDOWN_SECS` elapses first, in which case the SDK
/// cancels them and reports them failed so the server retries them elsewhere.
#[allow(clippy::future_not_send)] // the SDK's `Worker` is !Send by design
pub async fn run(mut worker: Worker, signals: ShutdownSignals) -> Result<()> {
    let initiate_shutdown = worker.shutdown_handle();
    run_until_shutdown(
        async move { worker.run().await.context("worker exited with error") },
        signals.into_stream(),
        initiate_shutdown,
    )
    .await
}

/// The signal that asked the worker to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    Terminate,
    Interrupt,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Terminate => "SIGTERM",
            Self::Interrupt => "SIGINT",
        })
    }
}

/// Installed signal handlers.
///
/// Installing replaces the default action, so `SIGINT` stops killing the
/// process the moment this exists. Install it once the worker is about to run,
/// not before startup: a Ctrl-C during a long project load should still exit.
#[derive(Debug)]
pub struct ShutdownSignals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    pub fn install() -> Result<Self> {
        #[cfg(unix)]
        {
            use anyhow::Context;
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                terminate: signal(SignalKind::terminate()).context("installing SIGTERM handler")?,
                interrupt: signal(SignalKind::interrupt()).context("installing SIGINT handler")?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Every signal received, in order. Never ends.
    pub fn into_stream(self) -> impl Stream<Item = ShutdownSignal> {
        futures::stream::unfold(self, |mut signals| async move {
            let signal = signals.recv().await;
            Some((signal, signals))
        })
    }

    async fn recv(&mut self) -> ShutdownSignal {
        #[cfg(unix)]
        {
            // `recv` yields `None` only if the driver is gone; the pattern then
            // disables that branch instead of reporting a signal that never came.
            tokio::select! {
                Some(()) = self.terminate.recv() => ShutdownSignal::Terminate,
                Some(()) = self.interrupt.recv() => ShutdownSignal::Interrupt,
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
            ShutdownSignal::Interrupt
        }
    }
}

/// Drive the worker's `run` future to completion around shutdown signals.
///
/// The first signal calls `initiate_shutdown` and then keeps awaiting the
/// *same* `run` future: the SDK resolves it once in-flight activities finish
/// or its graceful-shutdown period cancels them, and dropping it instead would
/// abandon exactly the work the drain exists to finish. A second signal stops
/// waiting — the interactive "press Ctrl-C again" escape from a long drain.
pub async fn run_until_shutdown(
    run: impl Future<Output = Result<()>>,
    signals: impl Stream<Item = ShutdownSignal>,
    initiate_shutdown: impl FnOnce(),
) -> Result<()> {
    let mut run = std::pin::pin!(run);
    let mut signals = std::pin::pin!(signals);

    tokio::select! {
        result = &mut run => return result,
        Some(signal) = signals.next() => {
            info!(%signal, "shutdown requested: polling stops, in-flight work drains");
            initiate_shutdown();
        }
    }

    tokio::select! {
        result = &mut run => {
            info!("worker shut down");
            result
        }
        Some(signal) = signals.next() => {
            warn!(%signal, "second shutdown signal: exiting without waiting for in-flight work");
            Err(anyhow!("forced shutdown on a second {signal}"))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    /// A stand-in for the SDK worker: `run` resolves only once shutdown has
    /// been initiated, as the real one does.
    fn drained_by_shutdown() -> (impl Future<Output = Result<()>>, impl FnOnce(), Arc<AtomicUsize>)
    {
        let notify = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let run = {
            let notify = Arc::clone(&notify);
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        let initiate = {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                // `notify_one` stores a permit, so it lands even if `run` has
                // not reached `notified()` yet.
                notify.notify_one();
            }
        };
        (run, initiate, calls)
    }

    #[tokio::test]
    async fn a_run_that_ends_on_its_own_never_initiates_shutdown() {
        let calls = AtomicUsize::new(0);
        let result = run_until_shutdown(async { Ok(()) }, futures::stream::pending(), || {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_run_error_is_returned_as_is() {
        let err = run_until_shutdown(
            async { Err(anyhow!("poller failed")) },
            futures::stream::pending(),
            || {},
        )
        .await
        .expect_err("the run failed");

        assert_eq!(err.to_string(), "poller failed");
    }

    #[tokio::test]
    async fn a_signal_initiates_shutdown_once_and_waits_for_the_drain() {
        let (run, initiate, calls) = drained_by_shutdown();

        let result =
            run_until_shutdown(run, futures::stream::iter([ShutdownSignal::Terminate]), initiate)
                .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// The drain's own outcome is what the process reports: an error raised
    /// while finishing in-flight work must not be swallowed by the shutdown.
    #[tokio::test]
    async fn an_error_during_the_drain_is_returned() {
        let notify = Arc::new(Notify::new());
        let run = {
            let notify = Arc::clone(&notify);
            async move {
                notify.notified().await;
                Err(anyhow!("activity cancellation failed"))
            }
        };

        let err = run_until_shutdown(
            run,
            futures::stream::iter([ShutdownSignal::Terminate]),
            move || notify.notify_one(),
        )
        .await
        .expect_err("the drain failed");

        assert_eq!(err.to_string(), "activity cancellation failed");
    }

    #[tokio::test]
    async fn a_second_signal_stops_waiting_for_the_drain() {
        let calls = AtomicUsize::new(0);

        let err = run_until_shutdown(
            std::future::pending(),
            futures::stream::iter([ShutdownSignal::Terminate, ShutdownSignal::Interrupt]),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .expect_err("a second signal forces the exit");

        assert_eq!(err.to_string(), "forced shutdown on a second SIGINT");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "shutdown is initiated only once");
    }

    #[test]
    fn signals_display_by_their_conventional_names() {
        assert_eq!(ShutdownSignal::Terminate.to_string(), "SIGTERM");
        assert_eq!(ShutdownSignal::Interrupt.to_string(), "SIGINT");
    }

    /// End to end through the real handler: the process signals itself, and
    /// the stream must report it rather than the default action killing us.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_installed_handler_reports_a_real_sigterm() -> Result<()> {
        let mut signals = std::pin::pin!(ShutdownSignals::install()?.into_stream());

        let status = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()?;
        assert!(status.success(), "kill exited with {status}");

        let signal = tokio::time::timeout(std::time::Duration::from_secs(5), signals.next())
            .await
            .expect("SIGTERM was not delivered to the handler");
        assert_eq!(signal, Some(ShutdownSignal::Terminate));
        Ok(())
    }
}
