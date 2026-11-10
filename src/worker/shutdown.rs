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
use tokio::sync::oneshot;
use tracing::{info, warn};

/// Run `worker` until it stops on its own or a signal shuts it down.
///
/// On the first signal the SDK stops polling, so no new activity is taken,
/// while those already running continue. They finish on their own unless
/// `WORKER_GRACEFUL_SHUTDOWN_SECS` elapses first, in which case the SDK
/// cancels them and reports them failed so the server retries them elsewhere.
///
/// `run_until` rather than `run`: after the drain it finalizes the Core
/// worker, which unregisters it, so the server records a clean shutdown
/// instead of inferring one from heartbeats that stop arriving.
#[allow(clippy::future_not_send)] // the SDK's `Worker` is !Send by design
pub async fn run(worker: Worker, signals: ShutdownSignals) -> Result<()> {
    run_until_shutdown(
        |requested| async move {
            worker
                .run_until(requested)
                .await
                .context("worker exited with error")
        },
        signals.into_stream(),
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

/// Drive a worker run to completion around shutdown signals.
///
/// `run` receives a future that resolves on the first signal; the SDK's
/// `run_until` initiates shutdown when it does and then keeps driving the
/// same run until in-flight activities finish or its graceful-shutdown period
/// cancels them. Dropping the run instead would abandon exactly the work the
/// drain exists to finish. A second signal stops waiting — the interactive
/// "press Ctrl-C again" escape from a long drain.
pub async fn run_until_shutdown<F, Fut>(
    run: F,
    signals: impl Stream<Item = ShutdownSignal>,
) -> Result<()>
where
    F: FnOnce(ShutdownRequested) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let (request, requested) = oneshot::channel();
    // Held until the first signal: dropping the sender would resolve
    // `requested` and shut the worker down with no signal at all.
    let mut request = Some(request);
    let mut run = std::pin::pin!(run(ShutdownRequested(requested)));
    let mut signals = std::pin::pin!(signals);

    loop {
        tokio::select! {
            result = &mut run => {
                if request.is_none() {
                    info!("worker shut down");
                }
                return result;
            }
            Some(signal) = signals.next() => {
                let Some(request) = request.take() else {
                    warn!(%signal, "second shutdown signal: exiting without waiting for in-flight work");
                    return Err(anyhow!("forced shutdown on a second {signal}"));
                };
                info!(%signal, "shutdown requested: polling stops, in-flight work drains");
                // The receiver lives inside `run`, which is still pending.
                let _ = request.send(());
            }
        }
    }
}

/// Resolves when the first shutdown signal arrives.
#[derive(Debug)]
pub struct ShutdownRequested(oneshot::Receiver<()>);

impl Future for ShutdownRequested {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        // A dropped sender also resolves it; `run_until_shutdown` holds the
        // sender until the signal, so that only happens once nobody is waiting.
        std::pin::Pin::new(&mut self.0).poll(cx).map(|_| ())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// A stand-in for the SDK's `run_until`: initiates shutdown when
    /// `requested` resolves, then reports the drain's outcome. `requested`
    /// records whether shutdown was ever asked for.
    async fn drained_on_request(
        requested: ShutdownRequested,
        asked: &AtomicBool,
        drain: Result<()>,
    ) -> Result<()> {
        requested.await;
        asked.store(true, Ordering::SeqCst);
        drain
    }

    #[tokio::test]
    async fn a_run_that_ends_on_its_own_returns_without_a_signal() {
        let result = run_until_shutdown(|_| async { Ok(()) }, futures::stream::pending()).await;

        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn a_run_error_is_returned_as_is() {
        let err = run_until_shutdown(
            |_| async { Err(anyhow!("poller failed")) },
            futures::stream::pending(),
        )
        .await
        .expect_err("the run failed");

        assert_eq!(err.to_string(), "poller failed");
    }

    #[tokio::test]
    async fn without_a_signal_shutdown_is_never_requested() {
        let asked = AtomicBool::new(false);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            run_until_shutdown(
                |requested| drained_on_request(requested, &asked, Ok(())),
                futures::stream::pending(),
            ),
        )
        .await;

        assert!(result.is_err(), "the run must still be going");
        assert!(!asked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_signal_requests_shutdown_and_waits_for_the_drain() {
        let asked = AtomicBool::new(false);
        let result = run_until_shutdown(
            |requested| drained_on_request(requested, &asked, Ok(())),
            futures::stream::iter([ShutdownSignal::Terminate]),
        )
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert!(asked.load(Ordering::SeqCst));
    }

    /// The drain's own outcome is what the process reports: an error raised
    /// while finishing in-flight work must not be swallowed by the shutdown.
    #[tokio::test]
    async fn an_error_during_the_drain_is_returned() {
        let asked = AtomicBool::new(false);
        let err = run_until_shutdown(
            |requested| {
                drained_on_request(requested, &asked, Err(anyhow!("activity cancellation failed")))
            },
            futures::stream::iter([ShutdownSignal::Terminate]),
        )
        .await
        .expect_err("the drain failed");

        assert_eq!(err.to_string(), "activity cancellation failed");
    }

    #[tokio::test]
    async fn a_second_signal_stops_waiting_for_the_drain() {
        let asked = AtomicBool::new(false);
        let draining = tokio::sync::Notify::new();
        // The second signal arrives only once the drain is under way, as a
        // person pressing Ctrl-C again would; two signals ready at once would
        // let `select!` take the second before the run sees the first.
        let signals = futures::stream::iter([ShutdownSignal::Terminate]).chain(
            futures::stream::once(async {
                draining.notified().await;
                ShutdownSignal::Interrupt
            }),
        );
        let err = run_until_shutdown(
            |requested| async {
                requested.await;
                asked.store(true, Ordering::SeqCst);
                // `notify_one` stores a permit, so it lands even if the stream
                // has not reached `notified()` yet.
                draining.notify_one();
                std::future::pending().await
            },
            signals,
        )
        .await
        .expect_err("a second signal forces the exit");

        assert_eq!(err.to_string(), "forced shutdown on a second SIGINT");
        assert!(asked.load(Ordering::SeqCst), "the first signal requested shutdown");
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
