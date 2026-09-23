//! The thread pool all dbt Jinja and warehouse work runs on.
//!
//! dbt keeps warehouse connections in thread-locals of its own blocking pool
//! (`dbt_runtime`), and that is what bounds how much work reaches the warehouse
//! at once: a connection only exists on a pool worker, so the pool's size is the
//! connection limit. `AdbcEngine::new_connection` asserts it is running on one of
//! those workers — in a debug build, opening a connection anywhere else panics.
//! A tokio blocking thread does not count.
//!
//! The assertion is debug-only, so a release worker would run without it; but a
//! panic inside an activity is retried indefinitely rather than reported, so a
//! missed call site shows up as a hung workflow in any debug build. Everything
//! that renders dbt Jinja or touches an adapter goes through [`run`].
//!
//! One pool per process, as in dbt itself. The worker sizes it with
//! [`set_capacity`] to match its activity slots, so an activity that holds a slot
//! never also waits for a thread.

use std::num::NonZeroUsize;
use std::sync::OnceLock;

/// dbt's own stack size for pool threads. Jinja recursion through nested macro
/// calls runs deep, and a thread that overflows takes the whole process with it.
const STACK_SIZE: usize = 8 * 1024 * 1024;

static POOL: OnceLock<dbt_runtime::Runtime> = OnceLock::new();

fn pool() -> &'static dbt_runtime::Runtime {
    POOL.get_or_init(|| {
        dbt_runtime::builder::Builder::new()
            .thread_name("dbt-worker")
            .thread_stack_size(STACK_SIZE)
            .build()
    })
}

/// Cap how many pool threads — and therefore warehouse connections — run at
/// once. Threads start on demand and exit when idle, so a generous cap costs
/// nothing until the work arrives.
pub fn set_capacity(threads: NonZeroUsize) {
    pool().handle().set_max_parallelism(threads);
}

/// Run `work` on the dbt pool and wait for it without blocking the async task.
///
/// The caller's span is re-entered on the pool thread: dbt's telemetry data
/// layer asserts that every dbt span descends from an `Invocation` root, and a
/// pool thread starts with no current span of its own.
pub async fn run<F, R>(work: F) -> anyhow::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = tracing::Span::current();
    pool()
        .handle()
        .spawn_blocking(move || {
            let _entered = span.enter();
            work()
        })
        .await
        .map_err(|e| anyhow::anyhow!("dbt pool task failed: {e}"))
}
