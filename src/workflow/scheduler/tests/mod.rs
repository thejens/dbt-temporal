//! Scheduler tests, by what they pin: readiness, failure handling, resume and
//! cancellation, and generated DAGs checked against the level loop.

mod cancel_and_resume;
mod failures;
mod fixtures;
mod generated;
mod readiness;
