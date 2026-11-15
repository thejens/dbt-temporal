use std::collections::BTreeMap;

/// When the logical run began, and how long it has taken so far.
///
/// One value rather than two loose parameters: they are only meaningful
/// together, and a continuation restarts this execution's clock while the
/// artifacts still describe one run — so the pair travels from the workflow
/// into both the checkpoint and `run_results.json`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunClock {
    pub started_at: Option<std::time::SystemTime>,
    pub elapsed_secs: f64,
}

/// What the run resolved to, for the artifact work that has to describe it.
///
/// The clock and the connection travel together because both answer "which
/// run": how long it took, and which warehouse it reached. Artifact generation
/// needs the second for the same reason node execution does — a workflow that
/// overrides the target or the profile's env is not describing the warehouse
/// the worker started against.
#[derive(Debug, Clone, Copy)]
pub struct RunFacts<'a> {
    pub clock: RunClock,
    pub env: &'a BTreeMap<String, String>,
    pub target: Option<&'a str>,
}

/// Seconds between two deterministic workflow timestamps, or 0.0 if either
/// is missing (workflow_time() is None outside of an executing workflow).
///
/// Using `ctx.workflow_time()` instead of `Instant::now()` keeps the value
/// stable across history replay; the workflow file's own docstring forbids
/// wall-clock time for this reason.
pub fn elapsed_secs(
    start: Option<std::time::SystemTime>,
    end: Option<std::time::SystemTime>,
) -> f64 {
    start
        .zip(end)
        .and_then(|(s, e)| e.duration_since(s).ok())
        .map_or(0.0, |d| d.as_secs_f64())
}

#[cfg(test)]
// float_cmp: tests compare directly-assigned floats, not computed values.
// unwrap_used: unwrap is acceptable in tests for brevity.
#[allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;
    use std::time::Duration;

    // --- elapsed_secs ---

    #[test]
    fn elapsed_secs_returns_zero_when_either_endpoint_missing() {
        let now = std::time::SystemTime::now();
        assert!(elapsed_secs(None, Some(now)).abs() < f64::EPSILON);
        assert!(elapsed_secs(Some(now), None).abs() < f64::EPSILON);
        assert!(elapsed_secs(None, None).abs() < f64::EPSILON);
    }

    #[test]
    fn elapsed_secs_returns_difference_when_both_present() {
        let start = std::time::SystemTime::UNIX_EPOCH;
        let end = start + Duration::from_millis(2500);
        let secs = elapsed_secs(Some(start), Some(end));
        assert!((secs - 2.5).abs() < 1e-9);
    }

    #[test]
    fn elapsed_secs_returns_zero_when_end_before_start() {
        // duration_since returns Err when end < start; helper should yield 0.
        let later = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let earlier = std::time::SystemTime::UNIX_EPOCH;
        assert!(elapsed_secs(Some(later), Some(earlier)).abs() < f64::EPSILON);
    }
}
