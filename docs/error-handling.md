# Error Handling

| Error Type | Retryable? | Examples |
|-----------|-----------|----------|
| `Compilation` | No | Bad SQL, missing ref, Jinja render failure |
| `Configuration` | No | Missing profile, bad config, invalid selector |
| `Adapter` | Yes | Connection timeout, rate limit, transient DB error |
| `ArtifactStore` | Yes | Object-store 5xx or throttle while reading/writing run artifacts |
| `TestFailure` | No | A dbt test query returned failing rows (data won't change on retry) |
| `UnitTestFailure` | No | A unit test's output differed from its fixture |
| `StaleSource` | No | A source exceeded its `error_after` freshness threshold |
| `ProjectNotFound` | No | Worker doesn't have the requested project loaded |

`plan_project` and `store_artifacts` classify the same way `execute_node` does,
but their default for an *untyped* error is permanent — their own failures mean
a bad selector or a bug, not a blip. Only their object-store round-trips (the
`state_manifest_ref` / `retry_from` / `defer_manifest_ref` reads, and every
artifact write) are tagged `ArtifactStore` and retried. That matters most for
`store_artifacts`, which runs after every node has finished: without it a single
5xx would discard a completed run's results.

## Retry Configuration

The `execute_node` activity retries transient adapter errors with exponential backoff. Defaults can be overridden in `dbt_temporal.yml`:

```yaml
retry:
  max_attempts: 3            # total attempts (1 = no retries)
  initial_interval_secs: 5   # first backoff delay
  backoff_coefficient: 2.0   # multiplier for successive backoffs
  max_interval_secs: 60      # upper bound on backoff delay
  non_retryable_errors:       # regex patterns — matching adapter errors won't be retried
    - "permission denied"
    - "relation .* does not exist"
    - "access denied"
```

All fields are optional and default to the values shown above (except `non_retryable_errors` which defaults to empty). The `non_retryable_errors` patterns are matched against the full adapter error message — if any pattern matches, the error is treated as non-retryable regardless of its type.

## `on_error: continue`

A model configured with `{{ config(on_error='continue') }}` (or `on_error: continue` in `dbt_project.yml`/schema config) that fails still reports `Error`, but its failure doesn't block downstream nodes from running — they execute as if the failing model had never been in the graph. This mirrors dbt-core's own scheduling: only models can set it (a failed test/seed/snapshot always blocks its dependents), and `fail_fast` overrides it — if the workflow (or a `set_fail_fast` update) has fail-fast enabled, any failure still halts the remaining levels regardless of `on_error`.

## Activity Timeouts

Each activity's start-to-close and heartbeat timeouts are configurable in
`dbt_temporal.yml` under `timeouts:`. Defaults reproduce the values these were
previously hardcoded to:

```yaml
timeouts:
  node_secs: 3600              # execute_node start-to-close
  node_heartbeat_secs: 300     # execute_node heartbeat
  hook_secs: 300               # run_project_hooks start-to-close
  hook_heartbeat_secs: 120     # run_project_hooks heartbeat
  plan_secs: 300               # plan_project start-to-close
  store_artifacts_secs: 120    # store_artifacts start-to-close
```

`node_secs` is the one most likely to need raising: a model that legitimately
runs longer than an hour is otherwise killed mid-statement and retried.

Values are validated when the worker loads the project, so a bad setting fails
startup rather than surfacing later as activities timing out mid-run:

- durations must be non-zero;
- heartbeat timeouts must be at least 60s — activities heartbeat every 30s, and
  a shorter timeout reports live workers as dead;
- a heartbeat timeout above its activity's start-to-close can never fire and is
  rejected.

`plan_secs` applies from the *next* run onward when changed: the plan activity
resolves the config, so it necessarily runs under the built-in default.

## Workflow History Size

Each node costs roughly three history events, plus one per retry and per memo
upsert. Temporal warns around 10,240 events and hard-fails at 51,200, so a wide
`build` over a large project can hit the ceiling mid-DAG. At each level boundary
the workflow checks Temporal's own `continue_as_new_suggested` signal and logs a
warning naming the current history length when it trips. The run is not split
automatically — narrow it with `--select`, or split the project across
workflows.
