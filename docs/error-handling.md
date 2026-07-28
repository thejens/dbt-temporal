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
`build` over a large project can hit the ceiling mid-DAG.

The workflow handles this itself. At each level boundary it checks Temporal's
own `continue_as_new_suggested` signal, and when it trips it spills the run's
state and restarts with a fresh history. The run keeps one identity across
every segment: the same `invocation_id`, one artifact set, continuous progress
numbering in the log, and a single `run_results.json` at the end.

What carries across a continuation:

- the **plan** — a continuation never re-plans, so the node set cannot shift
  mid-run;
- results, log lines and per-node status accumulated so far;
- the failed-node set, so downstream skipping keeps working;
- `effective_env` including anything `pre_run` hooks injected, and any hook
  errors collected.

`pre_run` and `on-run-start` hooks run **once** for the logical run, in the
first segment — a continuation does not re-fire their side effects.
`on-run-end` and the post-run hooks run once, in the final segment.

Continuation is only possible when artifact storage is configured
(`ARTIFACT_STORE` + `WRITE_ARTIFACTS`): the state is too large to ride in the
workflow input, which would land back in the history it is trying to escape.
Without a store the run simply continues and accepts the history growth rather
than failing — so a very large run on a worker without artifact storage can
still hit the server limit. Configure a store if you build projects at that
scale.

Every segment executes at least one level before it may hand off. A run that
arrives already over the threshold would otherwise continue forever without
making progress — and since a resumed segment starts partway through the plan,
that guard counts levels done *in the current segment*, not the level index.
