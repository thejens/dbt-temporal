# Lifecycle Hooks

Hooks let you run arbitrary Temporal workflows at specific points in the dbt run lifecycle. Each hook starts a child workflow, awaits its completion, and handles errors according to a configurable policy.

## Hook Events

| Event | Fires | Payload includes |
|-------|-------|------------------|
| `pre_run` | After planning, before the first node executes | `ExecutionPlan` + `DbtRunInput` |
| `on_success` | After all nodes succeed and artifacts are stored | Full `DbtRunOutput` |
| `on_failure` | After a failure, once artifacts are stored | Full `DbtRunOutput` (with errors) |

Each event supports multiple hooks, executed sequentially in list order.

## Configuration

Hooks can be configured in two ways:

**1. Project-level defaults** via `dbt_temporal.yml` (alongside `dbt_project.yml`):

```yaml
retry:
  max_attempts: 5
  non_retryable_errors:
    - "permission denied"

hooks:
  pre_run:
    - workflow_type: validate_permissions
      task_queue: auth-workers
      timeout_secs: 120
      on_error: fail

  on_success:
    - workflow_type: notify_pubsub
      task_queue: notifications
      on_error: warn
    - workflow_type: update_catalog
      task_queue: catalog
      on_error: warn

  on_failure:
    - workflow_type: alert_oncall
      task_queue: alerts
      on_error: warn
```

**2. Per-invocation overrides** via the workflow input `hooks` field:

```json
{
  "command": "run",
  "hooks": {
    "pre_run": [],
    "on_success": [
      {"workflow_type": "notify_pubsub", "task_queue": "notifications"}
    ],
    "on_failure": []
  }
}
```

If `hooks` is set in the workflow input, it fully replaces the file-based defaults. To suppress all hooks for a single run, pass empty arrays. If `hooks` is omitted, the `dbt_temporal.yml` defaults apply.

## Hook Config Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `workflow_type` | Yes | - | Temporal workflow type to start |
| `task_queue` | Yes | - | Task queue the hook worker listens on |
| `timeout_secs` | No | `300` | Execution timeout for the child workflow |
| `on_error` | No | `warn` | Error handling policy (see below) |
| `input` | No | - | Custom input payload (replaces `HookPayload`, see below) |
| `fire_and_forget` | No | `false` | Start the child workflow without awaiting its result |

## Error Handling Policies

| Policy | Behavior |
|--------|----------|
| `fail` | **pre_run**: abort the dbt run. **on_success/on_failure**: flip `success` to `false` in the output. |
| `warn` | Log a warning, continue to the next hook. Error is recorded in `DbtRunOutput.hook_errors`. |
| `ignore` | Swallow the error silently. |

## Hook Payload

By default, every hook workflow receives a single JSON input (`HookPayload`):

```json
{
  "event": "on_success",
  "invocation_id": "workflow-run-id",
  "input": { "command": "run", "select": "+marts", ... },
  "plan": { "levels": [...], "nodes": {...}, ... },
  "output": { "success": true, "node_results": [...], ... }
}
```

- `plan` is available for all events.
- `output` is only set for `on_success` and `on_failure`.

The hook workflow can be written in any language with a Temporal SDK — Rust, Python, Go, TypeScript, etc. It just needs to be registered on the specified task queue.

## Custom Input

When a hook sets the `input` field, that value is sent as the child workflow input instead of `HookPayload`. This lets hooks start any workflow type — including `dbt_run` itself — with the native input format:

```yaml
hooks:
  pre_run:
    - workflow_type: dbt_run
      task_queue: dbt-tasks
      timeout_secs: 120
      on_error: fail
      input:
        command: run
        select: customers
        hooks: {}   # Empty hooks prevent recursive hook execution
```

The `hooks: {}` override is important when starting `dbt_run` as a hook — without it, the child workflow would inherit the project's `dbt_temporal.yml` defaults and trigger hooks recursively.

## Fire-and-Forget Mode

When `fire_and_forget: true`, the hook child workflow is started but not awaited. The parent workflow continues immediately without waiting for the hook to complete. The child workflow runs independently with `ParentClosePolicy: Abandon`, meaning it will keep running even if the parent workflow completes, fails, or is cancelled.

```yaml
hooks:
  on_success:
    - workflow_type: notify_slack
      task_queue: notifications
      fire_and_forget: true
      on_error: warn
```

Error handling for fire-and-forget hooks only applies to the *start* phase — if the child workflow fails to start, `on_error` determines the behavior. Once started, failures in the child workflow are not reported back to the parent.

Fire-and-forget hooks cannot return skip sentinels (since their result is never read).

## Skip Sentinel (Pre-Run Hooks)

A `pre_run` hook can signal that the dbt run should be skipped entirely by returning a skip sentinel value. When a skip is detected, the workflow returns early with `success: true`, `skipped: true`, and no node results. Remaining hooks in the list are not executed.

Skip sentinel values (returned as the hook workflow's result):

| Return value | Effect |
|-------------|--------|
| `false` | Skip with no reason |
| `{"skip": true}` | Skip with no reason |
| `{"skip": true, "reason": "not ready"}` | Skip with a reason string |
| `{"skip": false}` | Continue (no skip) |
| Anything else | Continue (no skip) |

The skip reason (if provided) is included in `DbtRunOutput.skip_reason`. This is useful for conditional execution — e.g. a pre-hook that checks whether source data has changed and skips the run if nothing is new.

## Extra Env (Pre-Run Hooks)

A `pre_run` hook can inject environment variables into the dbt run by returning an `extra_env` map:

```json
{"extra_env": {"SNOWFLAKE_ACCOUNT": "org-myaccount", "DB_SCHEMA": "prod_2026_02"}}
```

These are merged with the workflow's `env` field (hook values take precedence) and forwarded to every `execute_node` activity. They are available via `{{ env_var('KEY') }}` in model SQL and macros, and — when `profiles.yml` uses `env_var()` — also drive per-workflow adapter engine rebuilding, so the hook can control the database connection.

`extra_env` and skip sentinels can coexist in the same return value. If a skip is detected, `extra_env` is irrelevant (no nodes run). Values must be strings; non-string values are silently ignored.

**Example: hook resolves account credentials at runtime**

```python
# Python Temporal worker
@workflow.defn
class ResolveCredentialsWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        account = await workflow.execute_activity(
            fetch_account_for_run,
            payload["input"],
            start_to_close_timeout=timedelta(seconds=30),
        )
        return {
            "extra_env": {
                "SNOWFLAKE_ACCOUNT": account["snowflake_account"],
                "DB_SCHEMA": account["schema"],
            }
        }
```

```yaml
# dbt_temporal.yml
hooks:
  pre_run:
    - workflow_type: resolve_credentials
      task_queue: auth-workers
      on_error: fail
```

```yaml
# profiles.yml — env_var() here triggers per-workflow adapter rebuilding
my_profile:
  outputs:
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      schema: "{{ env_var('DB_SCHEMA', 'public') }}"
```

## Limitations

- Hooks run as **child workflows** in the same Temporal namespace. Cross-namespace hooks are not yet supported (the Rust SDK doesn't expose a namespace field on `ChildWorkflowOptions`).
