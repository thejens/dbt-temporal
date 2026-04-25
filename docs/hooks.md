# Hooks

dbt-temporal supports two distinct kinds of hooks:

| Kind | Defined in | What runs | When |
|---|---|---|---|
| **dbt project hooks** | `dbt_project.yml` | Jinja templates that render to SQL (or have side effects via `run_query`/`log`) | `on-run-start`, `on-run-end`, plus per-model `pre-hook` / `post-hook` |
| **dbt-temporal lifecycle hooks** | `dbt_temporal.yml` (or workflow input) | Arbitrary Temporal child workflows (any language) | `pre_run`, `on_success`, `on_failure` |

The two are complementary. dbt project hooks are evaluated by dbt-fusion's Jinja engine in the same context as your models — they share `target`, `env`, `execute`, macros, and the warehouse adapter. dbt-temporal lifecycle hooks fan out to child workflows and are how you wire dbt runs into the rest of your platform (notifications, catalog updates, conditional skipping, credential resolution).

---

# dbt project hooks

Hooks defined in `dbt_project.yml`. These are standard dbt hooks and behave like they do under dbt-core.

| Event | Source | When it fires | Jinja context |
|---|---|---|---|
| `on-run-start` | `dbt_project.yml` (root or any package) | After planning, before any node executes | base context + `execute=true` |
| `on-run-end` | `dbt_project.yml` (root or any package) | After all nodes complete and artifacts are stored, regardless of success | base context + `execute=true` + `results` |
| `pre-hook` | model `config(pre_hook=…)` or `dbt_project.yml` `+pre-hook` | Inside each model's materialization, before the main SQL | full model context |
| `post-hook` | model `config(post_hook=…)` or `dbt_project.yml` `+post-hook` | Inside each model's materialization, after the main SQL | full model context |

**`pre-hook` / `post-hook`** are handled transparently by dbt-fusion's materialization templates — they're called from inside the materialization macro via `{{ run_hooks(pre_hooks) }}` / `{{ run_hooks(post_hooks) }}`. No dbt-temporal-specific configuration is required.

**`on-run-start` / `on-run-end`** are executed as a dedicated Temporal activity (`run_project_hooks`) once per phase, after planning and after artifact storage respectively. The activity:

- Builds the standard compile+run Jinja context (so `ref`, `source`, `log`, `target`, `env`, custom macros, and the adapter are all available).
- Adds `execute=true` as a Jinja **global** so cross-template macros that check `{% if execute %}` work correctly.
- For `on-run-end`, populates the `results` Jinja list — each entry has `unique_id`, `status`, `execution_time`, `message`, `failures`, `adapter_response`, and a `node` sub-object with `unique_id`, `name`, `resource_type`, `package_name`. This matches dbt-core's shape, so existing community macros like `log_run_results(results)` work unchanged.
- Renders each hook template with `jinja_env.render_str(...)`. Side-effect macros (`run_query`, `statement`, `log`) execute against the warehouse during rendering. If the rendered output is non-empty SQL, it's executed directly via the adapter.

Per-workflow `env` overrides apply: the activity rebuilds the adapter engine if `profiles.yml` uses `env_var()` and the workflow input set `env`, and patches `target.schema` / `target.database` accordingly. Hooks therefore see the same per-workflow connection as the models do.

**Failure semantics:**
- `on-run-start` failure aborts the workflow — no nodes execute.
- `on-run-end` failure is non-fatal: the error is logged and recorded in `DbtRunOutput.hook_errors`, but the run's `success` status is unchanged.
- Cancellation skips both `store_artifacts` and `on-run-end`, matching the existing artifact behavior.

**Example** (`dbt_project.yml` + a custom macro):

```yaml
# dbt_project.yml
on-run-start:
  - "{{ log('starting dbt run', info=True) }}"
on-run-end:
  - "{{ log_run_results(results) }}"
```

```sql
-- macros/log_run_results.sql
{% macro log_run_results(results) %}
  {% if execute and results %}
    {% set ns = namespace(pass=0, error=0) %}
    {% for r in results %}
      {% if r.status == 'success' %}{% set ns.pass = ns.pass + 1 %}{% endif %}
      {% if r.status == 'error' %}{% set ns.error = ns.error + 1 %}{% endif %}
    {% endfor %}
    {{ log("PASS=" ~ ns.pass ~ " ERROR=" ~ ns.error, info=True) }}
  {% endif %}
{% endmacro %}
```

Hooks from any loaded package are merged into the same lists by dbt-fusion's resolver — no extra configuration is needed for package-level hooks.

---

# dbt-temporal lifecycle hooks

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
