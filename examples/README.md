# Examples

Five example setups demonstrating dbt-temporal features. Most examples use Postgres locally (via Docker). The `bigquery/` example requires GCP credentials.

```
examples/
├── single-project/       Basic project with hooks + retry config (Postgres)
├── multi-project/        Two isolated projects: analytics + marketing (Postgres)
├── bigquery/             Feature-rich project exercising most dbt Jinja features (BigQuery)
├── env-override/         Per-workflow env vars + hooks with custom input (Postgres)
└── jaffle-shop (remote)  Classic dbt example project, loaded via git URL (Postgres)
```

## Quick Start

Prerequisites: [Temporal CLI](https://docs.temporal.io/cli), Rust toolchain, Docker, [psql](https://www.postgresql.org/docs/current/app-psql.html).

```bash
# Terminal 1: Start Temporal dev server
make dev

# Terminal 2: Start Postgres, seed databases, then start a worker
make postgres-up                # start Docker Postgres
make seed-postgres              # create tables with seed data
make run-worker-single          # queue: single-project
make run-worker-multi           # queue: multi-project
make run-worker-env             # queue: env-override
make run-worker-jaffle-shop     # queue: jaffle-shop
make run-worker-bigquery        # queue: bigquery (requires GOOGLE_CLOUD_PROJECT)

# Terminal 3: Open the UI and submit a workflow
make ui                             # opens http://localhost:8233
make submit-workflow-single         # → single-project queue
make submit-workflow-multi-analytics    # → multi-project queue (analytics)
make submit-workflow-multi-marketing    # → multi-project queue (marketing)
make submit-workflow-env            # → env-override queue
make submit-workflow-jaffle-shop    # → jaffle-shop queue
make submit-workflow-bigquery       # → bigquery queue
```

Or run everything at once with `make run-examples` — it seeds databases, starts Temporal, launches all workers, and submits workflows. BigQuery is automatically skipped if GCP credentials are not available.

All examples use the `default` namespace. Each worker listens on its own task queue so multiple workers can run side by side.

### Makefile targets

| Target | Task Queue | Description |
|--------|------------|-------------|
| `make dev` | — | Start Temporal dev server (gRPC :7233, UI :8233) |
| `make ui` | — | Open Temporal UI (`http://localhost:8233`) |
| `make seed-postgres` | — | Create Postgres tables with seed data for all examples |
| `make run-worker-single` | `single-project` | Start worker for single-project example |
| `make run-worker-multi` | `multi-project` | Start worker for multi-project example |
| `make run-worker-env` | `env-override` | Start worker for env-override example |
| `make run-worker-bigquery` | `bigquery` | Start worker for BigQuery example |
| `make run-worker-jaffle-shop` | `jaffle-shop` | Start worker for jaffle-shop (remote git project) |
| `make submit-workflow-single` | `single-project` | Run all models |
| `make submit-workflow-multi-analytics` | `multi-project` | Run the analytics project |
| `make submit-workflow-multi-marketing` | `multi-project` | Run the marketing project |
| `make submit-workflow-env` | `env-override` | Run all models |
| `make submit-workflow-bigquery` | `bigquery` | Run all models |
| `make submit-workflow-jaffle-shop` | `jaffle-shop` | Run jaffle-shop models |

---

## Example: Single Project

The `single-project/` directory contains a dbt project with models, tests, macros, a `dbt_temporal.yml` (hooks + retry config), and a Postgres-backed profile.

**Start the worker:**

```bash
make postgres-up           # start Docker Postgres (first time only)
make seed-postgres         # seed databases (first time only)
make run-worker-single
# or: TEMPORAL_TASK_QUEUE=single-project DBT_PROJECT_DIR=examples/single-project cargo run
```

**Trigger a workflow:**

```bash
make submit-workflow-single
# or: temporal workflow start --type dbt_run --task-queue single-project \
#       --workflow-id dbt_test_project --id-conflict-policy allow_duplicate \
#       --input '{"command": "run"}'
```

**What it demonstrates:**
- Model DAG with staging → marts levels (parallel execution within levels)
- Ephemeral materialization (`ephemeral_helper.sql`)
- Custom macros (`safe_round`, `log_run_results`)
- `var()` usage (`shared_var` in `station_summary.sql`)
- Schema tests (unique, not_null, accepted_range via dbt_utils)
- Pre/post hooks on models (in `schema.yml`)
- Lifecycle hooks + retry policy (in `dbt_temporal.yml`)

---

## Example: Multi-Project

The `multi-project/` directory contains two independent dbt projects — `analytics` and `marketing` — loaded into the same worker.

**Start the worker:**

```bash
make postgres-up
make seed-postgres
make run-worker-multi
# or: TEMPORAL_TASK_QUEUE=multi-project DBT_PROJECTS_DIR=examples/multi-project cargo run
```

The worker scans `examples/multi-project/` for subdirectories containing `dbt_project.yml` and loads both. Each project gets its own adapter engine, parsed state, and hooks config — fully isolated.

**Run the analytics project:**

```bash
make submit-workflow-multi-analytics
```

**Run the marketing project:**

```bash
make submit-workflow-multi-marketing
```

The `project` field is required when multiple projects are loaded. If only one project is loaded, it auto-selects and `project` can be omitted.

**Alternative: explicit project list:**

```bash
DBT_PROJECT_DIRS=examples/multi-project/analytics,examples/multi-project/marketing cargo run
```

---

## Example: BigQuery

The `bigquery/` directory contains a single dbt project configured for BigQuery using application default credentials. It queries the same public Austin bikeshare dataset as `single-project`, but writes results to a BigQuery dataset in your GCP project.

**Prerequisites:**

```bash
# Authenticate with application default credentials
gcloud auth application-default login

# Set your GCP project
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
```

**Start the worker:**

```bash
make run-worker-bigquery
# or: TEMPORAL_TASK_QUEUE=bigquery GOOGLE_CLOUD_PROJECT=my-project DBT_PROJECT_DIR=examples/bigquery cargo run
```

**Trigger a workflow:**

```bash
make submit-workflow-bigquery
```

**What it demonstrates:**
- BigQuery adapter with `oauth` method (application default credentials)
- Reading from public BigQuery datasets (`bigquery-public-data.austin_bikeshare`)
- Writing to a dataset (`dbt_temporal_example`) in the user's own GCP project
- Same model features as `single-project`: DAG levels, ephemeral materialization, custom macros, tests, hooks
- BigQuery-specific non-retryable error patterns in `dbt_temporal.yml`
- Incremental model with `merge` strategy, `partition_by`, `cluster_by`, `is_incremental()`, `{{ this }}`
- Ephemeral intermediate models (inlined as CTEs)
- CTE-based lookup data (inline status mapping — seeds are not yet supported, see Known Limitations)
- Custom macros: adapter dispatch (`format_station_id`), audit columns, query comments with BigQuery job labels, table metadata post-hooks, conditional source checking
- Comprehensive Jinja variable coverage: `target`, `invocation_id`, `run_started_at`, `env_var()`, `modules.datetime`, `execute` guard, `model` context, `schema`, `database`, `tojson` filter
- `on-run-start` hook with run metadata, enhanced `on-run-end` with per-node `results` logging
- Doc blocks (`{% docs %}`) referenced in schema descriptions
- Custom generic tests (`is_positive`, `greater_than` with threshold arg, `not_empty`)
- Singular test (`assert_stations_have_trips`)
- `accepted_values` and `relationships` tests
- In-model config: `tags`, `meta`, `alias`, `persist_docs`, `hours_to_expiration`
- Folder-level model config in `dbt_project.yml`
- Exposure definition for downstream dashboard tracking
- Analysis file (compiled but not executed)

---

## Example: Per-Workflow Env Overrides

The `env-override/` directory contains a project whose `profiles.yml` uses `env_var()` for all connection parameters. This lets parallel workflows target different databases. It also includes a `dbt_temporal.yml` demonstrating hooks with custom input and fire-and-forget mode — the pre_run hook runs `dbt run --select customers` before the main run, and the on_success hook fires off `dbt run --select customer_orders` without blocking.

**Start the worker:**

```bash
make postgres-up
make seed-postgres
make run-worker-env
# or: TEMPORAL_TASK_QUEUE=env-override DBT_PROJECT_DIR=examples/env-override cargo run
```

At startup, the worker detects `env_var()` in `profiles.yml`. When a workflow provides `env` overrides, the adapter engine is rebuilt per-workflow.

**Workflow A — writes to warehouse-a:**

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue env-override \
  --workflow-id env_override_demo \
  --id-conflict-policy allow_duplicate \
  --input '{
    "command": "run",
    "env": {
      "DB_HOST": "warehouse-a.example.com",
      "DB_SCHEMA": "team_a",
      "DEPLOYMENT_ENV": "staging"
    }
  }'
```

**Workflow B — writes to warehouse-b (can run concurrently):**

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue env-override \
  --workflow-id env_override_demo \
  --id-conflict-policy allow_duplicate \
  --input '{
    "command": "run",
    "env": {
      "DB_HOST": "warehouse-b.example.com",
      "DB_SCHEMA": "team_b",
      "DEPLOYMENT_ENV": "production"
    }
  }'
```

**How it works:**
1. **Model SQL / macros:** Each activity clones the Jinja env and re-registers `env_var()` with the workflow's env map. `{{ env_var('DEPLOYMENT_ENV', 'development') }}` resolves to the override.
2. **Database connections:** If `profiles.yml` contains `env_var()` and the workflow provides `env`, the profile is re-rendered and a fresh adapter engine is built per-workflow.
3. **Fallback:** Vars not in the workflow's `env` fall through to the process environment.

---

## Workflow Input Reference

All fields except `command` are optional.

```json
{
  "project": "analytics",
  "command": "run",
  "select": "+my_model",
  "exclude": "tag:wip",
  "vars": {"key": "value"},
  "target": "prod",
  "full_refresh": false,
  "fail_fast": true,
  "hooks": null,
  "env": {}
}
```

### Basic Run

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run"}'
```

Using the project name as the workflow ID makes runs easy to find in the Temporal UI. The `allow_duplicate` conflict policy lets you re-run the same project without changing the ID — Temporal creates a new execution each time.

### Build (Models + Tests)

Runs models, then executes tests:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "build"}'
```

### Select / Exclude

```bash
# Run only staging models
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "select": "staging.*"}'

# Run everything except station_summary
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "exclude": "station_summary"}'

# Run a model and all its upstream dependencies
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "select": "+active_stations"}'
```

### Full Refresh

Drops and recreates incremental models:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "full_refresh": true}'
```

### Fail Fast

Stop on first failure, skip all remaining nodes:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "build", "fail_fast": true}'
```

### Vars Override

Override dbt `var()` values:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "vars": {"shared_var": "overridden_value"}}'
```

### Target Override

Run against a different profile target:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run", "target": "prod"}'
```

### Combined Options

All options compose freely:

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue multi-project \
  --workflow-id analytics \
  --id-conflict-policy allow_duplicate \
  --input '{
    "command": "build",
    "project": "analytics",
    "select": "+user_activity",
    "full_refresh": true,
    "fail_fast": true,
    "vars": {"shared_var": "nightly"},
    "target": "prod"
  }'
```

---

## Config Files

### dbt_temporal.yml — Hooks + Retry

Place alongside `dbt_project.yml`. See [`single-project/dbt_temporal.yml`](single-project/dbt_temporal.yml) and [`env-override/dbt_temporal.yml`](env-override/dbt_temporal.yml) for working examples.

```yaml
# Retry policy for execute_node activities.
retry:
  max_attempts: 3              # Total attempts (1 = no retries). Default: 3
  initial_interval_secs: 5     # Starting backoff. Default: 5
  backoff_coefficient: 2.0     # Multiplier per retry. Default: 2.0
  max_interval_secs: 60        # Backoff ceiling. Default: 60
  non_retryable_errors:        # Regex patterns — matching adapter errors skip retries
    - "permission denied"
    - "relation .* does not exist"

# Lifecycle hooks — child workflows triggered at lifecycle events.
hooks:
  pre_run:
    - workflow_type: validate_permissions
      task_queue: hooks-queue
      timeout_secs: 120        # Default: 300
      on_error: fail           # fail | warn (default) | ignore

  on_success:
    # Fire-and-forget: start and don't wait for completion.
    - workflow_type: notify_slack
      task_queue: hooks-queue
      on_error: warn
      fire_and_forget: true

    # Custom input: start a dbt_run child workflow with native input.
    - workflow_type: dbt_run
      task_queue: dbt-tasks
      timeout_secs: 120
      on_error: warn
      input:
        command: run
        select: customer_orders
        hooks: {}              # Prevent recursive hook execution

  on_failure:
    - workflow_type: alert_oncall
      task_queue: hooks-queue
      on_error: warn
```

### profiles.yml with env_var()

See [`env-override/profiles.yml`](env-override/profiles.yml) for a working example.

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      port: "{{ env_var('DB_PORT', '5432') }}"
      user: "{{ env_var('DB_USER', 'postgres') }}"
      password: "{{ env_var('DB_PASSWORD', 'postgres') }}"
      dbname: "{{ env_var('DB_NAME', 'analytics') }}"
      schema: "{{ env_var('DB_SCHEMA', 'public') }}"
      threads: 4
```

The adapter engine is rebuilt per-workflow only when **both** conditions are true:
1. `profiles.yml` contains `env_var(` (detected once at startup)
2. The workflow's `env` field is non-empty

Otherwise the shared engine is used with zero overhead.

---

## Worker Configuration

All configuration is via environment variables passed when starting the worker.

### Temporal Connection

```bash
TEMPORAL_ADDRESS=localhost:7233 \
  TEMPORAL_NAMESPACE=dbtt \
  TEMPORAL_TASK_QUEUE=dbt-tasks \
  cargo run
```

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal server gRPC address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `dbt-tasks` | Task queue the worker polls |

### Project Discovery

Projects are discovered using a fallback chain:

```bash
# Option 1: Explicit list of project directories
DBT_PROJECT_DIRS=examples/multi-project/analytics,examples/multi-project/marketing cargo run

# Option 2: Base directory to scan (finds subdirs with dbt_project.yml)
DBT_PROJECTS_DIR=examples/multi-project cargo run

# Option 3: Single project directory
DBT_PROJECT_DIR=examples/single-project cargo run

# Option 4: No env var — uses cwd (if it has dbt_project.yml, or scans subdirs)
cd examples/single-project && cargo run
```

| Variable | Description |
|----------|-------------|
| `DBT_PROJECT_DIRS` | Comma-separated list of project sources (local paths or remote URLs) |
| `DBT_PROJECTS_DIR` | Base directory to scan for subdirs with `dbt_project.yml` |
| `DBT_PROJECT_DIR` | Single project directory |
| `DBT_PROFILES_DIR` | Override path to `profiles.yml` (default: project dir) |
| `DBT_TARGET` | Override default target for all workflows |

### Worker Tuning

**Fixed mode** (default) — static slot limits:

```bash
WORKER_MAX_CONCURRENT_ACTIVITIES=50 \
  WORKER_MAX_CONCURRENT_WORKFLOW_TASKS=100 \
  cargo run
```

| Variable | Default |
|----------|---------|
| `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS` | `200` |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | `100` |
| `WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES` | `100` |

**Resource-based mode** — dynamically adjusts slots using PID controllers that track system memory and CPU:

```bash
WORKER_TUNER=resource-based \
  WORKER_RESOURCE_TARGET_MEM=0.7 \
  WORKER_RESOURCE_TARGET_CPU=0.9 \
  WORKER_RESOURCE_ACTIVITY_MIN_SLOTS=5 \
  WORKER_RESOURCE_ACTIVITY_MAX_SLOTS=200 \
  cargo run
```

| Variable | Default |
|----------|---------|
| `WORKER_RESOURCE_TARGET_MEM` | `0.8` |
| `WORKER_RESOURCE_TARGET_CPU` | `0.9` |
| `WORKER_RESOURCE_ACTIVITY_MIN_SLOTS` | `1` |
| `WORKER_RESOURCE_ACTIVITY_MAX_SLOTS` | `500` |

The resource-based tuner is cgroup-aware — in containers it reads from `/sys/fs/cgroup` instead of host metrics.

### Artifact Storage

When `WRITE_ARTIFACTS=true`, each workflow run stores `run_results.json`, `manifest.json`, and optionally `log.txt`. Artifact writing is disabled by default.

**Local filesystem** (default):

```bash
ARTIFACT_STORE=/data/dbt-artifacts cargo run
```

**GCS:**

```bash
cargo build --features gcs
ARTIFACT_STORE=gs://my-bucket/dbt-artifacts cargo run
```

**S3 / Minio:**

```bash
cargo build --features aws
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  ARTIFACT_STORE=s3://my-bucket/dbt-artifacts cargo run

# Minio (S3-compatible)
AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true \
  ARTIFACT_STORE=s3://my-bucket/dbt-artifacts cargo run
```

| Variable | Default | Description |
|----------|---------|-------------|
| `WRITE_ARTIFACTS` | `false` | Enable artifact writing (`run_results.json`, `manifest.json`, `log.txt`) after each run |
| `ARTIFACT_STORE` | `/tmp/dbt-artifacts` | Local path or cloud URL (`gs://…`, `s3://…`). Cloud URLs require feature flag. |
| `WRITE_RUN_LOG` | `true` | Write CLI-style `log.txt` to artifact store |

### Remote Project Sources

`DBT_PROJECT_DIRS` accepts remote URLs alongside local paths. Remote sources are fetched at worker startup and scanned for `dbt_project.yml` files. Local and remote sources can be freely mixed.

```bash
# Public git repo
DBT_PROJECT_DIRS=git+https://github.com/org/dbt-models.git#main cargo run

# Git repo with dbt project in a subdirectory
DBT_PROJECT_DIRS=git+https://github.com/org/monorepo.git#main:path/to/dbt cargo run

# Private git repo
GITHUB_TOKEN=ghp_... \
  DBT_PROJECT_DIRS=git+https://github.com/org/dbt-models.git#main cargo run

# GCS
cargo build --features gcs
DBT_PROJECT_DIRS=gs://my-bucket/dbt-models cargo run

# S3
cargo build --features aws
DBT_PROJECT_DIRS=s3://my-bucket/dbt-models cargo run

# Mix local + remote
DBT_PROJECT_DIRS=/local/analytics,git+https://github.com/org/shared-models.git#main cargo run
```

| Variable | Description |
|----------|-------------|
| `GITHUB_TOKEN` | Auth for private git repos over HTTPS (falls back to `GIT_TOKEN`) |

The `#branch` suffix is required for git URLs. Append `:subdir` to point at a subdirectory (e.g. `#main:path/to/dbt`). Multiple projects in the same repo are supported — the worker scans for `dbt_project.yml` in downloaded subdirectories. Duplicate project names across sources are detected at startup.

**Staleness detection:** At the start of every workflow run, the worker checks whether remote sources have changed and logs a warning if stale. This never blocks or fails a workflow.

### Search Attributes

Workflows upsert [search attributes](https://docs.temporal.io/visibility#search-attribute) for filtering in the Temporal UI.

**Add static attributes via `TEMPORAL_SEARCH_ATTRIBUTES`:**

```bash
TEMPORAL_SEARCH_ATTRIBUTES='{"env":"prod","team":"data-eng"}' cargo run
```

**Dynamic attributes** (set automatically per-workflow):
- `DbtProject` — resolved project name
- `DbtCommand` — `run` or `build`
- `DbtTarget` — target name (only if specified)

Static values take precedence over dynamic ones if the same key is used. All search attributes (both static and dynamic) must be [registered on the Temporal namespace](https://docs.temporal.io/visibility#custom-search-attributes) — unregistered attributes are silently skipped.

### Health Check

```bash
# HTTP health server (recommended)
HEALTH_PORT=8080 cargo run

# File-based only
HEALTH_FILE=/tmp/dbt-temporal-health cargo run
```

Set `HEALTH_PORT` to enable an HTTP health endpoint (`200 OK` / `503 stale`). The health file is created automatically. See [deployment docs](../docs/deployment.md#health-check) for Kubernetes, Cloud Run, and ECS examples.

### Run Log

When `WRITE_RUN_LOG=true` (default), each run stores a `log.txt`:

```
Running with dbt-temporal=0.1.0
Found 5 models, 3 tests
Concurrency: 3 parallel levels

1 of 8 START table_model analytics.stg_users
2 of 8 START table_model analytics.stg_events
  => table_model analytics.stg_users  [OK in 1.23s]
  => table_model analytics.stg_events  [OK in 0.89s]
3 of 8 START view_model analytics.user_activity
  => view_model analytics.user_activity  [OK in 0.45s]
...

Finished running 8 nodes in 12.30s.
Done. PASS=7 ERROR=0 SKIP=1 TOTAL=8
```

Set `WRITE_RUN_LOG=false` to disable.

---

## Lifecycle Hooks

Hooks run arbitrary Temporal child workflows at lifecycle events. They can be written in any language (Rust, Python, Go, TypeScript) — they just need a worker on the specified task queue.

### Events

| Event | When | Payload includes |
|-------|------|------------------|
| `pre_run` | After planning, before first node | `ExecutionPlan` + `DbtRunInput` |
| `on_success` | After all nodes succeed | Full `DbtRunOutput` |
| `on_failure` | After a failure (once artifacts are stored) | Full `DbtRunOutput` with errors |

### Hook config fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `workflow_type` | Yes | - | Temporal workflow type to start |
| `task_queue` | Yes | - | Task queue the hook worker listens on |
| `timeout_secs` | No | `300` | Execution timeout for the child workflow |
| `on_error` | No | `warn` | `fail`, `warn`, or `ignore` |
| `input` | No | - | Custom input payload (replaces default `HookPayload`) |
| `fire_and_forget` | No | `false` | Start without awaiting — child runs independently |

### Error policies

| Policy | `pre_run` behavior | `on_success`/`on_failure` behavior |
|--------|-------------------|-----------------------------------|
| `fail` | Abort the dbt run | Flip `success` to `false` |
| `warn` | Log warning, continue | Record in `hook_errors`, continue |
| `ignore` | Swallow silently | Swallow silently |

### Hook payload

By default, every hook workflow receives a `HookPayload` JSON:

```json
{
  "event": "on_success",
  "invocation_id": "workflow-run-id",
  "input": {"command": "run", "select": "+marts"},
  "plan": {"levels": [...], "nodes": {...}},
  "output": {"success": true, "node_results": [...]}
}
```

`output` is only set for `on_success` and `on_failure`.

When a hook sets the `input` field, that custom value is sent instead of `HookPayload`. This lets hooks start any workflow type (including `dbt_run`) with its native input format.

### Fire-and-forget

When `fire_and_forget: true`, the child workflow is started but not awaited. It continues running independently even if the parent completes, fails, or is cancelled (`ParentClosePolicy: Abandon`). Error handling only applies to the start phase.

### Skip sentinel (pre_run only)

A `pre_run` hook can skip the entire dbt run by returning a skip sentinel: JSON `false`, or `{"skip": true}` (optionally with `"reason": "..."`). The workflow returns early with `skipped: true` and no node results.

---

## Resume, Retry, and Workflow Management

### List and inspect workflows

```bash
# List recent workflows
temporal workflow list

# Show details of a workflow
temporal workflow show --workflow-id <ID>

# Get result as JSON
temporal workflow show --workflow-id <ID> --output json
```

### Reset a failed workflow

Temporal's `workflow reset` replays a workflow from a specific event, re-executing from that point forward. Useful when a transient error caused a failure mid-DAG.

```bash
# Restart from the beginning (re-plans and re-runs all nodes)
temporal workflow reset \
  --workflow-id <ID> \
  --type FirstWorkflowTask \
  --reason "retrying after transient warehouse error"

# Resume from the last successful point
temporal workflow reset \
  --workflow-id <ID> \
  --type LastWorkflowTask \
  --reason "retrying from last checkpoint"
```

After a reset, the worker picks up the workflow automatically and re-executes from the reset point.

### Cancel a running workflow

Graceful cancellation — in-flight activities are cancelled, remaining nodes are marked as `cancelled`:

```bash
temporal workflow cancel --workflow-id <ID>
```

### Terminate a stuck workflow

Force-stop without cleanup:

```bash
temporal workflow terminate \
  --workflow-id <ID> \
  --reason "abandoning stuck workflow"
```

### Re-run with the same input

Start a fresh execution using the same workflow ID (`allow_duplicate` creates a new run each time):

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue single-project \
  --workflow-id dbt_test_project \
  --id-conflict-policy allow_duplicate \
  --input '{"command": "run"}'
```

---

## Observability

### Node status memo

The workflow stores a `node_status` memo updated after each DAG level. Query it to see per-node progress:

```bash
temporal workflow show \
  --workflow-id <ID> \
  --output json | jq '.memo.node_status'
```

Each node has a status: `pending`, `running`, `success`, `error`, `skipped`, or `cancelled`.

### Command memo

The `command` memo records what triggered the run:

```bash
temporal workflow show \
  --workflow-id <ID> \
  --output json | jq '.memo.command'
```

### Filter workflows by search attributes

```bash
# All runs for the analytics project
temporal workflow list \
  --query 'DbtProject = "analytics"'

# Failed prod runs
temporal workflow list \
  --query 'DbtTarget = "prod" AND ExecutionStatus = "Failed"'

# Runs by a specific team (static search attribute)
temporal workflow list \
  --query 'team = "data-eng"'
```

### Artifact inspection

After a workflow completes, artifacts are at the paths returned in the output:

```bash
# Get the workflow result to find artifact paths
temporal workflow show --workflow-id <ID> --output json \
  | jq '.result.artifacts'

# Example output:
# {
#   "run_results_path": "/tmp/dbt-artifacts/<invocation-id>/run_results.json",
#   "manifest_path": "/tmp/dbt-artifacts/<invocation-id>/manifest.json",
#   "log_path": "/tmp/dbt-artifacts/<invocation-id>/log.txt"
# }
```

---

## Known Limitations & Unsupported Features

dbt-temporal uses [dbt-fusion](https://github.com/dbt-labs/dbt-fusion) (Rust) as its rendering and execution engine rather than dbt-core (Python). This means some dbt features behave differently or are not yet available.

### Not yet supported

| Feature | Status | Notes |
|---------|--------|-------|
| **Seeds** | Broken (BigQuery) | `dbt seed` / `dbt build` with seeds fails on BigQuery with `bigquery__create_table_as macro didn't get supported language, it got None`. This is a dbt-fusion adapter bug. **Workaround:** use `source()` references to existing tables or inline CTEs instead of seed CSVs. All examples in this repo use pre-seeded Postgres databases or public BigQuery datasets as sources. |
| **Snapshots** | Broken (BigQuery) | Snapshot materialization generates invalid SQL on BigQuery. **Workaround:** avoid snapshots until dbt-fusion fixes the materialization. |
| **`store_failures` (tests)** | Broken | Test `store_failures: true` config requires a `dbt_test__audit` dataset that dbt-fusion does not auto-create. Tests work normally without `store_failures`. |

### Not yet wired

| Feature | Status | Notes |
|---------|--------|-------|
| **`query-comment` config** | Parsed but ignored | `dbt_project.yml` `query-comment` is read during project load but not passed to the adapter. The adapter uses its default comment behavior. The macro is still useful for testing Jinja rendering. |
| **`run_query()` in models** | Limited | Works inside `on-run-end` hooks and materializations. In model SQL, requires the full adapter execution context which may not be available during compilation. |
| **`graph` variable iteration** | Partial | `graph` is available but `.nodes.values()`, `.items()`, and Jinja filters like `selectattr()` may not work identically to dbt-core. Direct property access on `graph.nodes` works. |
| **Custom materializations** | Not tested | The `{% materialization %}` block syntax is parsed by dbt-fusion but custom materializations (e.g. `lazy_table`, `table_function`, `ml_model`) have not been validated with dbt-temporal. Built-in materializations (`view`, `table`, `incremental`, `ephemeral`) work. |
| **Python models** | Not supported | dbt-fusion does not execute Python models. |
| **Metrics / Semantic Layer** | Not supported | MetricFlow metrics and semantic models are not available. |

### Adapter support

| Adapter | Status | Notes |
|---------|--------|-------|
| **Postgres** | Supported | Used by all non-BigQuery examples. Uses the ADBC PostgreSQL driver built from source (see `docs/adbc-postgres-macos-segfault.md` for macOS build notes). |
| **BigQuery** | Supported | OAuth (application default credentials). The `bigquery/` example uses BigQuery. |
| **Snowflake** | Untested | dbt-fusion has Snowflake adapter code but it has not been validated with dbt-temporal. |
| **Redshift** | Untested | Not validated. |
| **Spark / Databricks** | Not supported | No adapter available in dbt-fusion. |

### Test argument format

dbt-fusion's parser emits deprecation warnings for the standard dbt test argument format:

```yaml
# This works but logs a deprecation warning:
data_tests:
  - accepted_values:
      values: ['a', 'b']
  - relationships:
      to: ref('other_model')
      field: id
```

The parser expects test arguments under an `arguments` key. Both formats load correctly and produce working test nodes; the warnings are cosmetic.

### Analyses and exposures

Analyses (`analyses/` directory) are compiled but never executed — this matches standard dbt behavior. Exposures are parsed as metadata and appear in the manifest but are not executable nodes.
