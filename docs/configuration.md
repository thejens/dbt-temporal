# Configuration

All configuration is via environment variables.

## Temporal Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `http://localhost:7233` | Temporal server address (URL with protocol) |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `dbt-tasks` | Task queue name |
| `TEMPORAL_API_KEY` | - | API key for Temporal Cloud (sent as `Authorization: Bearer` header) |
| `TEMPORAL_TLS_CERT` | - | Path to client certificate PEM file (for mTLS) |
| `TEMPORAL_TLS_KEY` | - | Path to client private key PEM file (for mTLS) |

**Temporal Cloud**: Set `TEMPORAL_API_KEY` for API-key auth (recommended) or `TEMPORAL_TLS_CERT` + `TEMPORAL_TLS_KEY` for mTLS. TLS is enabled automatically when any of these are set. The address should be `<namespace>.<account>.tmprl.cloud:7233` and the namespace `<namespace>.<account>`.

## dbt Project Discovery

Projects are discovered using a fallback chain:

| Variable | Description |
|----------|-------------|
| `DBT_PROJECT_DIRS` | Comma-separated list of project sources (local paths or remote URLs) |
| `DBT_PROJECTS_DIR` | Base directory to scan for subdirs containing `dbt_project.yml` |
| `DBT_PROJECT_DIR` | Single project directory (legacy) |

If none are set, the current working directory is used (either as a project if it contains `dbt_project.yml`, or scanned for subdirs).

**Multi-project**: When multiple projects are loaded, specify which one to run via the `project` field in the workflow input. If only one project is loaded, it is auto-selected and `project` can be omitted. Each project gets its own adapter engine and parsed state — they are fully isolated. Duplicate project names (from `dbt_project.yml`) across sources are detected at startup and cause a fatal error.

| Variable | Default | Description |
|----------|---------|-------------|
| `DBT_PROFILES_DIR` | project dir | Path to profiles.yml |
| `DBT_TARGET` | from profile | Target override |
| `GITHUB_TOKEN` | - | Auth token for private git repos over HTTPS. Falls back to `GIT_TOKEN`. |

## Remote Project Sources

Entries in `DBT_PROJECT_DIRS` can be local filesystem paths or remote URLs. Paths without a protocol prefix are treated as local directories. Remote URLs are fetched at worker startup and scanned for `dbt_project.yml` files. Local and remote sources can be freely mixed.

**Supported URL schemes:**

| Scheme | Feature flag | Example |
|--------|-------------|---------|
| `git+https://` | none | `git+https://github.com/org/dbt-models.git#main` |
| `git+ssh://` | none | `git+ssh://git@github.com/org/dbt-models.git#main` |
| `gs://` | `gcs` | `gs://my-bucket/dbt-models` |
| `s3://` | `aws` | `s3://my-bucket/dbt-models` |

The `#branch` suffix is required for git URLs and specifies which branch (or tag) to check out. An optional `:subdir` can be appended to point at a subdirectory within the repo:

```
git+https://github.com/org/dbt-models.git#main:path/to/dbt
```

When a subdirectory is specified, only that path is scanned for `dbt_project.yml` files.

```bash
# Git (public repo)
DBT_PROJECT_DIRS=git+https://github.com/org/dbt-models.git#main cargo run

# Git (subdirectory — dbt project lives under path/to/dbt/)
DBT_PROJECT_DIRS=git+https://github.com/org/monorepo.git#main:path/to/dbt cargo run

# Git (private repo, GitHub Actions)
GITHUB_TOKEN=${{ secrets.GITHUB_TOKEN }} \
  DBT_PROJECT_DIRS=git+https://github.com/org/dbt-models.git#main cargo run

# S3
DBT_PROJECT_DIRS=s3://my-bucket/dbt-models cargo run --features aws

# Mix local + remote
DBT_PROJECT_DIRS=/local/analytics,git+https://github.com/org/shared-models.git#main cargo run
```

## Artifact Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `WRITE_ARTIFACTS` | `false` | Enable artifact writing (`run_results.json`, `manifest.json`, `log.txt`) after each run |
| `ARTIFACT_STORE` | `/tmp/dbt-artifacts` | Local path or cloud URL (`gs://…`, `s3://…`). Cloud URLs require `gcs` or `aws` feature. |
| `WRITE_RUN_LOG` | `true` | Write a CLI-style run log (`log.txt`) to the artifact store alongside `run_results.json` |

`ARTIFACT_STORE` accepts the same URL schemes as `DBT_PROJECT_DIRS`:

| Scheme | Feature flag | Auth | Example |
|--------|-------------|------|---------|
| local path | none | — | `/data/dbt-artifacts` |
| `gs://` | `gcs` | Application Default Credentials | `gs://my-bucket/dbt-artifacts` |
| `s3://` | `aws` | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars | `s3://my-bucket/dbt-artifacts` |

```bash
# Local (default)
ARTIFACT_STORE=/data/dbt-artifacts cargo run

# GCS (uses ADC — no extra config needed on GCE/Cloud Run)
cargo build --features gcs
ARTIFACT_STORE=gs://my-bucket/dbt-artifacts cargo run

# S3 / Minio
cargo build --features aws
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true \
  ARTIFACT_STORE=s3://my-bucket/dbt-artifacts cargo run
```

### Run Log

When `WRITE_RUN_LOG` is enabled (the default), each workflow run writes a `log.txt` to the artifact store containing high-level, dbt-CLI-style output:

```
Running with dbt-temporal=0.1.0
Found 5 models, 3 tests, 2 seeds
Concurrency: 3 parallel levels

1 of 10 START table_model waffle_hut.stg_customers
2 of 10 START table_model waffle_hut.stg_orders
  => table_model waffle_hut.stg_customers  [OK in 1.23s]
  => table_model waffle_hut.stg_orders  [OK in 0.89s]
3 of 10 START view_model waffle_hut.customers
  => view_model waffle_hut.customers  [OK in 0.45s]
...

Finished running 10 nodes in 15.30s.
Done. PASS=8 ERROR=1 SKIP=1 TOTAL=10
```

The log path is returned in `DbtRunOutput.log_path` and `StoreArtifactsOutput.log_path`. Set `WRITE_RUN_LOG=false` to disable.

## Search Attributes

Workflows automatically upsert [Temporal search attributes](https://docs.temporal.io/visibility#search-attribute) for filtering and querying in the Temporal UI.

**Dynamic attributes** (set automatically from each workflow's input):

| Attribute | Description |
|-----------|-------------|
| `DbtProject` | Resolved project name |
| `DbtCommand` | `run` or `build` |
| `DbtTarget` | Target name (only if specified in workflow input) |

**Static attributes** can be added via the `TEMPORAL_SEARCH_ATTRIBUTES` env var — a JSON object of string key-value pairs applied to every workflow run by this worker:

```bash
export TEMPORAL_SEARCH_ATTRIBUTES='{"env":"prod","team":"data-eng"}'
```

Static values take precedence over dynamic ones if the same key is used.

> [!IMPORTANT]
> Search attributes must be [registered on the Temporal namespace](https://docs.temporal.io/visibility#custom-search-attributes) before they can be used. At startup the worker queries the namespace for registered attributes and **silently skips** any that are not registered — workflows will not fail, but unregistered attributes won't appear in the UI. Register them with:
> ```bash
> temporal operator search-attribute create --name DbtProject --type Keyword
> temporal operator search-attribute create --name DbtCommand --type Keyword
> temporal operator search-attribute create --name DbtTarget --type Keyword
> ```
> If the operator service is unreachable (e.g. some Temporal Cloud configurations), the worker logs a warning and skips all attribute upserts.

## Health Check

| Variable | Default | Description |
|----------|---------|-------------|
| `HEALTH_PORT` | - | Port for the built-in HTTP health server (`200 OK` when healthy, `503` when stale) |
| `HEALTH_FILE` | - | Path to the health file touched every 15s. Defaults to `/tmp/health` when `HEALTH_PORT` is set. |

**Healthcheck subcommand**: `dbt-temporal healthcheck` checks if the health file exists and was modified within the last 60 seconds. Exits `0` if healthy, `1` if stale. Designed for Kubernetes exec liveness probes (no curl/wget needed in the container image). Reads `HEALTH_FILE` (defaults to `/tmp/health`).

## Worker Tuning

Controls how many tasks the worker processes concurrently. Two modes are available, selected via the `WORKER_TUNER` env var.

**Fixed mode** (default) — simple concurrency caps:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS` | `200` | Max concurrent workflow tasks |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | `100` | Max concurrent activity tasks |
| `WORKER_MAX_CONCURRENT_LOCAL_ACTIVITIES` | `100` | Max concurrent local activity tasks |

**Resource-based mode** (`WORKER_TUNER=resource-based`) — dynamically adjusts slot availability using PID controllers that track system memory and CPU. The worker backs off when the machine is under pressure, preventing OOM kills and CPU starvation.

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_RESOURCE_TARGET_MEM` | `0.8` | Target memory utilization (0.0–1.0). Slots are withheld above this threshold. |
| `WORKER_RESOURCE_TARGET_CPU` | `0.9` | Target CPU utilization (0.0–1.0). |
| `WORKER_RESOURCE_ACTIVITY_MIN_SLOTS` | `1` | Minimum activity slots issued regardless of resource pressure |
| `WORKER_RESOURCE_ACTIVITY_MAX_SLOTS` | `500` | Hard ceiling on concurrent activity slots |

The resource-based tuner is cgroup-aware: in containers with CPU/memory limits, it reads from `/sys/fs/cgroup` rather than host-level metrics.

**Polling, caching & timing** — applies to both modes:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_MAX_CACHED_WORKFLOWS` | `1000` | Number of workflows kept cached on sticky queues (LRU eviction). Higher values reduce replay overhead for concurrent workflows. |
| `WORKER_STICKY_QUEUE_TIMEOUT_SECS` | `10` | How long a workflow task can sit on the sticky queue before falling back to the normal (non-sticky) task queue. Lower values reduce latency on fallback at the cost of more replays. |
| `WORKER_NONSTICKY_TO_STICKY_POLL_RATIO` | `0.2` | Ratio of non-sticky to sticky queue pollers (0.0–1.0). Higher values make the worker poll the normal queue more aggressively, reducing fallback latency when sticky delivery fails. |

**Rate limiting & shutdown** — applies to both modes:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_MAX_ACTIVITIES_PER_SECOND` | unlimited | Per-worker rate limit on activities/second. Useful for protecting downstream warehouses from burst load. |
| `WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND` | unlimited | Server-side rate limit on activities/second for the entire task queue (across all workers). |
| `WORKER_GRACEFUL_SHUTDOWN_SECS` | none | Grace period (in seconds) before canceling in-flight activities on shutdown. Without this, the worker waits for all activities to complete. |

```bash
# Fixed: limit to 50 concurrent activities
WORKER_MAX_CONCURRENT_ACTIVITIES=50 cargo run

# Resource-based: auto-scale, back off at 70% memory
WORKER_TUNER=resource-based WORKER_RESOURCE_TARGET_MEM=0.7 cargo run

# Reduce sticky queue fallback latency
WORKER_STICKY_QUEUE_TIMEOUT_SECS=5 WORKER_NONSTICKY_TO_STICKY_POLL_RATIO=0.4 cargo run

# Rate-limit BigQuery API calls, graceful 30s shutdown
WORKER_MAX_ACTIVITIES_PER_SECOND=10 WORKER_GRACEFUL_SHUTDOWN_SECS=30 cargo run
```

## Observability

### Workflow Memos

The workflow stores live metadata in [Temporal memos](https://docs.temporal.io/workflows#memo), visible in the Temporal UI under the "Memo" tab:

| Memo key | Updated | Contents |
|----------|---------|----------|
| `command` | Once (at start) | Workflow input metadata: command, project, select, exclude, target, full_refresh, fail_fast, vars |
| `node_status` | After each DAG level | Map of `unique_id` → status (`pending`, `running`, `success`, `error`, `skipped`, `cancelled`) |
| `log` | After each DAG level | Tail of the CLI-style run log (last 200 lines) |

Both `node_status` and `log` are truncated to stay within Temporal memo size limits (2000 node entries, 200 log lines). The full run log is written to the artifact store at workflow completion.

### Per-Node Activity Names

Each dbt node uses the `summary` field on the Temporal activity to display a descriptive label in the UI Gantt chart (e.g. `model:stg_customers`, `test:not_null_orders_id`, `seed:raw_orders`). The `activity_id` is also set to the same label for identification in event details.
