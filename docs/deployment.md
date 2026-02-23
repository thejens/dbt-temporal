# Deployment

`dbt-temporal` is a **long-running worker process** that polls a Temporal server for tasks. It maintains a persistent gRPC connection to Temporal and executes dbt workflows as activities arrive. It optionally serves a health endpoint (see [Health Check](#health-check)), but is otherwise not an HTTP service.

This means it works well on any platform that can run a persistent container, but **cannot run on request-based serverless** like AWS Lambda or Google Cloud Functions (those expect a request→response lifecycle and cold-start per invocation; a Temporal worker must stay alive to poll).

## Docker Image

The project ships a distroless Docker image (`gcr.io/distroless/cc-debian12`) — no shell, no package manager, just the static binary and CA certificates. Build with:

```bash
docker build -f docker/Dockerfile -t dbt-temporal:latest .
```

**Baking model files into the image** (optional — alternative to fetching from git/GCS/S3 at startup):

```dockerfile
FROM dbt-temporal:latest
COPY my-dbt-project/ /dbt/project/
ENV DBT_PROJECT_DIR=/dbt/project
```

This gives you an immutable, versioned image per model release.

## Configuration

All config is via environment variables. The important ones:

| Variable | Required | Example |
|----------|----------|---------|
| `TEMPORAL_ADDRESS` | Yes | `temporal.internal:7233` |
| `TEMPORAL_NAMESPACE` | | `default` |
| `TEMPORAL_TASK_QUEUE` | | `dbt-tasks` |
| `TEMPORAL_API_KEY` | | Temporal Cloud API key |
| `TEMPORAL_TLS_CERT` | | `/etc/temporal/client.pem` (mTLS client certificate) |
| `TEMPORAL_TLS_KEY` | | `/etc/temporal/client.key` (mTLS client private key) |
| `DBT_PROJECT_DIRS` | One of the `DBT_PROJECT_*` vars | `/dbt/project` or `git+https://github.com/org/repo.git#${GIT_COMMIT}` |
| `DBT_PROFILES_DIR` | | `/dbt/profiles` |
| `DBT_TARGET` | | `prod` |
| `WRITE_ARTIFACTS` | | `true` — write run_results.json, manifest.json, log.txt after each run (default: `false`) |
| `ARTIFACT_STORE` | | `/data/dbt-artifacts`, `gs://bucket/prefix`, or `s3://bucket/prefix` |
| `WORKER_TUNER` | | `fixed` (default) or `resource-based` |
| `WORKER_MAX_CACHED_WORKFLOWS` | | `1000` — workflows cached on sticky queues |
| `WORKER_STICKY_QUEUE_TIMEOUT_SECS` | | `10` — sticky queue fallback timeout |
| `WORKER_NONSTICKY_TO_STICKY_POLL_RATIO` | | `0.2` — normal-to-sticky queue poll ratio |
| `WORKER_MAX_ACTIVITIES_PER_SECOND` | | Per-worker activity rate limit |
| `WORKER_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND` | | Server-side task-queue activity rate limit |
| `WORKER_GRACEFUL_SHUTDOWN_SECS` | | Grace period before canceling in-flight activities |
| `HEALTH_PORT` | | `8080` — enables the built-in HTTP health server (on by default in the Docker image) |
| `HEALTH_FILE` | | `/tmp/health` — explicit health file path (auto-set when `HEALTH_PORT` is used) |

Warehouse credentials are passed via `profiles.yml` or adapter-specific env vars — treat them as secrets.

### Temporal Cloud

To connect to [Temporal Cloud](https://temporal.io/cloud) instead of a self-hosted server, set the address and namespace to your Cloud endpoint and choose an authentication method.

**API key auth** (recommended — simpler to manage and rotate):

```bash
TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233
TEMPORAL_NAMESPACE=<namespace>.<account>
TEMPORAL_API_KEY=<your-api-key>
```

**mTLS auth** (client certificate + private key):

```bash
TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233
TEMPORAL_NAMESPACE=<namespace>.<account>
TEMPORAL_TLS_CERT=/etc/temporal/client.pem
TEMPORAL_TLS_KEY=/etc/temporal/client.key
```

TLS is enabled automatically when any auth variable is set. For mTLS, both `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY` must be provided. API key and mTLS can be combined if your Cloud namespace requires both.

In Kubernetes, store the API key or certificate files as Secrets and mount/inject them:

```yaml
envFrom:
  - secretRef:
      name: temporal-cloud-credentials  # contains TEMPORAL_API_KEY
```

Or for mTLS, mount the cert files from a Secret volume:

```yaml
volumes:
  - name: temporal-tls
    secret:
      secretName: temporal-cloud-tls
containers:
  - name: worker
    volumeMounts:
      - name: temporal-tls
        mountPath: /etc/temporal
        readOnly: true
    env:
      - name: TEMPORAL_TLS_CERT
        value: /etc/temporal/client.pem
      - name: TEMPORAL_TLS_KEY
        value: /etc/temporal/client.key
```

## Health Check

The worker is not an HTTP service, but it has two built-in mechanisms for health checking:

### HTTP health server (recommended)

Set `HEALTH_PORT` to enable a built-in HTTP health endpoint. The server responds `200 OK` when healthy and `503 Service Unavailable` when stale. This works natively with any platform that supports HTTP health checks.

```bash
HEALTH_PORT=8080
```

Under the hood, the health server is backed by a canary file (`HEALTH_FILE`, defaults to `/tmp/health`) whose mtime is updated every 15 seconds. The file is considered stale after 60 seconds without an update. Setting `HEALTH_PORT` automatically enables the health file — you don't need to set `HEALTH_FILE` separately.

**Kubernetes** — HTTP liveness probe:

```yaml
livenessProbe:
  httpGet:
    path: /
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 15
```

**Cloud Run** — uses HTTP health checks natively:

```bash
gcloud run deploy dbt-temporal \
  --set-env-vars "HEALTH_PORT=8080" \
  --liveness-probe httpGet,path=/,port=8080
```

**ECS** — use the exec-based healthcheck subcommand (the distroless image has no `curl`):

```json
"healthCheck": {
  "command": ["CMD", "dbt-temporal", "healthcheck"],
  "interval": 15,
  "timeout": 5,
  "retries": 3,
  "startPeriod": 30
}
```

### Exec-based healthcheck subcommand

The binary includes a `healthcheck` subcommand that checks the health file's freshness and exits 0 (healthy) or 1 (stale/missing). This requires no shell or external tools — it works in the distroless image. When `HEALTH_PORT` is set, the health file is already being written, so both approaches work together.

**Kubernetes** — exec-based alternative (if you prefer exec over httpGet):

```yaml
livenessProbe:
  exec:
    command:
      - dbt-temporal
      - healthcheck
  initialDelaySeconds: 30
  periodSeconds: 15
```

The subcommand reads `HEALTH_FILE` from the environment (defaults to `/tmp/health`).

### Health file details

The health file path must be writable by the container. When using `HEALTH_PORT` without an explicit `HEALTH_FILE`, the default path is `/tmp/health`.

```bash
# Explicit health file (without HTTP server):
HEALTH_FILE=/tmp/health

# HTTP server (health file is created automatically at /tmp/health):
HEALTH_PORT=8080

# Both — HTTP server uses the explicit path:
HEALTH_FILE=/var/run/health
HEALTH_PORT=8080
```

## Filesystem Permissions

The project directory (`DBT_PROJECT_DIRS`) is **never written to** — not even a `target/` subdirectory. All temporary files go to `/tmp`. This means baked-in Docker images can use a fully read-only filesystem with just a writable `/tmp`.

### Writable paths

**Always required** (cannot be disabled — dbt-fusion engine needs filesystem paths):

| Path | Lifetime | Size | What |
|------|----------|------|------|
| `/tmp/dbtt-target-*` | Startup only — deleted after caches are populated | ~1 MB typical | dbt-fusion resolve output. Read into memory, then removed. |
| `/tmp/...` (per-activity) | Seconds — auto-deleted when each activity completes | < 100 KB per activity | Ephemeral dir for dbt-fusion's WriteConfig output. |

**Optional** (only when the corresponding feature is enabled):

| Path | Lifetime | Size | What |
|------|----------|------|------|
| `HEALTH_FILE` (default `/tmp/health`) | Process lifetime | 0 bytes (mtime-only) | Empty file, mtime updated every 15s. |
| `/tmp/dbtt-models-*` | Process lifetime | Size of dbt project | Git clone or object download at startup. |
| `ARTIFACT_STORE` (default `/tmp/dbt-artifacts`) | **Accumulates** — one dir per workflow run | ~10-100 KB per run | `run_results.json`, `manifest.json`, `log.txt`. Only when `WRITE_ARTIFACTS=true` with a local path. |

All writable paths are under `/tmp`. Nothing accumulates in normal operation — the resolve output is cleaned up after startup and per-activity temp dirs are cleaned up after each activity. The only growth risk is `ARTIFACT_STORE` when using a local path (disabled by default; use a cloud URL like `gs://…` or `s3://…` to avoid local accumulation).

### Read-only paths

| Path | Access | Notes |
|------|--------|-------|
| `DBT_PROJECT_DIR[S]` | Read-only | Model SQL, seed CSVs, dbt_project.yml. Never written to. |
| `DBT_PROFILES_DIR` | Read-only | profiles.yml. Never written to. |
| `TEMPORAL_TLS_CERT` / `TEMPORAL_TLS_KEY` | Read-only | mTLS certificate files (if using mTLS). |

### Summary

A writable `/tmp` is the only requirement in all modes. The project directory is always read-only. A fully read-only filesystem (including `/tmp`) is not possible — the dbt-fusion engine requires filesystem paths for its resolve step and per-activity WriteConfig output. However, nothing persists in `/tmp` between restarts, and nothing accumulates during normal operation. A small `emptyDir` (or tmpfs) is sufficient.

### Kubernetes with read-only root filesystem

A single `emptyDir` on `/tmp` is all you need. Nothing accumulates — the resolve output is cleaned up after startup and per-activity temp dirs are cleaned up after each activity. A memory-backed tmpfs works well and avoids disk I/O:

```yaml
containers:
  - name: worker
    securityContext:
      readOnlyRootFilesystem: true
    volumeMounts:
      - name: tmp
        mountPath: /tmp
volumes:
  - name: tmp
    emptyDir:
      medium: Memory
      sizeLimit: 128Mi    # generous — typical peak usage is ~10 MB
```

## Re-deploying on Changes

Two things can change: **the worker binary** and **the dbt model files**.

**Binary changes**: rebuild the Docker image and roll out as normal.

**Model file changes** — two strategies:

1. **Baked into image**: rebuild the image with new model files, re-deploy. This is the simplest and most reproducible approach.

2. **Fetched at startup** via `DBT_PROJECT_DIRS=git+https://...#${GIT_COMMIT}`: update the `GIT_COMMIT` env var (or equivalent) and restart the worker. The worker fetches fresh models on boot. This avoids rebuilding the image for every model change.

   ```bash
   # Kubernetes: update env var → triggers rolling restart
   kubectl set env deployment/dbt-temporal GIT_COMMIT=abc123

   # Cloud Run: update env var → new revision
   gcloud run services update dbt-temporal --update-env-vars GIT_COMMIT=abc123
   ```

---

## 1. Kubernetes (any provider)

The worker needs a Deployment (not a Job — it's long-running) and a way to inject config/secrets.

**Minimal resources:**

- **Deployment** — runs the `dbt-temporal` container with `replicas: N` (scale workers horizontally)
- **ConfigMap** — non-secret env vars (`TEMPORAL_ADDRESS`, `TEMPORAL_TASK_QUEUE`, etc.)
- **Secret** — warehouse credentials, Temporal mTLS certs if applicable
- **ServiceAccount** — if using workload identity for GCS/S3 artifact storage

No Service or Ingress needed — the worker makes outbound connections only (the health port is only used by the kubelet's liveness probe).

```yaml
# Example sketch — not a complete chart
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dbt-temporal
spec:
  replicas: 2
  selector:
    matchLabels:
      app: dbt-temporal
  template:
    metadata:
      labels:
        app: dbt-temporal
    spec:
      containers:
        - name: worker
          image: ghcr.io/your-org/dbt-temporal:latest
          ports:
            - name: health
              containerPort: 8080
          envFrom:
            - configMapRef:
                name: dbt-temporal-config
            - secretRef:
                name: dbt-temporal-secrets
          livenessProbe:
            httpGet:
              path: /
              port: health
            initialDelaySeconds: 30
            periodSeconds: 15
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
```

**Re-deploy**: update the image tag or change an env var. For model-only changes with git fetch, bumping an annotation works too:

```bash
kubectl patch deployment dbt-temporal -p \
  "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"deploy-trigger\":\"$(date +%s)\"}}}}}"
```

A Helm chart would wrap this into a parameterized package — a natural next step for packaging this deployment.

### Scaling with KEDA

The standard Kubernetes HPA requires at least one pod running and scales on CPU/memory — neither is a good fit for a Temporal worker whose load is determined by task queue backlog, not resource usage.

[KEDA](https://keda.sh/) (Kubernetes Event-Driven Autoscaler) has a native [Temporal scaler](https://keda.sh/docs/2.18/scalers/temporal/) (since KEDA v2.17) that monitors task queue backlog size and scales workers accordingly — including **scaling to zero** when there are no pending tasks.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: dbt-temporal
spec:
  scaleTargetRef:
    name: dbt-temporal          # your Deployment name
  pollingInterval: 10
  cooldownPeriod: 60
  minReplicaCount: 0            # scale to zero when idle
  maxReplicaCount: 10
  triggers:
    - type: temporal
      metadata:
        endpoint: temporal-frontend.temporal:7233
        namespace: default
        taskQueue: dbt-tasks
        targetQueueSize: "5"              # tasks-per-replica target
        activationTargetQueueSize: "1"    # backlog threshold to wake from zero
        queueTypes: workflow,activity
```

Key fields:
- `targetQueueSize` — KEDA aims for this many backlog items per replica (scales up when exceeded)
- `activationTargetQueueSize` — the threshold that triggers scale-up from zero replicas. Set to `"1"` so a single pending task wakes up a worker.
- `queueTypes` — monitor `workflow`, `activity`, or both

**Caveat on scale-to-zero**: backlog-based activation can't account for in-flight tasks or very low throughput workloads. If a worker scales down while tasks are still completing, Temporal will re-dispatch them — but there's a delay. Mitigate with a generous `cooldownPeriod` (e.g. 60-120s) and/or the HPA `scaleDown.stabilizationWindowSeconds`. For production workloads where latency matters, `minReplicaCount: 1` is safer; use zero for dev/staging or batch-only queues that sit idle most of the day.

If the Temporal server requires auth (e.g. Temporal Cloud API key), add a `TriggerAuthentication`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: temporal-auth
spec:
  secretTargetRef:
    - parameter: apiKey
      name: temporal-secret
      key: apiKey
```

Then reference it in the trigger with `authenticationRef: { name: temporal-auth }`.

## 2. Google Cloud Run

Cloud Run supports **always-on** containers (min-instances >= 1, no-cpu-throttle), which is what a Temporal worker needs. Without this, Cloud Run will throttle or shut down the container when there are no HTTP requests.

```bash
gcloud run deploy dbt-temporal \
  --image gcr.io/your-project/dbt-temporal:latest \
  --no-cpu-throttle \
  --min-instances 1 \
  --cpu 1 --memory 512Mi \
  --set-env-vars "TEMPORAL_ADDRESS=temporal.internal:7233,TEMPORAL_TASK_QUEUE=dbt-tasks,HEALTH_PORT=8080" \
  --set-secrets "DBT_PROFILES_DIR=/secrets/profiles:dbt-profiles:latest" \
  --liveness-probe httpGet,path=/,port=8080 \
  --vpc-connector your-connector  # if Temporal is on a private network
```

Key flags:
- `--no-cpu-throttle` — CPU is always allocated, not just during requests
- `--min-instances 1` — keeps the worker alive
- `--liveness-probe` — uses the built-in HTTP health server
- `--vpc-connector` — needed if Temporal runs in a VPC

**Re-deploy**: `gcloud run services update dbt-temporal --update-env-vars GIT_COMMIT=<new-sha>` or push a new image tag.

**Caveat**: Cloud Run is optimized for request-driven workloads. It works for a Temporal worker with the flags above, but you're paying for always-on compute anyway — at that point GCE or GKE may be simpler.

## 3. Google Compute Engine

The most straightforward option. Run the container on a VM with Docker or use a Container-Optimized OS instance.

```bash
gcloud compute instances create-with-container dbt-temporal \
  --container-image gcr.io/your-project/dbt-temporal:latest \
  --container-env "TEMPORAL_ADDRESS=temporal.internal:7233,TEMPORAL_TASK_QUEUE=dbt-tasks" \
  --machine-type e2-small \
  --zone us-central1-a
```

**Re-deploy**: update the container image or env vars:

```bash
gcloud compute instances update-container dbt-temporal \
  --container-image gcr.io/your-project/dbt-temporal:new-tag
```

For HA, run multiple instances behind an instance group, or just run N independent VMs on the same task queue (Temporal distributes work automatically).

## 4. AWS — ECS or EKS

**ECS (Fargate)** is the simplest AWS option — no cluster to manage:

- Create a Task Definition with the `dbt-temporal` container image
- Run as a **Service** (not a standalone task) with `desiredCount: N`
- Pass config via environment variables, secrets via AWS Secrets Manager / Parameter Store
- Ensure the security group allows outbound to the Temporal server

```bash
# Simplified — real setup uses a task definition JSON
aws ecs create-service \
  --cluster default \
  --service-name dbt-temporal \
  --task-definition dbt-temporal:1 \
  --desired-count 2 \
  --launch-type FARGATE
```

**EKS**: same Kubernetes manifests as section 1, just on AWS-managed K8s.

**Re-deploy**: update the task definition with a new image or env var, then `aws ecs update-service --force-new-deployment`.

### Can it run on AWS Lambda?

**No.** Lambda functions have a max execution time (15 min), cold-start on each invocation, and expect a request→response pattern. A Temporal worker is a long-running process that maintains a persistent gRPC connection. These are fundamentally incompatible.

(The same applies to Google Cloud Functions and Azure Functions.)

## 5. Azure

**Azure Container Instances (ACI)** — simplest option, comparable to Fargate:

```bash
az container create \
  --resource-group mygroup \
  --name dbt-temporal \
  --image your-registry.azurecr.io/dbt-temporal:latest \
  --cpu 1 --memory 1 \
  --environment-variables \
    TEMPORAL_ADDRESS=temporal.internal:7233 \
    TEMPORAL_TASK_QUEUE=dbt-tasks \
  --restart-policy Always
```

**Azure Kubernetes Service (AKS)**: same Kubernetes manifests as section 1.

**Azure Container Apps**: similar to Cloud Run — supports always-on containers with `--min-replicas 1`. Works but has the same caveat: it's designed for HTTP workloads, and you're paying for always-on anyway.

---

## Summary

| Platform | Complexity | Best for |
|----------|-----------|----------|
| **Kubernetes** (any) | Medium | Teams already on K8s; multiple workers; fine-grained control |
| **Cloud Run** | Low | Quick GCP deploy; works with `--no-cpu-throttle --min-instances 1` |
| **Compute Engine** | Low | Simplest GCP option; good for single-worker setups |
| **ECS Fargate** | Low-Medium | AWS without managing servers |
| **EKS** | Medium | AWS teams already on K8s |
| **Azure ACI** | Low | Simplest Azure option |
| **Lambda / Cloud Functions / Azure Functions** | N/A | **Not compatible** — worker is long-running, not request-driven |
