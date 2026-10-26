# dbt-temporal Helm chart

Runs the [dbt-temporal](https://github.com/thejens/dbt-temporal) worker on
Kubernetes: a Deployment that polls a Temporal task queue and executes dbt DAGs
as workflows.

```bash
helm install dbt oci://ghcr.io/thejens/charts/dbt-temporal \
  --version 0.6.1 \
  --set temporal.address=temporal-frontend.temporal:7233 \
  --set dbt.projectDirs='git+https://github.com/org/dbt-models.git#main' \
  --set dbt.profiles.existingSecret=dbt-profiles
```

The chart is published as an OCI artifact on GHCR with every release, next to
the container images it deploys. Chart and app share one version.

## Delivering the dbt project

`dbt.projectDirs` takes a comma-separated list of sources:

| Source | How | Notes |
|---|---|---|
| Baked into an image | `FROM ghcr.io/thejens/dbt-temporal:<v>` + `COPY project/ /dbt/project/`; set `image.repository`/`image.tag` and `dbt.projectDirs=/dbt/project` | Immutable and versioned per model release. |
| Mounted volume | `extraVolumes` + `extraVolumeMounts` (ConfigMap, PVC, a git-sync sidecar's volume) | |
| Git, at startup | `git+https://…#ref[:subdir]` or `git+ssh://…` | Needs the default image; distroless has no `git`. Private repos: `GITHUB_TOKEN` or `GIT_TOKEN` via `envFrom`. |
| Bucket, at startup | `gs://bucket/prefix`, `s3://bucket/prefix` | Credentials from workload identity (`serviceAccount.annotations`) or env. |

A project fetched at startup lands in `/tmp`, so size `tmp.sizeLimit` for it.
Changing the commit you deploy means changing `dbt.projectDirs`, which changes
the ConfigMap hash and rolls the pods.

## Warehouse credentials

Profiles come from `dbt.profiles.existingSecret` (a Secret with a
`profiles.yml` key) or `dbt.profiles.inline`. Keep secrets out of the file
itself: reference them with `env_var()` and supply them through `envFrom` or
`extraEnv`.

## Shutdown and rollouts

On SIGTERM the worker stops taking new work and drains what is in flight.
Activities still running after `worker.gracefulShutdownSeconds` are cancelled
and retried on another worker. The pod's `terminationGracePeriodSeconds` is
that window plus `worker.terminationGraceBufferSeconds`, which is time for
cancelled queries to stop. See
[Graceful Shutdown](https://github.com/thejens/dbt-temporal/blob/main/docs/deployment.md#graceful-shutdown).

Rollouts surge one pod before removing one (`maxUnavailable: 0`), so the queue
keeps a poller throughout. Set `temporal.deploymentName` for any rollout that
can change workflow code: it pins each in-flight run to the worker version that
started it.

## Scaling

`replicaCount` for a fixed size, or `keda.enabled` to scale on task-queue
backlog with [KEDA](https://keda.sh/docs/latest/scalers/temporal/) (>= 2.17),
down to zero if `keda.minReplicaCount: 0`. Keep `keda.cooldownPeriod` above the
drain window, or KEDA can scale a pod in while it is still draining. The
scaler authenticates with the same API key or mTLS Secret as the worker when
`keda.useTemporalApiKey` or `temporal.tls.existingSecret` is set.

## Security defaults

The pod runs as UID 65532 with a read-only root filesystem, no capabilities,
no privilege escalation and the `RuntimeDefault` seccomp profile. The only
writable paths are two `emptyDir`s:

- `/tmp`: dbt's resolve output, per-activity scratch space and fetched projects.
  It is also `HOME`, because `git` needs one.
- `/var/cache/dbt-temporal` (`XDG_CACHE_HOME`): ADBC warehouse drivers, which
  dbt downloads on first use.

No Service is created unless metrics need one. The worker only makes outbound
connections; the health port is for the kubelet's probes.

## Values

| Key | Default | Description |
|---|---|---|
| `temporal.address` | — | **Required.** Temporal frontend `host:port`. |
| `temporal.namespace` | `default` | |
| `temporal.taskQueue` | `dbt-tasks` | |
| `temporal.deploymentName` | `""` | Enables worker deployment versioning. Recommended. |
| `temporal.apiKey.existingSecret` / `.key` | `""` / `api-key` | Temporal Cloud API key. |
| `temporal.tls.existingSecret` | `""` | `kubernetes.io/tls` Secret for mTLS. |
| `dbt.projectDirs` | — | **Required.** Project sources, see above. |
| `dbt.target` | `""` | dbt target. Defaults to the profile's. |
| `dbt.profiles.existingSecret` / `.inline` | `""` | One of the two. |
| `artifacts.enabled` | `false` | Write `run_results.json`, `manifest.json` and the run log. |
| `artifacts.store` | `""` | `gs://…` / `s3://…`. A local path is lost with the pod. |
| `artifacts.catalog` / `.runLog` | `false` / `true` | |
| `worker.gracefulShutdownSeconds` | `300` | Drain window after SIGTERM. `null` waits forever, and then requires `worker.terminationGracePeriodSeconds`. |
| `worker.terminationGraceBufferSeconds` | `60` | Added to the drain for the pod deadline. |
| `worker.terminationGracePeriodSeconds` | `null` | Overrides the computed deadline. |
| `worker.tuning` | `{}` | Any `WORKER_*` variable, as `NAME: value`. |
| `image.repository` / `.tag` / `.variant` | GHCR / appVersion / `""` | `variant: distroless` for the distroless image. |
| `replicaCount` | `1` | Ignored with KEDA. |
| `health.port` | `8080` | |
| `health.startupProbe.*` | 10s × 60 | Covers the project load before liveness applies. |
| `health.livenessProbe.*` | 15s, 5s timeout, 4 failures | |
| `metrics.enabled` / `.port` | `false` / `9464` | Prometheus exporter. |
| `metrics.service.enabled` | `false` | ClusterIP Service for the metrics port. |
| `metrics.serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitor. Implies the Service. |
| `keda.*` | disabled | `minReplicaCount`, `maxReplicaCount`, `targetQueueSize`, `activationTargetQueueSize`, `queueTypes`, `cooldownPeriod`, `useTemporalApiKey`. |
| `podDisruptionBudget.enabled` / `.maxUnavailable` | `false` / `1` | |
| `serviceAccount.create` / `.name` / `.annotations` | `true` / `""` / `{}` | Annotations for workload identity. |
| `extraEnv` / `envFrom` | `[]` | Credentials, `GITHUB_TOKEN`, etc. |
| `extraVolumes` / `extraVolumeMounts` | `[]` | |
| `tmp.medium` / `.sizeLimit` | `""` / `1Gi` | `Memory` makes `/tmp` a tmpfs. |
| `cache.sizeLimit` | `2Gi` | |
| `resources` | 500m / 1Gi request, 2Gi limit | |
| `podSecurityContext` / `securityContext` | hardened, see above | |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints`, `priorityClassName`, `imagePullSecrets` | | Standard. |

`values.schema.json` validates all of these: an unknown or misspelt key fails
the install rather than being ignored.
