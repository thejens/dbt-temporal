# dbt-temporal Documentation

dbt-temporal runs dbt projects as durable Temporal workflows. New here? Start
with the [project README](../README.md) and its
[quick start](../README.md#quick-start).

## Run dbt

| Guide | What it covers |
|-------|----------------|
| [Workflow input reference](workflow-input.md) | Every `dbt_run` input field, the supported dbt commands, slim CI, retrying failed nodes, scheduling, live queries |
| [Per-workflow env overrides](env-overrides.md) | One worker, many warehouses, schemas or tenants: `env_var()` overrides per run |
| [Hooks](hooks.md) | dbt's `on-run-start` / `on-run-end`, and lifecycle hooks that run Temporal child workflows around a dbt run |
| [Error handling](error-handling.md) | What retries and what does not, cancellation, activity timeouts, history limits |
| [Examples](../examples/README.md) | Runnable example projects: single and multi-project, BigQuery, env overrides, jaffle-shop from git |

## Operate the worker

| Guide | What it covers |
|-------|----------------|
| [Deployment](deployment.md) | Container images, Kubernetes, Cloud Run, ECS, Azure, Temporal Cloud, health checks, graceful shutdown |
| [Helm chart](../charts/dbt-temporal/README.md) | Chart values, delivering the dbt project, private git repositories, KEDA autoscaling |
| [Configuration reference](configuration.md) | Every environment variable: connection, project sources, artifacts, tuning, observability |

## Understand and contribute

| Guide | What it covers |
|-------|----------------|
| [Architecture](architecture.md) | The run lifecycle, module layout and dependencies |
| [`path:` selectors and ephemeral models](selector-semantics.md) | A worked comparison of selector behaviour with dbt Core |
| [dbt v2 workarounds](workarounds.md) | Upstream issues in dbt v2 and how the worker works around them |
| [Releasing](releasing.md) | Cutting a release and what the release workflow publishes |
