# `dbt_run` Workflow Input Reference

Every dbt run is one execution of the `dbt_run` Temporal workflow. Its input is
a single JSON object that carries what you would otherwise pass to the dbt CLI:
the command, `--select` / `--exclude`, `--target`, `--vars`, `--full-refresh`,
and the state, defer and retry references. Start it from the Temporal CLI, a
Temporal Schedule, or any Temporal SDK.

```bash
temporal workflow start --type dbt_run --task-queue dbt-tasks --input '{
  "project": "my_project",
  "command": "build",
  "select": "+my_model",
  "exclude": "tag:wip",
  "target": "prod",
  "full_refresh": false,
  "fail_fast": true,
  "vars": {"key": "value"},
  "env": {"DB_SCHEMA": "prod_analytics"}
}'
```

All fields are optional. An empty object, `{}`, runs `dbt build` over the whole
project.

## Fields

| Field | dbt CLI equivalent | Default | Description |
|-------|--------------------|---------|-------------|
| `project` | none | the only loaded project | Which project to run, when the worker has [several loaded](configuration.md#dbt-project-discovery). |
| `command` | the subcommand | `build` | One of the [commands](#commands) below. |
| `select` | `--select` | everything | Node selection, in dbt's selector syntax. |
| `exclude` | `--exclude` | nothing | Nodes to drop from the selection. |
| `indirect_selection` | `--indirect-selection` | `eager` | How far to pull in tests attached to selected nodes: `eager`, `cautious`, `buildable` or `empty`. |
| `resource_types` | `--resource-type` | no restriction | Narrow the plan to these resource types (`model`, `seed`, `snapshot`, `test`, `unit_test`, …). |
| `exclude_resource_types` | `--exclude-resource-type` | none | Drop these resource types. Applied after `resource_types`, so an excluded type loses. |
| `target` | `--target` | the profile's default | The `profiles.yml` target to run against. |
| `vars` | `--vars` | `{}` | Project variables for this run. See [Vars and full refresh](#vars-and-full-refresh). |
| `full_refresh` | `--full-refresh` | `false` | Rebuild incremental models from scratch. |
| `fail_fast` | `--fail-fast` | `false` | Stop scheduling new DAG levels after the first failure. Can be toggled [while the run executes](#live-progress-and-control). |
| `env` | process environment | `{}` | Per-run `env_var()` values, including ones `profiles.yml` reads. See [per-workflow env overrides](env-overrides.md). |
| `hooks` | none | from `dbt_temporal.yml` | Lifecycle hooks for this run; replaces the project's defaults. See [hooks](hooks.md). |
| `defer_manifest_ref` | `--defer --state` | none | A previous `manifest.json`; refs to nodes this run does not build resolve to the relations it records. |
| `state_manifest_ref` | `--state` | none | A previous `manifest.json` for the `state:` selector methods (`modified`, `new`, `old`, `unmodified`). |
| `retry_from` | `dbt retry` | none | A previous run's `run_results.json`; the plan keeps only the nodes that did not succeed in it. |
| `event_time_start`, `event_time_end` | `--event-time-start`, `--event-time-end` | none | Microbatch window, RFC 3339. Both or neither, and the start must precede the end. |

The manifest and run-results references are paths or URIs in the worker's
[artifact store](configuration.md#artifact-storage), typically the paths a
previous run returned in its output.

## Commands

| `command` | What runs |
|-----------|-----------|
| `build` (default) | Models, tests, unit tests, seeds, snapshots and functions, in DAG order. |
| `run` | Models. |
| `test` | Data tests. Unit tests run under `build`. |
| `seed` | Seeds. |
| `snapshot` | Snapshots. |
| `compile` | Compiles models, tests, snapshots and functions without materializing them. |
| `list` | Returns the selected node set without executing any SQL. |
| `source-freshness` | Measures source freshness; writes `sources.json`. |
| `freshness` | Measures sources and models that declare a freshness SLA; also writes `freshness.json`. |

**Functions.** dbt v2 functions (scalar UDFs) are executed: `build`, `compile`
and `list` schedule them like any other buildable node. Whether a function
actually creates depends on the adapter. dbt ships generic `CREATE OR REPLACE
FUNCTION` SQL that Postgres, Snowflake, BigQuery and Databricks accept.
Adapters without it (DuckDB, for one) need a project-level
`<adapter>__scalar_function_sql` override, the same dispatch hook dbt uses
everywhere else.

**Freshness.** The two freshness commands differ in what they measure.
`source-freshness` covers sources only. `freshness` covers sources plus models
that declare an SLA: a `freshness:` config block with `warn_after` and/or
`error_after`. A model's `build_after` is deliberately not an SLA. It is a
scheduling rule for state-aware builds, so a `build_after`-only model is never
measured. Neither command builds anything, and both need a `loaded_at_field` or
`loaded_at_query` on every node they measure. dbt's relation-metadata fallback
needs an adapter metadata interface the worker does not drive, so nodes that
declare a rule without either are skipped and named in a warning. A rule naming
`count` without `period`, or the reverse, aborts the plan rather than reading
as "no rule".

**Resources that never execute.** Exposures, metrics, saved queries and
semantic models are parsed into the graph but have no execution path. They are
excluded from every plan, and a `build` over a project containing them logs a
warning naming the excluded resource types.

## Vars and full refresh

`vars` and `full_refresh` apply per workflow. `vars` layer over the project's
own with CLI-vars precedence, so a key you do not pass keeps its
`dbt_project.yml` value. `full_refresh` surfaces as `flags.FULL_REFRESH` /
`flags.full_refresh`, which is what dbt's `should_full_refresh()` and
`is_incremental()` read.

One limit is worth knowing. The worker parses each project **once at startup**,
so `var()` calls evaluated during parsing (inside `{{ config(...) }}` blocks or
`dbt_project.yml`) keep their startup value. Only render-time `var()` (model
bodies, macros, hooks) reflects the workflow's vars. The planner warns and
names the nodes when it spots a run's vars inside a `config()` block. The same
startup-parse constraint is why `config(schema=env_var(...))` is rejected
outright.

## Slim CI, defer and retry

Slim CI builds only what changed, against production for everything else. Point
both references at production's manifest:

```json
{
  "command": "build",
  "select": "state:modified+",
  "state_manifest_ref": "<prod manifest.json>",
  "defer_manifest_ref": "<prod manifest.json>"
}
```

To re-run only what failed, pass the failed run's `run_results.json` as
`retry_from`, with the same `select` / `exclude` as the original run:

```json
{
  "command": "build",
  "retry_from": "<previous run_results.json>"
}
```

A microbatch window that is half-specified or inverted is rejected by the
planner rather than silently running unwindowed.

## Live progress and control

While a run executes, query its progress and toggle fail-fast without
restarting it:

```bash
temporal workflow query -w <workflow-id> --type run_status
temporal workflow update execute -w <workflow-id> --name set_fail_fast -i true
```

`run_status` reports the phase, per-status node counts, level progress and the
effective `fail_fast`. A `set_fail_fast` update applies from the next DAG
level. See [observability](configuration.md#observability) for memos and
search attributes.

## Scheduling

A dbt run is an ordinary workflow, so [Temporal
Schedules](https://docs.temporal.io/schedule) replace cron:

```bash
temporal schedule create \
  --schedule-id nightly-dbt-build \
  --cron "0 3 * * *" \
  --workflow-id nightly-dbt-build \
  --task-queue dbt-tasks \
  --type dbt_run \
  --input '{"command": "build", "target": "prod"}'
```
