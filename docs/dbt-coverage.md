# dbt feature coverage

The goal is that a project which runs under the dbt CLI runs the same way as a
`dbt_run` workflow: the same nodes, in an order dbt would accept, with the same
SQL and the same artifacts. This page lists where that does not hold yet. It is
the working list for contributors; the user-facing references are
[workflow input](workflow-input.md) and [selector semantics](selector-semantics.md).

A feature belongs on the roadmap when it changes what a run executes or
produces. Features that only serve someone at a terminal (`dbt show`,
`--inline`, `dbt debug`, `dbt init`, `dbt docs serve`) are out of scope: a
deployed worker has no terminal.

## Not supported

In the order they are planned.

| # | dbt feature | What is missing | Notes for the implementation |
|---|-------------|-----------------|------------------------------|
| 1 | `selectors.yml` and `--selector` | The file is not read, and the `selector:` method is rejected. | The definitions produce `SelectExpression` trees with nested `Exclude` nodes, which the planner rejects today. A project's `default: true` selector also changes what a run with no `select` executes. |
| 2 | `--empty` | No input field. | Renders `ref()` and `source()` with a zero-row limit, so a run validates SQL and builds empty relations. The cheapest way to test a deployment end to end. |
| 3 | `run-operation` | No command. | Runs one macro with arguments. Projects use it for grants, clean-up and backfills, and schedule it next to their runs. Needs one activity and no DAG. |
| 4 | `result:` and `source_status:` selector methods | Rejected. | `result:` reads a previous `run_results.json`, as `retry_from` already does. `source_status:` reads a previous `sources.json`, which `source-freshness` writes. Together they give `source_status:fresher+` builds. |
| 5 | `--favor-state` | No input field. | With `defer`, prefer the deferred relation even when the node exists in the target. |
| 6 | `--warn-error` and `--warn-error-options` | No input field. | Turns warnings into failures, so it changes run outcomes. |
| 7 | `clone` | No command. | Zero-copy clones of the nodes in a state manifest. Adapter support varies. |
| 8 | `--sample` | No input field. | Time-based sampling of `ref()` and `source()` for development runs. |
| 9 | `state:modified.configs`, `.relation`, `.persisted_descriptions`, `.macros`, `.contract` | Rejected. | The state comparison reads node bodies and file checksums only. `state:modified` on its own therefore misses a config-only or macro-only change, and warns that it does. |
| 10 | Analyses in `compile` | `compile` plans models, data tests, snapshots and functions only; analyses are never compiled. | dbt compiles every `analyses/` file so its SQL can be inspected. An analysis is not part of the run's DAG here, and the planner does not select that resource type. |
| 11 | Full `list` output | `list` returns the selected nodes as ids with a success status. It leaves out sources, exposures, metrics and analyses. | dbt lists every resource type. The workflow answers from the plan, which holds only the node types the other commands execute. |

Not planned:

- **`--threads`.** Concurrency is a property of the worker
  (`WORKER_MAX_CONCURRENT_ACTIVITIES` and the per-run in-flight cap), not of one run.
- **`docs generate` as a command.** A run writes `catalog.json` when catalog
  writing is enabled; there is no separate command.
- **`column:` selector method.** dbt-internal column lineage.

## Supported, with a difference

| Area | dbt | Here | Why |
|------|-----|------|-----|
| Parse-time `var()` and `env_var()` | Every invocation parses, so `--vars` reach `config()` blocks and `dbt_project.yml`. | The worker parses once at start-up. A run's `vars` and `env` reach everything rendered at run time; values read during parsing keep their start-up value. The planner warns when a run's vars appear in a `config()` block. | Parsing per run would cost seconds per workflow. Relation names are recomputed per run for env overrides; see [env overrides](env-overrides.md). |
| `manifest.json` | Written from the invocation's own parse. | Written from the start-up parse, with the run's selection. | Same cause. |
| Microbatch models | One batch per period in the window, each retried on its own. | One activity covers the window given by `event_time_start` and `event_time_end`. | Per-batch activities are the natural Temporal shape and are not built yet. |
| `run_results.json` `thread_id` | The dbt thread name. | Always `main`. | There are no dbt threads. Nothing in dbt reads the field. |
| Nodes the run never reached, in `run_results.json` | A node skipped after a failure is `skipped`. An interrupted dbt Core run has no row for the nodes it did not reach. | A cancelled run records them as `cancelled`, which dbt has no word for; a run that fails after some nodes ran records them as `skipped`. `retry_from` re-runs both. | `cancelled` tells an operator the node was stopped, not that something upstream failed. |
| Failed `on-run-end` hook in `run_results.json` | A failed hook adds an `operation.<project>.<hook>` row. | No row. The failure is in `hook_errors` and fails the run. | `run_results.json` is stored before the hooks run. |
| `generate_alias_name` under an env override | Every invocation calls it. | Called at start-up only. An override recomputes database and schema, not the alias. | To be aligned. Matters only to an alias macro that reads the environment or `node.schema`. |
| Snapshot `target_schema` / `target_database` under an env override | dbt skips the naming macros for a snapshot that sets them. | The naming macros run for it. | To be aligned. |
| `@` with `+` (`@+model`) | dbt Core applies both. dbt v2 lets the `+` win. | Follows dbt v2. | The worker embeds dbt v2. |
| Artifact storage that fails | `run_results.json` is written after the nodes ran, and `on-run-end` runs regardless. | The `store_artifacts` activity retries under the project's retry policy: 3 attempts by default, a 5 s first interval doubling to at most 60 s, and 120 s per attempt (`retry` and `timeouts.store_artifacts_secs` in `dbt_temporal.yml`). If it still fails, the run fails and `on-run-end` does not run. | The workflow stores artifacts before it runs `on-run-end`. dbt has no storage step that can fail. Accepted as it is: a store that fails after its retries ends the run at once. |

## How to take an item

1. Read what dbt v2 does in its source at the pinned release, not in the
   documentation: the two disagree in places, and the worker embeds the source.
2. Decide whether the feature changes the plan, the render context, or both.
   Plan changes belong in `src/activities/plan/` and `selectors/`; render
   changes in `src/activities/render_env.rs` and `execute_node/`.
3. A new input field goes on `DbtRunInput` and in
   [workflow input](workflow-input.md). The input does not reject unknown
   fields, so a worker older than the field ignores it silently: say so in the
   field's description when ignoring it changes what the run does.
4. Add a DuckDB scenario that fails without the feature.
5. Remove the row from this page, and from the selector table if it has one.
