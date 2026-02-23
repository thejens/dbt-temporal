# dbt-temporal: Upstream dbt-fusion API improvements

Friction points in dbt-fusion's API that cause unnecessary workarounds in our render/run pipeline.

## Upstream changes (dbt-fusion PRs)

- [ ] **Accept `Option<ResultStore>` in context builders**
  `extend_base_context_stateful_fn()` and `build_compile_and_run_base_context()` unconditionally
  create a fresh `ResultStore::default()`. We create our own store, then they overwrite it, then
  we re-inject ours. Three stores created per node, two wasted.
  - `crates/dbt-jinja-utils/src/phases/run/run_node_context.rs` — `extend_base_context_stateful_fn()`
  - `crates/dbt-jinja-utils/src/phases/compile_and_run_context.rs` — `build_compile_and_run_base_context()`
  - Add `Option<ResultStore>` parameter, default to `ResultStore::default()` when `None`.

- [ ] **Add `call()` to `RunConfig`**
  `RunConfig` only implements `call_method()`, not `call()`. Raw SQL can contain `{{ config(...) }}`
  which invokes config as a function. We work around this with a NoopConfig swap before/after
  raw SQL compilation. `CompileConfig` already implements `call()` returning `""` — `RunConfig`
  should do the same.
  - `crates/dbt-jinja-utils/src/phases/run/run_config.rs`

- [ ] **Set `execute` as a Jinja global in `configure_compile_and_run_jinja_environment()`**
  Currently only set as a context variable. Cross-template macros (e.g. `statement.sql`) resolve
  variables from globals, not the caller's render context. We manually call
  `env.add_global("execute", true)` to work around this.
  - `crates/dbt-jinja-utils/src/phases/compile_and_run_context.rs`

- [ ] **Set `TARGET_PACKAGE_NAME` in `build_run_node_context()`**
  The compile-phase context builder sets this but the run-phase one doesn't. Required by
  `ConfiguredVar` (the `var()` function) to resolve project-scoped variables. We inject it
  manually from `common_attr.package_name`.
  - `crates/dbt-jinja-utils/src/phases/run/run_node_context.rs`

## Local cleanup (dbt-temporal)

- [x] **Remove debug SQL write to `/tmp`**
  ~~`src/activities/execute_node.rs:411-414` unconditionally writes compiled SQL to
  `/tmp/dbg-final-*.sql`. Should be removed or gated behind a debug flag.~~

- [ ] **Remove ResultStore re-injection** (after upstream accepts `Option<ResultStore>`)
  `src/activities/execute_node.rs:216-225` and `297-305` — pass our store to upstream instead.

- [ ] **Remove NoopConfig** (after upstream adds `call()` to `RunConfig`)
  `src/activities/execute_node.rs:636-758` — delete `NoopConfig` struct and the config swap
  at lines 391-409.

- [ ] **Remove `execute` global workaround** (after upstream fix)
  `src/activities/execute_node.rs:192-194`

- [ ] **Remove `TARGET_PACKAGE_NAME` injection** (after upstream fix)
  `src/activities/execute_node.rs:309-312`

## Cross-workflow env isolation

Per-workflow `env` overrides correctly isolate the Jinja `env_var()` function (cloned per
activity) and rebuild the adapter engine when `profiles.yml` uses `env_var()`. However,
`resolver_state` is built once at worker startup with the process's env — so node metadata
baked in during resolution reflects startup env, not per-workflow overrides.

- [x] **Node relation metadata uses startup env, not per-workflow env**
  Fixed in two parts:
  1. `target` / `env` Jinja globals are patched with per-workflow schema/database/target_name
     so all macros (generate_schema_name, materializations, custom macros) see correct values.
  2. `this`, `database`, `schema` context variables are recomputed for each node using the
     default `generate_schema_name` pattern: no custom schema → `target.schema`, custom →
     `target.schema_custom`. Handles both nodes without custom schema and nodes with static
     `config(schema='...')`.
  - `src/worker/profile.rs` — `RebuildResult` struct, returns schema/database
  - `src/worker_state.rs` — `default_schema`/`default_database` fields
  - `src/activities/execute_node.rs` — `patch_target_global`, relation patching
  Remaining limitation: nodes with `config(schema=env_var(...))` where the custom schema name
  itself came from env_var() — the evaluated value is baked at resolution time. Also, projects
  with a custom `generate_schema_name` macro that doesn't follow the default
  `target.schema_custom` pattern will fall back to no patching for the context variables
  (though the `target` global IS correct, so the macro would produce the right result if
  re-invoked during materialization).

- [x] **`compiled_sql_cache` reflects startup env** — non-issue
  The startup-compiled SQL from `compiled_sql_cache` is written to the activity temp dir, but
  immediately overwritten when the raw SQL is re-rendered with per-workflow env
  (`execute_node.rs:505-521`). The `{{ sql }}` and `{{ compiled_code }}` context variables are
  set to the per-workflow compiled SQL. The only stale path is `model.compiled_code` from the
  serialized node YAML — standard materializations use `{{ sql }}`, not `model.compiled_code`.

- [x] **`test_sql_cache` does NOT contain pre-resolved `env_var()` values** — non-issue
  Investigated: the resolver generates test SQL as Jinja macro call templates (e.g.
  `{{ test_not_null(model=get_where_subquery(ref('customers')), ...) }}`). These do NOT
  contain evaluated `env_var()` values — `ref()` and `source()` calls are preserved as Jinja
  expressions and re-evaluated at compile time with per-workflow env. `env_var()` calls appear
  in the test config section (schema/database/alias), which is baked into node metadata — but
  this is already handled by the relation patching above.
