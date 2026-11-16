# Per-Workflow Environment Variables and Multi-Tenant Runs

Run one dbt project against many warehouses, schemas or tenants from a single
worker: each workflow passes its own `env` map, which `env_var()` in models,
macros and `profiles.yml` resolves against.

Each workflow can provide its own environment variable overrides via the `env` field. These override `env_var()` calls in model SQL, macros, and (when profiles.yml uses `env_var()`) database connection configuration. Parallel workflows are fully isolated — no shared process-level state.

```bash
temporal workflow start \
  --type dbt_run \
  --task-queue dbt-tasks \
  --input '{
    "command": "run",
    "env": {
      "DB_HOST": "warehouse-a.example.com",
      "DB_SCHEMA": "analytics_team_a",
      "DB_PASSWORD": "hunter2"
    }
  }'
```

## How it works

1. **Model SQL / macros**: Each `execute_node` activity clones the shared Jinja environment and re-registers `env_var()` with a closure that checks the workflow's env map before falling back to process-level env vars. This means `{{ env_var('MY_SCHEMA') }}` in a model will resolve to the workflow's override if present.

2. **Database connections**: At worker startup, profiles.yml is scanned for `env_var()` usage. If detected and a workflow provides `env` overrides, the profile is re-rendered with the workflow's env map and fresh adapter engines (database connections) are built per workflow — one per adapter the target declares, since credentials for a non-default adapter go stale under an override the same way. If profiles.yml does not use `env_var()`, this step is skipped and the shared engines are used.

3. **Fallback**: Any env var not present in the workflow's `env` map falls through to the process environment. This means you only need to override the variables that differ between workflows.

## Example: profiles.yml with env_var()

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

With this profile, two parallel workflows can target different databases:

```bash
# Workflow A: writes to warehouse-a / schema analytics_a
temporal workflow start --type dbt_run --input '{
  "command": "run",
  "env": {"DB_HOST": "warehouse-a", "DB_SCHEMA": "analytics_a"}
}'

# Workflow B: writes to warehouse-b / schema analytics_b
temporal workflow start --type dbt_run --input '{
  "command": "run",
  "env": {"DB_HOST": "warehouse-b", "DB_SCHEMA": "analytics_b"}
}'
```

Both workflows run on the same worker, share the parsed project state (DAG, macros, compiled SQL templates), but each gets its own database connection and env_var resolution.

## When the adapter engines are rebuilt

The per-workflow adapter engine rebuild happens when the workflow can resolve to a different profile than the worker started on — either of:

- The workflow's `target` names a target other than the profile's default. A different `outputs:` block means different credentials, schema and database, whether or not the profile reads env vars.
- The workflow's `env` overrides a variable **that `profiles.yml` actually reads**.

If neither holds, the shared engines from worker startup are used with zero overhead.

The second condition is by key, not by "did the workflow supply any overrides". Every run carries an `_` override — the serialized workflow input, so models can read `env_var('_')` — so a test on "any override" would always hold, and any profile reading any env var would rebuild its engines once per node activity.

The startup scan is deliberately generous in every uncertain case. It matches `env_var` followed by its opening paren with any whitespace between and counts mentions in comments; and if a key cannot be named — a computed `env_var(some_var)`, or a file that cannot be read — every override is assumed relevant. Naming one key too many costs a rebuild; missing one would silently pin the run to startup credentials.

## Where the values are visible

A run's input is recorded by Temporal, so treat `env` and `vars` as readable by
anyone who can read the namespace:

- The whole input, `env` included, is in the workflow's history, and in the
  input of every activity the run schedules.
- A [lifecycle hook](hooks.md#hook-payload) receives the run's input in its
  payload, so `env` is also in the history of each hook's child workflow.
  Values a `pre_run` hook returns as `extra_env` are recorded the same way.
- `vars` are written to the workflow's memo, which the Temporal UI shows and
  which list queries return.

Pass a reference in `env`, not the credential: the name of a secret, a role, an
account. Keep the credential in the worker's own environment or in the
warehouse's native authentication (workload identity, key-pair files mounted
into the pod), where `profiles.yml` reads it without it entering a workflow. If
a secret has to travel in the input, run the namespace with a
[payload codec](https://docs.temporal.io/payload-codec) that encrypts payloads;
the worker itself applies none.

A run's `hooks` field replaces the project's lifecycle hooks for that run.
Anyone who can start a workflow can therefore run without a hook the project
configures, so do not rely on a lifecycle hook as an access control.

## Security: restricting what a workflow can override

A workflow's `env` and `target` win over the worker's own environment and
default target, and the worker authenticates with whatever the resulting
`profiles.yml` names. Warehouse credentials are best kept in the worker's
environment (see above), which means the worker presents them to whatever host
the profile resolves to. **Without a restriction, anyone who can start a
`dbt_run` workflow can set `DB_HOST` (or an account, role, project, or a
different `target`) to a server they control, and the worker will send its
password, token or key to it.** Being allowed to start workflows is therefore
also being allowed to redirect the worker's credentials.

The restriction is worker-side and off by default, so existing deployments keep
working. Two optional settings list what a workflow may override:

| Variable | Restricts |
|----------|-----------|
| `ALLOWED_ENV_OVERRIDES` | the names in the workflow's `env` |
| `ALLOWED_TARGETS` | the workflow's `target` |

```bash
ALLOWED_ENV_OVERRIDES=DB_SCHEMA,TENANT_ID
ALLOWED_TARGETS=prod,staging
```

Both take comma-separated, exact names (no patterns; case matters). For each:

- **Unset**: no restriction, as before.
- **Set to a list**: only those names may appear.
- **Set to an empty string**: nothing may be overridden.

List the variables that choose *data* (a schema, a tenant id) and leave out
every variable `profiles.yml` uses to say *where to connect or who to be*: host,
port, account, role, project, user, credentials. The worker's default target is
always selectable; `ALLOWED_TARGETS` lists the others.

A run that names anything else is refused, not trimmed: the planning activity
fails with a configuration error naming the offending keys, and it is not
retried. Dropping the override and running on would execute the SQL against the
worker's own warehouse, which is not what the caller asked for. The check
covers every use of the map — `profiles.yml`, `env_var()` in model SQL and
macros, hook SQL — and also the `extra_env` a `pre_run` hook returns, which the
planning activity cannot see; a node or hook that receives a refused key fails
the same way. The `_` entry, which the worker fills with the serialized input,
is never refused.

The allowlist limits which names can be set, not the values: a listed name can
still carry any value. Do not list a name that selects a host or an identity. It
also does not stop anyone who can start workflows from reading or running
anything the project itself can do, nor does it replace namespace-level access
control in Temporal.

## Known limitations

Per-workflow env overrides work fully for `env_var()` in **profiles.yml**, **model SQL**, and **macros**. The common pattern of setting schema/dataset and database/project in profiles.yml via `env_var()` is fully supported — each workflow gets its own connection, `target` context, and relation metadata. Static custom schemas (e.g. `+schema: staging` in `dbt_project.yml` or `config(schema='staging')` in SQL) are also correctly recomputed against the per-workflow target.

Edge cases with limited support:

- **`config(schema=env_var(...))` in model SQL**: If a model uses `env_var()` inside its in-file `config()` block to set the custom schema name (not in profiles.yml or dbt_project.yml), that value is evaluated once at worker startup and baked into node metadata. Per-workflow overrides for that env var will not change the custom schema. This is an unusual pattern — the standard approach is `env_var()` in profiles.yml (fully supported) or static `+schema` in dbt_project.yml (fully supported).

- **Custom naming macros**: A project's `generate_schema_name` and `generate_database_name` are called again for the run, with the custom name and the `node` dbt passes at parse time, so a macro that branches on node attributes or reads `env_var()` gives the schema a fresh parse under that environment would. `generate_alias_name` is not called again: an alias keeps its start-up value. A snapshot that sets `target_schema` or `target_database` goes through the naming macros here, where dbt skips them.
