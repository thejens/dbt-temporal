# Per-Workflow Environment Variables

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

2. **Database connections**: At worker startup, profiles.yml is scanned for `env_var()` usage. If detected and a workflow provides `env` overrides, the profile is re-rendered with the workflow's env map and a fresh adapter engine (database connection) is built per workflow. If profiles.yml does not use `env_var()`, this step is skipped and the shared adapter engine is used.

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

## When the adapter engine is rebuilt

The per-workflow adapter engine rebuild only happens when **both** conditions are true:

1. `profiles.yml` contains `env_var(` (detected once at worker startup)
2. The workflow's `env` field is non-empty

If either condition is false, the shared adapter engine from worker startup is used with zero overhead.

## Known limitations

Per-workflow env overrides work fully for `env_var()` in **profiles.yml**, **model SQL**, and **macros**. The common pattern of setting schema/dataset and database/project in profiles.yml via `env_var()` is fully supported — each workflow gets its own connection, `target` context, and relation metadata. Static custom schemas (e.g. `+schema: staging` in `dbt_project.yml` or `config(schema='staging')` in SQL) are also correctly recomputed against the per-workflow target.

Edge cases with limited support:

- **`config(schema=env_var(...))` in model SQL**: If a model uses `env_var()` inside its in-file `config()` block to set the custom schema name (not in profiles.yml or dbt_project.yml), that value is evaluated once at worker startup and baked into node metadata. Per-workflow overrides for that env var will not change the custom schema. This is an unusual pattern — the standard approach is `env_var()` in profiles.yml (fully supported) or static `+schema` in dbt_project.yml (fully supported).

- **Custom `generate_schema_name` macros**: Schema patching follows the default dbt convention (`target.schema` for no custom, `target.schema_custom` for custom schemas). Projects with a `generate_schema_name` override that uses a different naming pattern may not get correct context-variable patching. The `target` Jinja global is always correct, so materializations and macros that read `target.schema` directly will work.
