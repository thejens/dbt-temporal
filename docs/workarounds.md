# dbt-fusion Workarounds

dbt-temporal uses [dbt-fusion](https://github.com/dbt-labs/dbt-fusion) (Rust) as its rendering and execution engine. The crates are designed for the dbt CLI's single-invocation model, not for a long-lived worker that runs multiple workflows concurrently. Several workarounds are in place to bridge this gap.

## 1. `RunConfig` missing `call()` — `{{ config(...) }}` no-op during execution

**Issue:** [dbt-labs/dbt-fusion#1290](https://github.com/dbt-labs/dbt-fusion/issues/1290)

Model SQL frequently contains `{{ config(materialized='table') }}`. During the run phase this should be a silent no-op (the config was already parsed), but `RunConfig` only implements `call_method()` — not `call()` — so minijinja errors when invoking config as a function.

**Workaround:** Before compiling raw SQL, we swap the `config` context variable to a `NoopConfig` object that implements both `call()` and `call_method("get",...)`. After compilation, we restore `RunConfig` so materialization macros can still use `config.get()`.

## 2. `execute` not set as Jinja global

**Issue:** [dbt-labs/dbt-fusion#1289](https://github.com/dbt-labs/dbt-fusion/issues/1289)

`execute` is only set as a context variable in `build_compile_and_run_base_context()`, not as a Jinja global. Cross-template macros (like `statement()` in `statement.sql`) resolve variables from globals, not the caller's render context — so they see `execute` as undefined and skip SQL execution.

**Workaround:** We call `jinja_env.env.add_global("execute", true)` before rendering.

## 3. `ResultStore` not injectable into context builders

**Issue:** [dbt-labs/dbt-fusion#1291](https://github.com/dbt-labs/dbt-fusion/issues/1291)

`build_compile_and_run_base_context()` and `extend_base_context_stateful_fn()` each create their own `ResultStore` and inject its closures (`store_result`, `load_result`, `store_raw_result`) into the context. Callers that need to read adapter responses after materialization must use their own `ResultStore`, but the context builders overwrite its closures.

**Workaround:** We create our `ResultStore` upfront, let the context builders overwrite the closures, then re-inject our store's closures into both the context and Jinja globals. Three stores are allocated per node; two are immediately discarded.

## 4. `ref()` resolves schemas at parse time, not execution time

`ref()` calls are resolved during project parsing and baked into `Relation` objects with the startup default schema. When a per-workflow env override changes `DB_SCHEMA`, downstream models still reference the old schema in their compiled SQL (e.g. `"waffle_hut"."default_schema"."stg_customers"` instead of `"waffle_hut"."override_schema"."stg_customers"`).

**Workaround:** After Jinja renders the raw SQL, we string-replace quoted occurrences of the startup default schema with the per-workflow schema in the compiled SQL. This is brittle — it assumes the schema appears as a quoted identifier and that no column or alias happens to match the schema name.

## 5. ADBC PostgreSQL driver built from source (macOS ARM64)

**Issue:** [dbt-labs/arrow-adbc#31](https://github.com/dbt-labs/arrow-adbc/issues/31)

The CDN-shipped ADBC PostgreSQL driver is built with `-undefined dynamic_lookup`,
which defers unresolved symbols to `dlopen` time. On macOS, OpenSSL symbols
resolve to NULL (macOS ships LibreSSL, not OpenSSL) and GSSAPI symbols are
ABI-incompatible with macOS's Heimdal. This causes a `SIGSEGV` (NULL function
pointer call inside `pqConnectOptions2`) before any connection parameters —
including `sslmode=disable` — are processed.

**Workaround:** Build the driver from source, statically linking Homebrew's libpq
and OpenSSL. Run:

```bash
brew install cmake libpq openssl@3
bash scripts/build-adbc-postgres.sh
```

The script:
1. Clones `dbt-labs/arrow-adbc` at a pinned revision (`2caa4db`).
2. Patches `BuildUtils.cmake` to remove `-undefined dynamic_lookup`, so the
   linker fails fast on unresolved symbols instead of deferring them.
3. Configures cmake with Homebrew `libpq`/`openssl@3` and links
   `libpgcommon_shlib.a`, `libpgport_shlib.a`, `libssl.a`, `libcrypto.a`
   statically. Uses system `-lgssapi_krb5` (Heimdal) which is safe — GSSAPI
   is only invoked when the connection requests it.
4. Installs the dylib to
   `~/Library/Caches/com.getdbt/adbc/aarch64-apple-darwin/libadbc_driver_postgresql-0.21.0+dbt0.21.0.dylib`,
   where dbt-temporal picks it up automatically.

## Other temporary hacks

| Workaround | Description |
| **Telemetry compatibility layer** | dbt-fusion's adapter code expects `TelemetryAttributes` in span extensions (normally inserted by `TelemetryDataLayer`). Without it, `record_current_span_status_from_attrs` panics. We insert a default `TelemetryAttributes` for every span via [`DbtTelemetryCompatLayer`](../src/telemetry_compat.rs). |
| **Forked arrow-rs / arrow-adbc / ring** | dbt-fusion uses forked versions of `arrow-rs` (v56, sdf-labs fork), `arrow-adbc` (dbt-labs fork), and `ring` (sdf-labs fork). Without matching `[patch.crates-io]` entries, version conflicts prevent compilation. See `Cargo.toml`. |
| **Ephemeral CTE injection** | Ephemeral models are excluded from the execution plan. We detect `__dbt__cte__` references in compiled SQL and recursively compile + inline the ephemeral models as CTEs. |
