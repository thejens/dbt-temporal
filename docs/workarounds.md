# dbt-fusion Workarounds

dbt-temporal uses [dbt-fusion](https://github.com/dbt-labs/dbt-fusion) (Rust) as its rendering and execution engine. The crates are designed for the dbt CLI's single-invocation model, not for a long-lived worker that runs multiple workflows concurrently. Several workarounds are in place to bridge this gap.

## 1. `ResultStore` not injectable into context builders

**Issue:** [dbt-labs/dbt-core#14245](https://github.com/dbt-labs/dbt-core/issues/14245) (open; a draft PR was floated in review but not merged)

`build_compile_and_run_base_context()` and `extend_base_context_stateful_fn()` (called inside `build_run_node_context`) each create their own `ResultStore` and inject its closures (`store_result`, `load_result`, `store_raw_result`) into the context. Callers that need to read adapter responses after materialization must use their own `ResultStore`, but the context builders overwrite its closures.

**Workaround:** We create our `ResultStore` upfront, inject its closures into `base_context`, then re-inject after `build_run_node_context` overwrites them in `node_context`. Three stores are allocated per node; two are immediately discarded.

## 2. `ref()` resolves schemas at parse time, not execution time

`ref()` calls are resolved during project parsing and baked into `Relation` objects with the startup default schema. When a per-workflow env override changes `DB_SCHEMA`, downstream models still reference the old schema in their compiled SQL (e.g. `"waffle_hut"."default_schema"."stg_customers"` instead of `"waffle_hut"."override_schema"."stg_customers"`).

**Workaround:** After Jinja renders the raw SQL, we string-replace quoted occurrences of the startup default schema with the per-workflow schema in the compiled SQL. This is brittle — it assumes the schema appears as a quoted identifier and that no column or alias happens to match the schema name.

## 3. ADBC PostgreSQL driver built from source (macOS ARM64)

**Issue:** not yet filed. (This section previously linked `dbt-labs/arrow-adbc#31`, but that
number turned out to be an unrelated merged PR — a stale/incorrect reference, not a real
filing. Searched `dbt-labs/arrow-adbc` for existing reports of this SIGSEGV; found none.)

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
|---|---|
| **Telemetry compatibility layer** | dbt-fusion's adapter code expects `TelemetryAttributes` in span extensions (normally inserted by `TelemetryDataLayer`). Without it, `record_current_span_status_from_attrs` panics. We insert a default `TelemetryAttributes` for every span via [`DbtTelemetryCompatLayer`](../src/telemetry_compat.rs). |
| **Forked arrow-rs and ring** | dbt-fusion uses forked versions of `arrow-rs` (v56, sdf-labs fork) and `ring` (sdf-labs fork). Without matching `[patch.crates-io]` entries, version conflicts prevent compilation. See `Cargo.toml`. |
| **Ephemeral CTE injection** | Ephemeral models are excluded from the execution plan. We detect `__dbt__cte__` references in compiled SQL and recursively compile + inline the ephemeral models as CTEs. |

## Candidate upstream issues

Found while building error-classification/retry tests. **Filing requires
approval — check with the repo owner before opening any of these**, and never
mention dbt-temporal by name in a filed issue body (see internal filing policy).

**2026-07-08 audit:** a duplicate check turned up prior filings against
`dbt-labs/dbt-core` under the `thejens` account that predate this doc and
were never reconciled into it:

- [#14245](https://github.com/dbt-labs/dbt-core/issues/14245) `[FEAT] ResultStore` — open, matches item 1 above.
- [#14244](https://github.com/dbt-labs/dbt-core/issues/14244) `RunConfig` missing `call()` — **closed, fixed upstream in preview.134**
  (`dbt-labs/fs#7600`). Confirmed fixed at our pinned rev `37ba42bd`
  (`RunConfig` now implements `fn call`) — the `NoopConfig` swap workaround in
  `src/activities/execute_node.rs` (see `todo/upstream-dbt-fusion-api.md` item 2)
  is dead code and should be removed.
- [#14243](https://github.com/dbt-labs/dbt-core/issues/14243) `execute` not a Jinja global — **closed**, maintainer couldn't
  reproduce via the CLI and closed once we confirmed it's a library-consumer-only
  issue (`configure_compile_and_run_jinja_environment` embedders, not `dbt run`
  itself). Our workaround (item 3 in the todo file) is still needed on our side;
  re-filing isn't likely to land differently without a CLI-visible repro.
  **Note:** a follow-up comment on that thread was posted from the `jens-gilion`
  (work) account on a `thejens`-owned personal issue — an identity mixup worth
  being aware of before commenting further on that thread.
- `TARGET_PACKAGE_NAME` in `build_run_node_context()` (item 4 in the todo file) was
  only ever raised as a *comment* on the now-closed
  [#14148](https://github.com/dbt-labs/dbt-core/issues/14148), which fixed a
  narrower case (query-comment macros) but not the general run-phase gap.
  Confirmed still missing at `37ba42bd` (`grep TARGET_PACKAGE_NAME` on
  `run_node_context.rs` finds nothing) — still a candidate, needs its own issue.

Also found while auditing: three older, still-open reports not previously tracked in
this file — [#14550](https://github.com/dbt-labs/dbt-core/issues/14550) (DISPATCH_CONFIG
TOCTOU race), [#14551](https://github.com/dbt-labs/dbt-core/issues/14551) (dbt-antlr4 pin),
[#14552](https://github.com/dbt-labs/dbt-core/issues/14552) (`..` resource path misresolution).

### Confirmed against the current pinned rev (`37ba42bd`)

**DuckDB adapter never populates SQLSTATE, and collapses every error to one
`AdapterErrorKind`.** `dbt_common::AdapterError::sqlstate()` returns the
`"00000"` placeholder and `kind()` returns `Driver` for *every* DuckDB failure
we triggered — a dropped connection (`IO Error: Cannot open file ...`), a
missing table (`Catalog Error: ...`), and a syntax error (`Parser Error: ...`)
are structurally indistinguishable; only the free-text message differs.
Postgres/Redshift-family ADBC drivers populate real SQLSTATE codes for the
same error classes. This makes transient-vs-permanent classification (needed
for any retry logic) impossible via structured fields for DuckDB specifically
— callers are forced to pattern-match message text. Reproduced via
`tests/common/duckdb.rs` / `tests/duckdb_scenarios.rs` in this repo.

**`FsError` built from an `AdapterError` (via `run_query()` / `render_str`)
doesn't preserve the original error as `cause`.** Calling a materialization
macro directly (`Template::call`) surfaces adapter failures as a
`minijinja::Error` whose `Error::source()` chain still contains the original
`dbt_common::AdapterError` (kind/sqlstate/vendor_code intact). But
`run_query()` (via `JinjaEnv::render_str`, returning `FsResult<T> =
Result<T, Box<FsError>>`) converts the same underlying `AdapterError` into an
`FsError` (`dbt-error` crate) whose only preserved signal is a coarser
`ErrorCode` (derived from the AdapterError's kind/sqlstate) — `FsError.cause`
is `None`, so the original `AdapterError` is unrecoverable. Two code paths
handling the same failure class, one preserving full detail and one
discarding it; `FsError`'s constructors would need a variant that also
attaches `cause` from the source `AdapterError` for parity. Worked around in
`src/error.rs::classify_adapter_execution_error`, which checks `FsError.code`
directly as a fallback signal.

### From an earlier rev (`2928c13`, ~2026-06) — re-verify before filing

Observed two bumps back; confirm each still reproduces against `37ba42bd`
before filing (upstream cleanup may have already addressed some of these).

- **`State::lookup` on a missing macro name recurses without bound and
  overflows the stack.** A genuine crash bug — highest filing priority in
  this group if still reproducible.
- **Source freshness YAML silently drops `loaded_at_field`/`freshness` when
  not nested under `config:`** (dbt 1.10+ shape) — no parse error, just an
  ERROR-level log line. Silent data loss for a user writing the older (still
  common) YAML shape.
- **`PostgresMetadataAdapter::list_relations_schemas_inner` is `todo!()`** and
  panics; inside a long-lived worker activity this wedges the worker until
  the activity timeout fires, presenting as a hang rather than a crash.
- **Bundled `get_unit_test_sql` macro has no `ORDER BY`**, so
  `compare_record_batches`'s positional row comparison is nondeterministic
  against unordered query results.
- **`get_fixture_sql(rows, column_name_to_data_types)` emits broken SQL**
  (`as ` with an empty column name) when `column_name_to_data_types` is passed
  rather than `none`.
- **`DbtManifest` does not round-trip through `serde_json`** — dbt-fusion's
  own emitted `manifest.json` fails against the derived `Deserialize` impl
  (demands a literal `__warehouse_specific_config__` field the serializer
  doesn't always produce). Requires the `typed_struct_from_json_str`
  (JSON → YmlValue → typed) path instead of `serde_json::from_slice` directly.
