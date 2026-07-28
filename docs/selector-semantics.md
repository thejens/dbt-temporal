# `path:` Selector + Ephemeral Chains

**Question:** when a `path:` selector covers a directory whose models depend on
each other via an *ephemeral* helper, and that ephemeral helper transitively
`ref()`s a model **outside** the selected path, does dbt-temporal (via
dbt-fusion) behave the same as vanilla Python dbt?

**Answer: yes, identical behavior.** The `path:` selector does not traverse
ephemeral chains to pull in upstream deps; the consumer's compiled SQL ends up
with a direct table reference to the un-selected upstream and fails with
"relation does not exist" against an empty target schema. This is documented
dbt selector semantics, not a bug in either engine, and not something
dbt-temporal patches around.

## Repro project

```
models/stages/01a/source.sql              -- table, no refs
models/stages/01a/eph_using_01b.sql       -- ephemeral, refs forward_dep_target
models/stages/01a/consumer.sql            -- table, refs eph_using_01b
models/stages/01b/forward_dep_target.sql  -- table, refs source
```

Selector: `--select path:models/stages/01a` — should select the three models
under `01a/`, leaving `forward_dep_target` (in `01b/`) out.

## Selection (`dbt list` equivalents)

**Python dbt** (`dbt list --select path:models/stages/01a`):
```
consumer
eph_using_01b
source
```

**dbt-temporal** (workflow plan log):
```
Found 2 NODE_TYPE_MODELs        ← consumer + source (eph inlined)
1 of 2 START table model … source
2 of 2 START table model … consumer
```

Same selection — the "2" vs "3" difference is only that the plan log counts
non-ephemeral nodes (ephemerals don't execute as their own activity); the
manifest still has all three.

## Compiled SQL of `consumer`

Both engines emit the same shape — a CTE that selects from the un-selected
upstream table:

```sql
with __dbt__cte__eph_using_01b as (
    SELECT id, enriched_name AS display
    FROM "pg"."pselect_repro"."forward_dep_target"   -- direct table ref
)
SELECT *
FROM __dbt__cte__eph_using_01b
```

dbt-temporal's ephemeral-CTE injection (see `docs/workarounds.md`, "Ephemeral
CTE injection") produces the same SQL semantics as Python dbt's own inlining —
just wrapped with `--EPHEMERAL-SELECT-WRAPPER-START/END` markers instead of
Python dbt's native CTE assembly.

## Run against an empty schema

Both engines' `run --select path:01a` against a freshly-created empty target
schema produce the same result:

| Engine | Stats | Failure on `consumer` |
|--------|-------|----------------------|
| Python dbt | PASS=1 ERROR=1 | `relation "pselect_repro.forward_dep_target" does not exist` |
| dbt-temporal | PASS=1 ERROR=1 | `relation "pselect_repro.forward_dep_target" does not exist` |

Identical failure message, identical exit shape.

## Why this is not a divergence

dbt's `path:` selector is a *file-path match*. It does not expand the graph
along ephemeral edges. Ephemeral models are inlined into their consumers'
compiled SQL — which means the consumer's executed SQL references whatever
the ephemeral's `ref()` resolves to, *as a table name*. If that table doesn't
exist (because it wasn't selected for the run), the target database raises
the error. Both Python dbt and dbt-fusion honor this; there's no "smarter"
engine here.

**Takeaway for project authors:** if a `path:`-selected subdirectory's models
depend — even transitively through an ephemeral model — on a model in another
directory, either move the shared model into the selected path or run with a
broader selector (e.g. `+leaf_model`) so dbt resolves the upstream dependency
for you.

---

# Supported selector methods

dbt-temporal parses the full dbt selector grammar (via `dbt-common`) but
evaluates a subset of the methods. Anything outside this list is **rejected**
at plan time with an error naming the method.

| Method | Supported | Notes |
|---|---|---|
| *(bare name)* | yes | `fqn` — exact node name, or dotted FQN prefix, `*` wildcards |
| `tag:` | yes | exact tag match |
| `path:` | yes | whole-component path prefix (a file, or a directory and everything under it) |
| `package:` | yes | exact package name |
| `resource_type:` | yes | `model`, `test`, `seed`, `snapshot`, `unit_test`, … |
| `config.materialized:` | yes | the only `config.` sub-selector |
| `state:new` | yes | requires `state_manifest_ref` |
| `state:modified[.*]` | yes | all `modified.<sub>` forms coarsen to the full modified set |
| `config.<other>:` | **no** | |
| `state:old`, `state:unmodified` | **no** | no backing set is computed |
| `file:` | **no** | note a bare `foo.sql` value parses as `file:` |
| `source:`, `exposure:`, `metric:`, `saved_query:`, `semantic_model:`, `function:` | **no** | those resource types have no execution path either |
| `test_name:`, `test_type:`, `group:`, `access:`, `version:`, `result:`, `source_status:`, `column:` | **no** | |

Graph operators (`+model`, `model+`, `N+model`, `@model`), unions (space),
intersections (comma) and nested excludes all work with any supported method.

## Why rejection rather than "matches nothing"

An unevaluable method contributes an empty match set. Alone that surfaces as
"no nodes matched", but in the two positions that matter it is silent:

- inside a union (`--select "tag:nightly source:raw"`) it drops the nodes the
  second half asked for, and the run reports success having built less than
  requested;
- in `--exclude` it excludes nothing, so the run builds *more* than requested.

Both produce a green run with the wrong node set, so the planner fails fast
instead.

## `--indirect-selection` and `selectors.yml`

Neither is supported. dbt's `--indirect-selection` modes (`eager`, `cautious`,
`buildable`, `empty`) govern how tests are pulled in alongside selected models;
dbt-temporal always behaves as `eager` and additionally injects its own test
gates (see the `architecture-invariants` notes). Named selectors from
`selectors.yml` are not read — pass the expanded selector string instead.
