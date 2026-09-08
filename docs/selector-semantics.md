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

dbt-temporal parses the full dbt selector grammar (via `dbt-common`) and
evaluates every method decidable from the parsed manifest, plus the `state:`
comparison against a previous manifest. The four that need data the worker does
not have at plan time are **rejected** with an error naming the method.

Values are matched the way dbt matches them: verbatim when the value holds no
pattern characters, and as a shell glob (`*`, `?`, `[a-z]`, `[!a-z]`, and `**`
as a whole path component) when it does.

| Method | Supported | Notes |
|---|---|---|
| *(bare name)* | yes | `fqn` — node name, unique id, or dotted FQN prefix; versioned models answer to `name`, `name.v2` and `name_v2` |
| `tag:` | yes | |
| `path:` | yes | whole-component path prefix (a file, or a directory and everything under it) |
| `file:` | yes | file name or stem; a bare `foo.sql` value parses as this |
| `package:` | yes | `this` resolves to the root project |
| `resource_type:` | yes | `model`, `test`, `seed`, `snapshot`, `unit_test`, …, plus `relation` for anything that is not a test or check |
| `config.<key>:` | yes | any key in the rendered config, `config.meta.owner:` style nesting included; a list-valued key matches on any element |
| `access:`, `group:` | yes | exact match, as in dbt |
| `test_name:` | yes | a generic test answers to the macro behind it (`not_null`), not its generated node name |
| `test_type:` | yes | `unit`, `data`, `singular`, `generic` |
| `version:` | yes | `latest`, `prerelease`, `old`, `none`, read from the model's declared version |
| `source:` | yes | `<source>`, `<source>.<table>`, `<package>.<source>.<table>` |
| `exposure:`, `metric:`, `saved_query:`, `semantic_model:`, `function:`, `unit_test:` | yes | `<name>` or `<package>.<name>` |
| `state:new`, `state:modified[.body]` | yes | requires `state_manifest_ref`; compares node bodies, falling back to file checksums. `state:modified` warns that it reads only those two |
| `state:modified.configs`, `.relation`, `.persisted_descriptions`, `.macros`, `.contract` | **no** | names a dimension this comparison does not read; answering with the body comparison would select the wrong nodes |
| `state:old`, `state:unmodified` | yes | |
| `result:` | **no** | needs `run_results.json` from a previous run |
| `source_status:` | **no** | needs `sources.json` from a previous source freshness run |
| `column:` | **no** | dbt-internal column lineage, not a run selector |
| `selector:` | **no** | names a `selectors.yml` definition, which is not read |

Graph operators (`+model`, `model+`, `N+model`, `@model`), unions (space),
intersections (comma) and nested excludes all work with any supported method.

A selector may name a resource type the command cannot execute. The command's
own node-type filter runs first, so `--select exposure:weekly` under `run`
narrows to nothing and fails with "no nodes matched" — but `--select
+exposure:weekly` selects the models that exposure is built from, and
`--select source:raw+` selects everything downstream of a source. Naming a
non-executable resource as a *graph seed* is the point of supporting these
methods.

A supported method whose value it cannot read is rejected the same way an
unsupported method is: `exposure:a.b.c` names no exposure, `test_type:integration`
names no test type, and `config:materialized` names no config key. Silently
matching nothing would hide the typo.

## Why rejection rather than "matches nothing"

An unevaluable method contributes an empty match set. Alone that surfaces as
"no nodes matched", but in the two positions that matter it is silent:

- inside a union (`--select "tag:nightly result:error"`) it drops the nodes the
  second half asked for, and the run reports success having built less than
  requested;
- in `--exclude` it excludes nothing, so the run builds *more* than requested.

Both produce a green run with the wrong node set, so the planner fails fast
instead.

## `indirect_selection`

Supported, and matching dbt's semantics. When `select` or `exclude` narrows a
run, the tests hanging off the selected nodes are pulled in afterwards — the
selector never names them.

| mode | a test is included when… |
|---|---|
| `eager` (default) | any of its parents is selected |
| `cautious` | every parent is selected |
| `buildable` | every parent is selected or is an ancestor of the selection |
| `empty` | never — tests must be named explicitly |

```json
{"command": "build", "select": "my_model", "indirect_selection": "cautious"}
```

An unrecognised value is rejected at plan time rather than silently falling
back, because the mode changes which tests run.

Indirect selection only adds tests and unit tests — it never pulls in a model
you did not select. A unit test is judged by the model it tests, not by the
nodes named in its fixtures.

Selection runs in dbt's order: `--select` narrows, indirect selection expands
what survived, and `--exclude` is subtracted from that. Excluding last is what
makes an explicit exclusion stick — `--select my_model --exclude its_test` runs
the model without the test, and `--exclude resource_type:test` keeps every test
out. Expanding after the exclusion instead would let the model put its tests
straight back.

**Behavior change:** before this was implemented dbt-temporal effectively
behaved as `empty`, so `build --select my_model` ran the model and skipped its
tests. The default is now `eager`, matching dbt, and such a run executes more
nodes than it used to. Pass `"indirect_selection": "empty"` for the old
behavior.

## `selectors.yml`

Named selectors are not read — pass the expanded selector string instead.
