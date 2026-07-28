//! Per-activity Jinja environment + adapter setup, shared by every activity
//! that renders dbt templates against the warehouse.
//!
//! `execute_node` and `run_project_hooks` both need the same thing: a private
//! clone of the worker's Jinja environment with this workflow's overrides
//! applied, bound to an adapter that may itself have been rebuilt from
//! per-workflow credentials. They used to build it separately and had already
//! drifted (only the hooks path registered the `execute` global).
//!
//! Everything here is per-activity by construction — the worker's `WorkerState`
//! is never mutated, so concurrent workflows stay isolated.

use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_jinja_utils::jinja_environment::JinjaEnv;

use crate::error::DbtTemporalError;
use crate::worker::profile::RebuildResult;
use crate::worker_state::WorkerState;

use super::node_helpers::{json_to_minijinja, patch_target_global};

/// The per-workflow overrides an activity applies on top of worker state.
#[derive(Debug, Clone, Copy)]
pub struct RenderOverrides<'a> {
    /// `env_var()` overrides for this workflow.
    pub env: &'a BTreeMap<String, String>,
    /// `--target` override, if any.
    pub target: Option<&'a str>,
    /// `--vars` overrides for this workflow.
    pub vars: &'a BTreeMap<String, serde_json::Value>,
    /// `--full-refresh` for this workflow.
    pub full_refresh: bool,
}

/// A configured render environment, valid for one activity invocation.
pub struct RenderEnv {
    /// The activity's private Jinja environment.
    pub jinja_env: JinjaEnv,
    /// Adapter bound to the (possibly rebuilt) engine.
    pub adapter: Arc<dbt_adapter::Adapter>,
    /// Profile schema after env overrides — `None` when the shared startup
    /// engine is in use and nothing needs patching.
    pub env_schema: Option<String>,
    /// Profile database after env overrides, same conditions as `env_schema`.
    pub env_database: Option<String>,
    /// Keeps the rebuilt engine's `CancellationTokenSource` alive for the whole
    /// activity. The engine's token holds only a `Weak` ref to its source, so
    /// dropping this makes every subsequent adapter call report "cancelled".
    _rebuild_guard: Option<RebuildResult>,
}

impl std::fmt::Debug for RenderEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenderEnv")
            .field("env_schema", &self.env_schema)
            .field("env_database", &self.env_database)
            .finish_non_exhaustive()
    }
}

/// Build the render environment for one activity invocation.
///
/// `context` names the caller in error messages ("node", "on_run_start", …).
pub fn prepare_render_env(
    state: &WorkerState,
    overrides: &RenderOverrides<'_>,
    context: &str,
) -> Result<RenderEnv, DbtTemporalError> {
    let mut jinja_env = (*state.jinja_env).clone();

    override_env_var(&mut jinja_env, overrides.env);
    override_vars(&mut jinja_env, overrides.vars);
    override_flags(&mut jinja_env, overrides.full_refresh);

    // Rebuild the adapter engine only when profiles.yml actually reads env
    // vars — a rebuild re-renders the profile and opens fresh connections, so
    // it is not worth doing when nothing in the profile can change.
    let mut rebuild_guard = None;
    let (engine, env_schema, env_database) = if !overrides.env.is_empty()
        && state.profile_uses_env_vars
    {
        let result =
            crate::worker::rebuild_adapter_engine_with_env(state, overrides.target, overrides.env)
                .map_err(|e| {
                    DbtTemporalError::Configuration(format!(
                        "rebuilding adapter engine for {context}: {e:#}"
                    ))
                })?;
        let engine = Arc::clone(&result.engine);
        let (schema, database) = (result.schema.clone(), result.database.clone());
        rebuild_guard = Some(result);
        (engine, Some(schema), Some(database))
    } else {
        (Arc::clone(&state.adapter_engine), None, None)
    };

    let adapter_impl = dbt_adapter::AdapterImpl::new(engine, None);
    let adapter = Arc::new(dbt_adapter::Adapter::new(
        Arc::new(adapter_impl),
        None, // time_machine
        state.cancellation_source.token(),
    ));

    // Registers adapter/api/dialect globals and sets lenient undefined behavior.
    dbt_jinja_utils::phases::configure_compile_and_run_jinja_environment(
        &mut jinja_env,
        Arc::clone(&adapter),
    );

    // Set `execute` as a GLOBAL, not just a context variable. dbt-fusion only
    // puts it in the context built by `build_compile_and_run_base_context`,
    // which does not propagate into cross-template macro calls — a package
    // macro dispatched from a model that branches on `{% if execute %}` would
    // otherwise see it undefined (upstream issue #1289, closed as not-planned).
    jinja_env
        .env
        .add_global("execute", minijinja::Value::from(true));

    // Must run after `configure_compile_and_run_jinja_environment`: it reads
    // the current `target` back out by rendering a template, which needs the
    // fully configured environment.
    if let (Some(schema), Some(database)) = (env_schema.as_deref(), env_database.as_deref()) {
        patch_target_global(&mut jinja_env, schema, database, overrides.target);
    }

    Ok(RenderEnv {
        jinja_env,
        adapter,
        env_schema,
        env_database,
        _rebuild_guard: rebuild_guard,
    })
}

/// Replace `env_var()` with one that consults this workflow's overrides first.
///
/// Per-activity rather than process-level so parallel workflows never observe
/// each other's environment.
fn override_env_var(jinja_env: &mut JinjaEnv, env: &BTreeMap<String, String>) {
    if env.is_empty() {
        return;
    }
    let overrides = Arc::new(env.clone());
    jinja_env.env.add_func_func("env_var", move |state, args| {
        // dbt_jinja_utils::LookupFn is implicitly 'static, so the inner closure
        // must own its captures — bump the Arc refcount per call.
        let map = Arc::clone(&overrides);
        let lookup = move |key: &str| -> Option<minijinja::Value> {
            // Value::from(&str) uses minijinja's inline SmallStr where it fits,
            // avoiding the second alloc that Value::from(String) would do.
            map.get(key).map(|v| minijinja::Value::from(v.as_str()))
        };
        dbt_jinja_utils::env_var(false, Some(&lookup), state, args)
    });
}

/// Layer this workflow's `--vars` over the project's, matching dbt's precedence
/// (CLI vars win over `dbt_project.yml` vars).
///
/// Wraps rather than replaces the resolver's `var` object so package-scoped
/// project vars and `default=` handling keep working for every key the
/// workflow did not override.
///
/// This only affects `var()` calls evaluated at *render* time. Vars read during
/// parsing — `dbt_project.yml`, `{{ config(...) }}` blocks — were resolved once
/// at worker startup and cannot vary per workflow. `plan_project` warns and
/// names the nodes when a run's vars appear in a `config()` block.
fn override_vars(jinja_env: &mut JinjaEnv, vars: &BTreeMap<String, serde_json::Value>) {
    if vars.is_empty() {
        return;
    }
    let overrides: Arc<BTreeMap<String, minijinja::Value>> = Arc::new(
        vars.iter()
            .map(|(k, v)| (k.clone(), json_to_minijinja(v)))
            .collect(),
    );
    let original = jinja_env.env.get_global("var");

    jinja_env.env.add_func_func("var", move |state, args| {
        let name = args
            .first()
            .and_then(|v| v.as_str().map(ToString::to_string));
        if let Some(name) = name.as_deref()
            && let Some(value) = overrides.get(name)
        {
            return Ok(value.clone());
        }
        let Some(var_fn) = &original else {
            // No resolver `var` to fall back to (only reachable in tests that
            // build a bare environment): honour an explicit default, else
            // undefined, which is what an unknown var yields anyway.
            return Ok(args.get(1).cloned().unwrap_or_default());
        };
        var_fn.call(state, args, &[])
    });
}

/// Reflect `--full-refresh` in the `flags` Jinja global.
///
/// `flags` is built from `InvocationArgs` when the worker parses the project,
/// so it is frozen at startup defaults (`full_refresh: false`). dbt's
/// incremental materialization branches on `should_full_refresh()`, which reads
/// `flags.FULL_REFRESH` — without this patch a full-refresh run silently
/// performs an incremental merge.
fn override_flags(jinja_env: &mut JinjaEnv, full_refresh: bool) {
    if !full_refresh {
        return;
    }
    let Some(inner) = jinja_env.env.get_global("flags") else {
        tracing::warn!("no `flags` Jinja global to override; full_refresh not applied");
        return;
    };
    // dbt registers both spellings and macros use each — `should_full_refresh()`
    // reads the lowercase one, user macros usually the uppercase one.
    let overrides = [
        ("FULL_REFRESH".to_string(), minijinja::Value::from(true)),
        ("full_refresh".to_string(), minijinja::Value::from(true)),
    ]
    .into_iter()
    .collect();

    jinja_env
        .env
        .add_global("flags", minijinja::Value::from_object(OverlaidFlags { inner, overrides }));
}

/// A `flags` object that overlays per-workflow values on the worker's.
///
/// dbt-fusion's `Flags` is opaque — it renders to a Debug dump rather than a
/// map, so it cannot be read back and rebuilt the way `target` can. Wrapping
/// keeps every key the worker computed, including the `flags.get(name, default)`
/// method adapter macros call.
#[derive(Debug)]
struct OverlaidFlags {
    inner: minijinja::Value,
    overrides: BTreeMap<String, minijinja::Value>,
}

impl minijinja::value::Object for OverlaidFlags {
    fn repr(self: &Arc<Self>) -> minijinja::value::ObjectRepr {
        minijinja::value::ObjectRepr::Plain
    }

    fn get_value(self: &Arc<Self>, key: &minijinja::Value) -> Option<minijinja::Value> {
        if let Some(name) = key.as_str()
            && let Some(value) = self.overrides.get(name)
        {
            return Some(value.clone());
        }
        self.inner.get_item(key).ok()
    }

    fn call_method(
        self: &Arc<Self>,
        state: &minijinja::State<'_, '_>,
        name: &str,
        args: &[minijinja::Value],
        listeners: &[std::rc::Rc<dyn minijinja::listener::RenderingEventListener>],
    ) -> Result<minijinja::Value, minijinja::Error> {
        // `flags.get('full_refresh')` must see the overrides too.
        if name == "get"
            && let Some(key) = args.first().and_then(minijinja::Value::as_str)
            && let Some(value) = self.overrides.get(key)
        {
            return Ok(value.clone());
        }
        self.inner.call_method(state, name, args, listeners)
    }
}
