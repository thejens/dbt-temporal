//! Per-phase helpers for `DbtRunWorkflow::run`.
//!
//! Each submodule is one phase of a dbt run's lifecycle, or the options its
//! activities share; they are re-exported here so the workflow code imports
//! from one place.
//!
//! `WorkflowContext` uses interior mutability via Rc — methods take `&self`,
//! so helpers take `&WorkflowContext<DbtRunWorkflow>` matching the pattern
//! in `crate::hooks::execute_hooks`. State that crosses phases (effective
//! env, hook errors, success flag) is threaded by explicit `&mut` rather
//! than bundled into a struct, since each phase only needs a small subset
//! and bundling would force borrow-checker gymnastics.

mod activity_options;
mod artifacts;
mod lifecycle_hooks;
mod planning;
mod project_checks;
mod project_hooks;
mod segment_state;
mod terminal_status;
#[cfg(test)]
mod test_support;

pub use self::activity_options::HookPolicy;
pub use self::artifacts::{RunRecord, store_artifacts_of_ended_run, store_run_artifacts};
pub use self::lifecycle_hooks::{run_post_hooks, run_pre_run_hooks};
pub use self::planning::{
    build_list_output, plan_and_announce, resolve_project_config, write_command_memo,
};
pub use self::project_checks::run_project_checks;
pub use self::project_hooks::{OnRunEndRecord, run_on_run_end, run_on_run_start};
pub use self::segment_state::{load_segment_state, save_segment_state};
pub use self::terminal_status::upsert_terminal_status;
