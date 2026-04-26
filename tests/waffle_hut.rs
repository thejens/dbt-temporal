//! Integration tests: run the waffle_hut dbt project through Temporal workflows.
//!
//! Infrastructure:
//!   - Postgres via ADBC (testcontainer)
//!   - Temporal ephemeral dev server (workflow engine)

#![allow(
    clippy::too_many_lines,
    clippy::ignored_unit_patterns,
    clippy::items_after_statements,
    clippy::format_push_string,
    clippy::large_futures,
    clippy::cast_possible_wrap,
    clippy::struct_field_names,
    clippy::missing_panics_doc,
    clippy::significant_drop_tightening,
    clippy::missing_const_for_fn,
    clippy::expect_used,
    clippy::unwrap_used
)]

mod waffle_hut {
    pub mod hook_echo;
    pub mod infra;

    mod artifacts;
    mod basic;
    mod env_isolation;
    mod env_vars;
    mod failure;
    mod hooks;
    mod lifecycle_hooks;
    mod multi_project;
    mod robustness;
}
