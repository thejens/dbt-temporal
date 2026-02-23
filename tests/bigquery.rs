//! Integration tests: run the BigQuery example project through Temporal workflows.
//!
//! Gated behind `--features bigquery-tests` so they don't run without credentials.
//!
//! Infrastructure:
//!   - BigQuery (cloud, via GOOGLE_CLOUD_PROJECT env var)
//!   - Temporal ephemeral dev server (workflow engine)

#![cfg(feature = "bigquery-tests")]
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
    clippy::missing_const_for_fn
)]

mod bigquery {
    pub mod infra;

    mod basic;
    mod test_gating;
}
