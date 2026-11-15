//! dbt unit test execution.
//!
//! A unit test runs the tested model's SQL against fixture inputs (`given`)
//! and compares the output to fixture expectations (`expect`). The flow
//! mirrors dbt-core's macro-based path: this module builds the model SQL with
//! each mocked input replaced by a fixture CTE (rendered through the bundled
//! `get_fixture_sql` macro), the `unit` materialization probes column types
//! and unions actual vs expected rows labelled by an `actual_or_expected`
//! column, and `extract_unit_test_outcome` compares the two partitions
//! order-insensitively (the union query carries no ORDER BY, so a positional
//! comparison would be flaky).

mod fixtures;
mod given;
mod outcome;
mod sql;

pub use fixtures::inject_expected_config;
pub use outcome::extract_unit_test_outcome;
pub use sql::build_unit_test_sql;
