//! Seeds ingested through the real `execute_node` path.
//!
//! Its own binary so the two seed shapes load in a clean process — the CSV
//! reader batches at a fixed row count, so which code path a seed takes is
//! decided by its size.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::large_futures)]

mod common;

use common::duckdb::Harness;

/// Arrow's CSV reader batches at 1024 rows, so anything past that arrives as
/// several batches and has to be concatenated.
const ROWS_PAST_ONE_BATCH: usize = 2_500;

#[tokio::test]
async fn a_seed_that_fits_one_batch_loads() {
    let harness = Harness::build_files(&[("seeds/small.csv", "id,name\n1,a\n2,b\n3,c\n")]).await;

    let result = harness.run_uid("seed.spike.small").await.unwrap();
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{result:?}");
    assert_eq!(harness.query_scalar("select count(*) from main.small"), "3");
    assert_eq!(harness.query_scalar("select name from main.small where id = 2"), "b");
}

/// The concatenating path: every row has to survive it, and in order.
#[tokio::test]
async fn a_seed_spanning_several_batches_loads_every_row() {
    use std::fmt::Write as _;
    let mut csv = String::from("id,name\n");
    for i in 0..ROWS_PAST_ONE_BATCH {
        writeln!(csv, "{i},name_{i}").expect("writing to a String cannot fail");
    }
    let harness = Harness::build_files(&[("seeds/wide.csv", csv.as_str())]).await;

    let result = harness.run_uid("seed.spike.wide").await.unwrap();
    assert_eq!(result.status, dbt_temporal::types::NodeStatus::Success, "{result:?}");
    assert_eq!(
        harness.query_scalar("select count(*) from main.wide"),
        ROWS_PAST_ONE_BATCH.to_string()
    );
    // The last row is in the final batch, the boundary row in the second.
    assert_eq!(
        harness.query_scalar(&format!(
            "select name from main.wide where id = {}",
            ROWS_PAST_ONE_BATCH - 1
        )),
        format!("name_{}", ROWS_PAST_ONE_BATCH - 1)
    );
    assert_eq!(harness.query_scalar("select name from main.wide where id = 1024"), "name_1024");
}
