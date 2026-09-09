use std::sync::Arc;

use anyhow::Context;
use dbt_schemas::schemas::telemetry::NodeType;

/// Extract the node config as a serializable value for `deprecated_config`.
///
/// Serializes directly to `dbt_yaml::Value` to avoid a JSON round-trip
/// that breaks custom deserializers (Omissible, bool_or_string_bool, etc.).
///
/// A serialization failure is an error, not an empty config. The config is what
/// every `config.get(...)` in a materialization reads: an incremental model
/// silently loses its `unique_key` and merges on nothing, a snapshot loses its
/// strategy. Returning an empty mapping made that indistinguishable from a node
/// that genuinely declares nothing.
pub fn get_node_config_yml(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
) -> Result<dbt_yaml::Value, anyhow::Error> {
    use dbt_schemas::schemas::telemetry::NodeType;

    let serialized = match resource_type {
        NodeType::Model => nodes
            .models
            .get(unique_id)
            .map(|n| dbt_yaml::to_value(&n.deprecated_config)),
        NodeType::Seed => nodes
            .seeds
            .get(unique_id)
            .map(|n| dbt_yaml::to_value(&n.deprecated_config)),
        NodeType::Snapshot => nodes
            .snapshots
            .get(unique_id)
            .map(|n| dbt_yaml::to_value(&n.deprecated_config)),
        NodeType::Test => nodes
            .tests
            .get(unique_id)
            .map(|n| dbt_yaml::to_value(&n.deprecated_config)),
        NodeType::UnitTest => nodes
            .unit_tests
            .get(unique_id)
            .map(|n| dbt_yaml::to_value(&n.deprecated_config)),
        // Node types that carry no `deprecated_config` of this shape. An empty
        // mapping is the right answer for them, not a missing one.
        _ => return Ok(dbt_yaml::Value::mapping(dbt_yaml::Mapping::new())),
    };

    match serialized {
        Some(result) => result
            .with_context(|| format!("serializing config for {unique_id} ({resource_type:?})")),
        // The caller read `resource_type` off this node, so not finding it in
        // the matching map means the two disagree — a bug, not a config.
        None => anyhow::bail!("{unique_id} is not in the {resource_type:?} node map"),
    }
}

/// Build an AgateTable for seed nodes by reading their CSV file.
pub fn build_agate_table(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
    io_args: &dbt_common::io_args::IoArgs,
) -> Result<Option<dbt_agate::AgateTable>, anyhow::Error> {
    if resource_type != NodeType::Seed {
        return Ok(None);
    }

    let seed = nodes
        .seeds
        .get(unique_id)
        .ok_or_else(|| anyhow::anyhow!("seed {unique_id} not found"))?;

    let csv_path = io_args
        .in_dir
        .join(&seed.__common_attr__.original_file_path);

    let mut options = dbt_csv::CustomCsvOptions::default();
    if let Some(delimiter) = &seed.__seed_attr__.delimiter
        && let Some(byte) = delimiter.as_bytes().first()
    {
        options.delimiter = *byte;
    }
    if let Some(column_types) = &seed.__seed_attr__.column_types {
        options.text_columns = column_types.keys().map(|k| (**k).clone()).collect();
    }

    let result = dbt_csv::read_to_arrow_records(&csv_path, &options)
        .with_context(|| format!("reading seed CSV {}", csv_path.display()))?;

    let mut batches = result.batches;
    if batches.is_empty() {
        anyhow::bail!("seed CSV {} produced no data", csv_path.display());
    }

    // A seed that fits in one batch is handed straight over. Concatenating a
    // single batch allocates a second copy of every column to arrive at the
    // same data.
    //
    // A seed larger than the reader's batch size still peaks at input plus
    // output, because `read_to_arrow_records` reads the whole file and
    // collects every batch before returning: there is nothing to consume
    // incrementally on this side. Lowering that peak needs a reader that
    // yields batches lazily and an Agate table that can be built from a
    // stream of them — both upstream of here.
    let batch = if batches.len() == 1 {
        batches.remove(0)
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, batches.iter())
            .context("concatenating seed CSV batches")?
    };

    Ok(Some(dbt_agate::AgateTable::from_record_batch(Arc::new(batch))))
}

/// Extract sql_header from the node config (models only).
pub fn get_sql_header(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
) -> Option<minijinja::Value> {
    if resource_type != NodeType::Model {
        return None;
    }
    let model = nodes.models.get(unique_id)?;
    let header = model.deprecated_config.sql_header.as_ref()?;
    Some(minijinja::Value::from(header.clone()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use dbt_schemas::schemas::nodes::{DbtModel, DbtSeed, DbtSnapshot, DbtTest};

    /// The caller reads `resource_type` off the node, so a model id that is not
    /// in the model map means the two disagree. Answering with an empty config
    /// let the node materialize with no `config.get(...)` values at all.
    #[test]
    fn get_node_config_yml_missing_node_is_an_error() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let err = get_node_config_yml(&nodes, "model.waffle.nonexistent", NodeType::Model)
            .expect_err("a model that is not in the map must be reported");
        assert!(
            err.to_string().contains("model.waffle.nonexistent"),
            "should name the node: {err}"
        );
    }

    /// A node type that carries no `deprecated_config` of this shape genuinely
    /// has an empty one — that answer is not a fallback.
    #[test]
    fn get_node_config_yml_unknown_type_returns_empty_mapping() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let result = get_node_config_yml(&nodes, "some.node", NodeType::Operation)
            .expect("a type without config is not an error");
        assert_eq!(result, dbt_yaml::Value::mapping(dbt_yaml::Mapping::new()));
    }

    #[test]
    fn get_sql_header_non_model_returns_none() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        assert!(get_sql_header(&nodes, "seed.waffle.raw", NodeType::Seed).is_none());
        assert!(get_sql_header(&nodes, "test.waffle.t", NodeType::Test).is_none());
    }

    #[test]
    fn get_sql_header_missing_model_returns_none() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        assert!(get_sql_header(&nodes, "model.waffle.nope", NodeType::Model).is_none());
    }

    #[test]
    fn get_node_config_yml_returns_serialized_model_config() {
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes
            .models
            .insert("model.waffle.stg".to_string(), Arc::new(DbtModel::default()));
        let yml = get_node_config_yml(&nodes, "model.waffle.stg", NodeType::Model)
            .expect("config serializes");
        // Serializing default config produces a mapping (possibly empty); the contract
        // is "anything but the empty-mapping fallback for a present node".
        assert!(matches!(&yml, dbt_yaml::Value::Mapping(_, _)), "expected mapping config");
    }

    #[test]
    fn get_node_config_yml_returns_serialized_seed_config() {
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes
            .seeds
            .insert("seed.waffle.raw".to_string(), Arc::new(DbtSeed::default()));
        let yml = get_node_config_yml(&nodes, "seed.waffle.raw", NodeType::Seed)
            .expect("config serializes");
        assert!(matches!(&yml, dbt_yaml::Value::Mapping(_, _)), "expected mapping config");
    }

    #[test]
    fn get_node_config_yml_returns_serialized_snapshot_config() {
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes
            .snapshots
            .insert("snapshot.waffle.s".to_string(), Arc::new(DbtSnapshot::default()));
        let yml = get_node_config_yml(&nodes, "snapshot.waffle.s", NodeType::Snapshot)
            .expect("config serializes");
        assert!(matches!(&yml, dbt_yaml::Value::Mapping(_, _)), "expected mapping config");
    }

    #[test]
    fn get_node_config_yml_returns_serialized_test_config() {
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        nodes
            .tests
            .insert("test.waffle.t".to_string(), Arc::new(DbtTest::default()));
        let yml = get_node_config_yml(&nodes, "test.waffle.t", NodeType::Test)
            .expect("config serializes");
        assert!(matches!(&yml, dbt_yaml::Value::Mapping(_, _)), "expected mapping config");
    }

    #[test]
    fn build_agate_table_skips_non_seeds() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let io_args = dbt_common::io_args::IoArgs::default();
        for rt in [
            NodeType::Model,
            NodeType::Test,
            NodeType::Snapshot,
            NodeType::Operation,
        ] {
            let result = build_agate_table(&nodes, "any.id", rt, &io_args).unwrap();
            assert!(result.is_none(), "rt={rt:?} should return None");
        }
    }

    #[test]
    fn build_agate_table_errors_when_seed_not_found() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let io_args = dbt_common::io_args::IoArgs::default();
        let err =
            build_agate_table(&nodes, "seed.missing.x", NodeType::Seed, &io_args).unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    #[test]
    fn build_agate_table_reads_seed_csv_into_agate() {
        use std::path::PathBuf;

        let dir = tempfile::tempdir().unwrap();
        let in_dir = dir.path().to_path_buf();
        let seed_path = in_dir.join("seeds/raw.csv");
        std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();
        std::fs::write(&seed_path, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        let mut seed = DbtSeed::default();
        seed.__common_attr__.original_file_path = PathBuf::from("seeds/raw.csv").into();
        nodes
            .seeds
            .insert("seed.shop.raw".to_string(), Arc::new(seed));

        let io_args = dbt_common::io_args::IoArgs {
            in_dir,
            ..dbt_common::io_args::IoArgs::default()
        };

        let result = build_agate_table(&nodes, "seed.shop.raw", NodeType::Seed, &io_args).unwrap();
        let table = result.expect("Seed node should produce an AgateTable");
        // 3 rows in the CSV.
        assert_eq!(table.num_rows(), 3);
    }

    #[test]
    fn build_agate_table_errors_when_csv_path_missing_on_disk() {
        use std::path::PathBuf;

        let dir = tempfile::tempdir().unwrap();
        let in_dir = dir.path().to_path_buf();
        // Path points at a CSV that doesn't exist — read_to_arrow_records fails
        // with a file-not-found, which we wrap with `reading seed CSV` context.
        let mut nodes = dbt_schemas::schemas::Nodes::default();
        let mut seed = DbtSeed::default();
        seed.__common_attr__.original_file_path = PathBuf::from("seeds/missing.csv").into();
        nodes
            .seeds
            .insert("seed.shop.missing".to_string(), Arc::new(seed));

        let io_args = dbt_common::io_args::IoArgs {
            in_dir,
            ..dbt_common::io_args::IoArgs::default()
        };

        let err = build_agate_table(&nodes, "seed.shop.missing", NodeType::Seed, &io_args)
            .expect_err("missing CSV should error");
        assert!(err.to_string().contains("reading seed CSV"), "got: {err}");
    }

    #[test]
    fn build_agate_table_uses_custom_delimiter_from_seed_attrs() {
        use std::path::PathBuf;

        let dir = tempfile::tempdir().unwrap();
        let in_dir = dir.path().to_path_buf();
        let seed_path = in_dir.join("seeds/pipe.csv");
        std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();
        // Pipe-delimited body — would parse as a single column under default ",".
        std::fs::write(&seed_path, "id|name\n1|alice\n2|bob\n").unwrap();

        let mut nodes = dbt_schemas::schemas::Nodes::default();
        let mut seed = DbtSeed::default();
        seed.__common_attr__.original_file_path = PathBuf::from("seeds/pipe.csv").into();
        seed.__seed_attr__.delimiter = Some("|".to_string());
        nodes
            .seeds
            .insert("seed.shop.pipe".to_string(), Arc::new(seed));

        let io_args = dbt_common::io_args::IoArgs {
            in_dir,
            ..dbt_common::io_args::IoArgs::default()
        };

        let result = build_agate_table(&nodes, "seed.shop.pipe", NodeType::Seed, &io_args).unwrap();
        let table = result.expect("Seed node should produce an AgateTable");
        assert_eq!(table.num_rows(), 2);
    }
}
