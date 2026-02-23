use std::sync::Arc;

use dbt_schemas::schemas::telemetry::NodeType;

/// Serialize a node to YmlValue for `build_run_node_context`.
///
/// Tries the concrete type from the appropriate Nodes collection.
/// Injects `resource_type` so adapter functions (e.g. `get_view_options`) can
/// deserialize back to `InternalDbtNodeWrapper` (a `#[serde(tag = "resource_type")]` enum).
pub fn get_node_yml(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
) -> Result<dbt_yaml::Value, anyhow::Error> {
    use dbt_schemas::schemas::telemetry::NodeType;

    let (mut val, rt_str) = match resource_type {
        NodeType::Model => {
            if let Some(model) = nodes.models.get(unique_id) {
                (dbt_yaml::to_value(model.as_ref())?, "model")
            } else {
                return fallback_node_yml(nodes, unique_id, resource_type);
            }
        }
        NodeType::Seed => {
            if let Some(seed) = nodes.seeds.get(unique_id) {
                (dbt_yaml::to_value(seed.as_ref())?, "seed")
            } else {
                return fallback_node_yml(nodes, unique_id, resource_type);
            }
        }
        NodeType::Snapshot => {
            if let Some(snapshot) = nodes.snapshots.get(unique_id) {
                (dbt_yaml::to_value(snapshot.as_ref())?, "snapshot")
            } else {
                return fallback_node_yml(nodes, unique_id, resource_type);
            }
        }
        NodeType::Test => {
            if let Some(test) = nodes.tests.get(unique_id) {
                (dbt_yaml::to_value(test.as_ref())?, "test")
            } else {
                return fallback_node_yml(nodes, unique_id, resource_type);
            }
        }
        _ => return fallback_node_yml(nodes, unique_id, resource_type),
    };

    inject_resource_type(&mut val, rt_str);
    Ok(val)
}

/// Inject `resource_type` into a YmlValue mapping (required by `InternalDbtNodeWrapper` deserialization).
fn inject_resource_type(val: &mut dbt_yaml::Value, rt: &str) {
    if let dbt_yaml::Value::Mapping(map, _) = val {
        map.insert(
            dbt_yaml::Value::string("resource_type".to_string()),
            dbt_yaml::Value::string(rt.to_string()),
        );
    }
}

/// Fallback: build a minimal YmlValue from node traits.
fn fallback_node_yml(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
) -> Result<dbt_yaml::Value, anyhow::Error> {
    let node = nodes
        .get_node(unique_id)
        .ok_or_else(|| anyhow::anyhow!("node {unique_id} not found for serialization"))?;
    let common = node.common();
    let base = node.base();
    let mut map = dbt_yaml::Mapping::new();
    map.insert(
        dbt_yaml::Value::string("resource_type".to_string()),
        dbt_yaml::Value::string(resource_type.as_str_name().to_lowercase()),
    );
    map.insert(
        dbt_yaml::Value::string("unique_id".to_string()),
        dbt_yaml::Value::string(common.unique_id.clone()),
    );
    map.insert(
        dbt_yaml::Value::string("name".to_string()),
        dbt_yaml::Value::string(common.name.clone()),
    );
    map.insert(
        dbt_yaml::Value::string("database".to_string()),
        dbt_yaml::Value::string(base.database.clone()),
    );
    map.insert(
        dbt_yaml::Value::string("schema".to_string()),
        dbt_yaml::Value::string(base.schema.clone()),
    );
    map.insert(
        dbt_yaml::Value::string("alias".to_string()),
        dbt_yaml::Value::string(base.alias.clone()),
    );
    Ok(dbt_yaml::Value::mapping(map))
}

/// Extract the node config as a serializable value for `deprecated_config`.
///
/// Serializes directly to `dbt_yaml::Value` to avoid a JSON round-trip
/// that breaks custom deserializers (Omissible, bool_or_string_bool, etc.).
pub fn get_node_config_yml(
    nodes: &dbt_schemas::schemas::Nodes,
    unique_id: &str,
    resource_type: NodeType,
) -> dbt_yaml::Value {
    use dbt_schemas::schemas::telemetry::NodeType;

    match resource_type {
        NodeType::Model => {
            if let Some(model) = nodes.models.get(unique_id)
                && let Ok(v) = dbt_yaml::to_value(&model.deprecated_config)
            {
                return v;
            }
        }
        NodeType::Seed => {
            if let Some(seed) = nodes.seeds.get(unique_id)
                && let Ok(v) = dbt_yaml::to_value(&seed.deprecated_config)
            {
                return v;
            }
        }
        NodeType::Snapshot => {
            if let Some(snapshot) = nodes.snapshots.get(unique_id)
                && let Ok(v) = dbt_yaml::to_value(&snapshot.deprecated_config)
            {
                return v;
            }
        }
        NodeType::Test => {
            if let Some(test) = nodes.tests.get(unique_id)
                && let Ok(v) = dbt_yaml::to_value(&test.deprecated_config)
            {
                return v;
            }
        }
        _ => {}
    }

    // Fallback: empty config.
    dbt_yaml::Value::mapping(dbt_yaml::Mapping::new())
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
        options.text_columns = column_types.keys().map(|k| String::clone(k)).collect();
    }

    let result = dbt_csv::read_to_arrow_records(&csv_path, &options)
        .map_err(|e| anyhow::anyhow!("failed to read seed CSV {}: {e}", csv_path.display()))?;

    let batches = result.batches;
    let first = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("seed CSV {} produced no data", csv_path.display()))?;
    let schema = first.schema();
    let batch = arrow_select::concat::concat_batches(&schema, batches.iter())
        .map_err(|e| anyhow::anyhow!("failed to concat seed CSV batches: {e}"))?;

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
mod tests {
    use super::*;

    #[test]
    fn get_node_config_yml_missing_node_returns_empty_mapping() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let result = get_node_config_yml(&nodes, "model.waffle.nonexistent", NodeType::Model);
        assert_eq!(result, dbt_yaml::Value::mapping(dbt_yaml::Mapping::new()));
    }

    #[test]
    fn get_node_config_yml_unknown_type_returns_empty_mapping() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let result = get_node_config_yml(&nodes, "some.node", NodeType::Operation);
        assert_eq!(result, dbt_yaml::Value::mapping(dbt_yaml::Mapping::new()));
    }

    #[test]
    fn get_node_yml_missing_node_returns_error() {
        let nodes = dbt_schemas::schemas::Nodes::default();
        let result = get_node_yml(&nodes, "model.waffle.nonexistent", NodeType::Model);
        let Err(err) = result else {
            panic!("expected error");
        };
        assert!(err.to_string().contains("not found"), "error should mention node not found");
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
}
