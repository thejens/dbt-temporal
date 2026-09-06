//! Node fixtures shared by the selector unit tests.
//!
//! Every selector method is a question about a *typed* node — its resource
//! type, its declared version, the macro behind a generic test — so the tests
//! need one real node of each kind rather than a single generic stand-in. The
//! builders here set only the fields selection reads and leave the rest at
//! their schema defaults, which keeps each test's setup to the field it is
//! actually about.
//!
//! Everything belongs to a package called `shop` and sits at a file path dbt
//! would have written it to, because `package:`, `path:` and `file:` all read
//! those without the test naming them.

use std::path::PathBuf;
use std::sync::Arc;

use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::manifest::metric::DbtMetric;
use dbt_schemas::schemas::manifest::saved_query::DbtSavedQuery;
use dbt_schemas::schemas::manifest::semantic_model::DbtSemanticModel;
use dbt_schemas::schemas::nodes::{
    DbtExposure, DbtFunction, DbtModel, DbtSeed, DbtSource, DbtTest, DbtUnitTest, TestMetadata,
};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::serde::StringOrInteger;
use dbt_yaml::Value as YmlValue;

/// The common attributes every fixture shares, at the given project-relative path.
fn common(unique_id: &str, name: &str, path: &str) -> CommonAttributes {
    CommonAttributes {
        unique_id: unique_id.to_string(),
        name: name.to_string(),
        package_name: "shop".to_string(),
        fqn: vec!["shop".to_string(), name.to_string()],
        original_file_path: PathBuf::from(path).into(),
        ..CommonAttributes::default()
    }
}

/// A plain model at `models/<name>.sql`.
pub fn model_node(unique_id: &str, name: &str) -> Arc<DbtModel> {
    Arc::new(DbtModel {
        __common_attr__: common(unique_id, name, &format!("models/{name}.sql")),
        ..DbtModel::default()
    })
}

/// A model carrying `raw_code`, which is what the state comparison reads first.
pub fn model_with_code(unique_id: &str, name: &str, raw_code: &str) -> Arc<DbtModel> {
    let mut model = (*model_node(unique_id, name)).clone();
    model.__common_attr__.raw_code = Some(raw_code.to_string());
    Arc::new(model)
}

/// A model whose tags the `tag:` method reads.
pub fn tagged_model_node(unique_id: &str, name: &str, tags: &[&str]) -> Arc<DbtModel> {
    let mut model = (*model_node(unique_id, name)).clone();
    model.__common_attr__.tags = tags.iter().map(|tag| (*tag).to_string()).collect();
    Arc::new(model)
}

/// A model whose config carries the keys `yaml` names.
///
/// The fixture is layered over a serialized default rather than deserialized on
/// its own: a model config has required fields and wrapper-typed keys that no
/// selector test is about, and every fixture would otherwise have to restate
/// them. Round-tripping through the real type still keeps the result a config
/// the parser could have produced.
///
/// # Panics
///
/// When `yaml` is not a mapping of valid model config keys — a test fixture,
/// not runtime input.
pub fn model_with_config(unique_id: &str, name: &str, yaml: &str) -> Arc<DbtModel> {
    let mut model = (*model_node(unique_id, name)).clone();
    model.deprecated_config = config_from_yaml(yaml);
    Arc::new(model)
}

fn config_from_yaml(yaml: &str) -> ModelConfig {
    let mut base = dbt_yaml::to_value(ModelConfig::default())
        .unwrap_or_else(|e| panic!("the default model config must serialize: {e}"));
    let overlay: YmlValue =
        dbt_yaml::from_str(yaml).unwrap_or_else(|e| panic!("invalid config fixture: {e}"));

    let (YmlValue::Mapping(base_keys, ..), YmlValue::Mapping(overlay_keys, ..)) =
        (&mut base, &overlay)
    else {
        panic!("a config fixture must be a mapping");
    };
    for (key, value) in overlay_keys {
        base_keys.insert(key.clone(), value.clone());
    }

    dbt_yaml::from_value(base).unwrap_or_else(|e| panic!("invalid config fixture: {e}"))
}

/// A model that declares a version, and the version its group considers latest.
pub fn versioned_model_node(
    unique_id: &str,
    name: &str,
    version: Option<&str>,
    latest_version: Option<&str>,
) -> Arc<DbtModel> {
    let to_version = |v: &str| StringOrInteger::String(v.to_string());
    let mut model = (*model_node(unique_id, name)).clone();
    model.__model_attr__.version = version.map(to_version);
    model.__model_attr__.latest_version = latest_version.map(to_version);
    if let Some(version) = version {
        model.__common_attr__.fqn.push(version.to_string());
    }
    Arc::new(model)
}

pub fn seed_node(unique_id: &str, name: &str) -> Arc<DbtSeed> {
    Arc::new(DbtSeed {
        __common_attr__: common(unique_id, name, &format!("seeds/{name}.csv")),
        ..DbtSeed::default()
    })
}

/// A singular test: a `.sql` file the user wrote, with no test macro behind it.
pub fn singular_test_node(unique_id: &str, name: &str) -> Arc<DbtTest> {
    Arc::new(DbtTest {
        __common_attr__: common(unique_id, name, &format!("tests/{name}.sql")),
        ..DbtTest::default()
    })
}

/// A generic test: SQL dbt rendered under `generic_tests/` from a test macro.
///
/// Both halves matter to selection — the rendered path is the only signal
/// `test_type:` has, and `test_metadata.name` is what `test_name:` matches.
pub fn generic_test_node(unique_id: &str, name: &str, macro_name: &str) -> Arc<DbtTest> {
    let mut test = DbtTest {
        __common_attr__: common(
            unique_id,
            name,
            &format!("target/compiled/shop/generic_tests/{name}.sql"),
        ),
        ..DbtTest::default()
    };
    test.__test_attr__.test_metadata = Some(TestMetadata {
        name: macro_name.to_string(),
        kwargs: std::collections::BTreeMap::new(),
        namespace: None,
    });
    Arc::new(test)
}

pub fn unit_test_node(unique_id: &str, name: &str) -> Arc<DbtUnitTest> {
    Arc::new(DbtUnitTest {
        __common_attr__: common(unique_id, name, &format!("models/unit_tests/{name}.yml")),
        ..DbtUnitTest::default()
    })
}

/// A source table. `name` is the table; `source_name` is the source it sits under.
pub fn source_node(unique_id: &str, name: &str, source_name: &str) -> Arc<DbtSource> {
    let mut source = DbtSource {
        __common_attr__: common(unique_id, name, &format!("models/{source_name}.yml")),
        ..DbtSource::default()
    };
    source.__source_attr__.source_name = source_name.to_string();
    Arc::new(source)
}

pub fn exposure_node(unique_id: &str, name: &str) -> Arc<DbtExposure> {
    Arc::new(DbtExposure {
        __common_attr__: common(unique_id, name, &format!("models/exposures/{name}.yml")),
        ..DbtExposure::default()
    })
}

pub fn metric_node(unique_id: &str, name: &str) -> Arc<DbtMetric> {
    Arc::new(DbtMetric {
        __common_attr__: common(unique_id, name, &format!("models/metrics/{name}.yml")),
        ..DbtMetric::default()
    })
}

pub fn saved_query_node(unique_id: &str, name: &str) -> Arc<DbtSavedQuery> {
    Arc::new(DbtSavedQuery {
        __common_attr__: common(unique_id, name, &format!("models/saved_queries/{name}.yml")),
        ..DbtSavedQuery::default()
    })
}

pub fn semantic_model_node(unique_id: &str, name: &str) -> Arc<DbtSemanticModel> {
    Arc::new(DbtSemanticModel {
        __common_attr__: common(unique_id, name, &format!("models/semantic/{name}.yml")),
        ..DbtSemanticModel::default()
    })
}

pub fn function_node(unique_id: &str, name: &str) -> Arc<DbtFunction> {
    Arc::new(DbtFunction {
        __common_attr__: common(unique_id, name, &format!("functions/{name}.sql")),
        ..DbtFunction::default()
    })
}
