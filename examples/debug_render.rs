//! Standalone script to debug Jinja materialization rendering.
//! Run: cargo run --example debug_render
#![allow(
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::large_futures,
    clippy::uninlined_format_args,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::unnecessary_get_then_check
)]
use std::sync::Arc;

use dbt_adapter::load_store::ResultStore;
use dbt_schemas::schemas::telemetry::NodeType;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(true)
        .init();

    let project_dir = std::path::PathBuf::from(
        std::env::var("DBT_PROJECT_DIR").unwrap_or_else(|_| "examples/bigquery".to_string()),
    );
    eprintln!("=== Loading project from {} ===", project_dir.display());

    let config =
        dbt_temporal::config::DbtTemporalConfig::from_env().map_err(|e| anyhow::anyhow!("{e}"))?;

    let state = dbt_temporal::worker::initialize_project(&project_dir, &config, None).await?;
    eprintln!(
        "Project '{}' loaded, {} nodes, adapter={:?}",
        state.project_name,
        state.resolver_state.nodes.iter().count(),
        state.resolver_state.adapter_type,
    );

    // Pick a model node. Prefer one specified by DBT_NODE env var, otherwise first view model.
    let target_node = std::env::var("DBT_NODE").ok();
    let (unique_id, node) = if let Some(ref target) = target_node {
        state
            .resolver_state
            .nodes
            .iter()
            .find(|(uid, _)| uid.contains(target))
            .unwrap_or_else(|| panic!("node matching '{}' not found", target))
    } else {
        state
            .resolver_state
            .nodes
            .iter()
            .find(|(_, n)| {
                n.resource_type() == NodeType::Model
                    && n.base().materialized.to_string().to_lowercase() == "view"
            })
            .expect("no view model found")
    };

    let common = node.common();
    let base = node.base();
    let rt = node.resource_type();
    eprintln!("\n=== Testing node: {} ===", unique_id);
    eprintln!("  materialized: {}", base.materialized);
    eprintln!("  raw_code: {:?}", common.raw_code.as_deref().map(|s| &s[..s.len().min(50)]));
    eprintln!("  original_file_path: {}", common.original_file_path.display());

    // ---- Set up Jinja env (same as execute_node) ----
    let mut jinja_env = (*state.jinja_env).clone();

    let adapter_engine = Arc::clone(&state.adapter_engine);
    let concrete = dbt_adapter::typed_adapter::ConcreteAdapter::new(adapter_engine);
    let bridge = dbt_adapter::BridgeAdapter::new(Arc::new(concrete), None, None);
    let adapter: Arc<dyn dbt_adapter::BaseAdapter> = Arc::new(bridge);

    dbt_jinja_utils::phases::configure_compile_and_run_jinja_environment(&mut jinja_env, adapter);

    // Add execute as a global
    jinja_env
        .env
        .add_global("execute", minijinja::Value::from(true));

    // Build base context
    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(ToString::to_string).collect())
        .unwrap_or_default();

    let mut base_context = dbt_jinja_utils::phases::build_compile_and_run_base_context(
        Arc::clone(&state.resolver_state.node_resolver),
        &state.resolver_state.root_project_name,
        &state.resolver_state.nodes,
        Arc::clone(&state.resolver_state.runtime_config),
        namespace_keys,
    );

    // Our ResultStore — registered both in context and as globals.
    let result_store = ResultStore::default();
    let store_fn = minijinja::Value::from_function(result_store.store_result());
    let load_fn = minijinja::Value::from_function(result_store.load_result());
    base_context.insert("store_result".to_owned(), store_fn.clone());
    base_context.insert("load_result".to_owned(), load_fn.clone());
    jinja_env.env.add_global("store_result", store_fn);
    jinja_env.env.add_global("load_result", load_fn);

    // Build node context
    let model_yml = dbt_temporal::activities::node_serialization::get_node_yml(
        &state.resolver_state.nodes,
        unique_id,
        rt,
    )?;
    let deprecated_config = dbt_temporal::activities::node_serialization::get_node_config_yml(
        &state.resolver_state.nodes,
        unique_id,
        rt,
    );
    let agate_table = dbt_temporal::activities::node_serialization::build_agate_table(
        &state.resolver_state.nodes,
        unique_id,
        rt,
        &state.io_args,
    )?;
    let sql_header = dbt_temporal::activities::node_serialization::get_sql_header(
        &state.resolver_state.nodes,
        unique_id,
        rt,
    );

    let temp_dir = tempfile::tempdir()?;
    let io_args = dbt_common::io_args::IoArgs {
        in_dir: state.io_args.in_dir.clone(),
        out_dir: temp_dir.path().to_path_buf(),
        invocation_id: state.io_args.invocation_id,
        ..Default::default()
    };

    let mut node_context = dbt_jinja_utils::phases::run::build_run_node_context(
        model_yml,
        common,
        base,
        &deprecated_config,
        state.resolver_state.adapter_type,
        agate_table,
        &base_context,
        &io_args,
        rt,
        sql_header,
        state.packages.clone(),
    )
    .await;

    // Re-inject our ResultStore after build_run_node_context (it creates its own).
    let store_fn = minijinja::Value::from_function(result_store.store_result());
    let load_fn = minijinja::Value::from_function(result_store.load_result());
    node_context.insert("store_result".to_owned(), store_fn.clone());
    node_context.insert("load_result".to_owned(), load_fn.clone());
    jinja_env.env.add_global("store_result", store_fn);
    jinja_env.env.add_global("load_result", load_fn);

    // ---- Diagnostics ----
    eprintln!("\n=== Context checks ===");
    eprintln!("  execute in ctx: {:?}", node_context.get("execute"));
    eprintln!("  execute global: {:?}", jinja_env.env.get_global("execute"));
    eprintln!("  store_result in ctx: {}", node_context.get("store_result").is_some());
    eprintln!("  store_result global: {}", jinja_env.env.get_global("store_result").is_some());
    eprintln!("  adapter in ctx: {}", node_context.get("adapter").is_some());
    eprintln!("  adapter global: {}", jinja_env.env.get_global("adapter").is_some());
    eprintln!("  sql in ctx: {}", node_context.get("sql").is_some());

    // Test 1: render_str with {{ execute }}
    let test1 = jinja_env.render_str("EXECUTE={{ execute }}", &node_context, &[]);
    eprintln!("\n=== Test 1: render_str {{{{ execute }}}} ===");
    eprintln!("  result: {:?}", test1);

    // Test 2: adapter.execute with strict error handling
    // Temporarily switch to strict mode to see the actual error
    jinja_env
        .env
        .set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    let test2 = jinja_env.render_str(
        "{% set result = adapter.execute('SELECT 1', auto_begin=false, fetch=false) %}RESULT={{ result }}",
        &node_context,
        &[],
    );
    eprintln!("\n=== Test 2: adapter.execute() STRICT mode ===");
    eprintln!("  result: {:?}", test2);

    // Test 2b: adapter type check
    let test2b =
        jinja_env.render_str("ADAPTER={{ adapter }}|TYPE={{ adapter.type() }}", &node_context, &[]);
    eprintln!("\n=== Test 2b: adapter type ===");
    eprintln!("  result: {:?}", test2b);

    // Test 3: call statement + load_result in STRICT mode
    let test3 = jinja_env.render_str(
        "{% call statement('test') %}SELECT 1{% endcall %}{% set r = load_result('test') %}LOAD={{ r }}",
        &node_context,
        &[],
    );
    eprintln!("\n=== Test 3: statement('test') + load_result STRICT ===");
    eprintln!("  result: {:?}", test3);

    let adapter_resp_test =
        dbt_temporal::activities::node_helpers::extract_adapter_response(&result_store);
    eprintln!("  adapter_response: {:?}", adapter_resp_test);

    // Test 3b: inline what statement does, step by step
    let test3b = jinja_env.render_str(
        r#"{% set compiled_code = "SELECT 1" %}{% set res, table = adapter.execute(compiled_code, auto_begin=true, fetch=false) %}RES={{ res }}|{{ store_result('inline_test', response=res, agate_table=table) }}DONE"#,
        &node_context,
        &[],
    );
    eprintln!("\n=== Test 3b: inline statement steps STRICT ===");
    eprintln!("  result: {:?}", test3b);
    let adapter_resp_inline =
        dbt_temporal::activities::node_helpers::extract_adapter_response(&result_store);
    eprintln!("  adapter_response: {:?}", adapter_resp_inline);

    // Switch back to lenient
    jinja_env
        .env
        .set_undefined_behavior(minijinja::UndefinedBehavior::Lenient);

    // ---- Compile raw SQL ----
    let raw_sql_path = state.io_args.in_dir.join(&common.original_file_path);
    let raw_sql = std::fs::read_to_string(&raw_sql_path)?;
    eprintln!("\n=== Raw SQL ({} bytes) ===", raw_sql.len());
    eprintln!("{}", raw_sql.trim());

    let compiled = jinja_env
        .render_str(&raw_sql, &node_context, &[])
        .map_err(|e| anyhow::anyhow!("compiling SQL: {e}"))?;
    eprintln!("\n=== Compiled SQL ({} bytes) ===", compiled.len());
    eprintln!("{}", compiled.trim());

    node_context.insert("sql".to_owned(), minijinja::Value::from(compiled.clone()));

    // Write compiled for model.compiled_code
    let dest = io_args.out_dir.join("compiled").join(&common.path);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dest, &compiled)?;

    // ---- Find and render materialization template ----
    let materialization = base.materialized.to_string().to_lowercase();
    let adapter_type = state.resolver_state.adapter_type.to_string().to_lowercase();
    let adapter_leaf = adapter_type
        .split([':', '.'])
        .next_back()
        .unwrap_or(&adapter_type);
    let mat_name = format!("materialization_{materialization}_{adapter_leaf}");
    let fq_name = dbt_temporal::activities::node_helpers::find_materialization_template(
        &jinja_env, &mat_name,
    );
    eprintln!("\n=== Materialization template ===");
    eprintln!("  looking for: {}", mat_name);
    eprintln!("  found: {:?}", fq_name);

    // Ensure target schema/dataset exists (dbt does this before materializations).
    eprintln!("\n=== Creating schema ===");
    let create_result = jinja_env.render_str("{% do create_schema(this) %}", &node_context, &[]);
    eprintln!("  result: {:?}", create_result);

    if let Some(fq) = &fq_name {
        // Try Lenient mode first (realistic path)
        eprintln!("\n=== Rendering materialization (LENIENT): {} ===", fq);
        let rendered = dbt_temporal::activities::node_helpers::render_materialization(
            &jinja_env,
            fq,
            &node_context,
        );
        match &rendered {
            Ok(output) => {
                eprintln!("  output ({} bytes):", output.len());
                let trimmed = output.trim();
                if trimmed.is_empty() {
                    eprintln!("  (empty after trimming)");
                } else {
                    eprintln!("{}", &trimmed[..trimmed.len().min(500)]);
                }
            }
            Err(e) => eprintln!("  ERROR: {e}"),
        }

        // Also try Strict mode to see actual errors
        jinja_env
            .env
            .set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
        eprintln!("\n=== Rendering materialization (STRICT): {} ===", fq);
        let rendered_strict = dbt_temporal::activities::node_helpers::render_materialization(
            &jinja_env,
            fq,
            &node_context,
        );
        match &rendered_strict {
            Ok(output) => {
                eprintln!("  output ({} bytes):", output.len());
                let trimmed = output.trim();
                if trimmed.is_empty() {
                    eprintln!("  (empty after trimming)");
                } else {
                    eprintln!("{}", &trimmed[..trimmed.len().min(500)]);
                }
            }
            Err(e) => eprintln!("  ERROR: {e}"),
        }
        jinja_env
            .env
            .set_undefined_behavior(minijinja::UndefinedBehavior::Lenient);
    }

    // ---- Check adapter response ----
    let adapter_response =
        dbt_temporal::activities::node_helpers::extract_adapter_response(&result_store);
    eprintln!("\n=== Final adapter response ===");
    eprintln!("  {:?}", adapter_response);
    if adapter_response.is_empty() {
        eprintln!("  EMPTY! statement('main') never stored a result.");
    } else {
        eprintln!("  SUCCESS! Adapter executed SQL.");
    }

    eprintln!("\n=== Done ===");
    Ok(())
}
