mod ephemeral;
mod jinja_globals;
mod materialization;
mod results;
#[cfg(test)]
mod test_support;

pub use ephemeral::{
    EphemeralContext, EphemeralDirs, ephemeral_context_builder, inject_ephemeral_ctes,
};
pub(in crate::activities) use jinja_globals::{json_to_minijinja, patch_target_global};
pub use materialization::{find_materialization_template, render_materialization};
pub use results::{TestOutcome, extract_adapter_response, extract_test_outcome};
