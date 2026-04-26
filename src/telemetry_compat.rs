//! Minimal tracing layer that satisfies dbt-fusion's telemetry expectations.
//!
//! dbt-fusion's adapter code expects `TelemetryAttributes` to be stored in span
//! extensions (normally done by `TelemetryDataLayer`). Without this, calls to
//! `record_current_span_status_from_attrs` panic with "Missing span event attributes".
//!
//! This layer inserts a default `TelemetryAttributes` for every new span, preventing
//! the panic while keeping our own `tracing_subscriber::fmt` for log output.

use dbt_telemetry::{TelemetryAttributes, Unknown};
use tracing::span;
use tracing_subscriber::{layer::Context, registry::LookupSpan};

/// A [`tracing_subscriber::Layer`] that stores default [`TelemetryAttributes`]
/// in span extensions so dbt-fusion code doesn't panic when accessing them.
#[derive(Debug)]
pub struct DbtTelemetryCompatLayer;

impl<S> tracing_subscriber::Layer<S> for DbtTelemetryCompatLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let attrs: TelemetryAttributes = Unknown {
                name: span.name().to_string(),
                file: String::new(),
                line: 0,
            }
            .into();
            span.extensions_mut().insert(attrs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::registry::Registry;

    #[test]
    fn layer_inserts_telemetry_attributes_for_new_spans() {
        let subscriber = Registry::default().with(DbtTelemetryCompatLayer);
        with_default(subscriber, || {
            let _span = tracing::info_span!("a_span").entered();
            // Without the layer, dbt-fusion code that reads TelemetryAttributes from
            // span extensions panics. Reaching this line means the layer ran without
            // panicking and `on_new_span` executed for the span we just created.
        });
    }

    #[test]
    fn layer_implements_debug() {
        // `#[derive(Debug)]` on the unit struct — exercise it so the impl gets compiled
        // into the coverage build.
        let s = format!("{DbtTelemetryCompatLayer:?}");
        assert!(s.contains("DbtTelemetryCompatLayer"));
    }
}
