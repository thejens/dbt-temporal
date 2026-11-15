//! What every artifact's metadata block states about the producer.

use std::collections::BTreeMap;

/// dbt version stamped into artifact metadata.
///
/// The artifacts describe a dbt run, so consumers read this as dbt's version,
/// not the orchestrator's — reporting dbt-temporal's would make every artifact
/// claim a dbt that does not exist. None of the
/// pinned crates exports its version as a constant (each stamps its own
/// `CARGO_PKG_VERSION` where it needs one), so it is written here and moves
/// with the pin; `dbt_version_matches_the_pinned_crates` fails if it drifts.
pub(in crate::activities) const DBT_VERSION: &str = "2.0.11";

/// The `env` block every artifact carries: the orchestrator's own version,
/// kept where it does not pretend to be dbt's.
pub(super) fn artifact_env() -> BTreeMap<String, String> {
    BTreeMap::from([("DBT_TEMPORAL_VERSION".to_string(), env!("CARGO_PKG_VERSION").to_string())])
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use anyhow::Context;

    use super::*;

    /// `dbt_version` is written by hand because nothing in the pinned crates
    /// exports it. Cargo.lock does record it, so a bump that forgets this
    /// constant fails here rather than shipping artifacts that name the wrong
    /// dbt.
    #[test]
    fn dbt_version_matches_the_pinned_crates() -> anyhow::Result<()> {
        let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock"))
            .context("reading Cargo.lock")?;
        let pinned = lock
            .split("[[package]]")
            .find(|block| block.contains("name = \"dbt-schemas\""))
            .and_then(|block| {
                block
                    .lines()
                    .find_map(|line| line.trim().strip_prefix("version = "))
            })
            .map(|v| v.trim_matches('"').to_string())
            .context("dbt-schemas not found in Cargo.lock")?;

        assert_eq!(
            pinned, DBT_VERSION,
            "DBT_VERSION must move with the dbt-core pin in Cargo.toml"
        );
        Ok(())
    }
}
