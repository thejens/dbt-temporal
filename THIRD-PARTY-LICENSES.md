# Third-Party Licenses

dbt-temporal depends on two major libraries. Their licenses are summarized below
as of this writing — always check the linked sources for the current terms.

## dbt Core v2 (Fusion engine)

The [dbt Fusion engine](https://github.com/dbt-labs/dbt-core), developed in the
dbt-core repository as dbt Core v2, is **Apache 2.0**. The workspace license
applies to every crate dbt-temporal consumes, including the bundled `minijinja`
fork.

Earlier Fusion preview releases in the separate
[dbt-fusion](https://github.com/dbt-labs/dbt-fusion) repository used a split
Apache 2.0 / Elastic License 2.0 (ELv2) model that restricted offering the
software as a hosted service. dbt-temporal no longer depends on that repository,
and none of the crates it links against are under the Elastic License — the
restriction does not apply to dbt-temporal binaries.

- License file: [dbt-core LICENSE](https://github.com/dbt-labs/dbt-core/blob/main/LICENSE)

## Temporal Rust SDK

The [Temporal Rust SDK](https://github.com/temporalio/sdk-rust), consumed as the
published `temporalio-*` crates, is **MIT-licensed** as of this writing.

- License file: [sdk-rust LICENSE.txt](https://github.com/temporalio/sdk-rust/blob/main/LICENSE.txt)

## Other dependencies

The remaining dependencies (Arrow, tokio, serde, etc.) are standard open-source
Rust crates under permissive licenses (MIT and/or Apache 2.0). The project also
pins forks of arrow-rs and ring maintained by
[sdf-labs](https://github.com/sdf-labs) and a fork of arrow-adbc maintained by
[dbt-labs](https://github.com/dbt-labs) for Fusion-engine compatibility — the
forks retain the same licenses as their upstream projects.

See [Cargo.toml](Cargo.toml) for the full dependency list and source URLs.
