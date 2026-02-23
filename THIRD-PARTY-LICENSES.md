# Third-Party Licenses

dbt-temporal depends on two major libraries. Their licenses are summarized below
as of this writing — always check the linked sources for the current terms.

## dbt-fusion

[dbt-fusion](https://github.com/dbt-labs/dbt-fusion) uses a **split license
model**. As of this writing, some crates (like `dbt-adapter`, `dbt-auth`,
`dbt-agate`, `dbt-xdbc`, `dbt-jinja`) are **Apache 2.0**, while the remaining
crates (including `dbt-loader`, `dbt-parser`, `dbt-dag`, `dbt-schemas`,
`dbt-common`, `dbt-csv`, and their `minijinja` fork) are under the **Elastic
License 2.0 (ELv2)**.

The ELv2 restricts offering the software as a hosted or managed service to third
parties. Since dbt-temporal links against ELv2-licensed crates, compiled binaries
are subject to those terms.

- License details: [dbt-fusion LICENSES.md](https://github.com/dbt-labs/dbt-fusion/blob/main/LICENSES.md)
- Full ELv2 text: [elastic.co/licensing/elastic-license](https://www.elastic.co/licensing/elastic-license)

## Temporal Rust SDK

[Temporal's Rust SDK (sdk-core)](https://github.com/temporalio/sdk-core) is
**MIT-licensed** as of this writing.

- License file: [sdk-core LICENSE.txt](https://github.com/temporalio/sdk-core/blob/master/LICENSE.txt)

## Other dependencies

The remaining dependencies (Arrow, DataFusion, tokio, serde, etc.) are standard
open-source Rust crates under permissive licenses (MIT and/or Apache 2.0). The
project also uses forked versions of Arrow, DataFusion, sqlparser, and arrow-adbc
maintained by [sdf-labs](https://github.com/sdf-labs) and
[dbt-labs](https://github.com/dbt-labs) for dbt-fusion compatibility — these
retain the same Apache 2.0 license as their upstream projects.

See [Cargo.toml](Cargo.toml) for the full dependency list and source URLs.
