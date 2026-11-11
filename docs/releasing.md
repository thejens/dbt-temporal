# Releasing

One `v<version>` tag releases everything, built by
[`.github/workflows/release.yml`](../.github/workflows/release.yml):

- `.deb` and `.rpm` packages for linux/amd64 and linux/arm64, attached to the
  GitHub release
- multi-arch images on GHCR: `ghcr.io/thejens/dbt-temporal:<version>` (Debian
  slim) and `:<version>-distroless`, plus the moving `:<major.minor>`, `:latest`
  and `:distroless` tags
- the Helm chart at `oci://ghcr.io/thejens/charts/dbt-temporal`, same version

There is no crates.io release: the crate depends on dbt's unpublished git
crates, which `cargo publish` rejects.

## Steps

1. From an up-to-date `main`, on a release branch, set the version:

   ```bash
   make bump-version VERSION=0.8.0
   ```

   This updates `Cargo.toml`, `Cargo.lock`, the chart's `version` and
   `appVersion`, and the install command in the chart README. They must agree:
   `make chart-lint` and the release workflow fail when the chart version
   differs from the crate's or from the tag. Do not edit the files by hand.

2. Open a `chore: release v0.8.0` PR and merge it once CI is green.

3. Tag the merge commit and push the tag:

   ```bash
   git fetch origin && git tag v0.8.0 origin/main && git push origin v0.8.0
   ```

4. Watch the release workflow. When it finishes, check that the GitHub release
   is marked Latest and has the four packages, and that both image tags and the
   chart are on GHCR.

## Workflow changes

A release that changes workflow code can break runs still in flight on the
old worker. See
[Workflow changes and in-flight runs](deployment.md#workflow-changes-and-in-flight-runs).
