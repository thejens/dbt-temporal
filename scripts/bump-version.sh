#!/usr/bin/env bash
# Set the release version everywhere it is written down.
#
#   scripts/bump-version.sh 0.8.0
#
# One tag releases the crate, the packages, the images and the Helm chart, so
# they all carry one version: Cargo.toml (and Cargo.lock), the chart's
# `version` and `appVersion` (the chart defaults its image tag to appVersion),
# and the install command in the chart README. chart-lint and the release
# workflow both fail on a chart that disagrees with the crate.
set -euo pipefail

version=${1:-}
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <major.minor.patch>" >&2
  exit 1
fi

cd "$(dirname "$0")/.."
CHART=charts/dbt-temporal

current=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
if [[ "$current" == "$version" ]]; then
  echo "already at $version" >&2
  exit 1
fi

# Only the first `version = ` line: it is [package]'s, and dependency tables
# further down use the same key.
sed -i.bak "1,/^version = /s/^version = \".*\"$/version = \"$version\"/" Cargo.toml
sed -i.bak -e "s/^version: .*/version: $version/" \
  -e "s/^appVersion: .*/appVersion: \"$version\"/" "$CHART/Chart.yaml"
sed -i.bak "s/--version [0-9][0-9.]*/--version $version/" "$CHART/README.md"
rm -f Cargo.toml.bak "$CHART/Chart.yaml.bak" "$CHART/README.md.bak"

# Rewrites the crate's own entry in Cargo.lock and nothing else.
cargo update --workspace --offline --quiet

echo "$current -> $version"
git diff --stat
