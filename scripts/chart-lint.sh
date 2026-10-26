#!/usr/bin/env bash
# Lint the Helm chart and validate what it renders.
#
# For every values file under charts/dbt-temporal/ci/: `helm lint --strict`,
# then the rendered manifests through kubeconform against the Kubernetes schemas
# and the CRD catalog (KEDA, Prometheus Operator). Also enforces that the chart
# and the crate carry the same version, since one tag releases both.
set -euo pipefail

CHART=charts/dbt-temporal
K8S_VERSION=1.30.0
CRD_SCHEMAS='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

crate_version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
chart_version=$(sed -n 's/^version: \(.*\)$/\1/p' "$CHART/Chart.yaml")
app_version=$(sed -n 's/^appVersion: "\(.*\)"$/\1/p' "$CHART/Chart.yaml")
if [[ "$chart_version" != "$crate_version" || "$app_version" != "$crate_version" ]]; then
  echo "version mismatch: Cargo.toml $crate_version, Chart.yaml version $chart_version / appVersion $app_version" >&2
  exit 1
fi

for values in "$CHART"/ci/*.yaml; do
  echo "== $values"
  helm lint --strict "$CHART" -f "$values"
  helm template ci "$CHART" -f "$values" |
    kubeconform -strict -summary -kubernetes-version "$K8S_VERSION" \
      -schema-location default -schema-location "$CRD_SCHEMAS"
done
