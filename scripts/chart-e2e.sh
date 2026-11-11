#!/usr/bin/env bash
# End-to-end test of the Helm chart: install it on a throwaway k3d cluster,
# run one dbt_run workflow, and check that every node succeeded.
#
#   IMAGE_TAG=0.7.3 scripts/chart-e2e.sh
#
# Environment:
#   IMAGE_TAG       image tag to deploy (required)
#   IMAGE_VARIANT   "" for debian-slim (default) or "distroless"
#   IMAGE_REPO      default ghcr.io/thejens/dbt-temporal
#   IMAGE_IMPORT    1 to `k3d image import` a local image instead of pulling
#   CLUSTER         k3d cluster name, default dbt-temporal-e2e
#   KEEP_CLUSTER    1 to leave the cluster up afterwards, for debugging
#
# What it covers, beyond the templates rendering (chart-lint does that): the
# pod starting under the chart's security context and read-only root
# filesystem, the probes passing, the profile Secret and env wiring, a
# project mounted read-only from a ConfigMap (so dbt's .gitignore write takes
# the worker's staged-copy path), and the image's Postgres driver loading
# (libpq, the experimental-adapter gate). Needs k3d, kubectl, helm and jq.
set -euo pipefail

: "${IMAGE_TAG:?set IMAGE_TAG to the image tag to test}"
IMAGE_VARIANT=${IMAGE_VARIANT:-}
IMAGE_REPO=${IMAGE_REPO:-ghcr.io/thejens/dbt-temporal}
CLUSTER=${CLUSTER:-dbt-temporal-e2e}
NS=dbt-e2e
RELEASE=e2e
DEPLOYMENT=$RELEASE-dbt-temporal

cd "$(dirname "$0")/.."
E2E=tests/chart-e2e
# orders, customer_totals, and the two column tests in schema.yml.
EXPECTED_NODES=4

work=$(mktemp -d)
kube() { kubectl --context "k3d-$CLUSTER" -n "$NS" "$@"; }

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    echo "::group::diagnostics" >&2
    kube get pods -o wide >&2 || true
    kube describe pods -l "app.kubernetes.io/instance=$RELEASE" >&2 || true
    kube logs "deploy/$DEPLOYMENT" --tail=200 >&2 || true
    echo "::endgroup::" >&2
  fi
  if [[ "${KEEP_CLUSTER:-}" != 1 ]]; then
    k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

k3d cluster create "$CLUSTER" --no-lb --wait --timeout 180s >/dev/null
kubectl --context "k3d-$CLUSTER" create namespace "$NS" >/dev/null

image=$IMAGE_REPO:$IMAGE_TAG${IMAGE_VARIANT:+-$IMAGE_VARIANT}
if [[ "${IMAGE_IMPORT:-}" == 1 ]]; then
  k3d image import -c "$CLUSTER" "$image" >/dev/null
fi

kube apply -f "$E2E/infra.yaml" >/dev/null

# The project goes in as one ConfigMap. Keys cannot contain "/", so each file
# gets a flattened key, and the volume's `items` put it back at its path.
from_files=()
items='[]'
while IFS= read -r path; do
  rel=${path#"$E2E/project/"}
  key=${rel//\//__}
  from_files+=("--from-file=$key=$path")
  items=$(jq -c --arg k "$key" --arg p "$rel" '. + [{key: $k, path: $p}]' <<<"$items")
done < <(find "$E2E/project" -type f | sort)
kube create configmap dbt-project "${from_files[@]}" >/dev/null

jq -n --argjson items "$items" --arg repo "$IMAGE_REPO" --arg tag "$IMAGE_TAG" \
  --arg variant "$IMAGE_VARIANT" '{
    image: {
      repository: $repo, tag: $tag, variant: $variant,
      pullPolicy: (if env.IMAGE_IMPORT == "1" then "Never" else "IfNotPresent" end)
    },
    extraVolumes: [{name: "project", configMap: {name: "dbt-project", items: $items}}],
    extraVolumeMounts: [{name: "project", mountPath: "/dbt/project", readOnly: true}]
  }' >"$work/generated-values.json"

kube rollout status deploy/temporal deploy/postgres --timeout=180s >/dev/null
helm --kube-context "k3d-$CLUSTER" install "$RELEASE" charts/dbt-temporal -n "$NS" \
  -f "$E2E/values.yaml" -f "$work/generated-values.json" >/dev/null
kube rollout status "deploy/$DEPLOYMENT" --timeout=300s

# The Temporal image is the CLI, so the workflow is started from inside it:
# nothing to install on the machine running the test.
kube exec deploy/temporal -- temporal workflow execute \
  --address localhost:7233 --type dbt_run --task-queue dbt-tasks \
  --workflow-id e2e --input '{}' -o json >"$work/result.json" || true

jq -e --argjson expected "$EXPECTED_NODES" '
  (.result // .) as $r
  | $r.success == true
    and ($r.node_results | length) == $expected
    and all($r.node_results[]; .status == "success")
' "$work/result.json" >/dev/null || {
  echo "workflow did not succeed with $EXPECTED_NODES successful nodes:" >&2
  cat "$work/result.json" >&2
  exit 1
}

# The run must have gone through the staged copy, or the read-only mount was
# not read-only and the test proves less than it claims. Logs go to a file
# first: `kubectl logs | grep -q` fails under pipefail whenever grep matches
# early and kubectl dies writing to the closed pipe.
kube logs "deploy/$DEPLOYMENT" >"$work/worker.log"
grep -q "loading a copy" "$work/worker.log" || {
  echo "the worker loaded the read-only project in place: staged-copy path not exercised" >&2
  exit 1
}

echo "chart e2e passed: $image, $EXPECTED_NODES nodes"
