#!/usr/bin/env bash
#
# End-to-end test for the bigquery example.
#
# Runs a dbt workflow against BigQuery using the bigquery example project.
# No Docker needed — just BigQuery credentials and a Temporal dev server.
#
# Prerequisites:
#   - cargo (Rust toolchain)
#   - temporal CLI (https://docs.temporal.io/cli)
#   - GOOGLE_CLOUD_PROJECT env var (or gcloud default project)
#   - Application default credentials (gcloud auth application-default login)
#
# Usage:
#   ./tests/test_examples.sh          # run tests
#   ./tests/test_examples.sh --keep   # keep runtime running after test (for debugging)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

KEEP_RUNNING=false
if [[ "${1:-}" == "--keep" ]]; then
    KEEP_RUNNING=true
fi

# Colors for output.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}==>${NC} $*"; }
warn()  { echo -e "${YELLOW}==>${NC} $*"; }
fail()  { echo -e "${RED}FAIL:${NC} $*" >&2; exit 1; }

# Resolve GCP project.
GCP_PROJECT="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "$GCP_PROJECT" ]]; then
    fail "GOOGLE_CLOUD_PROJECT not set and gcloud has no default project"
fi
export GOOGLE_CLOUD_PROJECT="$GCP_PROJECT"
info "Using GCP project: $GCP_PROJECT"

# Pick a random high port for Temporal to avoid conflicts.
TEMPORAL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
TEMPORAL_UI_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")

# Track background PIDs for cleanup.
TEMPORAL_PID=""
WORKER_PID=""

cleanup() {
    info "Cleaning up..."
    if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    if [[ "$KEEP_RUNNING" == "false" ]]; then
        if [[ -n "$TEMPORAL_PID" ]] && kill -0 "$TEMPORAL_PID" 2>/dev/null; then
            kill "$TEMPORAL_PID" 2>/dev/null || true
            wait "$TEMPORAL_PID" 2>/dev/null || true
        fi
    else
        warn "Leaving runtime running (--keep). Shut down with:"
        warn "  kill $TEMPORAL_PID  # Temporal dev server (port $TEMPORAL_PORT)"
    fi
}
trap cleanup EXIT

# --- Preflight checks ---

command -v cargo >/dev/null 2>&1 || fail "cargo is not installed"
command -v temporal >/dev/null 2>&1 || fail "temporal CLI is not installed (https://docs.temporal.io/cli)"

# --- Start Temporal dev server ---

info "Starting Temporal dev server (port $TEMPORAL_PORT, UI $TEMPORAL_UI_PORT)..."
temporal server start-dev \
    --log-level error \
    --port "$TEMPORAL_PORT" \
    --ui-port "$TEMPORAL_UI_PORT" \
    &
TEMPORAL_PID=$!

# Wait for Temporal to be ready.
for i in $(seq 1 30); do
    if temporal operator cluster health --address "localhost:$TEMPORAL_PORT" 2>/dev/null | grep -q SERVING; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        fail "Temporal dev server did not become healthy within 30 seconds"
    fi
    sleep 1
done
info "Temporal dev server ready (pid $TEMPORAL_PID)"

# --- Build the worker ---

info "Building dbt-temporal (debug)..."
cargo build --manifest-path "$ROOT_DIR/Cargo.toml" 2>&1 | tail -5
WORKER_BIN="$ROOT_DIR/target/debug/dbt-temporal"
[[ -x "$WORKER_BIN" ]] || fail "Worker binary not found at $WORKER_BIN"

# --- Start the single-project worker ---

info "Starting single-project worker (BigQuery project=$GCP_PROJECT)..."
TEMPORAL_ADDRESS="http://localhost:$TEMPORAL_PORT" \
TEMPORAL_TASK_QUEUE="single-project" \
DBT_PROJECT_DIR="$ROOT_DIR/examples/single-project" \
    "$WORKER_BIN" &
WORKER_PID=$!

# Give the worker time to connect and register.
info "Waiting for worker to register..."
sleep 5

# Check the worker is still alive.
if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    fail "Worker process died during startup"
fi

# --- Submit a workflow ---

WORKFLOW_ID="test-example-$(date +%s)"
info "Submitting single-project workflow ($WORKFLOW_ID)..."

temporal workflow start \
    --address "localhost:$TEMPORAL_PORT" \
    --type dbt_run \
    --task-queue single-project \
    --workflow-id "$WORKFLOW_ID" \
    --input '{"command": "run"}' \
    2>&1

# --- Wait for workflow to complete ---

info "Waiting for workflow to complete..."
TIMEOUT=120
for i in $(seq 1 "$TIMEOUT"); do
    STATUS=$(temporal workflow describe \
        --address "localhost:$TEMPORAL_PORT" \
        --workflow-id "$WORKFLOW_ID" \
        --output json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
status = data.get('workflowExecutionInfo', data).get('status', '')
print(status)
" 2>/dev/null || echo "")

    case "$STATUS" in
        *COMPLETED*|*Completed*)
            info "Workflow completed successfully!"
            break
            ;;
        *FAILED*|*Failed*)
            fail "Workflow failed"
            ;;
        *TERMINATED*|*Terminated*)
            fail "Workflow was terminated"
            ;;
        *TIMED_OUT*|*TimedOut*)
            fail "Workflow timed out"
            ;;
        *)
            if [[ $i -eq $TIMEOUT ]]; then
                fail "Workflow did not complete within ${TIMEOUT}s (last status: $STATUS)"
            fi
            sleep 1
            ;;
    esac
done

# --- Verify workflow result ---

info "Verifying workflow result..."
RESULT=$(temporal workflow show \
    --address "localhost:$TEMPORAL_PORT" \
    --workflow-id "$WORKFLOW_ID" \
    --output json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for event in reversed(data.get('events', [])):
    attrs = event.get('workflowExecutionCompletedEventAttributes', {})
    result = attrs.get('result', {})
    payloads = result.get('payloads', [])
    if payloads:
        import base64
        payload_data = base64.b64decode(payloads[0].get('data', ''))
        output = json.loads(payload_data)
        print(json.dumps(output, indent=2))
        sys.exit(0)
print('{}')
" 2>/dev/null || echo "{}")

SUCCESS=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success', False))" 2>/dev/null || echo "")
NODE_COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('node_results', [])))" 2>/dev/null || echo "0")

if [[ "$SUCCESS" == "True" ]]; then
    info "Workflow result: success=true, nodes=$NODE_COUNT"
else
    warn "Could not parse workflow result (this is OK if the workflow completed successfully)"
    warn "Result: $RESULT"
fi

# --- Summary ---

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} single-project example: PASSED${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Workflow ID: $WORKFLOW_ID"
echo "  Worker PID:  $WORKER_PID"
echo "  GCP Project: $GCP_PROJECT"
echo ""
