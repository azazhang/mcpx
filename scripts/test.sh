#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

./scripts/build-filetxn.sh
npm run check
python3 -m py_compile bin/mcpx-verified-gate helpers/verified_gate.py workspace-control/test_adversarial.py workspace-control/test_claim_recovery.py workspace-control/mcp_test_client.py

T="$(mktemp -d /tmp/mcpx-public-adversarial.XXXXXX)"
SERVER_PID=""
cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$T"
}
trap cleanup EXIT
mkdir -p "$T/project/mutable" "$T/project/cold" "$T/project/clean" "$T/project/other" "$T/workspaces" "$T/recovery" "$T/operations" "$T/jobs" "$T/results" "$T/receipts" "$T/filetxns"
printf 'base\n' > "$T/project/mutable/file.txt"
printf 'cold-base\n' > "$T/project/cold/file.txt"

MCPX_BASE_DIR="$T/project" \
MCPX_WORKSPACE_ROOT="$T/workspaces" \
MCPX_RECOVERY_DIR="$T/recovery" \
MCPX_OPERATIONS_DIR="$T/operations" \
MCPX_JOB_DIR="$T/jobs" \
MCPX_RESULT_DIR="$T/results" \
MCPX_RECEIPT_DIR="$T/receipts" \
MCPX_FILETXN_DIR="$T/filetxns" \
MCPX_FILETXN_BIN="$ROOT/bin/mcpx-filetxn-atomic" \
MCPX_VERIFIED_GATE_BIN="$ROOT/bin/mcpx-verified-gate" \
MCPX_ENABLE_COMMAND_EXECUTION=1 \
MCPX_WORKSPACE_SANDBOX=none \
MCP_HTTP_HOST=127.0.0.1 MCP_HTTP_PORT=19092 \
node server.js >"$T/server.out" 2>"$T/server.err" &
SERVER_PID=$!

python3 - <<'PY'
import time,sys
sys.path.insert(0,'workspace-control')
from mcp_test_client import MCPClient
end=time.time()+15
while time.time()<end:
    try:
        c=MCPClient('http://127.0.0.1:19092/mcp'); c.init(); break
    except Exception: time.sleep(.1)
else: raise SystemExit('test server did not become ready')
PY
MCPX_TEST_RUNTIME="$T" python3 workspace-control/test_adversarial.py
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

MCPX_CLAIM_TEST_RUNTIME="$T/claim-recovery" python3 workspace-control/test_claim_recovery.py

# Safe-default surface: command-start tools are hidden unless execution is opted in.
SAFE="$(mktemp -d /tmp/mcpx-public-safe.XXXXXX)"
mkdir -p "$SAFE/project" "$SAFE/workspaces" "$SAFE/recovery" "$SAFE/operations" "$SAFE/jobs" "$SAFE/results" "$SAFE/receipts" "$SAFE/filetxns"
MCPX_BASE_DIR="$SAFE/project" MCPX_WORKSPACE_ROOT="$SAFE/workspaces" MCPX_RECOVERY_DIR="$SAFE/recovery" MCPX_OPERATIONS_DIR="$SAFE/operations" MCPX_JOB_DIR="$SAFE/jobs" MCPX_RESULT_DIR="$SAFE/results" MCPX_RECEIPT_DIR="$SAFE/receipts" MCPX_FILETXN_DIR="$SAFE/filetxns" MCPX_FILETXN_BIN="$ROOT/bin/mcpx-filetxn-atomic" MCP_HTTP_HOST=127.0.0.1 MCP_HTTP_PORT=19095 node server.js >"$SAFE/server.out" 2>"$SAFE/server.err" &
SPID=$!
python3 - <<'PY'
import time,sys
sys.path.insert(0,'workspace-control')
from mcp_test_client import MCPClient
end=time.time()+15
while True:
    try:
        c=MCPClient('http://127.0.0.1:19095/mcp'); c.init(); break
    except Exception:
        if time.time()>end: raise
        time.sleep(.1)
names={t['name'] for t in c.tools()}
for forbidden in {'start_command','execute_command','start_workspace_command','start_verified_workspace_producer','start_verified_workspace_consumer','send_command_input'}:
    assert forbidden not in names, forbidden
r=c.call('execute_command',{'command':'true','operation_id':'public-safe-default-check'})
assert r.get('isError') and (r.get('structuredContent') or {}).get('state')=='COMMAND_EXECUTION_DISABLED',r
print('SAFE_DEFAULT_PASS')
PY
kill "$SPID" 2>/dev/null || true; wait "$SPID" 2>/dev/null || true
rm -rf "$SAFE"
echo "ALL_PUBLIC_TESTS_PASS"
