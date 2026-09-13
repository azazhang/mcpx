#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -x "$ROOT/bin/mcpx-filetxn-atomic" ]]; then
  "$ROOT/scripts/build-filetxn.sh"
fi
exec node "$ROOT/server.js"
