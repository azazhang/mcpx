#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "MCPX v0.1.0 atomic file transaction helper currently requires macOS/Darwin." >&2
  echo "A Linux renameat2(RENAME_EXCHANGE) port is not yet included; refusing a weaker fallback." >&2
  exit 69
fi
command -v swiftc >/dev/null 2>&1 || { echo "swiftc is required (install Xcode Command Line Tools)." >&2; exit 69; }
mkdir -p "$ROOT/bin"
swiftc -O "$ROOT/helpers/mcpx-filetxn-atomic.swift" -o "$ROOT/bin/mcpx-filetxn-atomic"
chmod +x "$ROOT/bin/mcpx-filetxn-atomic"
echo "built $ROOT/bin/mcpx-filetxn-atomic"
