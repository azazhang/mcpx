# MCPX

**A durable control plane for long-running AI-agent work over the Model Context Protocol.**

MCPX is for workflows that cannot safely assume the model will remember the previous turn, the client will display every tool result, or a long-running command will finish before the MCP server restarts.

It adds durable work packets, crash-safe/idempotent side effects, revision-bound workspace handles, verified producer/audit generations, background-job recovery and scope-aware reconciliation around ordinary MCP filesystem and execution tools.

> **Status:** v0.1.0 is an early public release extracted from a production-used implementation. The strongest atomic file publication helper is currently macOS/Darwin-specific.

## Why MCPX exists

A normal MCP tool call is easy. A multi-hour or multi-day agent workflow is harder:

- the client may hide or lose a tool result after the side effect already happened;
- retrying can duplicate an expensive or destructive operation;
- a process may outlive the model turn or the MCP server;
- multiple agents may work under parent/child project roots that map to the same physical files;
- an old unfinished workspace may coexist with newer canonical results;
- a validator must audit an exact immutable generation rather than a moving directory.

MCPX makes those execution facts durable and queryable.

## Core ideas

### Workspaces are durable work packets

A workspace is both an isolation mechanism and a durable record that a logical piece of work is unfinished. `open` means unfinished; it does **not** mean “latest” or “authoritative.”

### Exact retries are idempotent

Every side effect carries an `operation_id`. A new logical action gets a new id. If a response is lost, the exact same call can be retried with the same id; MCPX recovers/returns the durable action instead of replaying it blindly.

### Scope is physical, not merely nominal

MCPX canonicalizes mutable filesystem paths across registered roots. A workspace rooted at `/project` mutating `paper/x` conflicts with a workspace rooted at `/project/paper` mutating `x`.

### READY generations are immutable audit boundaries

Verified producers publish a READY build only after validation and complete artifact hashing. Verified consumers bind to an exact build id and re-hash the candidate before and after the audit.

## Requirements

- macOS (v0.1.x atomic file transaction helper uses Darwin atomic swap primitives)
- Node.js 22+
- Python 3.10+
- Xcode Command Line Tools / `swiftc`
- optional: Poppler (`pdfinfo`, `pdftotext`) for PDF text extraction
- optional: Codex CLI if using `MCPX_WORKSPACE_SANDBOX=codex`

## Install

```bash
git clone https://github.com/azazhang/mcpx.git
cd mcpx
npm ci
npm run build:filetxn
```

Configure a canonical root:

```bash
export MCPX_BASE_DIR="$HOME/Research"
```

Start MCPX:

```bash
npm start
```

The default endpoint is:

```text
http://127.0.0.1:9090/mcp
```

MCPX does not provide authentication. Keep it loopback-only unless you deliberately place a trusted authenticated transport in front of it.

## Safe command-execution default

The public build does **not** expose command-starting tools by default.

To enable them in a trusted local deployment:

```bash
export MCPX_ENABLE_COMMAND_EXECUTION=1
export MCPX_WORKSPACE_SANDBOX=none
```

`none` is not an OS sandbox. To use the same optional Codex sandbox integration used by the original deployment:

```bash
export MCPX_ENABLE_COMMAND_EXECUTION=1
export MCPX_WORKSPACE_SANDBOX=codex
export MCPX_CODEX_BIN="$(command -v codex)"
```

See [SECURITY.md](SECURITY.md) before enabling command execution or remote access.

## Important tools

- `reconcile_project_state` — recover scope-relevant unfinished work plus recent integrated history.
- `create_agent_workspace` — create a BASE/work copy with declared mutable paths.
- `integrate_agent_workspace` — BASE/CURRENT/PROPOSED reconciliation into canonical state.
- `remove_agent_workspace` / `resume_workspace` — lifecycle management with preservation of divergent work.
- `start_verified_workspace_producer` — publish a validated READY generation.
- `start_verified_workspace_consumer` — audit an exact READY build and verify it stayed unchanged.
- `start_command`, `execute_command`, `command_status` — durable execution/job registry (opt-in for command starts).

## Tested failure modes

The adversarial suite covers, among other cases:

- exact retry after project topology changed;
- overlapping parent/child project roots;
- stale workspace handles;
- simultaneous same-handle mutation races;
- integration/retirement refusal while jobs run;
- server restart and durable job recovery;
- crash after an operation is claimed but before dispatch;
- verified producer/consumer build binding;
- dirty retirement preservation;
- suspension/resume of divergent idle work;
- orphan workspace quarantine;
- maintenance racing a workspace command;
- oversized result preservation.

Run the suite with:

```bash
npm test
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Operating model](docs/OPERATING_MODEL.md)
- [Security](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## Project history

MCPX grew out of repeated failures observed in long-running agent/research workflows: hidden tool responses, duplicate retries, stale connector schemas, phantom jobs, nested-root concurrency, abandoned workspaces and audits accidentally targeting stale generations. The public repository contains the generalized control-plane implementation and synthetic/adversarial tests, not the private projects that originally exercised it.

## License

Apache License 2.0. See [LICENSE](LICENSE).
