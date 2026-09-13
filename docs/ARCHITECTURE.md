# MCPX architecture

MCPX is a durable control plane around Model Context Protocol tools for work that can outlive a single model turn or even the visible chat history.

## Three durable objects

**Workspace** — an unfinished logical work packet plus an optional filesystem isolation boundary. A workspace has a canonical project scope, declared mutable paths, a BASE snapshot, a working copy, lifecycle state, revision-bound handle, jobs, READY builds and integration history.

**Job** — an execution inside or outside a workspace. Jobs are durably registered before/while they run so status can be recovered after an MCP server restart or client-context loss.

**Operation** — an idempotent logical side effect keyed by `operation_id`. Exact retries use the same arguments and operation id and return/recover the same durable action rather than duplicating it.

## Workspace lifecycle

- `open`: unfinished work; not a claim that the result is current or authoritative.
- `suspended`: unfinished divergent work preserved cold.
- `integrated`: successfully reconciled into canonical state.
- `retired`: closed without becoming the continuing work packet.

Payload garbage collection is recorded separately from work lifecycle.

## Reconciliation

`reconcile_project_state` is designed for continuation/new-turn recovery. Given a canonical scope it returns:

- open/suspended work packets whose **physical canonical mutable paths** intersect that scope, including packets registered from a parent or child project root;
- revision-bound workspace handles;
- recent integrated work history, followed by a small amount of closure provenance.

This prevents nested roots from becoming separate concurrency universes. For example, `/project` + `paper/x` and `/project/paper` + `x` are the same physical mutable path and must conflict.

## Integration

Workspace integration uses BASE/CURRENT/PROPOSED reconciliation. File publication is compare-and-swap guarded; on macOS the included Swift helper uses atomic exchange primitives so an external writer racing publication is detected rather than silently overwritten.

## Verified producer/consumer pipeline

A verified producer creates a BUILDING gate, runs the producer, validates its result, hashes the complete declared artifact generation and publishes an immutable READY build id.

A verified consumer must name that exact build id. MCPX re-hashes the complete candidate generation before and after the audit so a consumer cannot unknowingly validate a moving target.

## Execution state is not domain truth

MCPX records what work happened and what is unfinished. It deliberately does not decide which scientific, business or engineering conclusion is correct. Canonical project artifacts remain the domain-level source of truth after integration.
