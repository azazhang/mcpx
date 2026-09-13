# Changelog

## 0.1.0

Initial public release extracted from a production-tested MCPX control plane.

Highlights:

- durable idempotent side effects and exact-retry recovery;
- revision-bound workspace handles;
- `open` / `suspended` / `integrated` / `retired` lifecycle;
- BASE/CURRENT/PROPOSED integration with atomic CAS file publication;
- scope-aware reconciliation across nested project roots;
- durable background job registry;
- verified READY producer/consumer pipelines;
- bounded result preservation for oversized MCP responses;
- maintenance/recovery for abandoned, suspended and integrated workspaces;
- command execution disabled by default in the public build.
