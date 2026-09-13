# Security

MCPX is a local control plane, not an authentication or multi-tenant security boundary.

## Safe defaults

- HTTP binds to `127.0.0.1` by default.
- Command-starting tools are not exposed unless `MCPX_ENABLE_COMMAND_EXECUTION=1`.
- Filesystem mutation uses compare-and-swap preconditions and an atomic publication helper.
- Raw paths into MCPX workspace storage are refused; state-bound workspace handles are required.

## Command execution

Enabling command execution gives MCPX the ability to run shell commands as the operating-system user. Treat it accordingly.

`MCPX_WORKSPACE_SANDBOX=none` only changes the working directory and MCPX environment. It is **not** OS-level containment; a command can access anything the user account can access.

`MCPX_WORKSPACE_SANDBOX=codex` delegates workspace command containment to `codex sandbox` and requires a locally configured Codex installation/profile. This is an optional integration, not a dependency of the control plane.

Do not expose MCPX directly to an untrusted network. If remote access is required, place an authenticated, encrypted transport in front of the loopback service and re-evaluate the threat model for your environment.

## Sensitive runtime data

MCPX durable state can contain commands, paths, job output, operation receipts, and workspace history. Runtime directories (`jobs/`, `operations/`, `recovery/`, `results/`, `receipts/`, `file-transactions/`) must not be committed or published.

## Reporting vulnerabilities

Please use GitHub's private vulnerability reporting when available. Do not include credentials, private datasets, or other people's sensitive data in reports.
