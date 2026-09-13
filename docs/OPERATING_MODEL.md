# Operating model

The intended continuation pattern is intentionally short:

1. Reconcile the relevant canonical scope at the start of a continuation/new turn or whenever execution state is uncertain.
2. Reuse the returned open work packet when it represents the unfinished logical task; do not infer current work from remembered UUIDs.
3. Give each new side effect a new `operation_id`. Reuse the same id only for an exact retry with identical arguments.
4. Do not start a dependent consumer/audit until its producer is terminal and the required READY build is verified.
5. Integrate a completed work packet promptly. Retire or suspend obsolete/abandoned work through MCPX lifecycle tools rather than deleting workspace storage manually.

An `open` workspace means “unfinished,” not “latest result.” Recent integrated history helps an agent recover what happened recently, but the project's canonical artifacts determine the substantive current state.
