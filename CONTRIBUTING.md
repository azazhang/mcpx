# Contributing

Issues and pull requests are welcome.

For changes to workspace lifecycle, operation recovery, locking, integration or file publication, add an adversarial/regression test. Concurrency and crash-safety changes should be tested under failure, not only on a successful path.

Run before submitting:

```bash
npm ci
npm test
```

The v0.1.x atomic file transaction helper is macOS-specific. A Linux port should preserve equivalent atomic CAS/exchange semantics (for example via `renameat2(..., RENAME_EXCHANGE)`); do not replace it with a weaker check-then-rename implementation and call it equivalent.
