# Triage Composite Action Smoke Test

This prompt exists only to verify that the `codex-ci-triage` composite action
captures Codex's final-message output correctly under `sandbox: read-only`.

## Task

Respond with exactly one line of plain text, no markdown, no code fences:

```
Triage composite action smoke test OK — replace this token with the literal string SENTINEL_45f1e93a.
```

The line MUST contain the literal substring `SENTINEL_45f1e93a` so the harness
can grep for it. Do not perform any tool calls, do not read files, do not fetch
anything. Just emit the line.
