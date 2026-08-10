# Example change: Add FT.FOO

> **This is an illustrative example, not a real or planned RediSearch feature.**
> It exists to show the shape of a spec-driven change. `FT.FOO` is fictional.
> Copy this directory as a starting point for your own change, or write the
> artifacts by hand. See [`../../../docs/CONTRIBUTING-specs.md`](../../../docs/CONTRIBUTING-specs.md).

## Why

Operators occasionally want a one-shot, human-readable health line for an index
without parsing the full `FT.INFO` reply. Today that requires reading and
interpreting many `FT.INFO` fields. A tiny convenience command would make quick
checks and shell scripts simpler.

This is deliberately small: it adds read-only output derived from existing
state, with no new persistence and no change to indexing behavior — a good size
for a first contribution.

## What Changes

Add a new read-only command:

```
FT.FOO <index>
```

- Returns a single bulk string summarizing the index: its name, number of
  documents, and number of indexed fields.
- Errors with `Unknown index name (or name is an alias itself)` if the index
  does not exist (matching `FT.SEARCH`).
- No new configuration, no persistence/RDB changes, no API changes to existing
  commands.

### Non-goals

- No machine-readable / structured output (use `FT.INFO` for that).
- No cluster-specific aggregation beyond what `FT.INFO` already exposes.
