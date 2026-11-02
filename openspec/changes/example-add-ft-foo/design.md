# Design: FT.FOO (example)

> Illustrative example — `FT.FOO` is fictional. Shows the level of detail a
> real `design.md` should carry.

## Overview

`FT.FOO <index>` is a thin, read-only command that resolves an index spec and
formats a one-line summary from data already held in memory. It introduces no
new data structures and no background work.

## Command dispatch

- Register `FT.FOO` in `src/module.c` alongside the other `FT.*` handlers, as a
  read-only command (same command flags as `FT.INFO`).
- The handler resolves the index spec via the existing spec-lookup path used by
  `FT.INFO` / `FT.SEARCH`. On a miss it returns the standard
  `Unknown index name (or name is an alias itself)` error so behavior matches
  the rest of the command surface.

## Producing the summary

- Read the already-maintained counters from the resolved spec:
  - index name,
  - number of documents (the same value `FT.INFO` reports as `num_docs`),
  - number of fields in the schema.
- Format as a single human-readable bulk string, e.g.
  `index=<name> docs=<n> fields=<m>`.

## Edge cases

- **Empty index:** `docs=0`, `fields=<schema size>` — not an error.
- **Alias as argument:** resolves through the alias like other commands.
- **Cluster mode:** the command runs per-shard like `FT.INFO`; no fan-out/reduce
  is added in this example.

## Alternatives considered

- **Structured (array) reply** — rejected; `FT.INFO` already covers structured
  consumption. The value of `FT.FOO` is a terse single line.
- **Extending `FT.INFO` with a flag** — rejected; a separate command keeps
  `FT.INFO` parsing untouched and is easier to document.

## Testing strategy

- C unit test for the formatting helper.
- Python end-to-end tests: summary on a populated index, empty index, unknown
  index error, and alias resolution.
