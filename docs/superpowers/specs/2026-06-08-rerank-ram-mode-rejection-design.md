# MOD-16015 — Reject `RERANK` for HNSW vector fields in RAM mode

## Problem

`RERANK` is a disk-only (bigredis/Flex) option for HNSW vector field creation.
It is parsed in the unconditional HNSW parsing loop in `parseVectorField_hnsw()`
(`src/spec.c:873-890`). The disk-mode validation block (`src/spec.c:910-942`)
only *requires* `RERANK` when `isSpecOnDiskForValidation(sp)` is true — it never
*forbids* it in RAM mode.

As a result, in RAM mode `RERANK TRUE` is silently accepted: `*rerank` is set
but never used, because the value only flows into `VecSimDiskContext.rerank`
when `sp->diskSpec` is set, which does not happen in RAM mode. The other forms
already error, but with messages that imply `RERANK` is otherwise valid:

| Input (RAM mode)        | Current result                                   |
| ----------------------- | ------------------------------------------------ |
| `RERANK TRUE`           | Accepted, value discarded (the bug)              |
| `RERANK FALSE`          | `Syntax error: RERANK only supports TRUE currently` |
| `RERANK` (no arg)       | `RERANK requires an argument`                    |
| duplicate `RERANK`      | `Duplicate RERANK parameter`                     |

## Expected behavior

In RAM mode, `RERANK` is not a valid HNSW field option at all. Any occurrence —
including `RERANK TRUE` — must return an error stating the option is disk-only.

| Input (RAM mode)        | New result                                       |
| ----------------------- | ------------------------------------------------ |
| `RERANK TRUE`           | `RERANK is only supported for disk-based vector indexes` |
| `RERANK FALSE`          | `RERANK is only supported for disk-based vector indexes` |
| `RERANK` (no arg)       | `RERANK is only supported for disk-based vector indexes` |
| duplicate `RERANK`      | `RERANK is only supported for disk-based vector indexes` |

Disk mode is unchanged: `RERANK TRUE` accepted, missing/`FALSE`/no-arg/duplicate
keep their existing disk-mode errors.

## Fix

Add an early guard at the top of the `RERANK` branch in `parseVectorField_hnsw()`,
before the duplicate / argument / value checks:

```c
} else if (AC_AdvanceIfMatch(&subAc, VECSIM_RERANK)) {
  if (!isSpecOnDiskForValidation(sp)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_INVAL,
      "RERANK is only supported for disk-based vector indexes");
    return 0;
  }
  // existing duplicate / requires-argument / TRUE-only checks unchanged
```

Rationale for rejecting at the token (rather than a post-loop check on a parsed
`RERANK TRUE`): in RAM mode `RERANK` is simply not a recognized option, so every
form should produce the same disk-only error. This matches the ticket's
"including `RERANK TRUE`" wording and avoids leaking the TRUE-only / requires-arg
messages that imply the option is otherwise usable.

- `sp` is the first parameter of `parseVectorField_hnsw()` and is already in
  scope (used by the disk-validation block at `src/spec.c:910`).
- The disk path is unaffected: when `isSpecOnDiskForValidation(sp)` is true the
  guard is skipped and the existing parsing + disk validation run as before.

## Affected code

- `src/spec.c:873-890` — `RERANK` parsing branch (add guard).
- `src/spec.c:910-942` — disk-mode validation block (unchanged, reference only).

## Tests

Add a pytest in `tests/pytests/test_flex_validation.py`, sibling to
`test_flex_disk_hnsw_rerank_requires_true_value`, decorated
`@with_simulate_in_flex(False)`. Assert that `RERANK TRUE`, `RERANK FALSE`,
`RERANK` (no arg), and duplicate `RERANK` on an HNSW field all
`.error().contains('RERANK is only supported for disk-based vector indexes')`.

The existing `@with_simulate_in_flex(True)` test
(`test_flex_disk_hnsw_rerank_requires_true_value`) confirms the disk path keeps
its current behavior.

## Out of scope

- Query-time `=>[KNN ... RERANK TRUE]` / `=>{$RERANK: TRUE;}` handling
  (`src/query.c:2249`). That is a separate path and already errors in RAM mode
  (`tests/pytests/test_vecsim.py:696-699`).

## Release notes

Required — user-facing change: `FT.CREATE` now rejects `RERANK` on HNSW vector
fields in RAM mode instead of silently accepting `RERANK TRUE`.
