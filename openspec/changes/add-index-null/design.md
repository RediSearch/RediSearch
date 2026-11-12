# Design: INDEXNULL

## Overview

`INDEXNULL` mirrors `INDEXMISSING` end to end: a per-field doc-ids-only inverted
index records the documents to match, and a dedicated query node reads it. The
two differ only in *which* documents get recorded, so the implementation shares
the `INDEXMISSING` machinery wherever it can rather than duplicating it.

## Schema and persistence

- `FieldSpec_IndexNull = 0x400` in `FieldSpecOptions` (and the Rust
  `FieldSpecOption::IndexNull`), with `FieldSpec_IndexesNull(fs)`.
- Parsed wherever `INDEXMISSING` is: the TEXT and TAG option loops, and a shared
  helper for NUMERIC, GEO, GEOSHAPE and VECTOR that takes `INDEXMISSING` and
  `INDEXNULL` once each, in either order.
- Rejected with `NOINDEX` (a null value of a field that isn't indexed can't be
  queried), and on disk-backed specs, at each parse site through
  `SearchDisk_MarkUnsupportedArgumentIfDiskEnabled`, as `WITHSUFFIXTRIE` is.
- `FieldSpecOptions` is already persisted as a whole, so the new bit round-trips
  through RDB without an encoding version bump, as `INDEXMISSING` (2.10) and
  `INDEXEMPTY` did.

## Indexing

- `IndexSpec.nulls.indexes`: a dict from field name to a doc-ids-only inverted
  index, created with the same `dictType` as `missing.indexes`.
- `writeNullFieldDocs` runs after `writeMissingFieldDocs` in
  `indexDocumentMemory`. It records each field with `INDEXNULL` whose document
  field is `FLD_VAR_T_NULL`. Both share a `writeFieldDoc` helper that gets or
  creates the field's inverted index, finishes the dict's rehashing, writes the
  doc ID and updates the stats.
- A field is `FLD_VAR_T_NULL` only when its JSONPath yields a single `null`
  result (`json.c`); multi-value results skip their `null`s, so they never make
  the field null.
- An update re-indexes the document under a new doc ID, so a field going from
  `null` to a value (or back) needs no special handling; the old ID's entry is
  left for the GC like any other.

## Querying

- Lexer token `ISNULL` and parser rule `expr ::= ISNULL LP modifier RP`
  (generated with ragel 6.10 / lemon, matching upstream), producing a
  `QN_ISNULL` node carrying the field index. A field without `INDEXNULL` yields
  the error `'isnull' requires defining the field with 'INDEXNULL'`.
- `FT.EXPLAIN` prints `ISNULL{field}`.
- Rust: `QueryNode::IsNull` evaluates to the Missing iterator, generalized over a
  `FieldDocsKind` (`Missing` | `Null`) that selects the spec's dict, the
  iterator type (`InvIdxNull`, profiled as `NULL`), and the dict revalidation
  re-reads after a lock release. Null docs skip the field-expiration check: only
  JSON documents hold nulls, and JSON fields don't expire individually.

## Garbage collection

- The missing-docs scanner is generalized over `FieldDocsKind`; both the fork
  and in-process drivers run a null-docs pass after the missing-docs one.
  Emptied indexes are removed from the dict.
- The fork wire protocol gains a sixth scanner stream. Parent and child are the
  same binary, so there's no cross-version concern.

## Edge cases

- **Field that is `null` and absent at once:** impossible; absent wins (it's
  missing, not null).
- **Hash indexes:** hashes never produce `FLD_VAR_T_NULL`, so the null index
  stays empty.
- **`FT.ALTER`:** goes through the same field parsing, so it accepts and
  validates `INDEXNULL` identically, and the re-scan indexes existing nulls.
- **Cluster:** the coordinator distributes `isnull` like `ismissing`.

## Alternatives considered

- **Indexing `null` as a value of the field itself** (e.g. a reserved tag) —
  rejected: it would collide with real values, isn't expressible for NUMERIC,
  GEO, GEOSHAPE or VECTOR, and would change existing query results.
- **Folding null into `ismissing` behind an option** — rejected: absent and
  `null` are distinct states that applications need to tell apart, which is the
  point of this change.
- **A separate code path instead of generalizing the Missing iterator and GC
  scanner** — rejected as duplication of identical logic.

## Testing strategy

- Python end-to-end: validation errors, `FT.INFO`, every field type, negation
  and union with `ismissing`, updates, `FT.EXPLAIN`/`FT.PROFILE`, fork GC,
  `FT.ALTER` with RDB reload, the multi-value matrix, and Flex rejection.
- Rust: in-process GC null-docs pass (partial collection, and removal once
  empty); existing iterator and GC suites updated for the generalization.
