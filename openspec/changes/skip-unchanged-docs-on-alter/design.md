# Design: Skip Documents Without Newly Added Fields on FT.ALTER

## Scheduling

`AlterIndexInternalCommand` snapshots `sp->numFields` while holding the spec write lock, immediately
before calling `IndexSpec_AddFields`. After a successful call, the snapshot and the new field count
form the added-field range. This avoids changing either field-addition function's signature.

The command schedules the backfill through a new `IndexSpec_ScanAndReindexForAlter` entry point.
Before creating a scanner, it uses the existing full-scan entry point unless all of these conditions
hold:

- the added range contains no `INDEXMISSING` field;
- the added range contains no `SORTABLE` field (only the full reindex path rebuilds a document's
  sorting vector; a skipped document would keep one sized for the old schema, and a later
  sortables-only update writing the new `sortIdx` into it would index out of bounds);
- the spec is not disk-backed (defensive: `FT.ALTER` is rejected under Flex via `DiskDisabledCmd`,
  and the disk backfill driver never consults the range, so a non-empty one would be silently
  ignored);
- no per-index scanner is active or pending;
- `scan_failed_OOM`, read through its atomic accessor, is false; and
- neither `Index_SkipInitialScan` nor a new `Index_HasSkippedAlterScan` flag is set.

These checks happen before scanner construction, because construction cancels an active scanner and
clears the previous OOM state. A full scan is required in either case to complete potentially missing
work from the earlier scan.

When `FT.ALTER ... SKIPINITIALSCAN` successfully adds fields, it sets
`Index_HasSkippedAlterScan` before returning without a scan. This dedicated history bit is persisted
through the existing `IndexFlags` RDB value and is not cleared by this change.
`Index_SkipInitialScan` keeps its current CREATE-only meaning.

The history only needs to hold within one process lifetime. An RDB load reindexes every document
from the keyspace with the loaded schema (the `loaded` keyspace notification drives
`Indexes_UpdateMatchingWithSchemaRules` for each key), which repairs any field an earlier
`SKIPINITIALSCAN` left unbackfilled. The bit still rides along in the persisted flags word like every
other `IndexFlags` bit; after a load it is stale but safe, forgoing only the optimization. No RDB
field or encoding change is involved, and an older module ignores the bit.

## Scanner state

Store the immutable start and end of the added-field range directly on `IndexesScanner`. An empty
range means a normal full scan. The private scan scheduler accepts the optional range, creates the
normal or debug scanner exactly as it does today, and then records the range on the base scanner.

No ALTER-specific scanner subtype or mode flag is needed. Keeping the state on the base scanner
preserves the existing debug controls, cancellation, and OOM restart behavior without adding another
scanner lifecycle.

The existing `IndexSpec_ScanAndReindex` passes an empty range, so all other callers keep their
current behavior. Disk scan code is unchanged: `FT.ALTER` is rejected under Flex, and the gate above
additionally excludes disk-backed specs, so the async scan driver never sees a non-empty range.

## Per-document probe

Only `IndexScanner_DrainPendingScanKeys` consults the optional range. After the existing type and
schema-rule checks pass, it tests the added fields on the key handle that was already opened for type
detection:

- Hash fields use `RedisModule_HashGet` with `FieldSpec.fieldPath`.
- JSON fields use the existing JSON root, `japi->get`, and `japi->len` flow with
  `FieldSpec.fieldPath`.

A small helper in `document_basic.c` owns these Hash and JSON details and releases every returned
string or JSON iterator. It reports probe success separately from field presence. The scanner skips
the document only when the probe succeeds and reports that every added field is absent. A probe
failure falls through to the existing full-reindex path. The probe checks presence only; value
parsing and validation remain in the full-reindex path.

The open handle is deliberately not passed to `SchemaRule_ShouldIndex`: a `FILTER` clause evaluates
through `RLookup_LoadRuleFields`, which opens the document with `DOCUMENT_OPEN_KEY_QUERY_FLAGS`,
while the scan's handle carries the narrower `DOCUMENT_OPEN_KEY_INDEXING_FLAGS`. The scan closes
the handle before invoking `IndexSpec_UpdateDoc` with `openKey == NULL`, exactly as the current scan
does. The handle was opened read-only, while `IndexSpec_UpdateDoc` can update key
metadata and therefore must retain its existing key-opening behavior.

Skipping means only omitting the `IndexSpec_UpdateDoc` call. No document table, index entry, or
internal document ID is modified. When any added field is present, the existing full-replace path is
used unchanged.

## Scope boundaries

The change does not add schema generations, per-document markers, partial field indexing, vector
relabeling, or new cancellation/OOM behavior. Those mechanisms are unnecessary for skipping
documents that contain none of the newly added fields.

## Validation

Tests cover skipped-document ID preservation, unchanged full replacement when an added field is
present, Hash aliases, nested and multi-value JSONPath fields, and each conservative fallback. The
skipped-ALTER test includes an RDB restart before a later ALTER to verify that the full-scan fallback
survives reload. Focused single-shard Python cases measure sparse and dense replacement counts from
the `max_doc_id` delta and report elapsed ALTER-to-scan-completion time. Replacement counts are
asserted; elapsed times have no CI threshold because they depend on host speed and polling cadence.
