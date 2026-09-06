# Tasks: Skip Documents Without Newly Added Fields on FT.ALTER

## 1. Carry selective ALTER scan intent

- [x] 1.1 Add the persisted `Index_HasSkippedAlterScan` history bit and set it only after
  `FT.ALTER ... SKIPINITIALSCAN` successfully adds fields
- [x] 1.2 Snapshot the added-field range in `AlterIndexInternalCommand` and add the ALTER-specific
  scan entry point with the `INDEXMISSING`, `SORTABLE`, disk-backed, active-scan, OOM, and
  skipped-scan fallbacks
- [x] 1.3 Store the optional range on the base `IndexesScanner`; keep the existing scan entry point,
  debug scanner, and non-ALTER scan behavior unchanged

## 2. Skip documents with no added fields

- [x] 2.1 Add a Hash/JSON field-presence helper that distinguishes probe failure from confirmed
  absence and owns all returned strings and JSON iterators
- [x] 2.2 Reuse the scan-opened key for the selective probe (not for schema-rule evaluation, which
  opens with query flags), then close it before entering the existing full-reindex path
- [x] 2.3 Omit `IndexSpec_UpdateDoc` only when every added field is confirmed absent; use the existing
  full replacement for present fields and probe failures

## 3. Add behavioral coverage

- [x] 3.1 Verify Hash documents without added fields retain their internal IDs, while documents with
  any added field are fully reindexed with unchanged searchable content
- [x] 3.2 Cover multiple added fields, Hash aliases, and nested and multi-value JSONPath fields
- [x] 3.3 Cover full-scan fallbacks for `INDEXMISSING`, `SORTABLE`, probe failure, an active or
  pending scan, OOM recovery, and indexes created with `SKIPINITIALSCAN`
- [x] 3.4 Verify `Index_HasSkippedAlterScan` survives RDB reload and forces a later ALTER to backfill
  fields omitted by the earlier `SKIPINITIALSCAN`
- [x] 3.5 Confirm coordinator behavior and existing debug scan controls remain unchanged

## 4. Measure and verify

- [x] 4.1 Add single-shard sparse and dense cases to the ALTER Python flow tests; assert replacement
  count from the `max_doc_id` delta and report elapsed time around ALTER plus
  `waitForIndexFinishScan`
- [x] 4.2 Keep elapsed time informational: print the sparse and dense measurements and record them
  with the test environment, without a timing threshold or relative-speed assertion
- [x] 4.3 Run the targeted C/C++ and Python tests and formatting checks, then update the delta spec,
  command-documentation decision, and release-note handling to match what shipped
