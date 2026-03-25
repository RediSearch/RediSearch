# Extraction Plan: Stats & Info Reporting

**Source**: `src/spec.c` (~200 lines)
**Target**: `src/spec/spec_info.c` + `src/spec/spec_info.h`
**Extractability**: High

## Goal

Extract all read-only statistics collection and INFO rendering into a dedicated file pair. These functions never mutate IndexSpec — pure reporting.

## Functions to Move

### To `spec_info.c`
```
IndexesScanner_IndexedPercent      (public)
IndexSpec_collect_numeric_overhead (public)
IndexSpec_collect_tags_overhead    (public)
IndexSpec_collect_text_overhead    (public)
IndexSpec_TotalMemUsage            (public)
IndexSpec_GetStats                 (public)
IndexSpec_GetIndexErrorCount       (public)
RSIndexStats_FromScoringStats      (static)
IndexSpec_AddToInfo                (public)
```

### To `spec_info.h`
```
IndexSpec_collect_numeric_overhead
IndexSpec_collect_tags_overhead
IndexSpec_collect_text_overhead
IndexSpec_TotalMemUsage
IndexSpec_GetStats
IndexSpec_GetIndexErrorCount
IndexesScanner_IndexedPercent
IndexSpec_AddToInfo
```

## Dependencies

### For overhead collectors
- `NumericRangeTree_BaseSize` — from `numeric_index.h`
- `openNumericOrGeoIndex` — from `numeric_index.h`
- `TagIndex_GetOverhead` — from `tag_index.h`
- `TrieType_MemUsage` — from `trie/trie_type.h`
- `TrieMap_MemUsage` — from `triemap.h`

### For AddToInfo
- `IndexSpec_VectorIndexesSize` — vector index size aggregator
- `GCContext_RenderStatsForInfo` — from `gc.h`
- `Cursors_RenderStatsForInfo` — from `cursor.h`
- `AddStopWordsListToInfo` — from `stopwords.h`
- `SchemaRule` fields — for the definition section
- `FieldSpec_FormatName`, `FieldSpec_FormatPath`, `FieldSpec_GetTypeNames` — from `field_spec_info.h`
- `Obfuscate_*` — from `obfuscation/obfuscation_api.h`
- `TotalIIBlocks` — from `inverted_index.h` or similar
- `DocumentType_ToString` — from `doc_types.h`
- `RSLanguage_ToString` — from language header
- `global_spec_scanner` — extern, for indexing status
- `RedisModule_Info*` APIs

### For GetStats
- `RSIndexStats` type — from `redisearch_api.h`
- `IndexError_ErrorCount` — from `info/index_error.h`

## External Callers (need `#include "spec_info.h"`)

- `src/info/info_command.c:270,272,274,292` — calls `collect_tags_overhead`, `collect_text_overhead`, `TotalMemUsage`, `IndexedPercent`.
- `src/info/indexes_info.c:48,72` — calls `TotalMemUsage`, `GetIndexErrorCount`.
- `src/redisearch_api.c:648,965` — calls `GetStats`, `TotalMemUsage`.
- `src/pipeline/pipeline_construction.c:289` — calls `GetStats`.
- `src/info/info_redis/info_redis.c:429` — calls `AddToInfo`.

## Notes

- All functions are read-only — no mutations to IndexSpec.
- `AddToInfo` is the largest function (160 lines). It builds Redis INFO dict entries with field details, stats, memory sizes, GC info, cursor info, and stopwords.
- `AddToInfo` has a `skip_unsafe_ops` parameter for signal-handler safety (avoids allocations and locks).
- The overhead collectors iterate `spec->fields` and call into field-type-specific modules.
- `IndexesScanner_IndexedPercent` reads `scanner->scannedKeys` and calls `RedisModule_DbSize` — simple.

## Potential Concern

`IndexSpec_collect_numeric_overhead` calls `openNumericOrGeoIndex(sp, fs, DONT_CREATE_INDEX)` which does a dict lookup in `sp->keysDict`. This is a read but accesses internal data structures. Should be fine since it's called under appropriate locking.

## Build Integration

- Add `src/spec/spec_info.c` to build system.
- Remove moved function declarations from `spec.h`, add `#include "spec_info.h"` where needed.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_PYTEST TEST=test_info` — FT.INFO output.
- `./build.sh RUN_PYTEST TEST=test_info_modules_command` — Redis INFO modules output.
- Verify FT.INFO output format is unchanged.

**Validated 2026-03-19**: All functions, callers, and dependencies verified against codebase. No blockers.
