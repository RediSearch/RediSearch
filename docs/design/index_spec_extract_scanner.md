# Extraction Plan: Background Scanner

**Source**: `src/spec.c` (~350 lines)
**Target**: `src/spec_scanner.c` + `src/spec_scanner.h`
**Extractability**: High

## Goal

Extract the background keyspace scanning and reindexing machinery into a dedicated file pair. This submodule already has its own structs and lifecycle — clean boundary.

## Functions to Move

### To `spec_scanner.c`

**Memory/OOM helpers:**
```
setMemoryInfo                      (static)
isBgIndexingMemoryOverLimit        (static)
threadSleepByConfigTime            (static)
scanStopAfterOOM                   (static)
```

**Scanner lifecycle:**
```
IndexesScanner_New                 (static → may need to expose for debug)
IndexesScanner_NewGlobal           (static)
IndexesScanner_Free                (internal — not in spec.h, only used within scan task)
IndexesScanner_Cancel              (public — declared in spec.h)
IndexesScanner_ResetProgression    (public — declared in spec.h, only used internally)
```

**Scan execution:**
```
IndexSpec_DoneIndexingCallabck     (static — unused callback stub)
Indexes_ScanProc                   (static)
Indexes_ScanAndReindexTask         (static)
IndexSpec_ScanAndReindexAsync      (static)
IndexSpec_ScanAndReindex           (public — declared in spec.h)
Indexes_ScanAndReindex             (internal — NOT declared in spec.h, called from EndRDBLoadingEvent)
ReindexPool_ThreadPoolDestroy      (public — declared in spec.h)
```

**Debug scanner:**
```
DebugIndexesScanner_New            (static)
DebugIndexes_ScanProc              (static)
DebugIndexesScanner_pauseCheck     (static)
DEBUG_INDEX_SCANNER_STATUS_STRS    (extern const)
static_assert for array size
```

**RDB loading events:**
```
Indexes_StartRDBLoadingEvent       (public)
Indexes_EndRDBLoadingEvent         (public)
Indexes_EndLoading                 (public)
```

**Globals:**
```
reindexPool                        (static)
global_spec_scanner                (extern)
```

### To `spec_scanner.h`
```
IndexesScanner (struct — currently in spec.h)
DebugIndexesScanner (struct — currently in spec.h)
DebugIndexScannerCode (enum — currently in spec.h)
DEBUG_INDEX_SCANNER_STATUS_STRS

IndexesScanner_Cancel
IndexesScanner_ResetProgression
IndexSpec_ScanAndReindex
Indexes_ScanAndReindex
ReindexPool_ThreadPoolDestroy
Indexes_StartRDBLoadingEvent
Indexes_EndRDBLoadingEvent
Indexes_EndLoading
```

## Struct Relocation

The `IndexesScanner` and `DebugIndexesScanner` structs and the `DebugIndexScannerCode` enum currently live in `spec.h`. They should move to `spec_scanner.h`. `spec.h` would then forward-declare `struct IndexesScanner` (it already does this on line 41) and include `spec_scanner.h` only where needed.

The `IndexSpec` struct has a `struct IndexesScanner *scanner` member — this only needs a forward declaration, not the full struct definition. The `scan_in_progress` and `scan_failed_OOM` bools stay on `IndexSpec`.

## Interface with IndexSpec

The scanner reads/writes these `IndexSpec` fields:
- `spec->scanner` (set on creation, cleared on free)
- `spec->scan_in_progress` (set to true on start, false on end)
- `spec->scan_failed_OOM` (set to true on OOM)
- `spec->stats.indexError` (add error on OOM failure)
- `spec->specName` (for logging)

It calls:
- `IndexSpec_UpdateDoc` — to index a document
- `IndexSpec_FormatName` — for logging
- `Indexes_UpdateMatchingWithSchemaRules` — for global scan
- `SchemaRule_ShouldIndex` — to check if key matches index

These are all via function calls — no direct struct mutation beyond the scanner/progress/OOM fields.

## Shared Helpers Needed

- `IndexSpec_FormatName` (spec.c)
- `IndexSpec_UpdateDoc` (spec.c)
- `Indexes_UpdateMatchingWithSchemaRules` (registry submodule)
- `Indexes_SetTempSpecsTimers` (registry submodule)
- `Indexes_Free` (registry submodule — called from `StartRDBLoadingEvent`)
- `Indexes_UpgradeLegacyIndexes` (RDB submodule — called from `EndRDBLoadingEvent`)
- `LegacySchemaRulesArgs_Free` (external)
- `WeakRef_Promote`, `StrongRef_Get/Release` (references)
- `RSGlobalConfig.*` fields (config)
- `globalDebugCtx` (debug mode)
- `redisearch_thpool_*` (thread pool)
- `RedisModule_Scan*` APIs
- `RedisMemory_GetUsedMemoryRatioUnified`
- `IncrementBgIndexYieldCounter`

## Overlap Resolution

`IndexesScanner_IndexedPercent` was originally listed in both this plan and the stats/info plan.
**Decision**: It belongs in **spec_info.c** (stats plan) — it's a read-only reporting function called from `info_command.c`. Removed from this plan. The scanner module can call it via `spec_info.h` if needed.

## External Callers

- `IndexSpec_ScanAndReindex` — called from `module.c:815` (FT.ALTER).
- `ReindexPool_ThreadPoolDestroy` — called from `module.c:1829`, `debug_commands.c:1884`.
- `Indexes_StartRDBLoadingEvent` — called from `notifications.c:711`.
- `Indexes_EndRDBLoadingEvent` — called from `notifications.c:716`.
- `Indexes_EndLoading` — called from `notifications.c:718,723`.
- `IndexesScanner_Cancel` — internal use only (spec.c:2703).
- `IndexesScanner_ResetProgression` — internal use only (spec.c:2878).

## Risks

- `Indexes_StartRDBLoadingEvent` calls `Indexes_Free` and touches `legacySpecDict` — couples scanner to registry and RDB.
- `Indexes_EndRDBLoadingEvent` calls `Indexes_UpgradeLegacyIndexes` and `Indexes_ScanAndReindex` — couples to RDB.
- **Recommendation**: Move the RDB loading event functions (`Start/End/EndLoading`) to the RDB submodule instead, since they're more about RDB lifecycle than scanning. The scanner would only export `IndexSpec_ScanAndReindex` / `Indexes_ScanAndReindex`.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_PYTEST TEST=test_background_indexing` — bg scan.
- `./build.sh RUN_PYTEST TEST=test_follow_hash` — schema rule scanning.
- `./build.sh RUN_PYTEST TEST=test_alter` — rescan after FT.ALTER.
- `./build.sh RUN_PYTEST TEST=test_rdb_compatibility` — RDB load triggers scan.

**Validated 2026-03-19**: All functions verified. Key corrections: removed `IndexesScanner_IndexedPercent` (belongs in stats), clarified visibility of `IndexesScanner_Free` and `Indexes_ScanAndReindex` (not declared in spec.h).
