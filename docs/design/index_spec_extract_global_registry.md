# Extraction Plan: Global Index Registry

**Source**: `src/spec.c` (~380 lines)
**Target**: `src/spec_registry.c` + `src/spec_registry.h`
**Extractability**: Medium

## Goal

Extract the global index dictionary management, schema-rule matching dispatch, and temporary index timer management into a dedicated file pair.

## Functions to Move

### To `spec_registry.c`

**Global dict management:**
```
Indexes_Init                       (public)
Indexes_Free                       (public)
Indexes_Count                      (public)
Indexes_List                       (public)
onFlush                            (static — Redis event callback)
```

**Index load/lookup:**
```
IndexSpec_LoadUnsafe               (public)
IndexSpec_LoadUnsafeEx             (public)
IndexSpec_IncreasCounter           (static inline → move or keep)
checkIfSpecExists                  (static)
```

**Index creation (orchestrator):**
```
IndexSpec_CreateNew                (public)
```

**Removal:**
```
IndexSpec_RemoveFromGlobals        (public)
```

**Schema-rule matching dispatch:**
```
Indexes_FindMatchingSchemaRules    (public)
Indexes_UpdateMatchingWithSchemaRules   (public)
Indexes_DeleteMatchingWithSchemaRules   (public)
Indexes_ReplaceMatchingWithSchemaRules  (public)
Indexes_SpecOpsIndexingCtxFree     (public)
hashFieldChanged                   (static)
```

**Temporary index timers:**
```
IndexSpec_TimedOutProc             (static)
IndexSpec_SetTimeoutTimer          (static)
IndexSpec_ResetTimeoutTimer        (static)
Indexes_SetTempSpecsTimers         (public)
```

**Clean pool & pending drops:**
```
CleanPool_ThreadPoolStart          (public)
CleanPool_ThreadPoolDestroy        (public)
CleanInProgressOrPending           (public)
getPendingIndexDrop                (static)
addPendingIndexDrop                (static → needs to be callable from spec_rdb.c too)
removePendingIndexDrop             (static → called from IndexSpec_FreeUnlinkedData)
```

**Globals:**
```
specDict_g                         (extern)
global_spec_scanner                (extern)
pending_global_indexing_ops        (extern)
legacySpecDict                     (extern)
legacySpecRules                    (extern)
pendingIndexDropCount_g            (static → expose if needed)
cleanPool                          (static)
redisVersion, rlecVersion, isCrdt, isTrimming, isFlex  (extern globals)
memoryLimit, used_memory           (file-scoped to spec.c — NOT extern, set via setMemoryInfo)
```

### To `spec_registry.h`
```
Indexes_Init, Indexes_Free, Indexes_Count, Indexes_List
IndexSpec_CreateNew
IndexSpec_LoadUnsafe, IndexSpec_LoadUnsafeEx
IndexSpec_RemoveFromGlobals
Indexes_FindMatchingSchemaRules
Indexes_UpdateMatchingWithSchemaRules
Indexes_DeleteMatchingWithSchemaRules
Indexes_ReplaceMatchingWithSchemaRules
Indexes_SpecOpsIndexingCtxFree
Indexes_SetTempSpecsTimers
CleanPool_ThreadPoolStart, CleanPool_ThreadPoolDestroy, CleanInProgressOrPending
SpecOpIndexingCtx, SpecOpCtx, SpecOp (types — currently in spec.h)
```

## Design Decisions

### Where do the globals live?

Option A: Globals declared in `spec_registry.c`, externed in `spec_registry.h`.
Option B: Keep globals in `spec.c` (or a new `spec_globals.c`), extern everywhere.

**Recommendation**: Option A — the registry owns the global dict. Other modules access via functions (`Indexes_Count`, `IndexSpec_LoadUnsafe`) rather than directly touching `specDict_g`. However, RDB and other modules currently access `specDict_g` directly via `dictAdd`/`dictFetchValue` — this coupling is hard to eliminate without refactoring callers.

### Where does IndexSpec_CreateNew live?

It orchestrates: check existence → parse → add to dict → start GC → init cursors → set timer → start scan. It's a natural part of the registry since it's the "add to global state" function.

### Pending drop counter

`addPendingIndexDrop`/`removePendingIndexDrop` are called from `IndexSpec_RemoveFromGlobals` (registry) and `IndexSpec_FreeUnlinkedData` (lifecycle in spec.c). Need to expose them.

## Shared Helpers Needed

- `IndexSpec_Parse` (from spec.c) — called by `CreateNew`
- `IndexSpec_ScanAndReindex` (from scanner submodule) — called by `CreateNew`
- `IndexSpec_StartGC` (from spec.c) — called by `CreateNew`
- `IndexSpec_Free` (from spec.c) — called indirectly via ref release
- `IndexSpec_UpdateDoc`, `IndexSpec_DeleteDoc` (from spec.c) — called by Update/Delete matching
- `SchemaRule_ShouldIndex`, `SchemaRule_Create`, `SchemaRule_FilterFields` — external
- `EvalCtx_Create/Destroy`, `RLookup_*` — for filter evaluation
- `SchemaPrefixes_g`, `SchemaPrefixes_RemoveSpec`, `SchemaPrefixes_Create` — external
- `IndexAlias_*` — alias management
- `Initialize_KeyspaceNotifications` — external
- `CurrentThread_SetIndexSpec/ClearIndexSpec` — external

## Note on specDict_g Coupling

`specDict_g` is directly accessed (not just via registry functions) by at least 6 external files:
- `src/module.c` — dict iteration for FT._LIST
- `src/spec_rdb.c` (future) — `dictAdd`/`dictFetchValue` during RDB load
- `src/config.c` — iterating all specs for config changes
- `src/info/indexes_info.c` — iterating specs for info
- `src/info/info_command.c` — iterating specs for info
- `src/search_disk.c` — dict lookup

This coupling cannot be fully eliminated without refactoring callers. Keep `specDict_g` as extern in `spec_registry.h`.

## Note on pendingIndexDropCount_g

`pendingIndexDropCount_g` can stay static in `spec_registry.c`. It's only read from `CleanInProgressOrPending` (also in registry) and modified by `addPendingIndexDrop`/`removePendingIndexDrop`. `removePendingIndexDrop` is called from `IndexSpec_FreeUnlinkedData` (spec.c) — expose it as internal.

## Risks

- This is the most cross-cutting submodule. Many other modules directly access `specDict_g`.
- `Indexes_FindMatchingSchemaRules` is 100 lines with complex filter evaluation logic involving `EvalCtx` and `RLookup` — significant dependency surface.
- Timer management (temp indexes) interacts with Redis module timer APIs and requires GIL awareness.
- `IndexSpec_CreateNew` pulls in parsing, scanning, GC, cursors, timers, disk — it's a nexus function.
- `memoryLimit` and `used_memory` are file-scoped to spec.c (set by `setMemoryInfo`), not extern globals. If registry functions need them, either pass as params or move `setMemoryInfo` to a shared location.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_PYTEST TEST=test_create` — index creation.
- `./build.sh RUN_PYTEST TEST=test_drop` — index deletion.
- `./build.sh RUN_PYTEST TEST=test_list` — FT._LIST.
- `./build.sh RUN_PYTEST TEST=test_temporary` — temporary index expiration.
- `./build.sh RUN_PYTEST TEST=test_follow_hash` — schema rule matching.

**Validated 2026-03-19**: All functions verified. Key corrections: `memoryLimit`/`used_memory` are file-scoped not extern, `specDict_g` directly accessed by 6+ external files, `Indexes_FindMatchingSchemaRules` and `Indexes_SpecOpsIndexingCtxFree` added to header declarations, `pendingIndexDropCount_g` can stay static.
