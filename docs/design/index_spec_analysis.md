# IndexSpec Module Analysis

## Overview

`src/spec.h` (808 lines) + `src/spec.c` (4302 lines) form the central metadata hub for RediSearch indexes.
The `IndexSpec` struct owns the schema, stats, data structures, and lifecycle for a single index.
It is the "god object" of the codebase — nearly every other module depends on it.

## Core Structs

### IndexSpec (spec.h:298–368)
- **Identity**: `specName` (HiddenString), `obfuscatedName`, `flags` (65+ options)
- **Schema**: `fields[]` (FieldSpec array), `numFields`, `numSortableFields`
- **Data structures**: `keysDict` (inverted indexes), `docs` (DocTable), `terms`/`suffix` (tries), numeric trees, tag indexes, vector indexes
- **Runtime**: stopwords, synonym map, schema rule, GC context, scanner, refcount, stats, cache
- **Disk**: `diskSpec` handle (NULL for memory-only)
- **Concurrency**: `rwlock`, atomic `activeQueries`/`activeWrites` counters

### FieldSpec (field_spec.h:96–141)
- **Identity**: `fieldName`, `fieldPath`, `index`, `ftId` (text field bitmask id)
- **Type**: `types` bitmask (FULLTEXT|NUMERIC|GEO|TAG|VECTOR|GEOMETRY)
- **Options**: sortable, no-stemming, phonetics, UNF, suffix trie, index-empty, index-missing
- **Weight**: `ftWeight` (text relevance weight)
- **Sorting**: `sortIdx`
- **Type-specific**: union of `tagOpts`, `vectorOpts`, `geometryOpts`
- **Numeric**: `tree` (NumericRangeTree)
- **Error tracking**: `indexError`

### IndexSpecCache (spec.h:416–420)
Immutable, refcounted snapshot of index fields for thread-safe access by query threads.

### IndexesScanner (spec.h:657–666)
Background scanning state for follow-hash/JSON rule-based indexing with OOM handling.

### IndexFlags (spec.h:170–197)
Bitmask enum with 20+ flags covering storage options (offsets, freqs, field flags), features (phonetic, synonym, vecsim, suffix trie, geometry), and behavior (wide schema, temporary, async, skip initial scan).

## Identified Submodules

Six natural submodules have been identified within spec.c:

| # | Submodule | Lines | Extractability |
|---|-----------|-------|----------------|
| 1 | RDB Serialization | ~530 | High |
| 2 | Field Parsing | ~750 | Medium-High |
| 3 | Global Index Registry | ~380 | Medium |
| 4 | Background Scanner | ~350 | High |
| 5 | Stats & Info Reporting | ~200 | High |
| 6 | Spec Cache | ~80 | Very High |

Remaining after extraction: ~1000 lines of core lifecycle (NewIndexSpec, CreateNew, CreateField, MakeKeyless, Free, InitLock, document ops, locking, misc utilities).

---

## Submodule 1: RDB Serialization (~530 lines)

### Scope
Lines 2460–2670 (field RDB load/save, stats RDB), 3210–3397 (spec RDB save/load), 3399–3674 (store after load, legacy RDB, serialize/deserialize, register type), 3580–3633 (aux load/save).

### Functions
- `FieldSpec_RdbLoadCompat8`, `FieldSpec_RdbLoad`, `FieldSpec_RdbSave`
- `IndexStats_RdbLoad`, `IndexScoringStats_RdbLoad`, `IndexScoringStats_RdbSave`
- `IndexSpec_RdbSave`, `IndexSpec_RdbLoad`
- `IndexSpec_Serialize`, `IndexSpec_Deserialize`
- `IndexSpec_StoreAfterRdbLoad`, `IndexSpec_CreateFromRdb`
- `IndexSpec_LegacyRdbLoad`, `IndexSpec_LegacyRdbSave`
- `Indexes_RdbLoad`, `Indexes_RdbSave`, `Indexes_RdbSave2`
- `IndexSpec_RdbLoad_Logic`, `IndexSpec_RegisterType`
- `IndexSpec_RdbSave_Wrapper`
- `Indexes_Propagate`
- `IndexSpec_DropLegacyIndexFromKeySpace`, `Indexes_UpgradeLegacyIndexes`
- Version constants and compat macros (~50 lines in header)

### Callers
- Redis module type system (rdb_load/rdb_save/aux_load/aux_save callbacks)
- `Indexes_Propagate` (slot migration, called from module.c)
- `IndexSpec_Serialize`/`IndexSpec_Deserialize` (replication/migration)

### Dependencies
- `VecSim_RdbLoad*` / `VecSim_RdbSave` (vector serialization)
- `SchemaRule_RdbLoad/Save`
- `StopWordList_RdbLoad/Save`
- `SynonymMap_RdbLoad/Save`
- `DocTable_LegacyRdbLoad`
- `TrieType_GenericLoad/Save`
- `SearchDisk_IndexSpecRdbLoad/Save`, `SearchDisk_OpenIndex`
- `IndexAlias_Add`
- `HiddenString_SaveToRdb`/`HiddenString_LoadFromRdb`
- `IndexSpec` struct (all fields)
- `initializeIndexSpec` (shared with NewIndexSpec)
- `IndexSpec_MakeKeyless`, `IndexSpec_StartGC`, `IndexSpec_BuildSpecCache`
- `Cursors_initSpec`, `FieldsGlobalStats_UpdateStats`
- `specDict_g`, `legacySpecDict`, `legacySpecRules` globals

### Notes
- Version compat branches (v2–v26) are purely internal to this submodule.
- Legacy RDB load (v2–v16) is already partially separated but still inline.
- The `RdbLoad` function calls `initializeIndexSpec` which is also used by `NewIndexSpec` — shared helper.

---

## Submodule 2: Field Parsing (~750 lines)

### Scope
Lines 583–1422 + 1440–1695.

### Functions
- `checkPhoneticAlgorithmAndLang`
- `parseVectorField_GetType`, `parseVectorField_GetMetric`, `parseVectorField_GetQuantBits`
- `parseVectorField_validate_hnsw`, `parseVectorField_validate_flat`, `parseVectorField_validate_svs`
- `parseVectorField_hnsw`, `parseVectorField_flat`, `parseVectorField_svs`
- `parseTextField`, `parseTagField`, `parseGeometryField`, `parseVectorField`
- `parseFieldSpec`
- `IndexSpec_AddFieldsInternal`, `IndexSpec_AddFields`
- `IndexSpec_PopulateVectorDiskParams`
- `handleBadArguments`
- `VecSimIndex_validate_params`

### Callers
- `IndexSpec_Parse` (FT.CREATE flow)
- `IndexSpec_AddFields` (FT.ALTER flow, called from module.c)
- `VecSimIndex_validate_params` (called during RDB load for memory limit checks)

### Dependencies
- `ArgsCursor` (argument parsing framework)
- `VecSimParams`, `VecSimIndex_EstimateElementSize`, `VecSimIndex_EstimateInitialSize`, `VecSim_TieredParams_Init`
- `SearchDisk_IsEnabledForValidation`, `SearchDisk_MarkUnsupportedFieldIfDiskEnabled`
- `FieldSpec` struct and helpers (`FieldSpec_SetSortable`, `FieldSpec_IsPhonetics`, etc.)
- `IndexSpec` struct (reads/modifies flags, fields, suffixMask, suffix trie)
- `QueryError` for error reporting
- JSON path API (`japi->pathParse`, `pathIsSingle`, `pathHasDefinedOrder`)
- Memory limit globals (`memoryLimit`, `BLOCK_MEMORY_LIMIT`, `setMemoryInfo`)
- `IndexSpec_CreateTextId`, `IndexSpec_BuildSpecCache`, `FieldsGlobalStats_UpdateStats`
- `NewTrie` (suffix trie creation)

### Notes
- Vector parsing alone is ~500 lines — almost self-contained, reads ArgsCursor and fills VecSimParams.
- Text/tag/geo/geometry parsers are 30–70 lines each.
- Main coupling: `parseFieldSpec` and `AddFieldsInternal` modify IndexSpec struct directly.
- `VecSimIndex_validate_params` is also called from outside the parsing flow (RDB load) — may need to stay accessible.

---

## Submodule 3: Global Index Registry (~380 lines)

### Scope
Lines 56–77 (globals), 355–434 (legacy free, timers), 524–579 (CreateNew, checkIfSpecExists), 1947–1977 (CleanPool, pending drops), 2090–2175 (RemoveFromGlobals, Indexes_Free), 2177–2225 (LoadUnsafe), 3879–3901 (onFlush, Init, Count), 3903–4139 (FindMatchingSchemaRules, Update/Delete/Replace, List).

### Functions
- `Indexes_Init`, `Indexes_Free`, `Indexes_Count`, `Indexes_List`
- `IndexSpec_CreateNew`
- `IndexSpec_RemoveFromGlobals`
- `IndexSpec_LoadUnsafe`, `IndexSpec_LoadUnsafeEx`
- `Indexes_FindMatchingSchemaRules`
- `Indexes_UpdateMatchingWithSchemaRules`, `Indexes_DeleteMatchingWithSchemaRules`, `Indexes_ReplaceMatchingWithSchemaRules`
- `Indexes_SpecOpsIndexingCtxFree`
- `CleanPool_ThreadPoolStart`, `CleanPool_ThreadPoolDestroy`, `CleanInProgressOrPending`
- `getPendingIndexDrop`, `addPendingIndexDrop`, `removePendingIndexDrop`
- `onFlush`
- `IndexSpec_TimedOutProc`, `IndexSpec_SetTimeoutTimer`, `IndexSpec_ResetTimeoutTimer`, `Indexes_SetTempSpecsTimers`
- `checkIfSpecExists`
- `IndexSpec_IncreasCounter`

### Callers
- `module.c` — FT.CREATE, FT.DROP, FT.ALTER, FT._LIST, module load/unload
- `notifications.c` — keyspace notification handlers
- `rules.c` — `SchemaPrefixes_RemoveSpec`
- RDB loading events
- `IndexSpec_CreateNew` called from FT.CREATE handler

### Dependencies
- `specDict_g` (global dict)
- `SchemaPrefixes_g` (prefix trie for rule matching)
- `IndexAlias_*` (alias management)
- `EvalCtx`, `RLookup` (filter evaluation in FindMatchingSchemaRules)
- `SchemaRule_ShouldIndex`, `SchemaRule_Create`, `SchemaRule_FilterFields`
- `FieldsGlobalStats_UpdateStats/UpdateIndexError`
- `CurrentThread_SetIndexSpec/ClearIndexSpec`
- `CursorList_Empty`
- `IndexSpec_Parse`, `IndexSpec_ScanAndReindex`, `IndexSpec_StartGC`
- `IndexSpec_Free`, `IndexSpec_UpdateDoc`, `IndexSpec_DeleteDoc`
- `Initialize_KeyspaceNotifications`
- `Dictionary_Clear`
- `SearchDisk_*` APIs

### Notes
- Most cross-cutting of all submodules. The schema-rule matching logic (~200 lines) is the most self-contained piece.
- `RemoveFromGlobals` touches aliases, prefixes, timers, stats, refs — hard to isolate.
- Timer management for temporary indexes is tightly coupled with the registry.

---

## Submodule 4: Background Scanner (~350 lines)

### Scope
Lines 114–183 (memory/OOM helpers), 2674–2960 (scanner lifecycle, scan proc, async task), 3127–3208 (ScanAndReindex entry points), 4140–4261 (debug scanner, RDB loading events).

### Functions
- `IndexesScanner_New`, `IndexesScanner_NewGlobal`, `IndexesScanner_Free`
- `IndexesScanner_Cancel`, `IndexesScanner_ResetProgression`
- `Indexes_ScanProc`, `Indexes_ScanAndReindexTask`
- `IndexSpec_ScanAndReindexAsync`, `IndexSpec_ScanAndReindex`
- `Indexes_ScanAndReindex` (global)
- `threadSleepByConfigTime`, `scanStopAfterOOM`
- `setMemoryInfo`, `isBgIndexingMemoryOverLimit`
- `DebugIndexesScanner_New`, `DebugIndexes_ScanProc`, `DebugIndexesScanner_pauseCheck`
- `ReindexPool_ThreadPoolDestroy`
- `Indexes_StartRDBLoadingEvent`, `Indexes_EndRDBLoadingEvent`, `Indexes_EndLoading`

### Callers
- `IndexSpec_CreateNew` (after FT.CREATE)
- `IndexSpec_AddFields` (after FT.ALTER)
- `Indexes_EndRDBLoadingEvent` (legacy upgrade)
- Thread pool (background execution of `Indexes_ScanAndReindexTask`)

### Dependencies
- `RedisModule_Scan*` APIs (cursor-based keyspace scanning)
- `WeakRef`/`StrongRef` (reference management)
- `SchemaRule_ShouldIndex`
- `IndexSpec_UpdateDoc`, `Indexes_UpdateMatchingWithSchemaRules`
- `RSGlobalConfig` (bgIndexingSleepDurationMicroseconds, numBGIndexingIterationsBeforeSleep, indexingMemoryLimit, bgIndexingOomPauseTimeBeforeRetry)
- `IndexError_AddError`, `IndexError_RaiseBackgroundIndexFailureFlag`
- `globalDebugCtx` (debug mode state)
- `reindexPool` (thread pool)
- `RedisMemory_GetUsedMemoryRatioUnified`
- `getDocType`, `getDocTypeFromString`
- `Indexes_SetTempSpecsTimers`

### Notes
- Already has its own struct (`IndexesScanner`/`DebugIndexesScanner`) and lifecycle.
- Clean interface with IndexSpec: reads `spec->scanner`, `spec->scan_in_progress`, `spec->scan_failed_OOM`; calls `IndexSpec_UpdateDoc`.
- The debug scanner is a testing extension — could stay or be extracted further.

---

## Submodule 5: Stats & Info Reporting (~200 lines)

### Scope
Lines 438–508 (overhead collectors, TotalMemUsage, IndexedPercent), 1866–1880 (GetStats, GetIndexErrorCount), 2964–3125 (AddToInfo).

### Functions
- `IndexesScanner_IndexedPercent`
- `IndexSpec_collect_numeric_overhead`, `IndexSpec_collect_tags_overhead`, `IndexSpec_collect_text_overhead`
- `IndexSpec_TotalMemUsage`
- `IndexSpec_GetStats`, `IndexSpec_GetIndexErrorCount`
- `IndexSpec_AddToInfo`
- `RSIndexStats_FromScoringStats`

### Callers
- `module.c` — FT.INFO command handler
- Redis INFO callback (via `IndexSpec_AddToInfo`)
- Scoring functions (via `IndexSpec_GetStats`)

### Dependencies
- `NumericRangeTree_BaseSize`, `openNumericOrGeoIndex`
- `TagIndex_GetOverhead`
- `TrieType_MemUsage`, `TrieMap_MemUsage`
- `IndexSpec_VectorIndexesSize`
- `GCContext_RenderStatsForInfo`
- `Cursors_RenderStatsForInfo`
- `AddStopWordsListToInfo`
- `SchemaRule` fields (for definition section in INFO)
- `FieldSpec_FormatName`, `FieldSpec_FormatPath`, `FieldSpec_GetTypeNames`
- Obfuscation API (`Obfuscate_*`)
- `TotalIIBlocks`

### Notes
- Pure read-only: zero mutations to IndexSpec.
- `AddToInfo` is 160 lines of building Redis INFO dict output — straightforward extraction.
- The overhead collectors just iterate fields and sum sizes.

---

## Submodule 6: Spec Cache (~80 lines)

### Scope
Lines 1897–1943.

### Functions
- `IndexSpecCache_Free`
- `IndexSpecCache_Decref`
- `IndexSpec_BuildSpecCache`
- `IndexSpec_GetSpecCache`

### Callers
- `IndexSpec_AddFieldsInternal` (rebuilds cache after schema change)
- `IndexSpec_RdbLoad` (builds initial cache)
- Query execution threads (reads cache for thread-safe field access)
- `IndexSpec_FreeUnlinkedData` (decrefs cache)

### Dependencies
- `FieldSpec` struct
- `HiddenString_Duplicate`, `HiddenString_Free`
- Atomic ops (`__atomic_fetch_add`, `__atomic_sub_fetch`)

### Notes
- Tiny, self-contained, immutable snapshot with refcounting.
- Simplest extraction target.

---

## Remaining Core (~1000 lines)

After extracting all six submodules, what remains is the essential IndexSpec lifecycle:

- `NewIndexSpec`, `initializeIndexSpec`, `initializeFieldSpec` (~70 lines)
- `IndexSpec_CreateField` (~30 lines)
- `IndexSpec_MakeKeyless` (~5 lines)
- `IndexSpec_Free`, `IndexSpec_FreeUnlinkedData` (~110 lines)
- `IndexSpec_InitLock` (~15 lines)
- `IndexSpec_Parse` (the top-level FT.CREATE orchestrator, ~140 lines — calls into field parsing + registry)
- `IndexSpec_DeleteDoc`, `IndexSpec_DeleteDoc_Unsafe` (~70 lines)
- `IndexSpec_UpdateDoc` (~65 lines)
- `IndexSpec_AddTerm` (~10 lines)
- Field lookup functions (`GetField`, `GetFieldByBit`, `GetFieldsByMask`, etc.) (~100 lines)
- Field validation (`CheckPhoneticEnabled`, `CheckAllowSlopAndInorder`) (~35 lines)
- Misc: `IndexSpec_FormatName`, `FormatObfuscatedName`, `InitializeSynonym`, `LegacyGetFormattedKey`, `bit()`, `CharBuf_*`, dict types (~150 lines)
- `IndexSpec_IsCoherent` (~20 lines)
- Compaction FFI functions (`AcquireWriteLock`, `ReleaseWriteLock`, `DecrementTrieTermCount`, `DecrementNumTerms`) (~40 lines)
- Ref management (`IndexSpecRef_Promote`, `IndexSpecRef_Release`, `GetStrongRefUnsafe`) (~15 lines)
- `CompareVersions`, `isRdbLoading` (~25 lines)
- `Spec_AddToDict` (testing) (~5 lines)
