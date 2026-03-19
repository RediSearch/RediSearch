# Extraction Plan: RDB Serialization

**Source**: `src/spec.c` (~530 lines)
**Target**: `src/spec_rdb.c` + `src/spec_rdb.h`
**Extractability**: High

## Goal

Extract all RDB persistence logic (save, load, legacy compat, serialize/deserialize, type registration) into a dedicated file pair.

## Functions to Move

### To `spec_rdb.c`
```
FieldSpec_RdbLoadCompat8          (static)
FieldSpec_RdbLoad                 (static → internal linkage in new file)
FieldSpec_RdbSave                 (static → internal linkage in new file)
IndexScoringStats_RdbLoad         (static)
IndexScoringStats_RdbSave         (static)
IndexStats_RdbLoad                (static)
IndexSpec_RdbSave                 (public)
IndexSpec_RdbLoad                 (public)
IndexSpec_RdbSave_Wrapper         (static)
IndexSpec_StoreAfterRdbLoad       (static)
IndexSpec_CreateFromRdb           (static)
IndexSpec_LegacyRdbLoad           (public - RedisModuleType callback)
IndexSpec_LegacyRdbSave           (public - RedisModuleType callback)
IndexSpec_LegacyFree              (public - RedisModuleType callback)
Indexes_RdbLoad                   (public - aux_load callback)
Indexes_RdbSave                   (public - aux_save callback)
Indexes_RdbSave2                  (public - aux_save2 callback)
IndexSpec_RdbLoad_Logic           (public - rdb_load callback)
IndexSpec_Serialize               (public)
IndexSpec_Deserialize             (public)
IndexSpec_RegisterType            (public)
Indexes_Propagate                 (public)
IndexSpec_DropLegacyIndexFromKeySpace  (public)
Indexes_UpgradeLegacyIndexes      (public)
CheckRdbSstPersistence            (public)
IndexSpec_PopulateVectorDiskParams (static — only used in RDB load, safe to move)
fieldTypeMap                      (static const — only used in RDB compat code)
bit()                             (static — only used in FieldSpec_RdbLoadCompat8, safe to move)
```

### To `spec_rdb.h`
```
IndexSpec_RdbSave
IndexSpec_RdbLoad
IndexSpec_Serialize
IndexSpec_Deserialize
IndexSpec_RegisterType
Indexes_RdbLoad
Indexes_RdbSave
Indexes_RdbSave2
Indexes_Propagate
IndexSpec_DropLegacyIndexFromKeySpace
Indexes_UpgradeLegacyIndexes
CheckRdbSstPersistence
IndexSpec_LegacyRdbLoad
IndexSpec_LegacyRdbSave
IndexSpec_LegacyFree
IndexSpec_RdbLoad_Logic
```

## Shared Helpers Needed from spec.c

These functions are called by the RDB code but also used elsewhere — they stay in spec.c and get declared in spec.h (or a shared internal header):

- `initializeIndexSpec` — called by `RdbLoad` and `NewIndexSpec`
- `initializeFieldSpec` — called by `RdbLoad` and `CreateField`
- `IndexSpec_MakeKeyless` — called by `RdbLoad` and `Parse`
- `IndexSpec_StartGC` — called by `StoreAfterRdbLoad` and `CreateNew`
- `IndexSpec_BuildSpecCache` — called by `RdbLoad` and `AddFieldsInternal`
- `Cursors_initSpec` — called by `StoreAfterRdbLoad` and `CreateNew`
- `IndexSpec_Free` — passed as RefManager_Free callback
- `FieldsGlobalStats_UpdateStats` — called on field load
- `specDict_g`, `legacySpecDict`, `legacySpecRules` — globals

## Version Constants

**Must stay in `spec.h`** — used by external modules:
- `doc_table.c` uses: `INDEX_MIN_COMPACTED_DOCTABLE_VERSION`, `INDEX_MIN_BINKEYS_VERSION`, `INDEX_MIN_DOCLEN_VERSION`, `INDEX_MIN_EXPIRE_VERSION`
- `rules.c` uses: `INDEX_INDEXALL_VERSION`
- `debug_commands.c` uses: `INDEX_CURRENT_VERSION`

Do NOT move these to `spec_rdb.h`.

## Build Integration

- Add `spec_rdb.c` to CMakeLists.txt / Makefile alongside `spec.c`.
- `spec.c` gains `#include "spec_rdb.h"`.
- `spec_rdb.c` gains `#include "spec.h"` + all RDB-related includes.

## Note on RDB Loading Events

`Indexes_StartRDBLoadingEvent`, `Indexes_EndRDBLoadingEvent`, `Indexes_EndLoading` (currently in scanner plan) should move here instead — they are about RDB lifecycle, not scanning. They call `Indexes_Free`, `Indexes_UpgradeLegacyIndexes`, and `Indexes_ScanAndReindex` but are triggered by RDB events via `notifications.c`.

## Note on Callback Declarations

The following functions are currently NOT declared in any header — they're registered directly as callbacks in `IndexSpec_RegisterType`. The new `spec_rdb.h` must declare them:
- `Indexes_RdbLoad`, `Indexes_RdbSave`, `Indexes_RdbSave2`
- `IndexSpec_RdbLoad_Logic`
- `IndexSpec_LegacyRdbLoad`, `IndexSpec_LegacyRdbSave`, `IndexSpec_LegacyFree`
- `IndexSpec_DropLegacyIndexFromKeySpace`, `Indexes_UpgradeLegacyIndexes`
- `CheckRdbSstPersistence`

## Risks

- ~~`IndexSpec_PopulateVectorDiskParams` is called from both RDB load and `IndexSpec_Parse`~~ **CORRECTED**: only called from RDB load code (new + legacy). Safe to move as static.
- Legacy RDB load calls `IndexSpec_StartGC`, `dictAdd(specDict_g, ...)` — tight coupling with global state.
- `IndexSpec_StoreAfterRdbLoad` calls `dictFetchValue(specDict_g, ...)` and `dictAdd` — needs access to the global dict.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_UNIT_TESTS` must pass.
- `./build.sh RUN_PYTEST TEST=test_rdb_compatibility` must pass.
- Verify RDB save/load round-trips still work.

**Validated 2026-03-19**: All functions verified. Key corrections: `PopulateVectorDiskParams` is RDB-only (simpler than assumed), version constants must stay in spec.h, `bit()` and `fieldTypeMap` are RDB-only (safe to move), 10 callback functions need declarations in spec_rdb.h.
