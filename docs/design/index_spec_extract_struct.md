# Extraction Plan: Struct Utilities & Globals

**Source**: `src/spec.c` (part of ~1000-line "remaining core")
**Target**: `src/spec/spec_struct.c` + `src/spec/spec_struct.h`
**Extractability**: Very High

## Goal

Extract field lookup functions, validation helpers, formatting utilities, reference management, version comparison, and global variable declarations into a dedicated file pair. These are stateless utility functions and accessors that many other submodules depend on.

## Functions in `spec_struct.c` (261 lines)

```
// Field lookup
IndexSpec_GetFieldWithLength      (public)
IndexSpec_GetField                (public)
IndexSpec_GetFieldBit             (public)
IndexSpec_GetFieldNameByBit       (public)
IndexSpec_GetFieldByBit           (public)
IndexSpec_GetFieldsByMask         (public)
IndexSpec_GetFieldBySortingIndex  (public)
getFieldsByType                   (public)

// Field validation
IndexSpec_CheckPhoneticEnabled    (public)
IndexSpec_CheckAllowSlopAndInorder (public)

// Formatting
IndexSpec_FormatName              (public)
IndexSpec_FormatObfuscatedName    (public)

// Coherence check
IndexSpec_IsCoherent              (public)

// Reference management
IndexSpec_GetStrongRefUnsafe      (public)
IndexSpecRef_Promote              (public)
IndexSpecRef_Release              (public)

// Version utilities
CompareVersions                   (public)
isRdbLoading                      (public)

// Cursor init
Cursors_initSpec                  (public)
```

## Declarations in `spec_struct.h`

```
// Global variables
extern IndexAlias_GetUserTableName   (function pointer)
extern IndexSpecType                 (RedisModuleType *)
extern redisVersion, rlecVersion     (Version)
extern isEnterprise, isCrdt, isTrimming, isFlex  (bool)
extern memoryLimit, used_memory      (size_t)

// Field lookup
IndexSpec_GetField, IndexSpec_GetFieldWithLength
IndexSpec_GetFieldBit, IndexSpec_GetFieldNameByBit
IndexSpec_GetFieldByBit, IndexSpec_GetFieldsByMask
IndexSpec_GetFieldBySortingIndex, getFieldsByType

// Field validation
IndexSpec_CheckPhoneticEnabled
IndexSpec_CheckAllowSlopAndInorder

// Formatting
IndexSpec_FormatName, IndexSpec_FormatObfuscatedName

// Coherence
IndexSpec_IsCoherent

// References
IndexSpec_GetStrongRefUnsafe
IndexSpecRef_Promote, IndexSpecRef_Release

// Utilities
Cursors_initSpec, isRdbLoading, CompareVersions
```

## Key Dependencies

- `IndexSpec` struct (read-only access for most functions)
- `FieldSpec`, `HiddenString` types
- `RefManager` / `StrongRef` / `WeakRef`
- `Obfuscate_*` APIs (for `FormatName`)
- `CursorList_*` APIs (for `Cursors_initSpec`)
- `SchemaRule` (for `IsCoherent`)

## Design Rationale

These functions were grouped because they share a common trait: they are stateless accessors/utilities that operate on `IndexSpec` without mutating its core state (no creation, destruction, or registration side effects). They form the "read API" of IndexSpec that nearly every other submodule uses.

The global variables were placed here because they are cross-cutting — used by registry, scanner, RDB, and lifecycle code. Previously they were file-scoped or extern in `spec.c`; now `spec_struct.h` is the canonical extern declaration point.

## Callers

Widely used across the codebase:
- `src/module.c`, `src/query.c`, `src/document.c` — field lookups
- `src/rlookup.c` — field lookups by bit/mask
- `src/search_ctx.c` — `IndexSpec_FormatName`
- `src/spec/spec_scanner.c` — `IndexSpec_FormatName`
- `src/spec/spec_registry.c` — `IndexSpecRef_Promote`, `IndexSpecRef_Release`
- `src/spec/spec_rdb.c` — `CompareVersions`, `isRdbLoading`, globals
- C++ tests — field lookups, version checks

## Notes

- All field lookup functions are pure read-only — they iterate `spec->fields` and compare names/bits.
- `Cursors_initSpec` initializes the cursor list on the spec — lightweight, called from `spec_rdb.c` and `spec_registry.c`.
- `memoryLimit` and `used_memory` were previously file-scoped in `spec.c` (set by `setMemoryInfo`). Now externed here, set by `setMemoryInfo` in `spec_scanner.c`.

**Extracted 2026-03-25**: All functions verified against implementation.
