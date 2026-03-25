# Extraction Plan: Lifecycle

**Source**: `src/spec.c` (part of ~1000-line "remaining core")
**Target**: `src/spec/spec_lifecycle.c` + `src/spec/spec_lifecycle.h`
**Extractability**: High

## Goal

Extract IndexSpec creation, destruction, field creation, document operations, locking, and compaction FFI into a dedicated file pair. These are the core lifecycle operations that other submodules depend on.

## Functions in `spec_lifecycle.c` (563 lines)

```
NewIndexSpec                      (public)
initializeIndexSpec               (public — also called by RDB load)
IndexSpec_Free                    (public)
IndexSpec_FreeUnlinkedData        (static)
IndexSpec_CreateField             (public)
IndexSpec_CreateTextId            (public)
IndexSpec_MakeKeyless             (public)
IndexSpec_InitLock                (public)
IndexSpec_InitializeSynonym       (public)
IndexSpec_AddTerm                 (public)
IndexSpec_StartGC                 (public)
IndexSpec_StartGCFromSpec         (public)
IndexSpec_UpdateDoc               (public)
IndexSpec_DeleteDoc               (public)
IndexSpec_DeleteDoc_Unsafe        (public)
Spec_AddToDict                    (public — testing)
isSpecOnDisk                      (static inline)

// Dict callback helpers
CharBuf_HashFunction              (public)
CharBuf_KeyDup                    (public)
CharBuf_KeyCompare                (public)
CharBuf_KeyDestructor             (public)
InvIndFreeCb                      (public)

// Compaction FFI
IndexSpec_AcquireWriteLock        (public)
IndexSpec_ReleaseWriteLock        (public)
IndexSpec_DecrementTrieTermCount  (public)
IndexSpec_DecrementNumTerms       (public)
```

## Declarations in `spec_lifecycle.h`

```
NewIndexSpec
IndexSpec_Free
initializeIndexSpec
IndexSpec_InitLock
IndexSpec_CreateField
IndexSpec_CreateTextId
IndexSpec_MakeKeyless
IndexSpec_StartGC
IndexSpec_StartGCFromSpec
IndexSpec_UpdateDoc
IndexSpec_DeleteDoc
IndexSpec_DeleteDoc_Unsafe
IndexSpec_AddTerm
Spec_AddToDict
IndexSpec_InitializeSynonym

// Compaction FFI
IndexSpec_AcquireWriteLock
IndexSpec_ReleaseWriteLock
IndexSpec_DecrementTrieTermCount
IndexSpec_DecrementNumTerms
```

## Key Dependencies

- `IndexSpec` struct (full definition from `spec.h`)
- `FieldSpec`, `HiddenString` types
- `RefManager` / `StrongRef` / `WeakRef` (reference management)
- `GCContext` (GC start)
- `DocTable`, `TrieMap`, `SynonymMap` (data structure init/free)
- `SchemaRule_Free`, `SchemaPrefixes_RemoveSpec` (cleanup)
- `IndexSpecCache_Decref` (from `spec_cache.h`)
- `removePendingIndexDrop` (from `spec_registry.c`)
- `FieldsGlobalStats_UpdateStats` (external)
- `SearchDisk_*` APIs

## Callers

- `spec_rdb.c` — calls `initializeIndexSpec`, `IndexSpec_MakeKeyless`, `IndexSpec_StartGC`
- `spec_registry.c` — calls `IndexSpec_Free` (via ref release), `IndexSpec_UpdateDoc`, `IndexSpec_DeleteDoc`
- `spec_scanner.c` — calls `IndexSpec_UpdateDoc`
- `spec_field_parse.c` — calls `IndexSpec_CreateTextId`
- `spec_parse.c` — calls `NewIndexSpec`, `IndexSpec_MakeKeyless`
- `src/module.c` — calls `IndexSpec_DeleteDoc`
- `src/notifications.c` — calls `IndexSpec_UpdateDoc`, `IndexSpec_DeleteDoc`
- Rust FFI (compaction) — calls `AcquireWriteLock`, `ReleaseWriteLock`, `DecrementTrieTermCount`, `DecrementNumTerms`

## Notes

- `IndexSpec_FreeUnlinkedData` is static — only called from `IndexSpec_Free`.
- Dict callback helpers (`CharBuf_*`, `InvIndFreeCb`) are used to initialize `keysDict` dict type in `IndexSpec_MakeKeyless`.
- `isSpecOnDisk` is duplicated as static inline in `spec_lifecycle.c`, `spec_parse.c`, and `spec_rdb.c`.

**Extracted 2026-03-25**: All functions verified against implementation.
