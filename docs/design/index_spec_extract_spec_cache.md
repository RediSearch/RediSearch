# Extraction Plan: Spec Cache

**Source**: `src/spec.c` (~80 lines)
**Target**: `src/spec_cache.c` + `src/spec_cache.h`
**Extractability**: Very High

## Goal

Extract the IndexSpecCache — an immutable, refcounted snapshot of index fields used for thread-safe access by query threads. This is the smallest and cleanest extraction target.

## Functions to Move

### To `spec_cache.c`
```
IndexSpecCache_Free                (static)
IndexSpecCache_Decref              (public)
IndexSpec_BuildSpecCache           (was static, needs to become internal/public)
IndexSpec_GetSpecCache             (public)
```

### To `spec_cache.h`
```
IndexSpecCache (struct — currently in spec.h:416–420)
IndexSpecCache_Decref
IndexSpec_BuildSpecCache           (internal — called by spec.c after schema changes)
IndexSpec_GetSpecCache
```

## Dependencies

Minimal:
- `FieldSpec` struct — from `field_spec.h`
- `HiddenString_Duplicate`, `HiddenString_Free` — from `obfuscation/hidden.h`
- `rm_calloc`, `rm_malloc`, `rm_free` — from `rmalloc.h`
- `__atomic_fetch_add`, `__atomic_sub_fetch` — compiler builtins
- `RS_LOG_ASSERT` — from `rmutil/rm_assert.h`

Zero dependency on IndexSpec itself (only reads `spec->numFields` and `spec->fields` via the `BuildSpecCache` parameter, and `spec->spcache` via `GetSpecCache`).

## Struct Relocation

Move `IndexSpecCache` struct definition from `spec.h` to `spec_cache.h`. `spec.h` can either:
- Forward-declare `struct IndexSpecCache` and include `spec_cache.h` (simplest), or
- Include `spec_cache.h` directly.

The `IndexSpec` struct has `struct IndexSpecCache *spcache` — only needs a forward declaration.

## Callers

- `IndexSpec_AddFieldsInternal` (spec.c) — calls `IndexSpecCache_Decref(sp->spcache)` then `sp->spcache = IndexSpec_BuildSpecCache(sp)`.
- `IndexSpec_RdbLoad` (spec_rdb.c) — calls `sp->spcache = IndexSpec_BuildSpecCache(sp)`.
- `IndexSpec_FreeUnlinkedData` (spec.c) — calls `IndexSpecCache_Decref(sp->spcache)`.
- Query execution paths — call `IndexSpec_GetSpecCache` to get a thread-safe snapshot, later `IndexSpecCache_Decref`.

## Notes

- This is a pure data structure with no side effects.
- The refcounting is done with relaxed atomics — appropriate for the access pattern (spec thread writes, query threads read).
- `BuildSpecCache` deep-copies field names/paths via `HiddenString_Duplicate`. Field type-specific data (tagOpts, vectorOpts) is shallow-copied, which is fine since those are read-only after creation.

## Build Integration

- Add `spec_cache.c` to build system.
- `spec.h` gains `#include "spec_cache.h"` (or forward-declares + includes where needed).
- `spec.c`, `spec_rdb.c` include `spec_cache.h`.

## Risks

Essentially none. This is a self-contained immutable snapshot with refcounting.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_UNIT_TESTS` — unit tests.
- `./build.sh RUN_PYTEST` — any test exercising concurrent queries during schema changes.
