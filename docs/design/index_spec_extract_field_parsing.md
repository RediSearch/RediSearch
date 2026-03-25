# Extraction Plan: Field Parsing

**Source**: `src/spec.c` (~750 lines)
**Target**: `src/spec/spec_field_parse.c` + `src/spec/spec_field_parse.h`
**Extractability**: Medium-High

## Goal

Extract all field definition parsing (from FT.CREATE / FT.ALTER argument streams) into a dedicated file pair. This is the largest submodule by line count.

## Functions to Move

### To `spec_field_parse.c`
```
checkPhoneticAlgorithmAndLang      (static)
parseVectorField_GetType           (static)
parseVectorField_GetMetric         (static)
parseVectorField_GetQuantBits      (static)
parseVectorField_validate_hnsw     (static)
parseVectorField_validate_flat     (static)
parseVectorField_validate_svs      (static)
parseVectorField_hnsw              (static)
parseVectorField_flat              (static)
parseVectorField_svs               (static)
parseTextField                     (static)
parseTagField                      (static)
parseGeometryField                 (static)
parseVectorField                   (static)
parseFieldSpec                     (static → expose as internal)
IndexSpec_AddFieldsInternal        (static → expose as internal)
IndexSpec_AddFields                (public)
VecSimIndex_validate_params        (public)
initializeFieldSpec                (static → expose as internal, also called by RDB load)
isSpecOnDiskForValidation          (static inline — shared)
```

### To `spec_field_parse.h`
```
IndexSpec_AddFields                (already in spec.h — keep there or move)
VecSimIndex_validate_params        (already in spec.h — keep there or move)
parseFieldSpec                     (internal, for use by spec.c's IndexSpec_Parse)
IndexSpec_AddFieldsInternal        (internal, for use by spec.c's IndexSpec_Parse)
```

## Design Decision: Where Does IndexSpec_Parse Live?

`IndexSpec_Parse` (lines 1720–1859, ~140 lines) is the FT.CREATE orchestrator. It:
1. Creates a new IndexSpec
2. Parses top-level options (NOOFFSETS, STOPWORDS, ON, PREFIX, etc.)
3. Creates a SchemaRule
4. Opens disk spec
5. Calls `IndexSpec_AddFieldsInternal` for the SCHEMA section
6. Validates Flex constraints

**Decision**: `IndexSpec_Parse` was extracted into its own file `src/spec/spec_parse.c` — separate from field parsing, since it orchestrates multiple submodules. See `index_spec_extract_parse.md`.

## Shared Helpers

These helpers are used by field parsing but defined/used elsewhere:

- `isSpecOnDiskForValidation` — defined as static inline in both `spec_parse.c` and `spec_field_parse.c`.
- `isSpecJson` — macro/inline, stays in spec.h or field_spec.h.
- `setMemoryInfo` — moved to `spec_scanner.c`.
- `memoryLimit`, `BLOCK_MEMORY_LIMIT` — globals externed via `spec_struct.h`.
- `IndexSpec_CreateTextId` — in `spec_lifecycle.c`, declared in `spec_lifecycle.h`.
- `IndexSpec_BuildSpecCache` — in `spec_cache.c`, declared in `spec_cache.h`.
- `FieldsGlobalStats_UpdateStats` — external dependency, just include header.

## Potential Further Split

The vector parsing functions alone are ~500 lines and could become `spec_field_parse_vector.c` if desired. The text/tag/geo/geometry parsers would stay in `spec_field_parse.c`. This is optional — one file is fine at 750 lines.

## Build Integration

- Add `src/spec/spec_field_parse.c` to build system.
- `spec_parse.c` and `spec_rdb.c` include `spec_field_parse.h`.
- `spec_field_parse.c` includes `spec.h`, `field_spec.h`, VecSim headers, ArgsCursor.

## Note on handleBadArguments

`handleBadArguments` is used by `IndexSpec_Parse` (in spec.c) during top-level option parsing, not field parsing. It stays in `spec.c`.

## Note on VecSimIndex_validate_params

`VecSimIndex_validate_params` is also called from `src/vector_index.c:434` (during RDB load memory limit checks). It must be declared in `spec_field_parse.h`.

## Risks

- `AddFieldsInternal` modifies `IndexSpec` directly (flags, suffix trie, spcache). This coupling is inherent — the function needs write access to the spec.
- JSON path API (`japi->*`) is a global function pointer. Just need the include.
- `initializeFieldSpec` is also called by RDB load code — must be declared in `spec_field_parse.h` so `spec_rdb.c` can call it.

## Validation

After extraction:
- `./build.sh` must succeed.
- `./build.sh RUN_PYTEST TEST=test_create` — FT.CREATE with all field types.
- `./build.sh RUN_PYTEST TEST=test_alter` — FT.ALTER adding fields.
- `./build.sh RUN_PYTEST TEST=test_vecsim` — vector field parsing.
- `./build.sh RUN_PYTEST TEST=test_tags` — tag field parsing.

**Validated 2026-03-19**: All functions verified. Key corrections: `handleBadArguments` stays in spec.c (not field parsing), `initializeFieldSpec` added to move list, `VecSimIndex_validate_params` also called from `vector_index.c`.
