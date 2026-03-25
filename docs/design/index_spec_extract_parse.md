# Extraction Plan: Parse (FT.CREATE Orchestrator)

**Source**: `src/spec.c` (part of ~1000-line "remaining core")
**Target**: `src/spec/spec_parse.c` + `src/spec/spec_parse.h`
**Extractability**: High

## Goal

Extract the top-level FT.CREATE parsing orchestrator into a dedicated file. This is the function that ties together option parsing, schema rule creation, disk spec setup, and field parsing.

## Functions in `spec_parse.c` (220 lines)

```
isSpecOnDisk                      (static inline — shared helper)
isSpecOnDiskForValidation         (static inline — shared helper)
handleBadArguments                (static)
IndexSpec_ParseRedisArgs          (public)
IndexSpec_Parse                   (public)
IndexSpec_ParseC                  (public — convenience wrapper)
```

## Declarations in `spec_parse.h`

```
IndexSpec_ParseRedisArgs
IndexSpec_Parse
IndexSpec_ParseC
```

## Key Dependencies

- `NewIndexSpec` (from `spec_lifecycle.h`)
- `IndexSpec_MakeKeyless` (from `spec_lifecycle.h`)
- `IndexSpec_AddFieldsInternal` (from `spec_field_parse.h`)
- `SchemaRule_Create` (external)
- `SearchDisk_OpenIndex` (external)
- `StopWordList_*` APIs (stopwords parsing)
- `ArgsCursor` (argument parsing framework)
- `ACArgSpec` / `ACTableSpec` (argument table specs)
- `HiddenString_*` APIs (name handling)
- `IndexSpec` struct (full definition)

## Design Rationale

`IndexSpec_Parse` was separated from field parsing (`spec_field_parse.c`) because it orchestrates multiple submodules:
1. Creates a new IndexSpec via `NewIndexSpec`
2. Parses top-level options (NOOFFSETS, STOPWORDS, ON, PREFIX, LANGUAGE, etc.)
3. Creates a SchemaRule
4. Opens disk spec
5. Calls `IndexSpec_AddFieldsInternal` for the SCHEMA section
6. Validates Flex constraints

It was also separated from the registry (`spec_registry.c`) because `IndexSpec_CreateNew` in the registry calls `IndexSpec_Parse` — keeping them together would mix "parse definition" with "register globally".

## Callers

- `spec_registry.c` — `IndexSpec_CreateNew` calls `IndexSpec_Parse`
- `spec_rdb.c` — may call `IndexSpec_Parse` for deserialization
- `src/redisearch_api.c` — calls `IndexSpec_ParseC`
- C++ tests — call `IndexSpec_Parse` and `IndexSpec_ParseRedisArgs`

## Notes

- `handleBadArguments` is static — only used within `IndexSpec_ParseRedisArgs` for unknown top-level args.
- `isSpecOnDisk` and `isSpecOnDiskForValidation` are duplicated as static inline here and in other spec files.
- `IndexSpec_ParseC` is a thin wrapper that converts a `const char *` name to `HiddenString` and calls `IndexSpec_Parse`.

**Extracted 2026-03-25**: All functions verified against implementation.
