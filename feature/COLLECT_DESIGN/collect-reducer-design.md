# COLLECT Reducer: Technical Design Document

> **Feature:** A new `COLLECT` reducer for `FT.AGGREGATE` that lets users explicitly compose multi-field entries within groups — with optional sorting and limiting.
>
> **Status:** Design Sketch
>
> **Authors:** Itzik Vaknin
>
> **Date:** 2026-03-23
>
> **References:** [COLLECT Proposal](../FINAL_DESIGN/collect-reducer-proposal.md) · [Sorter Technicalities](../technicalities/sorter.md) · [Coordinator Technicalities](../technicalities/coordinator.md)

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Current State](#2-current-state)
3. [Syntax](#3-syntax)
4. [Response Format](#4-response-format)
5. [Architecture Overview](#5-architecture-overview)
6. [Shard-Side — Heap Pipeline (Sort & Limit)](#6-shard-side--heap-pipeline-sort--limit)
7. [Implicit Field Loading](#7-implicit-field-loading)
8. [Coordinator Distribution](#8-coordinator-distribution)
9. [Coordinator Merge](#9-coordinator-merge)
10. [Response Serialization (RESP2 / RESP3)](#10-response-serialization-resp2--resp3)
11. [Null & Missing Field Handling](#11-null--missing-field-handling)
12. [Decision Log](#12-decision-log)
13. [Files Changed](#13-files-changed)

---

## 1. Motivation

Within a GROUPBY, users need to collect per-document data from each group — not just aggregated scalars. The use cases range from simple ("give me the top-3 titles sorted by rating") to complex ("give me the top-2 full documents per group, sorted by sweetness, including the Redis key and score").

The `COLLECT` reducer introduces explicit multi-field projection (including whole-document via `*`, `@__key`, `@__score`) and in-group SORTBY/LIMIT.

---

## 2. Current State

Today's GROUPBY reducers that retain per-document data:

| Reducer | What It Does | Limitations |
|---------|-------------|-------------|
| `TOLIST @field` | Collects all values of a single field | Single field. Unordered. No limiting. |
| `FIRST_VALUE @field BY @sortfield` | Returns the single best value | Top-1 only. Single return field, single sort field. |
| `RANDOM_SAMPLE @field N` | Random sample of N values | Single field. Random, not sorted. |

All other reducers (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `STDDEV`, `QUANTILE`, `COUNT_DISTINCT`, `COUNT_DISTINCTISH`) reduce the group to a single scalar.

Per-document extraction is single-field only, and sorting is limited to top-1 (`FIRST_VALUE`).

---

## 3. Syntax

```
REDUCE COLLECT <narg> FIELDS <num_fields> <field_1> [<field_2> ...]
  [SORTBY <narg> <@field> [ASC|DESC] [<@field> [ASC|DESC] ...]]
  [LIMIT <offset> <count>]
  AS <alias>
```

### Token Breakdown

| Token | Required | Description |
|-------|----------|-------------|
| `FIELDS <num_fields>` | Yes | Number of fields to project, followed by field names |
| `<field>` | Yes | `@field_name`, `@__key`, `@__score`, or `*` (full payload, nested) |
| `SORTBY <narg> ...` | No | In-group sort by one or more fields with ASC/DESC |
| `LIMIT <offset> <count>` | No | Bound the output per group |
| `AS <alias>` | Yes | Output column name (outside the narg window) |

### narg Counting

`narg` counts all tokens between `COLLECT` and `AS` (exclusive). `AS` and `<alias>` are outside the window.

```
REDUCE COLLECT 12 FIELDS 3 @__key @__score * SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
               ^^
               narg = 12 tokens: FIELDS 3 @__key @__score * SORTBY 2 @sweetness DESC LIMIT 0 2
```

### Entry Width

Each collected entry is always `2 × num_fields` elements wide (name-value pairs). The field names appear literally in the output.

### Notes

- `num_fields >= 1`.
- If `*` appears in fields, `LOAD *` must precede the GROUPBY.
- SORTBY fields do **not** need to be in the FIELDS list (they are auxiliary — used for ordering but not projected unless explicitly listed).
- LIMIT without SORTBY is allowed (arbitrary top-K, bounded memory).

---

## 4. Response Format

Each collected entry follows the same key-value map format used by `FT.AGGREGATE` results. The COLLECT output is an array of such entries nested under the alias.

### Example 1: Key + Score + Full Payload

```
FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 12 FIELDS 3 @__key @__score * SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

```
1# extra_attributes =>
      1# "color" => "yellow"
      2# "top_fruits" =>
          1) 1# "__key" => "doc_10"
             2# "__score" => "0.95"
             3# "*" =>
                  1# "fruit" => "apple"
                  2# "color" => "yellow"
                  3# "sweetness" => "6"
          2) 1# "__key" => "doc_1"
             2# "__score" => "0.82"
             3# "*" =>
                  1# "fruit" => "banana"
                  2# "color" => "yellow"
                  3# "sweetness" => "5"
                  4# "origin" => "ecuador"
   2# values => (empty array)
```

The `*` field contains a nested map (the full document payload). All other fields are flat key-value pairs.

### Example 2: Key + Single Field

```
FT.AGGREGATE idx:fruits *
  GROUPBY 1 @color
    REDUCE COLLECT 8 FIELDS 2 @__key @fruit SORTBY 2 @sweetness DESC LIMIT 0 3 AS top_fruits
```

```
1# extra_attributes =>
      1# "color" => "yellow"
      2# "top_fruits" =>
          1) 1# "__key" => "doc_10"
             2# "fruit" => "apple"
          2) 1# "__key" => "doc_1"
             2# "fruit" => "banana"
          3) 1# "__key" => "doc_7"
             2# "fruit" => "grape"
   2# values => (empty array)
```

### Example 3: Multi-Field Projection (No Key)

```
FT.AGGREGATE idx:movies *
  GROUPBY 1 @genre
    REDUCE COLLECT 7 FIELDS 2 @title @rating SORTBY 2 @rating DESC LIMIT 0 3 AS top_movies
```

```
1# extra_attributes =>
      1# "genre" => "scifi"
      2# "top_movies" =>
          1) 1# "title" => "Inception"
             2# "rating" => "8.8"
          2) 1# "title" => "The Matrix"
             2# "rating" => "8.7"
   2# values => (empty array)
```

### Format Invariant

Every collected entry is a key-value map with exactly `num_fields` pairs. The `*` slot is one pair whose value is itself a nested map (the full document).

---

## 5. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SHARD / STANDALONE                           │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────────────┐                  │
│  │ Implicit     │───▶│ Heap Pipeline             │                  │
│  │ Field Load   │    │ (heap → finalize)         │                  │
│  │              │    │                            │                  │
│  └──────────────┘    └──────────┬─────────────────┘                 │
│                                 │                                    │
│                           ┌─────▼──────┐                            │
│                           │ Wire       │                            │
│                           │ Output     │                            │
│                           └─────┬──────┘                            │
│                                 │                                    │
└─────────────────────────────────┼────────────────────────────────────┘
                                  │ RESP (network)
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          COORDINATOR                                  │
│                                                                       │
│  ┌──────────┐    ┌──────────────────────────────────────────────┐    │
│  │  Dist    │───▶│ Coordinator Merge                            │    │
│  │  Function│    │ (unwrap entries → heap → finalize)           │    │
│  └──────────┘    └──────────────────────────────────────────────┘    │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 6. Shard-Side — Heap Pipeline (Sort & Limit)

This is the core accumulation engine. On each `Add()` call (one per document in the group), the reducer captures the projected fields and inserts into the heap.

### 6.1 Add() Flow

```
                    Incoming document (Add)
                           │
                           ▼
                 ┌──────────────────┐
                 │ 1. Capture       │
                 │    projected     │
                 │    fields +      │
                 │    sort keys     │
                 │    (IncrRef all) │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ 2. Build entry   │
                 │    (flat struct) │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ 3. Heap insert   │
                 │    (bounded to   │
                 │     K = off+cnt) │
                 │                  │
                 │  heap.size < K → push
                 │  heap.size = K → compare
                 │    with worst entry
                 │    better → replace
                 │    worse  → drop
                 └──────────────────┘
```

### 6.2 Heap Entry Structure

Each heap entry captures the full projection plus sort metadata:

```
┌───────────────────────────────────────────────────────────────┐
│  CollectHeapEntry                                              │
│                                                                │
│  ┌─────────────────────────┐                                  │
│  │ projected[0]: RSValue *  │  ← value of field_0             │
│  │ projected[1]: RSValue *  │  ← value of field_1             │
│  │ ...                      │                                  │
│  │ projected[N-1]: RSValue* │  ← value of field_(N-1)         │
│  ├─────────────────────────┤                                  │
│  │ sortvals[0]: RSValue *   │  ← first sort key value         │
│  │ sortvals[1]: RSValue *   │  ← second sort key value        │
│  │ ...                      │                                  │
│  ├─────────────────────────┤                                  │
│  │ docId: t_docId           │  ← tie-break (shard only)       │
│  └─────────────────────────┘                                  │
│                                                                │
│  All RSValue* are IncrRef'd — they survive the row wipe.      │
│  On eviction or free: DecrRef every pointer.                  │
└───────────────────────────────────────────────────────────────┘
```

Instead of a single payload, we store an array of `projected[]` values — one per field in the FIELDS clause.

### 6.3 Finalize

```
         Heap (K entries)
                      │
                      ▼
              ┌───────────────┐
              │ Pop K times   │  mmh_pop_max() per call
              │ → sorted      │  yields entries best → worst
              └───────┬───────┘
                      │
                      ▼
        ┌──────────────────────────┐
        │ entries[0] ... entries[K-1]
        │ ◄─ offset ─▶◄── count ──▶
        │              ╔═══════════╗
        │              ║ return    ║
        │              ╚═══════════╝
        └──────────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │ Build output  │  For each surviving entry:
              │ array of      │    build flat KV array from
              │ KV entries    │    field_names[] + projected[]
              └───────────────┘
```

Each entry in the output becomes:

```
RSValue_Array [
  RSValue_String("__key"),    RSValue_String("doc_10"),
  RSValue_String("__score"),  RSValue_String("0.95"),
  RSValue_String("*"),        RSValue_Array([...full payload...])
]
```

Width = `2 × num_fields`. Field names are baked from the parsed FIELDS list.

### 6.4 In-Group Comparator

```
compare(entry_a, entry_b):
    for i in 0..nsortkeys:
        va = entry_a.sortvals[i]
        vb = entry_b.sortvals[i]

        ┌─ va == NULL and vb == NULL  → continue (tie)
        │  va == NULL                 → va loses (sort last)
        │  vb == NULL                 → vb loses (sort last)
        └─ both non-NULL             → rc = RSValue_Cmp(va, vb)
                                        if ASC: use rc as-is
                                        if DESC: flip rc
                                        if rc != 0: return rc

    // all sort keys tied → tie-break by doc ID (shard only)
    // on coordinator: arbitrary (first-seen)
    return entry_a.docId < entry_b.docId ? -1 : 1
```

Same algorithm as RPSorter's `cmpByFields`, reading from `sortvals[]` instead of `RLookupRow_Get()`.

### 6.5 Per-Group Instance

```
┌──────────────────────────────────────┐
│  CollectGroupInstance                │
│                                      │
│  heap: mm_heap_t *                   │  ← bounded to K = offset + count
│                                      │
│  Allocated via BlkAlloc per group.   │
└──────────────────────────────────────┘
```

### 6.6 No SORTBY / No LIMIT Behavior

| SORTBY | LIMIT | Behavior |
|--------|-------|----------|
| Present | Present | Bounded heap, sorted output, sliced window |
| Present | Absent | Unbounded — collect all, sort in finalize (heap size = group size) |
| Absent | Present | Bounded heap with insertion-order semantics (arbitrary top-K) |
| Absent | Absent | Collect all entries, insertion order |

When no SORTBY and no LIMIT: the heap degenerates into a simple list. Implementation may use a growing array instead of a heap for this case.

---

## 7. Implicit Field Loading

COLLECT preserves the current GROUPBY-LOAD behavior. The fields referenced in the FIELDS clause and SORTBY clause are implicitly available if they were loaded upstream (via `LOAD *`, `LOAD @field`, or from the index schema).

This means the following two queries are interchangeable:

```
FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 11 FIELDS 2 @__key @sweetness SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

```
FT.AGGREGATE idx:fruits *
  GROUPBY 1 @color
    REDUCE COLLECT 11 FIELDS 2 @__key @sweetness SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

If a field is in the schema (indexed), it's already available without explicit `LOAD`. If it's not in the schema, the user must `LOAD` it. This is standard GROUPBY behavior — no special handling needed in COLLECT.

**Exception:** The `*` field in FIELDS requires `LOAD *` to precede the GROUPBY.

### 7.1 `LOAD *` Detection and Optimization

When COLLECT has `*` in FIELDS, it needs to know whether `LOAD *` was already requested upstream. At reducer creation time, this is a single flag check on the source lookup:

```c
bool loadAllActive = (options->srclookup->_options & RLOOKUP_OPT_ALLLOADED) != 0;
```

- **Flag set** — An upstream `LOAD *` step already loads all document fields into every source row. COLLECT simply iterates them during `Add()`.
- **Flag not set** — COLLECT is the only consumer of `*`. This opens an optimization path: instead of forcing a full `LOAD *` for every document (most of which may not survive the heap), COLLECT can defer loading to `Finalize()` — loading only the K winners.

**Constraint:** This check is stable because the pipeline enforces that `LOAD` steps can only appear before any GROUPBY/APPLY/FILTER. No step after the GROUPBY can set `RLOOKUP_OPT_ALLLOADED` on the root lookup. If a future change relaxes this ordering constraint, the optimization check must be revisited.

---

## 8. Coordinator Distribution

### 8.1 Strategy

The distribution function injects SORTBY fields into the FIELDS list if not already present, so shards include them in their output. On the coordinator, those sort keys are opened as **unresolved keys** in the `RLookup` (same pattern as FT.HYBRID's `@__key`), so RPNet populates them into the `RLookupRow` during deserialization. The coordinator's COLLECT reducer reads them via `RLookupRow_Get()` — same API as the shard side.

### 8.2 Example: Sort Key Injection

**User writes:**

```
FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 10 FIELDS 1 @__key SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

**Shard receives:**

```
_FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 11 FIELDS 2 @__key @sweetness SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

`@sweetness` is injected into FIELDS (narg updated accordingly). On the coordinator it's opened as an unresolved key, accessible via `RLookupRow_Get()`.

### 8.3 No Tie-Breaker Injection

Keys (`@__key`) are **not** required on the coordinator. There is no tie-breaker injection. When all sort keys tie on the coordinator, ordering is arbitrary (first-seen from shard merge order). This matches `FIRST_VALUE` coordinator behavior.

### 8.4 Distribution Function

```c
static int distributeCollect(ReducerDistCtx *rdctx, QueryError *status) {
    PLN_Reducer *src = rdctx->srcReducer;

    // 1. Build augmented FIELDS list (inject missing sort keys)
    // 2. Forward COLLECT with augmented args to shards
    const char *alias;
    if (!rdctx->addRemote(src->name, &alias, status, /* augmented args */)) {
        return REDISMODULE_ERR;
    }

    // 3. Open SORTBY fields as unresolved keys in the coordinator's RLookup
    //    RPNet will map them by name when deserializing shard responses
    //    → accessible via RLookupRow_Get() in the coordinator's reducer

    // 4. Coordinator gets the same reducer, reading from shard alias
    if (!rdctx->addLocal(src->name, status, /* coordinator args */)) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
```

---

## 9. Coordinator Merge

### 9.1 Wire Format: Shard Output

The shard's `Finalize()` output is an array of entries. Each entry is a flat key-value array (the COLLECT response format). Sort key values injected into FIELDS by the distribution function are included in the entries alongside user-requested fields.

**No special tuple wrapping needed.** COLLECT entries are self-describing KV arrays. The coordinator accesses sort key values via `RLookupKey` (populated by RPNet during deserialization), not by manual positional parsing.

### 9.2 Coordinator Add() Flow

```
    Shard 1 output          Shard 2 output
    for group "yellow":     for group "yellow":
    ┌────────────────┐      ┌────────────────┐
    │ [["__key",     │      │ [["__key",     │
    │   "doc_10",    │      │   "doc_5",     │
    │   "sweetness", │      │   "sweetness", │
    │   "6"],        │      │   "4"],        │
    │  ["__key",     │      │  ["__key",     │
    │   "doc_1",     │      │   "doc_8",     │
    │   "sweetness", │      │   "sweetness", │
    │   "5"]]        │      │   "3"]]        │
    │ (sweetness in  │      │ (injected into │
    │  FIELDS by     │      │  FIELDS by     │
    │  distribution) │      │  distribution) │
    └───────┬────────┘      └───────┬────────┘
            │                       │
            └───────────┬───────────┘
                        │
                        ▼
           Coordinator Add():
           ┌─────────────────────────────────────┐
           │ 1. Read alias column → RSValue_Array │
           │    (shard's output for this group)   │
           │                                      │
           │ 2. For each entry in the array:      │
           │    a. Read sort keys via RLookupKey   │
           │       (populated by RPNet)            │
           │    b. Extract projected field values  │
           │    c. Insert into bounded heap        │
           │       (same K = offset + count)       │
           └─────────────────────────────────────┘
                        │
                        ▼
           Coordinator Finalize():
           ┌─────────────────────────────────────┐
           │ Pop from heap → build KV entries     │
           │ → output (user's FIELDS only,        │
           │   no stripping needed)               │
           └─────────────────────────────────────┘
```

### 9.3 Sort Key Access on Coordinator — Unresolved Key Opening

The distribution function injects sort keys into the FIELDS list so shards include them in their output. On the coordinator side, the sort keys are opened as **unresolved keys** in the `RLookup` — the same pattern FT.HYBRID uses for `@__key`.

**How it works:**

1. During distribution plan construction, the distribution function opens the sort key names in the coordinator's `RLookup` with `RLOOKUP_OPT_ALLOWUNRESOLVED` enabled. This creates `RLookupKey` entries marked with `RLOOKUP_F_UNRESOLVED`.
2. When shard results arrive, RPNet deserializes the response and maps fields by name to their `RLookupKey`, populating the `RLookupRow`.
3. The COLLECT reducer's `Add()` on the coordinator reads sort keys via `RLookupRow_Get(sortkey, srcrow)` — the same API as the shard side. No manual positional extraction from the KV array.

**Advantages:**
- Reuses proven infrastructure (`RLOOKUP_OPT_ALLOWUNRESOLVED`).
- Sort keys are first-class `RLookupKey` entries — accessible via O(1) index lookup.
- The coordinator's `Finalize()` strips injected sort-key fields from the output entries, so the user sees only their original FIELDS.

### 9.4 Stripping Injected Fields

The coordinator knows which fields were injected by the distribution function (they are appended at the end of the FIELDS list). During `Finalize()`, the coordinator builds output entries using only the user's original `num_fields`, omitting the injected sort keys. The user sees exactly the fields they requested.

---

## 10. Response Serialization (RESP2 / RESP3)

The output is an array of entries, where each entry is a flat key-value array. This reuses the existing `FT.AGGREGATE` / `FT.SEARCH` serialization path:

| Protocol | Outer Array | Each Entry |
|----------|-------------|------------|
| RESP2 | `*N` (array of N entries) | `*M` (array of M elements, M = 2 × num_fields) |
| RESP3 | Array | Map (if client supports it) or Array |

The `*` field's value (full payload) is itself a nested array/map, serialized the same way `FT.SEARCH` serializes document content.

---

## 11. Null & Missing Field Handling

| Scenario | Behavior |
|----------|----------|
| Projected field is NULL/missing | Include in entry as `NULL` value |
| Sort key is NULL/missing | NULL loses — entry sorts last (regardless of ASC/DESC) |
| `@__key` not available | Should always be available (built-in) |
| `@__score` not available | Returns `"0"` or NULL if no scoring context |

---

## 12. Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Reducer name | COLLECT | Explicit composition semantics |
| Response format | Flat KV array per entry | Consistent with FT.AGGREGATE/FT.SEARCH document format |
| Coordinator tie-break | Arbitrary (first-seen) | Keys not required; matches FIRST_VALUE behavior |
| Shard tie-break | Doc ID | Consistent with RPSorter |
| Sort key injection | Append to FIELDS on coordinator distribute | Coordinator needs sort values for merge; shards include them in output |
| Sort key access on coordinator | Unresolved key opening (same as FT.HYBRID `@__key`) | RPNet populates RLookupRow by name; reducer reads via RLookupRow_Get() — no manual KV parsing |
| Injected field stripping | Coordinator strips before final output | User sees only their requested fields |
| Wire format | Self-describing KV entries (no special tuples) | Entries contain field names and values |
| Implicit field loading | Preserve GROUPBY-LOAD behavior | No surprises; standard behavior |
| `LOAD *` detection | Check `RLOOKUP_OPT_ALLLOADED` on `srclookup` at parse time | Stable because LOAD is restricted to before GROUPBY; enables deferred-load optimization when COLLECT is the sole `*` consumer |
| Max sort keys | 8 (SORTASCMAP_MAXFIELDS) | Consistent with RPSorter |

---

## 13. Files Changed

| File | What Changes | Effort |
|------|-------------|--------|
| `src/aggregate/reducers/collect.c` (new) | **New file.** Full reducer implementation: Add() with heap pipeline, Finalize() with KV entry construction. | High |
| `src/aggregate/reducers/reducers.h` | Register `COLLECT` in the reducer factory table. | Low |
| `src/coord/dist_plan.cpp` | New `distributeCollect` function with sort-key injection logic. Registry entry. | Medium |
| `src/result_processor.c` (or Grouper code) | Doc ID injection before reducer loop. | Low |
| `src/aggregate/aggregate_request.c` | Wire up `COLLECT` to the reducer factory. | Low |

---

## Appendix A: End-to-End Data Flow (Cluster)

```
USER QUERY:
  FT.AGGREGATE idx:fruits *
    LOAD *
    GROUPBY 1 @color
      REDUCE COLLECT 10 FIELDS 1 @__key SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits

                                    │
                     ┌──────────────┼──────────────┐
                     │              │              │
                 COORDINATOR REWRITES TO:
                     │              │              │
                Shard 1         Shard 2         Shard 3

   Each shard receives:
   _FT.AGGREGATE idx:fruits *
     LOAD *
     GROUPBY 1 @color
       REDUCE COLLECT 11 FIELDS 2 @__key @sweetness
         SORTBY 2 @sweetness DESC LIMIT 0 2 AS __gen_alias_collect_top_fruits

                     │              │              │
      ┌──────────────────────┐  (same per shard)
      │ Pipeline per shard:  │
      │  LOAD * → GROUPBY    │
      │  → COLLECT Add()     │
      │    per doc in group  │
      │                      │
      │ COLLECT Finalize():  │
      │  heap → sort → slice │
      │  → array of KV       │
      │    entries            │
      └──────────┬───────────┘
                 │
    Shard output for group "yellow":
    [
      ["__key", "doc_10", "sweetness", "6"],
      ["__key", "doc_1",  "sweetness", "5"]
    ]
                 │
    ─────────── network ───────────
                 │
      ┌──────────▼───────────┐
      │ COORDINATOR          │
      │                      │
      │ Grouper groups shard │
      │ rows by @color       │
      │                      │
      │ COLLECT Add():       │
      │  read sort keys via  │
      │  RLookupKey (opened  │
      │  as unresolved)      │
      │  → heap insert       │
      │  (across all shards) │
      │                      │
      │ COLLECT Finalize():  │
      │  pop from heap       │
      │  → build KV entries  │
      │  → strip injected    │
      │    @sweetness field  │
      └──────────┬───────────┘
                 │
    User response for group "yellow":
    top_fruits: [["__key", "doc_10"],
                 ["__key", "doc_1"]]
```

---

## Appendix B: Extended Reducer Struct

```c
typedef struct {
    Reducer base;                          // must be first (allows cast)

    // Field projection configuration
    const RLookupKey *fields[MAX_COLLECT_FIELDS];   // projected fields
    const char *fieldNames[MAX_COLLECT_FIELDS];      // field name strings for output
    size_t numFields;
    bool hasStarField;                     // true if "*" is one of the fields
    int starFieldIndex;                    // index of "*" in fields[] (-1 if absent)

    // Sort configuration
    const RLookupKey *sortkeys[SORTASCMAP_MAXFIELDS];  // up to 8
    size_t nsortkeys;
    uint64_t sortAscMap;                   // bitmap: bit i = ASC for key i

    // Limit configuration
    uint64_t offset;
    uint64_t count;

    // For "*" field — needed to iterate fields during Add()
    const RLookup *srcLookup;

    // Coordinator metadata
    size_t numInjectedFields;              // count of sort-key fields injected by distribution
    // (injected fields are always at the end of fields[])
} CollectReducer;
```

---

## Open Questions

- **LIMIT default value:** When LIMIT is absent, what is the default? Unbounded (collect all)? A sensible cap? Needs product decision.
- **Null & missing field handling:** How should NULL/missing sort keys affect ordering? How should NULL projected fields appear in the output? Needs decision.
