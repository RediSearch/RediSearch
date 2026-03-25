# Enhanced TOLIST: Technical Design Document

> **Feature:** In-group sorting, limiting, deduplication control, and full-document payload for the TOLIST reducer.
>
> **Scope:** Standalone + Cluster (coordinator merge). JSON + Hash documents.
>
> **References:** [PRD](../PRD/enhanced-ft-aggregate-tolist.md) · [Decisions](../PRD/prd-addendum-decisions.md) · [Delivery Plan](../DESIGN/delivery-plan.md)

---

## Table of Contents

1. [Feature Summary](#1-feature-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Component 1: Shard-Side — Dedup, Sort, Limit (The Heap Pipeline)](#3-component-1-shard-side--dedup-sort-limit-the-heap-pipeline)
4. [Component 2: Wire Format Change — Shard Output with Sort Key Embedding](#4-component-2-wire-format-change--shard-output-with-sort-key-embedding)
5. [Component 3: Coordinator-Side — Unwrap, Merge, Re-Sort](#5-component-3-coordinator-side--unwrap-merge-re-sort)
6. [Component 4: Arg Parsing Extension](#6-component-4-arg-parsing-extension)
7. [Component 5: Distribution Function](#7-component-5-distribution-function)
8. [Component 6: TOLIST * Payload Construction](#8-component-6-tolist--payload-construction)
9. [Component 7: Doc ID Injection for Tie-Breaking](#9-component-7-doc-id-injection-for-tie-breaking)
10. [Files Changed](#10-files-changed)

---

## 1. Feature Summary

Enhanced TOLIST extends the `REDUCE TOLIST` reducer with:

```
REDUCE TOLIST narg <@field | *> [ALLOWDUPS] [SORTBY narg (@field [ASC|DESC])+] [LIMIT offset count] AS alias
```

**New capabilities:**
- **In-group sorting** — sort collected items by one or more fields within each group
- **In-group limiting** — return only the top-K items per group (bounded heap)
- **Dedup control** — `ALLOWDUPS` flag to skip deduplication
- **Full-document payload** — `TOLIST *` collects all loaded fields per document

**Backward compatible:** `TOLIST 1 @field` remains unchanged (dict-based, unordered, deduplicated).

---

## 2. Architecture Overview

The feature touches 7 components across the shard and coordinator:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SHARD / STANDALONE                           │
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │  4. Arg  │───▶│ 6. Payload   │───▶│ 3. Heap Pipeline         │  │
│  │  Parser  │    │ Construction │    │ (dedup → heap → finalize)│  │
│  └──────────┘    └──────────────┘    └──────────┬───────────────┘  │
│       │               │                         │                   │
│       │          ┌────────────┐           ┌─────▼──────┐           │
│       │          │ 7. Doc ID  │           │ 2. Wire    │           │
│       │          │ Injection  │           │ Format     │           │
│       │          └────────────┘           └─────┬──────┘           │
│       │                                         │                   │
└───────┼─────────────────────────────────────────┼───────────────────┘
        │                                         │ RESP (network)
        │                                         ▼
┌───────┼─────────────────────────────────────────────────────────────┐
│       │               COORDINATOR                                   │
│       │                                                             │
│  ┌────▼─────┐    ┌──────────────────────────────────────────────┐  │
│  │  5. Dist │───▶│ 3'. Coordinator Heap Pipeline                │  │
│  │  Function│    │ (unwrap tuples → dedup → heap → finalize)    │  │
│  └──────────┘    └──────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component 1: Shard-Side — Dedup, Sort, Limit (The Heap Pipeline)

This is the core of the feature. On each `Add()` call (one per document in the group), the reducer runs the new document through a 3-stage pipeline: **dedup check → heap insertion → eviction**.

### 3.1 The Flow: Step by Step

```
                       Incoming document (Add)
                              │
                              ▼
                    ┌──────────────────┐
                    │ 1. Capture       │
                    │    payload +     │
                    │    sort keys +   │
                    │    doc ID        │
                    │    (IncrRef all) │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐     ┌──────────┐
                    │ 2. Dedup check   │────▶│ REJECT   │
                    │    (dict lookup) │ dup │ DecrRef   │
                    │                  │     │ captured  │
                    │  skip if         │     │ values    │
                    │  ALLOWDUPS       │     └──────────┘
                    └────────┬─────────┘
                             │ unique
                             ▼
                    ┌──────────────────┐
                    │ 3. Dict insert   │  (add payload to dedup dict)
                    │    (IncrRef key) │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ 4. Heap insert   │
                    │                  │
                    │  heap.size < K?  │
                    │  ┌─yes──────┐    │
                    │  │ push     │    │
                    │  └──────────┘    │
                    │                  │
                    │  heap.size == K? │
                    │  ┌─compare──┐    │
                    │  │ with     │    │
                    │  │ worst    │    │
                    │  │ entry    │    │
                    │  └──┬───┬──┘    │
                    │   better worse   │
                    │     │     │      │
                    │     ▼     ▼      │
                    │  replace  drop   │
                    │  worst   new     │
                    │  entry   entry   │
                    │  (DecrRef (DecrRef│
                    │   old)    new)   │
                    └──────────────────┘
                             │
                             ▼
                    K = offset + count
                    (heap never exceeds K entries)
```

Where `K = offset + count`. The heap is a bounded min-max heap (`mm_heap_t`) that keeps the **best** K entries seen so far.

### 3.2 Finalize: Extract, Sort, Slice

```
         Heap (K entries, partially ordered)
                      │
                      ▼
              ┌───────────────┐
              │ Extract all   │  mm_heap_extract_all()
              │ K entries     │  → flat array of HeapEntry
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Sort in-place │  qsort with in-group comparator
              │ (full order)  │  (same comparator used by heap)
              └───────┬───────┘
                      │
                      ▼
        ┌──────────────────────────┐
        │ entries[0]  entries[1]  ... entries[K-1]
        │ ◄─ offset ─▶◄───── count ─────▶
        │              ╔═══════════════╗
        │              ║ return these  ║
        │              ╚═══════════════╝
        └──────────────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │ Build output  │  RSValue_Array of payloads only
              │ (strip sort   │  (sort keys discarded)
              │  keys)        │
              └───────────────┘
```

### 3.3 Heap Entry Structure

Each entry in the heap is a flat struct:

```
┌─────────────────────────────────────────────────────────────┐
│  HeapEntry                                                   │
│                                                              │
│  ┌─────────────────────┐                                    │
│  │ payload: RSValue *   │  ← what gets returned to user     │
│  ├─────────────────────┤                                    │
│  │ sortvals[0]: RSValue*│  ← first sort key value           │
│  │ sortvals[1]: RSValue*│  ← second sort key value          │
│  │ ...                  │                                    │
│  │ sortvals[N-1]        │  ← Nth sort key value             │
│  ├─────────────────────┤                                    │
│  │ docId: t_docId       │  ← tie-break (shard only)         │
│  └─────────────────────┘                                    │
│                                                              │
│  All RSValue* are IncrRef'd — they survive the row wipe.    │
│  On eviction or free: DecrRef every pointer.                │
└─────────────────────────────────────────────────────────────┘
```

### 3.4 Per-Group Instance

```
┌──────────────────────────────────────┐
│  ToListGroupInstance                  │
│                                      │
│  heap: mm_heap_t *                   │  ← bounded to K = offset + count
│  dedup_dict: dict *   (nullable)     │  ← NULL when ALLOWDUPS
│                                      │
│  Allocated via BlkAlloc per group.   │
└──────────────────────────────────────┘
```

### 3.5 In-Group Comparator

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

    // all sort keys tied → tie-break by doc ID
    return entry_a.docId < entry_b.docId ? -1 : 1
```

Same algorithm as RPSorter's `cmpByFields`, but reading from `sortvals[]` instead of `RLookupRow_Get()`. This is faster — no per-comparison key lookup.

### 3.6 Dedup + Heap Interaction Detail

Important invariant: **dedup dict and heap are independent containers.**

```
          dedup dict                    heap (size K)
         ┌──────────┐               ┌──────────────┐
Doc A ──▶│ add "A"  │──── unique ──▶│ push entry A │
         └──────────┘               └──────────────┘

         ┌──────────┐
Doc B ──▶│ add "B"  │──── unique ──▶ push entry B
         └──────────┘

         ┌──────────┐
Doc A ──▶│ lookup   │──── DUP ────▶ REJECT (don't touch heap)
(again)  │ "A"      │
         └──────────┘

         ┌──────────┐               ┌──────────────────────┐
Doc C ──▶│ add "C"  │──── unique ──▶│ heap full (K entries) │
         └──────────┘               │ compare C vs worst    │
                                    │   C better? → replace │
                                    │   C worse?  → drop C  │
                                    └──────────────────────┘

Note: when the heap evicts an entry, the evicted payload
STAYS in the dedup dict. It was "seen" — future duplicates
of the evicted value are still rejected.

→ Dict can grow to N (all unique docs in group)
→ Heap stays bounded at K
```

---

## 4. Component 2: Wire Format Change — Shard Output with Sort Key Embedding

### 4.1 The Problem

On the shard, `HeapEntry` holds both **payload** and **sort key values**. But the current `Finalize()` outputs only payloads — sort keys are discarded. The coordinator receives payloads with no sort metadata and cannot re-sort entries across shards.

### 4.2 Solution: Uniform Array-of-Tuples

The shard's `Finalize()` wraps each entry in a tuple containing both the value and the sort keys:

### 4.3 Before vs After — Shard `Finalize()` Output

**BEFORE** (current TOLIST — bare values):

```
Shard Finalize() output for one group:

  RSValue_Array [
    "Inception",
    "The Matrix",
    "Interstellar"
  ]

  → RESP:  *3\r\n$9\r\nInception\r\n$10\r\nThe Matrix\r\n$13\r\nInterstellar\r\n
```

**AFTER** (enhanced TOLIST — tuples with sort keys):

With `SORTBY 2 @rating DESC`:

```
Shard Finalize() output for one group:

  RSValue_Array [
    RSValue_Array [ "Inception",     "8.8" ],      ← [value, sortkey_0]
    RSValue_Array [ "The Matrix",    "8.7" ],
    RSValue_Array [ "Interstellar",  "8.6" ]
  ]

  Tuple layout:  [ payload, sortval_0, sortval_1, ..., sortval_N-1 ]
                    ▲                ▲
                    │                └─── sort keys (N of them)
                    └─── always element 0
```

Without SORTBY (legacy-compatible):

```
  RSValue_Array [
    RSValue_Array [ "Inception" ],                  ← [value] (tuple of length 1)
    RSValue_Array [ "The Matrix" ],
    RSValue_Array [ "Interstellar" ]
  ]
```

### 4.4 Diff: What Changes in `to_list.c` Finalize

```diff
  // BEFORE: tolistFinalize (current code)
  static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
      dict *values = ctx;
      size_t len = dictSize(values);
      dictIterator *it = dictGetIterator(values);
      RSValue **arr = RSValue_NewArrayBuilder(len);
      for (size_t i = 0; i < len; i++) {
          dictEntry *de = dictNext(it);
-         arr[i] = RSValue_IncrRef(dictGetKey(de));    // bare value
      }
      dictReleaseIterator(it);
      return RSValue_NewArrayFromBuilder(arr, len);
  }

  // AFTER: enhanced tolistFinalize (conceptual)
  static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
      ToListGroupInstance *inst = ctx;
+     // 1. Extract all entries from heap
+     HeapEntry *entries = heap_extract_all(inst->heap);
+     size_t len = ...;
+
+     // 2. Sort in-place with in-group comparator
+     qsort_r(entries, len, sizeof(HeapEntry), cmpHeapEntries, reducer);
+
+     // 3. Slice window: skip offset, take count
+     size_t start = reducer->offset;
+     size_t end = MIN(start + reducer->count, len);
+
+     // 4. Build output array of tuples
+     RSValue **arr = RSValue_NewArrayBuilder(end - start);
+     for (size_t i = start; i < end; i++) {
+         // Build tuple: [payload, sortval_0, ..., sortval_N-1]
+         size_t tuple_len = 1 + reducer->nsortkeys;
+         RSValue **tuple = RSValue_NewArrayBuilder(tuple_len);
+         tuple[0] = RSValue_IncrRef(entries[i].payload);
+         for (size_t k = 0; k < reducer->nsortkeys; k++) {
+             tuple[1 + k] = RSValue_IncrRef(entries[i].sortvals[k]);
+         }
+         arr[i - start] = RSValue_NewArrayFromBuilder(tuple, tuple_len);
+     }
+
+     // 5. DecrRef all heap entries (including those outside the window)
+     for (size_t i = 0; i < len; i++) {
+         heapEntry_free(&entries[i]);
+     }
+
+     return RSValue_NewArrayFromBuilder(arr, end - start);
  }
```

### 4.5 Why Uniform Tuples (No Legacy Split)

The format applies to **all** enhanced TOLIST, even `TOLIST 1 @field` without SORTBY — those just produce tuples of length 1 (`[value]`). This means:

- **One shard `Finalize()`** — always wraps entries in tuples
- **One coordinator `Add()`** — always unwraps element 0 as the value
- **Zero branching** on "is this legacy or enhanced" at the wire format level

The user-facing response is unchanged — the coordinator's `Finalize()` strips the wrapper and outputs bare values.

---

## 5. Component 3: Coordinator-Side — Unwrap, Merge, Re-Sort

The coordinator receives per-group arrays from each shard and must merge them.

### 5.1 Coordinator `Add()` Flow

```
    Shard 1 output            Shard 2 output           Shard 3 output
    for group "scifi":        for group "scifi":       for group "scifi":
    ┌──────────────────┐      ┌──────────────────┐     ┌──────────────────┐
    │ [["Inception",   │      │ [["Alien",       │     │ [["Arrival",     │
    │   "8.8"],        │      │   "8.5"],        │     │   "7.9"],        │
    │  ["Matrix",      │      │  ["Blade Runner",│     │  ["Dune",        │
    │   "8.7"]]        │      │   "8.1"]]        │     │   "8.0"]]        │
    └────────┬─────────┘      └────────┬─────────┘     └────────┬─────────┘
             │                         │                         │
             └─────────────┬───────────┴─────────────────────────┘
                           │
                           ▼
              Coordinator Add() — called once per shard row:
              ┌───────────────────────────────────────────┐
              │ 1. Read alias column → RSValue_Array      │
              │    (the shard's output for this group)    │
              │                                           │
              │ 2. For each tuple in the array:           │
              │    ┌─────────────────────────────────┐    │
              │    │ a. Unwrap:                      │    │
              │    │    payload  = tuple[0]           │    │
              │    │    sortvals = tuple[1..N]        │    │
              │    │                                  │    │
              │    │ b. Dedup check (if !ALLOWDUPS)   │    │
              │    │    → dict lookup on payload      │    │
              │    │                                  │    │
              │    │ c. Insert HeapEntry into         │    │
              │    │    coordinator's bounded heap    │    │
              │    │    (same K = offset + count)     │    │
              │    └─────────────────────────────────┘    │
              └───────────────────────────────────────────┘
                           │
                           ▼
              Coordinator Finalize():
              ┌───────────────────────────────────────────┐
              │ Same as shard Finalize (section 3.2):     │
              │ extract → sort → slice → output           │
              │                                           │
              │ BUT: output bare values only.             │
              │ Sort keys are stripped — user never sees   │
              │ the tuples.                               │
              └───────────────────────────────────────────┘
```

### 5.2 Before vs After — Coordinator `Add()` Behavior

**BEFORE** (current `tolistAdd`):

```c
static int tolistAdd(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
    dict *values = ctx;
    RSValue *v = RLookupRow_Get(rbase->srckey, srcrow);
    // v is the shard's TOLIST output (an RSValue_Array)

    if (!RSValue_IsArray(v)) {
        dictAdd(values, v, NULL);        // shard-side: scalar → dict
    } else {
        for (uint32_t i = 0; i < RSValue_ArrayLen(v); i++) {
            dictAdd(values, RSValue_ArrayItem(v, i), NULL);
                                         // coordinator: unpack array → dict
        }
    }
}
```

**AFTER** (enhanced — coordinator detects tuples):

```c
static int tolistAdd_enhanced(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
    ToListGroupInstance *inst = ctx;
    ToListReducer *reducer = (ToListReducer *)rbase;
    RSValue *v = RLookupRow_Get(rbase->srckey, srcrow);

    if (RSValue_IsArray(v)) {
        // Coordinator path: v is the shard's output array-of-tuples
        uint32_t len = RSValue_ArrayLen(v);
        for (uint32_t i = 0; i < len; i++) {
            RSValue *tuple = RSValue_ArrayItem(v, i);

            // Unwrap tuple
            RSValue *payload = RSValue_ArrayItem(tuple, 0);
            RSValue *sortvals[MAX_SORT_KEYS];
            for (size_t k = 0; k < reducer->nsortkeys; k++) {
                sortvals[k] = RSValue_ArrayItem(tuple, 1 + k);
            }

            // Dedup check → heap insert (same as shard pipeline)
            heapPipeline_insert(inst, payload, sortvals, /*docId=*/0);
        }
    } else {
        // Shard path: v is a scalar from the pipeline row
        // ... capture payload + sort keys from srcrow (section 3.1)
    }
}
```

### 5.3 Coordinator Finalize — Bare Values Only

The coordinator `Finalize()` extracts from the heap and outputs **bare payloads** — no tuples, no sort keys. The user-facing response is the same format as today's TOLIST.

```
Coordinator Finalize output for group "scifi":

  RSValue_Array [
    "Inception",          ← bare value (sort keys stripped)
    "The Matrix",
    "Alien"
  ]
```

---

## 6. Component 4: Arg Parsing Extension

### 6.1 Grammar

```
REDUCE TOLIST <narg> <@field | *> [ALLOWDUPS]
                     [SORTBY <inner_narg> (<@field> [ASC|DESC])+]
                     [LIMIT <offset> <count>]
              AS <alias>
              ▲                                        ▲
              └──── narg tokens counted here ───────────┘
              (AS and alias are OUTSIDE the narg window)
```

**Example token counts:**

```
REDUCE TOLIST 1  @title                                          → narg=1
REDUCE TOLIST 4  @title SORTBY 2 @rating DESC                   → narg=4  (title + SORTBY + 2 + @rating DESC → wait, inner narg is 2)
REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 3 → narg=10
```

### 6.2 Parsing Logic

```
Parse @field or *
  │
  ├─ "@field" → resolve srckey via ReducerOpts_GetKey (existing path)
  │             set isStarPayload = false
  │
  └─ "*"     → set isStarPayload = true
               validate LOAD * precedes GROUPBY (fail fast if not)
  │
  ▼
Check for ALLOWDUPS (case-insensitive AC_AdvanceIfMatch)
  │
  ▼
Check for SORTBY
  ├─ Read inner_narg
  ├─ Parse pairs: @field [ASC|DESC]
  │   → resolve each via RLookup_GetKey_Read
  │   → set bits in sortAscMap
  │   → store in sortkeys[] (max 8)
  │
  ▼
Check for LIMIT
  ├─ Read offset (non-negative integer)
  └─ Read count (non-negative integer)
  │
  ▼
Done (remaining tokens → error)
```

**Backward compatibility:** When `narg == 1`, the parser reads exactly one field name and falls into the existing code path. No SORTBY, no LIMIT, no ALLOWDUPS, no `*`.

### 6.3 Where It Lives

`RDCRToList_New` in `src/aggregate/reducers/to_list.c` — the factory function. Today it calls `ReducerOptions_GetKey` and wires up the callbacks. The enhanced version parses the full grammar and populates the `ToListReducer` extended struct.

---

## 7. Component 5: Distribution Function

### 7.1 Before vs After — Distribution Registry

```diff
  // src/coord/dist_plan.cpp
  static struct {
    const char *key;
    reducerDistributionFunc func;
  } reducerDistributors_g[] = {
      {"COUNT",              distributeCount},
      {"SUM",                distributeSingleArgSelf},
      {"MAX",                distributeSingleArgSelf},
      {"MIN",                distributeSingleArgSelf},
-     {"TOLIST",             distributeSingleArgSelf},
+     {"TOLIST",             distributeToList},
      // ...
  };
```

### 7.2 Before vs After — What Gets Sent to Shards

**BEFORE** (`distributeSingleArgSelf` — hardcodes 1 arg):

```
User writes:
  REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 3 AS top_docs

Shard receives:
  REDUCE TOLIST 1 field AS __generated_aliastolistfield
                 ▲
                 └─ only 1 arg forwarded (the field name)
                    SORTBY, LIMIT, ALLOWDUPS are LOST
```

**AFTER** (`distributeToList` — forwards full arg set):

```
User writes:
  REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 3 AS top_docs

Shard receives:
  REDUCE TOLIST 10 * SORTBY 4 target DESC bestByDate ASC LIMIT 0 3
                     AS __generated_aliastolist...
                 ▲
                 └─ full arg set forwarded (@ stripped from field names per convention)

Coordinator plan:
  REDUCE TOLIST 10 * SORTBY 4 __generated_alias... target DESC bestByDate ASC LIMIT 0 3
                     AS top_docs
                 ▲
                 └─ reads from shard's auto-generated alias column
```

### 7.3 `distributeToList` Implementation (Conceptual)

```c
static int distributeToList(ReducerDistCtx *rdctx, QueryError *status) {
    PLN_Reducer *src = rdctx->srcReducer;

    // Forward ALL args to the shard (not just 1)
    const char *alias;
    if (!rdctx->addRemote(src->name, &alias, status,
                          /* all narg tokens from src->args */)) {
        return REDISMODULE_ERR;
    }

    // Coordinator gets the same reducer, reading from the shard alias
    if (!rdctx->addLocal(src->name, status,
                         /* same args, but with shard alias as the field */)) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
```

The key difference from `distributeSingleArgSelf`: instead of `CHECK_ARG_COUNT(1)` and forwarding only the field name, it forwards the entire `ArgsCursor` contents.

---

## 8. Component 6: TOLIST * Payload Construction

### 8.1 Two Capture Modes

| Mode | `TOLIST @field` | `TOLIST *` (JSON) | `TOLIST *` (Hash) |
|------|----------------|-------------------|-------------------|
| **Source** | `RLookupRow_Get(srckey, srcrow)` | `RLookupRow_Get($key, srcrow)` | Iterate all visible `RLookup` keys |
| **Capture** | `IncrRef` one RSValue | `IncrRef` one RSValue_Map | `IncrRef` N individual field values |
| **HeapEntry payload** | `RSValue *` (scalar) | `RSValue *` (map) | `RSValue *[]` (field values) |
| **Finalize map build** | N/A | Already a map | Deferred — build map only for K survivors |

### 8.2 JSON Document Capture

JSON documents loaded via `LOAD *` produce one `RSValue_Map` under the `"$"` key:

```
RLookupRow.dyn[]:
  [0]: @genre = "scifi"
  [1]: @rating = "8.8"
  [2]: "$" = RSValue_Map { title: "Inception", genre: "scifi", rating: 8.8, ... }
              ▲
              └─ one IncrRef captures the entire document
```

### 8.3 Hash Document Capture

Hash documents loaded via `LOAD *` produce separate entries per field:

```
RLookupRow.dyn[]:
  [0]: @fruit = "banana"
  [1]: @color = "yellow"
  [2]: @sweetness = "5"
  [3]: @origin = "ecuador"

→ During Add(): IncrRef each field value individually
→ During Finalize(): build RSValue_Map for K survivors only

  This avoids building maps for all N documents (which could be thousands)
  when only K=5 survive the heap.
```

### 8.4 The Reducer Holds `RLookup *`

For `TOLIST *`, the reducer stores `options->srclookup` at construction time. During `Add()`, it iterates the `RLookup`'s key list to discover which fields are available. This is essential for Hash documents and useful for JSON to locate the `"$"` key.

---

## 9. Component 7: Doc ID Injection for Tie-Breaking

### 9.1 The Problem

The reducer's `Add()` receives only `const RLookupRow *srcrow`, not the `SearchResult`. The doc ID is in `SearchResult._doc_id`, inaccessible to the reducer.

### 9.2 Solution: Inject Into Row Before Reducer Loop

The Grouper injects the doc ID as a hidden `RSValue_Number` into the `RLookupRow` under a reserved key before calling reducers:

```
Grouper_rpAccum loop:
  while (upstream->Next(res) == RS_RESULT_OK) {
+     // Inject doc ID as hidden field
+     RSValue *docIdVal = RSValue_NewNumber(SearchResult_GetDocId(res));
+     RLookupRow_Set(&res->rowdata, docid_key, docIdVal);

      invokeGroupReducers(group, &res->rowdata);
      SearchResult_Clear(res);  // wipes row + DecrRef's the injected value
  }
```

The TOLIST reducer reads it like any other field via `RLookupRow_Get(docid_key, srcrow)`. Other reducers ignore it. Zero interface changes.

On the coordinator, doc IDs are not available from shards. Tie-breaking is arbitrary (first-seen). This is documented and acceptable — `FIRST_VALUE` already has the same behavior.

---

## 10. Files Changed

| File | What Changes | Effort |
|------|-------------|--------|
| `src/aggregate/reducers/to_list.c` | **Major rewrite.** Extended struct, new `Add()` with heap pipeline, new `Finalize()` with tuple output, arg parsing, dedup+heap interaction, both shard and coordinator paths. | High |
| `src/coord/dist_plan.cpp` | New `distributeToList` function. Registry entry change. | Medium |
| `src/result_processor.c` (or Grouper code) | Doc ID injection before reducer loop. | Low |
| `src/aggregate/reducer.h` | (Possibly) extended struct definition for `ToListReducer`, or keep it local to `to_list.c`. | Low |
| `src/aggregate/aggregate_request.c` | Arg parsing integration — the narg window must pass through to the factory. | Low-Medium |

---

## Appendix A: End-to-End Data Flow (Cluster)

```
USER QUERY:
  FT.AGGREGATE idx * LOAD * GROUPBY 1 @genre
    REDUCE TOLIST 8 @title SORTBY 2 @rating DESC LIMIT 0 3 AS top_titles

                                    │
                     ┌──────────────┼──────────────┐
                     │              │              │
                Shard 1         Shard 2         Shard 3
                     │              │              │

      ┌──────────────────────┐  (same on each shard)
      │ Pipeline per shard:  │
      │  LOAD * → GROUPBY    │
      │  → TOLIST Add()      │
      │    per doc in group  │
      │                      │
      │ TOLIST Finalize():   │
      │  heap → sort → slice │
      │  → array of tuples   │
      └──────────┬───────────┘
                 │
    Shard output (RESP) for group "scifi":
    [
      ["Inception", "8.8"],     ← [payload, sortval_0]
      ["The Matrix", "8.7"],
      ["Interstellar", "8.6"]
    ]
                 │
    ─────────── network ───────────
                 │
      ┌──────────▼───────────┐
      │ COORDINATOR          │
      │                      │
      │ Grouper groups shard │
      │ rows by @genre       │
      │                      │
      │ TOLIST Add():        │
      │  unwrap tuples       │
      │  → dedup check       │
      │  → heap insert       │
      │  (across all shards) │
      │                      │
      │ TOLIST Finalize():   │
      │  heap → sort → slice │
      │  → bare values only  │
      └──────────┬───────────┘
                 │
    User response for group "scifi":
    top_titles: ["Inception", "The Matrix", "Interstellar"]
                (sort keys stripped — user never sees them)
```

---

## Appendix B: Extended Reducer Struct

```c
typedef struct {
    Reducer base;                          // must be first (allows cast)

    // Sort configuration
    const RLookupKey *sortkeys[SORTASCMAP_MAXFIELDS];  // up to 8
    size_t nsortkeys;
    uint64_t sortAscMap;                   // bitmap: bit i = ASC for key i

    // Limit configuration
    uint64_t offset;
    uint64_t count;

    // Payload configuration
    bool isStarPayload;                    // true for TOLIST *
    bool allowDups;                        // true for ALLOWDUPS

    // For TOLIST * — needed to iterate fields during Add()
    const RLookup *srcLookup;

    // For doc ID tie-breaking
    const RLookupKey *docIdKey;            // injected by Grouper
} ToListReducer;
```

Follows the `FVReducer` pattern from `first_value.c`: the `Reducer base` is the first field, allowing safe casting between `Reducer *` and `ToListReducer *`.

---

## Appendix C: Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Wire format | Uniform array-of-tuples | One code path, zero legacy branching |
| Tuple element order | `[payload, sortval_0, ..., sortval_N-1]` | Payload always at index 0 |
| Dedup timing | Before heap insertion | Consistent with current TOLIST |
| Tie-breaking (shard) | Doc ID | Consistent with RPSorter |
| Tie-breaking (coordinator) | Arbitrary (first-seen) | Doc IDs not available; matches FIRST_VALUE |
| Doc ID availability | Inject into RLookupRow | No interface changes, minimal blast radius |
| Map dedup for `TOLIST *` | Deferred (ALLOWDUPS implicit for `*`) | High cost, low real-world demand |
| Heap type | `mm_heap_t` (existing) | Proven infrastructure, reuse |
| Max sort keys | 8 (`SORTASCMAP_MAXFIELDS`) | Consistent with RPSorter |
| Hash doc map assembly | Deferred to Finalize() | Build maps only for K survivors, not all N docs |
