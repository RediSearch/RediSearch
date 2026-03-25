# Deferred Document Loading for COLLECT

> **Status:** Optimization Proposal (not required for V1 correctness)
>
> **Authors:** Itzik Vaknin
>
> **Date:** 2026-03-23
>
> **References:** [COLLECT Design](./collect-reducer-design.md)

---

## Table of Contents

1. [Problem](#1-problem)
2. [Key Insight](#2-key-insight)
3. [Scope — `*` Field Only](#3-scope--field-only)
4. [How It Works — No Loader Changes Needed](#4-how-it-works--no-loader-changes-needed)
5. [Level 1 — Deferred Loading in Finalize](#5-level-1--deferred-loading-in-finalize)
6. [Level 2 — Grouper-Level Dedup Hashmap](#6-level-2--grouper-level-dedup-hashmap)
7. [Reuse of Existing Infrastructure](#7-reuse-of-existing-infrastructure)
8. [GIL Considerations](#8-gil-considerations)
9. [Decision Log](#9-decision-log)

---

## 1. Problem

Consider:

```
FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 12 FIELDS 3 @__key @__score * SORTBY 2 @sweetness DESC LIMIT 0 5 AS top_fruits
```

With 3 groups and 10,000 matching documents, the current pipeline:

1. **`LOAD *`** runs an `RPLoader` that performs HGETALL on **all 10,000 documents**.
2. **GROUPBY** accumulates them into 3 groups.
3. **COLLECT** keeps only the top 5 per group → **15 survivors**.

9,985 full document loads are wasted. The `*` field (full payload) is only needed for the 15 entries that survive the heap.

---

## 2. Key Insight

COLLECT is unique among reducers: it retains **per-document identity**. Each heap entry already stores `docId` (for tie-breaking). This means we know *which* documents survived — and can defer loading them until after the heap has pruned the rest.

Other reducers (COUNT, SUM, AVG, etc.) collapse documents into scalars — there is no document to "go back to." COLLECT is the only reducer where deferred loading is viable.

---

## 3. Scope — `*` Field Only

This optimization is scoped exclusively to the `*` field (full document payload). Regular projected fields and sort fields are unaffected:

| Field type | Loading mechanism | Affected? |
|------------|------------------|-----------|
| `*` (full payload) | `LOAD *` → HGETALL per doc | **Yes — deferred to Finalize** |
| `@title`, `@rating` (regular) | Implicit per-field loader or sorting vector | No change |
| `@__key`, `@__score` (built-in) | Available from iterator, no Redis access | No change |
| Sort/group keys | Sorting vector (if sortable) or implicit per-field loader | No change |

This scoping is clean because `*` is the **only** field that triggers HGETALL — the expensive operation. Regular per-field loads (`HGET @field`) are already efficient through the implicit loading mechanism in `ReducerOpts_GetKey` / `buildGroupRP`.

---

## 4. How It Works — No Loader Changes Needed

The existing implicit loading mechanism (`ReducerOpts_GetKey` → `loadKeys` → `RPLoader`) already operates per-field. It only loads what reducers explicitly request. The `LOAD *` / HGETALL behavior only kicks in when the **user** writes `LOAD *` in the query.

The optimization: **COLLECT does not register `*` with the loader.** It handles `*` as a special internal concern:

```
COLLECT's construction:
  ├── @sweetness (sort key)  → ReducerOpts_GetKey → loadKeys → implicit RPLoader
  ├── @__key (projected)     → built-in, no load needed
  ├── @__score (projected)   → built-in, no load needed
  └── * (projected)          → NOT registered with loader.
                                Set hasStarField = true.
                                Handle in Finalize() via deferred load.
```

This means:
- No changes to `RPLoader`, `RLookup`, or the pipeline construction logic.
- No need to "tell the loader to skip" anything — `*` is simply never requested through it.
- The COLLECT design's requirement "`*` in FIELDS requires `LOAD *` to precede the GROUPBY" is **removed**. COLLECT owns `*` loading internally.

### Opportunistic Capture

If the user writes `LOAD *` explicitly (for other pipeline steps — APPLY, FILTER, other reducers), the full payload is already in the row. COLLECT's `Add()` detects this and captures it directly — no deferral needed:

```c
// In COLLECT Add():
if (cr->hasStarField) {
    RSValue *starPayload = buildStarFromRow(srcrow, cr->srcLookup);
    if (starPayload) {
        // LOAD * was done upstream — data is already available
        entry->projected[cr->starFieldIndex] = starPayload;
    } else {
        // No LOAD * — defer to Finalize
        entry->projected[cr->starFieldIndex] = NULL;
        // docId is always stored (for tie-breaking), so no extra work
    }
}
```

Both paths produce correct results. The optimization kicks in automatically when `LOAD *` is absent.

---

## 5. Level 1 — Deferred Loading in Finalize

### Strategy

When `*` was not loaded upstream, COLLECT's `Finalize()` loads the full document for each surviving heap entry using the stored `docId`.

### Flow

```
                 Without optimization                    With optimization
                 ────────────────────                    ─────────────────
  LOAD * (HGETALL × 10,000)              [no LOAD *]
          │                               implicit loader: @sweetness only
      GROUPBY                                        │
          │                                      GROUPBY
    COLLECT Add()                                    │
    (capture * from row)                       COLLECT Add()
          │                                (store sort keys + docId, skip *)
    COLLECT Finalize()                               │
    (pop heap, build output)                   COLLECT Finalize()
                                          (pop heap, load 5 docs from Redis,
                                           build output)
```

### Finalize Pseudocode

```c
static RSValue *collectFinalize(Reducer *rbase, void *ctx) {
    CollectGroupInstance *inst = ctx;
    CollectReducer *cr = (CollectReducer *)rbase;

    // Pop entries from heap (sorted)
    while (heap has entries) {
        CollectHeapEntry *entry = pop();

        // Deferred * field — load now
        if (cr->hasStarField && entry->projected[cr->starFieldIndex] == NULL) {
            RSDocumentMetadata *dmd = DocTable_GetById(docTable, entry->docId);
            RLookupRow tmpRow = {0};
            RLookupLoadOptions opts = {
                .sctx = sctx,
                .dmd = dmd,
                .forceString = 1,
            };
            RLookup_LoadDocumentAll(cr->srcLookup, &tmpRow, &opts);
            entry->projected[cr->starFieldIndex] = buildStarPayload(&tmpRow);
            RLookupRow_Reset(&tmpRow);
        }

        // Build KV entry as normal
        ...
    }
}
```

### Impact

| Metric | Before | Level 1 |
|--------|--------|---------|
| HGETALL calls | 10,000 | 15 |
| Memory during grouping | 10,000 × full payload | 10,000 × (group key + sort key) |
| Finalize complexity | Pure (no I/O) | Loads surviving docs |

### Design Requirement

The `CollectHeapEntry` must store `docId` (already in the COLLECT design for tie-breaking). During `Add()`, if `*` is deferred, `projected[starFieldIndex]` is left NULL and populated in `Finalize()`.

---

## 6. Level 2 — Grouper-Level Dedup Hashmap

### When It Matters

Level 1 handles the common case (one COLLECT reducer). Level 2 addresses **multiple COLLECT reducers with `*`** in the same GROUPBY, where the same document can appear in multiple heaps.

Example — 3 COLLECTs on the same GROUPBY, all projecting `*`:

```
GROUPBY 1 @color
  REDUCE COLLECT ... FIELDS 1 * SORTBY @rating DESC  LIMIT 0 5 AS top_rated
  REDUCE COLLECT ... FIELDS 1 * SORTBY @price ASC    LIMIT 0 3 AS cheapest
  REDUCE COLLECT ... FIELDS 1 * SORTBY @date DESC    LIMIT 0 2 AS newest
```

`doc_42` could survive in all three heaps for the same group. With multi-value group keys (`@color = ["red", "yellow"]`), it could appear across groups too:

```
                top_rated    cheapest    newest
group "red"      doc_42 ─┐    doc_42 ─┐    doc_42 ─┐
group "yellow"   doc_42 ─┤    doc_7  ─┤    doc_42 ─┤
group "green"    doc_7  ─┘    doc_42 ─┘    doc_15 ─┘

Level 1:  doc_42 loaded up to 7 times
Level 2:  doc_42 loaded once
```

### Strategy — Three-Phase Grouper

Extend the grouper's state machine from two phases to three:

```
Current:    Accum ──────────────► Yield
Proposed:   Accum ──► Load ──────► Yield
```

Each phase is a `Next` function pointer, swapped at the transition:

| Phase | Function | Runs | Purpose |
|-------|----------|------|---------|
| Accum | `Grouper_rpAccum` | Once (drains upstream) | Call `Add()` on all reducers for all rows |
| Load | `Grouper_rpLoad` | Once | Collect surviving doc_ids, dedup, batch load, distribute |
| Yield | `Grouper_rpYield` | Once per group | Call `Finalize()`, yield one group per `Next()` |

### Load Phase Flow

```
Grouper_rpLoad():
  1. Collect doc_ids:
     for each group:
       for each COLLECT reducer with hasStarField and deferred entries:
         for each heap entry where projected[starFieldIndex] is NULL:
           docIdSet.add(entry.docId)

  2. Batch load (single GIL acquisition):
     loadedDocs = hashmap<t_docId, RSValue*>
     for each docId in docIdSet:
       dmd = DocTable_GetById(docId)
       load document → build payload RSValue
       loadedDocs[docId] = payload

  3. Distribute into heap entries:
     for each group:
       for each COLLECT reducer with deferred entries:
         for each heap entry where projected[starFieldIndex] is NULL:
           entry.projected[starFieldIndex] = IncrRef(loadedDocs[entry.docId])

  4. Swap to Grouper_rpYield
```

### Grouper Extension

```c
typedef struct Grouper {
    ResultProcessor base;
    khash_t(khid) *groups;
    BlkAlloc groupsAlloc;
    const RLookupKey **srckeys;
    const RLookupKey **dstkeys;
    size_t nkeys;
    Reducer **reducers;
    khiter_t iter;

    // Level 2: deferred loading
    bool hasDeferredLoad;          // set when any COLLECT reducer has hasStarField
    dict *loadedDocs;              // docId → RSValue* (populated in Load phase)
} Grouper;
```

### Backward Compatibility

- `hasDeferredLoad` is only set when a COLLECT reducer with `*` is registered.
- Without `*` fields, the path is `Accum → Yield` — unchanged, zero overhead.

### Reducer Interface Extension

```c
// New optional methods on Reducer:
typedef struct {
    // ... existing fields ...

    // Return doc_ids of surviving entries (NULL if no deferred fields)
    const t_docId *(*GetDeferredDocIds)(Reducer *r, void *instance, size_t *count);

    // Populate deferred * field from the loaded docs hashmap
    void (*ResolveDeferredFields)(Reducer *r, void *instance, dict *loadedDocs);
} Reducer;
```

COLLECT implements both. All other reducers leave them NULL (grouper skips them).

---

## 7. Reuse of Existing Infrastructure

The loading functions in `rlookup_load_document.c` are **already decoupled** from `RPLoader`:

```c
int RLookup_LoadDocumentAll(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);
int RLookup_LoadDocumentIndividual(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);
```

These are standalone functions that take an `RLookup`, `RLookupRow`, and `RLookupLoadOptions`. Both Level 1 and Level 2 call them directly — no refactoring of `RPLoader` needed.

---

## 8. GIL Considerations

If the pipeline runs in a background thread, Redis keyspace access requires the GIL.

| Level | GIL Acquisitions | Notes |
|-------|-----------------|-------|
| Level 1 | Once per group (in Finalize) | K loads per acquisition. Acceptable for small K. |
| Level 2 | Once total (in Load phase) | All surviving docs loaded in one batch. Optimal. |

The `RPSafeLoader` pattern (buffer results, lock, load batch, unlock) is a reference for how to safely batch keyspace access from a background thread.

---

## 9. Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Optimization scope | `*` field only | Only `*` triggers HGETALL; regular fields use efficient per-field loading |
| `LOAD *` requirement | Removed for COLLECT `*` | COLLECT owns `*` loading internally; no upstream loader needed |
| Loader changes | None | COLLECT simply doesn't register `*` with the implicit loader; existing mechanism handles regular fields |
| V1 approach | Level 1 (Finalize-only) | Captures 99%+ of the savings with minimal complexity |
| Store docId in heap entry | Yes, from day one | Enables both levels without heap redesign |
| Reuse existing loader functions | Yes, call directly | Already decoupled from RPLoader; no refactoring needed |
| Level 2 timing | Deferred to V2 | Multi-COLLECT dedup is marginal vs. the primary N→K reduction |
| Grouper three-phase model | Level 2 only | Avoids grouper changes for V1 |
