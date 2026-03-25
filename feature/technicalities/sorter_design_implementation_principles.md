# Design Principles: In-Group Sorter

> Conclusions from our analysis of the sorter technicality.

---

## 1. It's a New Sorter — but FIRST_VALUE Is the Closest Precedent

Not RPSorter (pipeline-level, operates on `SearchResult *`). Not the coordinator heap (FT.SEARCH-specific, operates on parsed replies). This sorter lives **inside** the reducer's per-group state and operates on captured `HeapEntry` structs.

However, the FIRST_VALUE reducer (`src/aggregate/reducers/first_value.c`) already solves a miniature version of the same problem: *from the rows within a group, select a value based on a sort criterion on a different field.* FIRST_VALUE is structurally a **bounded heap of capacity K=1**. The in-group sorter generalizes it to K = offset + count.

The sorter is a synthesis of three existing pieces:
- **From FIRST_VALUE:** the data access pattern (`RLookupRow_Get` + `IncrRef` capture), the separate return-field / sort-field concept, null handling, `RSValue_Replace` lifecycle, and the two-level struct architecture.
- **From RPSorter:** the multi-key comparison algorithm (`RSValue_Cmp` + ASC/DESC bitmap) and the `mm_heap_t` bounded heap infrastructure.
- **From current TOLIST:** the `RSValueSet` dict type for optional dedup.

## 2. RLookupKey Pointers Are Persistent, Row Data Is Ephemeral

The reducer holds `RLookupKey *` pointers from parse time — they live in the `RLookup` struct and remain valid for the reducer's entire lifetime. The row data (`RLookupRow.dyn[]`) is wiped after each `Add()` call. Values must be `IncrRef`'d during `Add()` to survive the wipe.

## 3. For `TOLIST *`, the Reducer Holds the `RLookup *`

Stored from `options->srclookup` at construction. Needed during `Add()` to iterate all visible keys and capture field values — especially for Hash documents where each field is a separate entry.

## 4. Capture During `Add()`: IncrRef, Don't Copy

This is the same pattern FIRST_VALUE uses in `fvAdd_sort`: read the field via `RLookupRow_Get`, then `IncrRef` to retain it past the row wipe. The sorter generalizes the single-field capture to multiple sort keys and a payload:

- **Sort keys:** `IncrRef(RLookupRow_Get(sortkey[i], srcrow))` → store in `sortvals[]` by position. FIRST_VALUE does exactly this for its single `sortprop`.
- **JSON payload:** `IncrRef` the single `RSValue_Map` at `"$"`. One operation. Analogous to FIRST_VALUE's `IncrRef` of `retprop`.
- **Hash payload:** `IncrRef` each individual field value. Defer map construction to `Finalize()` — only build maps for the K survivors.
- **Single field:** `IncrRef(RLookupRow_Get(srckey, srcrow))`. Same as today's TOLIST and FIRST_VALUE, just with sorting on top.

## 5. Heap Entry Is a Flat Struct

```
HeapEntry {
    payload       — RSValue * (JSON/single-field) or RSValue *[] (Hash fields)
    sortvals[N]   — RSValue *, indexed by position (0..nsortkeys-1)
}
```

Comparator indexes `entry->sortvals[i]` directly. No `RLookupRow_Get` at compare time. Simpler and faster than RPSorter's `cmpByFields`.

This is the generalization of FIRST_VALUE's `fvCtx`, which holds one `value` (payload) and one `sortval`. The sorter scales it to N sort keys via a positional array, and replaces the single-slot with a heap of K entries.

## 6. Comparator: Same Algorithm, Different Data Source

- `RSValue_Cmp` per sort key — same call used by both RPSorter and FIRST_VALUE.
- ASC/DESC via `sortAscMap` bitmap — generalizes FIRST_VALUE's single `int ascending` flag.
- Nulls lose (sorted last) — same policy as both FIRST_VALUE (`RSValue_IsNull` guard) and RPSorter (`cmpByFields` null check).
- Max 8 sort fields (`SORTASCMAP_MAXFIELDS`).

## 7. Bounded Top-K via `mm_heap_t` — Per-Instance Only

Reuse existing heap infrastructure. Bounded to K = offset + count. On `Finalize()`: extract, sort in-place, skip offset, return count entries.

**The heap bounds memory within one reducer instance, NOT the input stream.** This is a critical invariant — see principle 8.

## 8. No Early Capping: Every Reducer Sees Every Row

Multiple reducers (including multiple enhanced TOLIST instances with different SORTBY/LIMIT) coexist in the same GROUPBY. The Grouper feeds *every* row to *every* reducer for that group. No individual reducer may prevent sibling reducers from seeing rows.

This means:
- A TOLIST with `LIMIT 0 3` evicts entries from its own heap but does not skip the `Add()` call or signal the Grouper to stop feeding rows.
- Scalar reducers (COUNT, MAX, SUM, etc.) require the full input stream to produce correct results.
- As more reducers gain in-group sorting semantics, each instance independently accumulates from the full stream.

The bounded heap's eviction inside `Add()` is strictly local. The row continues to the next reducer regardless of whether the current reducer kept or discarded it.

**Verified empirically:** see [multiple-reducers-coexistence findings](../findings/multiple-reducers-coexistence-with-tolist.md).

## 9. Dedup Happens Before Sort Insertion

Check the dedup dict **before** inserting into the heap. A duplicate is rejected regardless of its sort rank.

## 10. Eviction Lifecycle: DecrRef on Replace, Inspired by FIRST_VALUE

When a heap entry is evicted (a better entry replaces the worst), all its captured `RSValue *` pointers must be `DecrRef`'d — both the payload and each `sortvals[i]`. On teardown (`FreeInstance`), every remaining entry in the heap gets the same treatment.

This extends FIRST_VALUE's proven pattern: `fvFreeInstance` decrefs `value` and `sortval`; `fvAdd_sort` uses `RSValue_Replace` (which is DecrRef old + IncrRef new) when a better row arrives. The sorter applies the same lifecycle to every field in a `HeapEntry`, for every eviction and cleanup event.

## 11. Two-Level Struct: Shared Config + Per-Group State

Following FIRST_VALUE's `FVReducer` + `fvCtx` pattern:

- **Reducer-level struct** (one instance, shared across all groups): holds parsed configuration — `RLookupKey *sortkeys[]`, `ascmap`, `nsortkeys`, `offset`, `count`, optional `RLookup *` for wildcard payload. Allocated at construction time.
- **Per-group instance struct** (one per unique group key): holds accumulated state — `mm_heap_t *` heap, optional `dict *` dedup dictionary. Allocated by `NewInstance` via `BlkAlloc`.

This separation keeps the per-group instance small (just the heap + optional dict pointer), while the larger config (sort keys, flags, lookup pointers) lives once in the reducer and is shared. Same economy as FIRST_VALUE, where `FVReducer` holds `sortprop` and `ascending` once, while each `fvCtx` copies only the pointers it needs for `Add()`.

## 12. Hash Map Construction Is Deferred to `Finalize()`

During `Add()`, Hash fields are captured individually (cheap IncrRef per field). Only the K survivors get assembled into `RSValue_Map`s for the output array. If K = 5 and 1000 docs pass through, 5 maps are built, not 1000.
