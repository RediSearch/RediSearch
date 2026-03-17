# Design Principles: In-Group Sorter

> Conclusions from our analysis of the sorter technicality.

---

## 1. It's a New Sorter

Not RPSorter (pipeline-level, operates on `SearchResult *`). Not the coordinator heap (FT.SEARCH-specific, operates on parsed replies). This sorter lives **inside** the reducer's per-group state and operates on captured `HeapEntry` structs.

## 2. RLookupKey Pointers Are Persistent, Row Data Is Ephemeral

The reducer holds `RLookupKey *` pointers from parse time — they live in the `RLookup` struct and remain valid for the reducer's entire lifetime. The row data (`RLookupRow.dyn[]`) is wiped after each `Add()` call. Values must be `IncrRef`'d during `Add()` to survive the wipe.

## 3. For `TOLIST *`, the Reducer Holds the `RLookup *`

Stored from `options->srclookup` at construction. Needed during `Add()` to iterate all visible keys and capture field values — especially for Hash documents where each field is a separate entry.

## 4. Capture During `Add()`: IncrRef, Don't Copy

- **Sort keys:** `IncrRef(RLookupRow_Get(sortkey[i], srcrow))` → store in `sortvals[]` by position.
- **JSON payload:** `IncrRef` the single `RSValue_Map` at `"$"`. One operation.
- **Hash payload:** `IncrRef` each individual field value. Defer map construction to `Finalize()` — only build maps for the K survivors.
- **Single field:** `IncrRef(RLookupRow_Get(srckey, srcrow))`. Same as today's TOLIST, just with sorting on top.

## 5. Heap Entry Is a Flat Struct

```
HeapEntry {
    payload       — RSValue * (JSON/single-field) or RSValue *[] (Hash fields)
    sortvals[N]   — RSValue *, indexed by position (0..nsortkeys-1)
}
```

Comparator indexes `entry->sortvals[i]` directly. No `RLookupRow_Get` at compare time. Simpler and faster than RPSorter's `cmpByFields`.

## 6. Comparator: Same Algorithm, Different Data Source

- `RSValue_Cmp` per sort key.
- ASC/DESC via `sortAscMap` bitmap.
- Nulls lose (sorted last).
- Max 8 sort fields (`SORTASCMAP_MAXFIELDS`).

## 7. Bounded Top-K via `mm_heap_t`

Reuse existing heap infrastructure. Bounded to K = offset + count. On `Finalize()`: extract, sort in-place, skip offset, return count entries.

## 8. Dedup Happens Before Sort Insertion

Check the dedup dict **before** inserting into the heap. A duplicate is rejected regardless of its sort rank.

## 9. Hash Map Construction Is Deferred to `Finalize()`

During `Add()`, Hash fields are captured individually (cheap IncrRef per field). Only the K survivors get assembled into `RSValue_Map`s for the output array. If K = 5 and 1000 docs pass through, 5 maps are built, not 1000.
