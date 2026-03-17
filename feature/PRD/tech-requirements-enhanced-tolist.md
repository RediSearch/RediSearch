# Technical Requirements: Enhanced TOLIST Reducer

> These are the **closed** technical requirements — things that must be true regardless of outstanding PM decisions.
> Derived from codebase analysis of the current TOLIST implementation, aggregation pipeline, coordinator distribution, and RSValue infrastructure.

---

## 1. Current Architecture (Baseline)

### TOLIST Reducer (`src/aggregate/reducers/to_list.c`)

The current TOLIST reducer:
- Accepts exactly **1 argument**: a field name (e.g., `@field`).
- Maintains a `dict` (hash map) per group for **dedup** using `RSValue_Hash` (FNV-64) and `RSValue_Equal`.
- On `Add()`: inserts the field's RSValue into the dict. For array values, flattens and inserts each element.
- On `Finalize()`: iterates the dict and builds an RSValue array. **Order is non-deterministic** (dict iteration order).
- No sorting. No limiting. No `*` (whole-document) support.

### Reducer Interface (`src/aggregate/reducer.h`)

```c
struct Reducer {
  const RLookupKey *srckey;     // Source field
  RLookupKey *dstkey;           // Output field
  BlkAlloc alloc;               // Per-group allocator
  void *(*NewInstance)(Reducer *r);
  int  (*Add)(Reducer *parent, void *instance, const RLookupRow *srcrow);
  RSValue *(*Finalize)(Reducer *parent, void *instance);
  void (*FreeInstance)(Reducer *parent, void *instance);
  void (*Free)(Reducer *r);
};
```

The `Reducer` struct has a single `srckey`. The enhanced TOLIST needs:
- Multiple sort keys (with ASC/DESC directives)
- A payload key (`*` or `@field`)
- Configuration flags (ALLOWDUPS, LIMIT offset/count)

This means we need an **extended reducer struct** (like `FVReducer` in `first_value.c`, which extends `Reducer` with `sortprop` and `ascending`).

### FIRST_VALUE Precedent (`src/aggregate/reducers/first_value.c`)

`FIRST_VALUE` already implements:
- A **return property** (what to output) separate from a **sort property** (what to rank by)
- Single-field ASC/DESC sort via `RSValue_Cmp`
- `BY @field ASC|DESC` syntax within the reducer args
- Keeps only the single best value (not top-K)

This is the closest existing pattern to what we need.

### RPSorter Precedent (`src/result_processor.c`)

The global-pipeline `SORTBY` uses:
- `mm_heap_t` (min-max heap) bounded to `offset + count` entries
- Multi-field comparator `cmpByFields` using `RSValue_Cmp` per field with a `sortAscMap` bitmap
- Tie-breaker: `SearchResult_GetDocId` (doc ID comparison)
- Nulls: treated as missing → sorted last (item with null loses)
- Max 8 sort fields (`SORTASCMAP_MAXFIELDS`)

### Coordinator Distribution (`src/coord/dist_plan.cpp`)

TOLIST currently uses `distributeSingleArgSelf`, which:
- Requires exactly 1 arg (`CHECK_ARG_COUNT(1)`)
- Sends the same reducer to each shard
- Coordinator runs TOLIST again on shard results (concatenation + dedup)

---

## 2. Closed Technical Requirements

### TR-1: Extended Reducer Struct

**Rationale:** The current `Reducer` struct holds a single `srckey`. The enhanced TOLIST needs sort keys, ASC/DESC flags, limit/offset, and an ALLOWDUPS flag.

**Requirement:** Create an extended struct (e.g., `ToListReducer`) embedding `Reducer` as its first field, adding:

| Field | Type | Purpose |
|-------|------|---------|
| `sortkeys` | `const RLookupKey **` | Array of sort field lookup keys |
| `nsortkeys` | `size_t` | Number of sort fields |
| `sortAscMap` | `uint64_t` | Bitmap of ASC/DESC per sort field |
| `offset` | `uint64_t` | LIMIT offset |
| `count` | `uint64_t` | LIMIT count |
| `allowDups` | `bool` | Whether to disable dedup |
| `isStarPayload` | `bool` | Whether payload is `*` (all fields) |

This follows the `FVReducer` pattern from `first_value.c`.

### TR-2: Arg Parsing Extension

**Rationale:** The current parser reads exactly 1 field name. The new syntax packs multiple tokens into the `narg` window.

**Requirement:** Extend `RDCRToList_New` to parse the following grammar within the narg tokens:

```
TOLIST <narg> (<@field> | *) [ALLOWDUPS]
  [SORTBY <inner_narg> (<@field> [ASC|DESC])+ ]
  [LIMIT <offset> <count>]
```

Parser rules:
- First token: either `*` or a `@field` name.
- If `*`: set `isStarPayload = true`. No `srckey` resolution — payload will be all loaded fields.
- If `@field`: resolve via `ReducerOpts_GetKey` (existing behavior).
- `ALLOWDUPS`: optional flag, case-insensitive match via `AC_AdvanceIfMatch`.
- `SORTBY`: followed by inner narg, then pairs of `@field [ASC|DESC]`. Parse like `parseSortby` in `aggregate_request.c`.
- `LIMIT`: followed by offset and count (both non-negative integers).
- `AS`: outside the narg window (handled by the caller, not the reducer).
- **Backward compatibility:** `TOLIST 1 @field` must parse exactly as before (no SORTBY, no LIMIT, no ALLOWDUPS, not `*`).

**Validation errors (fail fast):**
- `TOLIST *` without a preceding `LOAD *` or `LOAD @f1 @f2 ...` (see TR-5).
- `SORTBY` fields not available in the pipeline (not loaded, not in schema).
- `LIMIT` with negative values.
- Unknown tokens within the narg window.

### TR-3: In-Group Multi-Field Comparator

**Rationale:** We need to rank documents within each group by multiple fields with ASC/DESC control. The RPSorter's `cmpByFields` does exactly this for rows.

**Requirement:** Implement an in-group comparator that:

1. Accepts an array of sort-key `RLookupKey *` pointers and a `sortAscMap` bitmap.
2. For each sort key, retrieves the RSValue from the accumulated row data using the sort key.
3. Compares using `RSValue_Cmp`.
4. Respects ASC/DESC per field via the `sortAscMap` bitmap.
5. **Null handling:** nulls sort last (consistent with RPSorter — missing value loses regardless of ASC/DESC).
6. **Tie-breaker on standalone/shard:** compare by doc ID. The doc ID must be available in the accumulated state (see TR-6).
7. **Tie-breaker on coordinator:** arbitrary/first-seen (no doc ID available on coordinator).
8. **Max sort fields:** 8 (consistent with `SORTASCMAP_MAXFIELDS`).

**Implementation note:** Unlike `cmpByFields` which compares `SearchResult` rows, this comparator operates on accumulated per-document state within the reducer. The data format depends on how documents are stored in the heap (see TR-4).

### TR-4: In-Group Bounded Top-K Collector

**Rationale:** When SORTBY and LIMIT are specified, we need to keep only the top `K = offset + count` documents per group during accumulation, rather than collecting all documents and sorting after.

**Requirement:** Per-group instance maintains a bounded collection:

**When SORTBY + LIMIT are present:**
- Use a **min-max heap** (`mm_heap_t`) bounded to K = offset + count.
- On `Add()`:
  - If heap size < K: insert the new entry.
  - If heap size == K: compare with the worst entry (heap min or max depending on sort direction). Replace if the new entry is better.
- On `Finalize()`:
  - Extract all K entries from the heap.
  - Sort them (in-place) using the comparator.
  - Slice the window: skip `offset`, return `count` entries.
  - Build and return an RSValue array.

**When SORTBY without LIMIT:**
- Collect all entries (no bound). Sort on Finalize.

**When LIMIT without SORTBY:**
- Collect up to `offset + count` entries (arbitrary selection). Slice on Finalize.

**When neither (legacy behavior):**
- Use current dict-based collection. No change.

**Memory bound:** `O(groups × K)` per group, where K = offset + count. Total memory = sum over all groups.

**Each heap entry must contain:**
- The payload RSValue(s) — either the single field value or the full-document map.
- The sort key RSValue(s) — needed for the comparator.
- The doc ID (on shard) — needed for tie-breaking.

### TR-5: `TOLIST *` — Full-Document Payload Construction

**Rationale:** When using `TOLIST *`, the reducer needs to capture all loaded fields from the current row, not just a single field.

**Requirement:**

1. `TOLIST *` requires a preceding `LOAD *` or explicit `LOAD @f1 @f2 ...` in the pipeline. If not present, fail at parse time with a clear error.
2. On `Add()`: build an `RSValue_Map` from all visible (non-hidden) fields in the current `RLookupRow`.
   - Iterate the `RLookup` keys with read access.
   - For each key, call `RLookupRow_Get` and include the key name + value in the map.
   - The map is a **snapshot** — values must be copied/ref-incremented since the source row is cleared after each `Add()`.
3. The resulting RSValue_Map is what goes into the heap/dict as the payload.
4. On `Finalize()`: the map values are what gets returned in the output array.

**RESP serialization:** The existing `RedisModule_Reply_RSValue` already handles `RSValueType_Map` → RESP Map (RESP3) or flat array (RESP2). No new serialization code needed.

### TR-6: Doc ID Availability for Tie-Breaking

**Rationale:** The in-group comparator uses doc ID as tie-breaker on standalone/shard. But the doc ID is not a regular field — it's document metadata.

**Requirement:**

1. On shard/standalone, the doc ID is available via `SearchResult_GetDocId` during the Grouper's accumulation phase. The reducer's `Add()` is called with the `srcrow`, but the doc ID is in the `SearchResult`, not the row.
2. **Solution:** Pass the doc ID to the reducer entry alongside the row data. Options:
   - (a) Store the doc ID in each heap entry as a `t_docId` value.
   - (b) Temporarily inject the doc ID into the `RLookupRow` as a hidden field before calling `Add()`.
3. On the coordinator, doc ID is not available (results from shards don't carry doc IDs). Tie-breaking is arbitrary (first-seen).

### TR-7: Dedup Behavior

**Rationale:** Current TOLIST uses a `dict` for dedup. ALLOWDUPS disables it.

**Requirement:**

**Default (no ALLOWDUPS):**
- Before adding an entry to the heap/collection, check the dict for duplicates.
- If duplicate found: skip the entry (don't add to heap).
- If no duplicate: add to both dict and heap.
- Dedup is based on **payload** (the collected value), not on sort keys.

**With ALLOWDUPS:**
- Skip the dict entirely. All entries go into the heap/collection.

**Interaction with SORTBY + LIMIT:**
- Dedup happens **before** insertion into the bounded heap.
- A duplicate is rejected even if it would have a different position in the sort order (because the payload is identical).

### TR-8: RSValue Map Equality and Hashing Gap

**Rationale:** For `TOLIST *` with default dedup (no ALLOWDUPS), we need to compare and hash full-document maps. The current infrastructure has gaps:

| Operation | Current behavior for Maps | Problem |
|-----------|--------------------------|---------|
| `RSValue_CmpNC` (C) | Returns 0 ("can't compare maps ATM") | All maps treated as equal — dedup would keep only 1 doc per group |
| `RSValue_Hash` (C + Rust) | Iterates entries in order | Hash depends on entry order — two identical maps with different entry order get different hashes |
| `RSValue_Equal` (C) | Delegates to `RSValue_CmpNC` → 0 → equal | All maps treated as equal |

**Note on PR #8622 ([RsValue: Comparison functionality](https://github.com/RediSearch/RediSearch/pull/8622)):**
This PR ports `RSValue_Cmp`, `RSValue_Equal`, and `RSValue_BoolTest` to Rust, but does NOT solve the map gap. The Rust `compare()` function explicitly returns `Err(CompareError::MapComparison)` for map-vs-map comparisons, which the FFI layer maps to `0` (Cmp) / `true` (Equal) — same effective behavior as the C code. `RSValue_Hash` is not modified.

However, PR #8622 provides a good foundation: extending the Rust `value::comparison` module with a `compare_maps` function is the natural next step.

**Requirement:** Implement proper map comparison and hashing:

1. **Map equality (`RSValue_Equal` for maps):** Deep comparison — two maps are equal iff they have the same set of key-value pairs (regardless of entry order). Compare by:
   - Same number of entries.
   - For each key in map1, find the same key in map2 and compare values recursively.
   - O(n²) for unordered maps, or O(n log n) if entries are sorted by key first.

2. **Map hashing (`RSValue_Hash` for maps):** Order-independent hashing. Options:
   - (a) XOR the hashes of individual (key, value) pairs (commutative).
   - (b) Sort entries by key hash before hashing (deterministic order).
   - Option (a) is simpler but has more collisions. Option (b) is more robust.

3. **Scope:** These changes to `RSValue_Equal` and `RSValue_Hash` affect the global value infrastructure. Must ensure no regressions in other uses (GROUPBY key hashing, other reducers, etc.).
   - Alternatively, implement TOLIST-specific comparison/hashing that doesn't modify the global functions.

### TR-9: Custom Coordinator Distribution Function

**Rationale:** The current `distributeSingleArgSelf` requires exactly 1 arg and sends/merges TOLIST as a simple concatenation + dedup. The enhanced TOLIST with SORTBY/LIMIT/ALLOWDUPS needs a different distribution strategy.

**Requirement:**

1. **New distribution function** in `dist_plan.cpp` for TOLIST (replace `distributeSingleArgSelf` in the registry for `"TOLIST"`).

2. **Shard behavior:**
   - Each shard runs the full enhanced TOLIST (with SORTBY, LIMIT, ALLOWDUPS, `*`).
   - Each shard emits at most K = offset + count candidates per group (already bounded by TR-4).
   - Shard results are pre-sorted by the in-group comparator.

3. **Coordinator behavior:**
   - Receives per-group lists from each shard (each list has up to K entries, pre-sorted).
   - **Merge:** For each group, merge the N shard lists (merge-sort, since each is pre-sorted).
   - **Dedup (if not ALLOWDUPS):** During merge, deduplicate across shards.
   - **Re-rank:** Apply the full sort comparator across merged results.
   - **Window:** Apply LIMIT offset/count on the merged, sorted, deduped result.

4. **Coordinator TOLIST reducer:** The coordinator's TOLIST instance must understand that its input is pre-sorted arrays from shards, not raw field values. This is different from the current behavior where the coordinator TOLIST just does dict-based concatenation.

5. **Arg forwarding:** The extended args (ALLOWDUPS, SORTBY, LIMIT) must be forwarded to shard commands. The distribution function must serialize them into the remote plan's REDUCE clause.

6. **Backward compatibility:** `TOLIST 1 @field` (no extensions) should continue to use `distributeSingleArgSelf` or equivalent simple logic.

### TR-10: Backward Compatibility

**Rationale:** Existing queries using `TOLIST 1 @field` must not change behavior.

**Requirement:**

1. `TOLIST 1 @field` → unchanged dict-based accumulation, dedup, unordered output.
2. `TOLIST 1 @field AS alias` → unchanged.
3. No change to the Reducer interface contract (`Add`, `Finalize`, `NewInstance`, `FreeInstance`, `Free`).
4. No change to the Grouper — it calls reducers through the same interface.
5. The extended TOLIST should use the same reducer registry entry (`"TOLIST"` → `RDCRToList_New`). The parser distinguishes old vs new behavior based on the arg content.

### TR-11: Sort Key Availability

**Rationale:** SORTBY fields within TOLIST must be available in the pipeline at the point where the reducer runs. The reducer's `Add()` fetches values from the `RLookupRow`.

**Requirement:**

1. Sort fields must be resolvable via `RLookup_GetKey_Read` or `RLookup_GetKey_Load` against the source lookup (the lookup before GROUPBY).
2. If a sort field is not in the pipeline, fail at parse time with a clear error (consistent with `ReducerOpts_GetKey` behavior).
3. Sort fields that are loaded but not part of the TOLIST payload (e.g., `TOLIST 5 @title SORTBY 2 @rating DESC` — `@rating` is not in the output) still need to be captured in the heap entry for comparison during accumulation. They can be discarded on Finalize.

---

## 3. Technical Risks and Considerations

### Risk: RSValue Map Comparison Performance

Deep map equality is O(n × m) where n and m are the number of entries. For large JSON documents with many fields, this could be expensive per-insert. Mitigation: lazy hash comparison first (fast reject on different hashes), then deep equality only on hash collisions.

### Risk: Memory with Large K and Many Groups

If K (offset + count) is large and there are many groups, memory usage could spike. Each heap entry holds copies of the payload + sort key RSValues. For `TOLIST *` with large documents, each entry could be substantial.

Mitigation: Consider a configurable cap on K (deferred to PM decision — Open Question #8).

### Risk: Coordinator Merge Complexity

Merging N sorted lists per group across S shards, with dedup, is O(G × S × K × log(S)) where G = number of groups. For moderate values this is fine, but with many groups and large K, coordinator CPU could be a bottleneck.

### Risk: Modifying Global RSValue Functions

Changing `RSValue_Equal` and `RSValue_Hash` for maps affects all code paths that use these functions. Thorough regression testing required.

Alternative: Implement TOLIST-specific comparison functions that handle maps correctly, leaving the global functions unchanged. This is safer but introduces code duplication.

---

## 4. Component Dependency Graph

```
TR-2 (Arg Parsing)
  ├── TR-1 (Extended Struct)  — needs fields to populate
  ├── TR-5 (TOLIST * payload) — needs isStarPayload flag
  └── TR-11 (Sort Key Availability) — needs sort key resolution

TR-3 (In-Group Comparator)
  ├── TR-1 (Extended Struct) — reads sort config
  └── TR-6 (Doc ID) — tie-breaker

TR-4 (Top-K Collector)
  ├── TR-3 (Comparator) — used by the heap
  ├── TR-7 (Dedup) — filter before insert
  └── TR-8 (Map Equality) — needed for TOLIST * dedup

TR-9 (Coordinator Distribution)
  ├── TR-2 (Arg Parsing) — args must be serializable
  ├── TR-4 (Top-K Collector) — shard emits bounded results
  └── TR-7 (Dedup) — coordinator dedup across shards

TR-10 (Backward Compat)
  └── All of the above must preserve existing behavior
```

---

## 5. Questions for the Implementor

1. **C or Rust?** The current TOLIST and the entire aggregation pipeline are in C. Is this feature being implemented in C, or is there a plan to port the aggregation infrastructure to Rust first?

2. **RSValue Map changes — global or local?** Should we modify the global `RSValue_Equal`/`RSValue_Hash` to properly handle maps, or implement TOLIST-specific comparison functions?

3. **Doc ID passing mechanism:** Which approach for making doc ID available to the reducer?
   - (a) Store in heap entry (requires changing the Add() call signature or adding context).
   - (b) Inject into RLookupRow as a hidden field before calling Add().
   - (c) Extend the Reducer interface to include a doc-ID-aware Add variant.

4. **Heap entry design:** Should each heap entry be:
   - (a) A flat struct with payload RSValue + sort key RSValues + doc ID.
   - (b) A full RLookupRow clone (more general, but heavier).
   - (c) An RSValue array combining payload and sort keys.
