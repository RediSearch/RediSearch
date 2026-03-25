# In-Group Sorting for Enhanced TOLIST

## Existing Sorters in the System

There are two sorters relevant to FT.AGGREGATE:

### 1. RPSorter — Pipeline-level SORTBY (`src/result_processor.c`)

This is the sorter that runs when you write `SORTBY 2 @field ASC` in an FT.AGGREGATE (or FT.SEARCH) pipeline. It's a **ResultProcessor** — a node in the pipeline chain.

**Operates on:** `SearchResult *` — each row flowing through the pipeline is a `SearchResult`, which wraps an `RLookupRow` (the actual field values).

**Mechanism:** Bounded min-max heap (`mm_heap_t`) of size `offset + count`. As rows stream through:
- If heap has room → insert.
- If heap is full → compare new row against the worst entry. If better, swap.
- On drain: extract all, sort in-place, skip `offset`, yield `count` rows.

**Comparator:** `cmpByFields` — iterates up to 8 sort keys, calls `RSValue_Cmp` per key, flips sign based on ASC/DESC bitmap (`sortAscMap`). Tie-break: `SearchResult_GetDocId`.

**Null handling:** Missing value loses — the row without the sort key is pushed to the end regardless of ASC/DESC.

```c
// Simplified from result_processor.c:702
static int cmpByFields(const void *e1, const void *e2, const void *udata) {
  const RPSorter *self = udata;
  const SearchResult *h1 = e1, *h2 = e2;
  for (size_t i = 0; i < self->fieldcmp.nkeys; i++) {
    const RSValue *v1 = RLookupRow_Get(self->fieldcmp.keys[i], &h1->rowdata);
    const RSValue *v2 = RLookupRow_Get(self->fieldcmp.keys[i], &h2->rowdata);
    int ascending = SORTASCMAP_GETASC(self->fieldcmp.ascendMap, i);
    if (!v1 || !v2) {
      // null → loses
      if (v1) return 1;
      else if (v2) return -1;
      else continue;
    }
    int rc = RSValue_Cmp(v1, v2, qerr);
    if (rc != 0) return ascending ? -rc : rc;
  }
  // tie-break: doc ID
  return SearchResult_GetDocId(h1) < SearchResult_GetDocId(h2) ? -1 : 1;
}
```

**Construction:** `RPSorter_NewByFields(maxresults, keys, nkeys, ascmap)` in `pipeline_construction.c`.

**Where it runs:** After GROUPBY (sorts the **groups** themselves), or after LOAD/APPLY. It does NOT sort items inside a group — it sorts pipeline rows.

---

### 2. Coordinator Search Merge Heap (`src/module.c`)

This sorter merges shard results for **FT.SEARCH** on the coordinator.

**Operates on:** `searchResult *` — a coordinator-specific struct, different from the pipeline's `SearchResult`. Contains parsed shard reply fields: `id`, `score`, `sortKey`, `sortKeyNum`, `payload`, `fields`.

**Mechanism:** Bounded max-heap (`heap_t`) of size `requestedResultsCount`. Shard replies are parsed and inserted one by one.

**Comparator:** `cmp_results` — if `withSortby`, compares the sort key (numeric or string); otherwise compares score. Tie-break: string comparison on document `id`.

```c
// Simplified from module.c
static int cmp_results(const void *p1, const void *p2, const void *udata) {
  const searchResult *r1 = p1, *r2 = p2;
  const searchReducerCtx *rCtx = udata;
  if (rCtx->withSortby) {
    int rc = rCtx->withSortbyNum
      ? cmp_numbers(r1->sortKeyNum, r2->sortKeyNum)
      : strcmp(r1->sortKey, r2->sortKey);
    if (rc != 0) return rCtx->sortAscending ? rc : -rc;
  } else {
    if (r1->score != r2->score) return r1->score < r2->score ? 1 : -1;
  }
  return strcmp(r1->id, r2->id);  // tie-break
}
```

**Key difference from RPSorter:** This operates on raw parsed RESP replies (strings/numbers), NOT on `RSValue`s or `RLookupRow`s. It only supports a **single** sort key. It's FT.SEARCH-specific and not used by FT.AGGREGATE.

---

### 3. FIRST_VALUE Reducer — In-Group Sort Precedent (`src/aggregate/reducers/first_value.c`)

While not a general-purpose sorter, FIRST_VALUE is the **closest existing relative** to the in-group sorter. It already solves a miniature version of the same problem: *"from the rows within a group, pick one value based on a sort criterion on a different field."*

**Operates on:** Captured `RSValue *` pointers inside a per-group accumulator (`fvCtx`), within the reducer lifecycle.

**Mechanism:** Running min/max tracker — a **degenerate heap of size 1.** Each incoming row is compared against the current best; the winner is kept.

**Two-level struct pattern:**

```c
// Reducer-level (shared across all groups) — holds config
typedef struct {
  Reducer base;
  const RLookupKey *sortprop;  // The property the value is sorted by
  int ascending;
} FVReducer;

// Per-group instance — holds accumulated state
typedef struct {
  const RLookupKey *retprop;   // The key to return (payload)
  const RLookupKey *sortprop;  // The key to sort by
  RSValue *value;              // Best value seen so far (payload)
  RSValue *sortval;            // Best sort-key seen so far
  int ascending;
} fvCtx;
```

**Data access pattern — the same one the sorter needs:**

```c
// Simplified from first_value.c:fvAdd_sort
static int fvAdd_sort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  // 1. Read the return value (payload)
  RSValue *val = RLookupRow_Get(fvx->retprop, srcrow);
  if (!val) val = RSValue_NullStatic();

  // 2. Read the sort key
  RSValue *curSortval = RLookupRow_Get(fvx->sortprop, srcrow);
  if (!curSortval) curSortval = RSValue_NullStatic();

  if (!fvx->sortval) {
    // First value — always accept
    fvx->value = RSValue_IncrRef(val);          // 3. IncrRef to survive wipe
    fvx->sortval = RSValue_IncrRef(curSortval);
  } else if (RSValue_IsNull(curSortval)) {
    // Null sort key loses — skip
  } else if (RSValue_IsNull(fvx->sortval)) {
    // Stored sort key is null, current is not — replace
    RSValue_Replace(&fvx->sortval, curSortval);
  } else {
    // Both non-null: compare and keep the winner
    int rc = RSValue_Cmp(curSortval, fvx->sortval, NULL);
    if (fvx->ascending ? rc < 0 : rc > 0) {
      RSValue_Replace(&fvx->sortval, curSortval);   // DecrRef old + IncrRef new
      RSValue_Replace(&fvx->value, val);
    }
  }
  return 1;
}
```

**Key observations for the sorter design:**

1. **Separate return field and sort field.** `retprop` (what gets returned) and `sortprop` (what determines which row wins) are two distinct `RLookupKey *` pointers. This is the same concept the sorter generalizes: `payload` vs `sortvals[0..N-1]`.

2. **`RLookupRow_Get` + `IncrRef` capture pattern.** FIRST_VALUE proves this works: read a field by its `RLookupKey`, `IncrRef` it to survive the row wipe, store it in the per-group accumulator. The sorter does the same in a loop over N sort keys.

3. **Null handling: nulls lose.** A null sort key never beats a real value. This matches RPSorter's `cmpByFields` behavior and becomes the sorter's null policy.

4. **ASC/DESC toggle.** A single `int ascending` flag that flips the comparison direction — `ascending ? rc < 0 : rc > 0`. The sorter generalizes this to a bitmap for multi-key support.

5. **`RSValue_Replace` for eviction.** When a better row arrives, `RSValue_Replace` handles the DecrRef-old + IncrRef-new atomically. The sorter's heap eviction uses the same lifecycle pattern, applied to each field in a `HeapEntry`.

6. **`BlkAlloc` for per-group instances.** `fvCtx` is allocated via `BlkAlloc_Alloc` from the reducer's block allocator, reducing fragmentation across thousands of groups. The sorter's per-group state can use the same allocator.

7. **Cleanup via DecrRef.** `fvFreeInstance` decrefs both `value` and `sortval`. The sorter extends this to all captured `RSValue *` pointers in each heap entry.

**What FIRST_VALUE is NOT:**
- No multi-key sort (single `sortprop` vs bitmap + array).
- No bounded heap (single slot vs `mm_heap_t` of size K).
- No dedup.
- No wildcard payload (`TOLIST *`).
- No doc ID tie-breaking.

**Structural equivalence:** FIRST_VALUE is a **bounded heap of capacity K=1.** The sorter generalizes it to K = offset + count. The comparison logic, data access pattern, null handling, and lifecycle management are identical — only the container changes.

---

## What the In-Group Sorter Needs

The TOLIST in-group sorter is fundamentally different from both pipeline-level sorters, and is a generalization of FIRST_VALUE's approach:

| Aspect | RPSorter | Coord Heap | FIRST_VALUE | TOLIST In-Group |
|--------|----------|------------|-------------|-----------------|
| **Operates on** | `SearchResult *` (pipeline row) | `searchResult *` (parsed reply) | Captured `RSValue *` in per-group `fvCtx` | Captured `RSValue *` in per-group `HeapEntry` structs |
| **Lifetime** | Exists as a pipeline node | Exists during coord merge | Lives inside reducer's per-group state | Lives inside reducer's per-group state |
| **Data available** | Full `RLookupRow` with all fields | Parsed strings/numbers | Single payload + single sort key (IncrRef'd) | Multiple sort keys + payload (IncrRef'd) |
| **Multi-field sort** | Yes (up to 8) | No (single key) | No (single key) | Yes (up to 8, same as RPSorter) |
| **Capacity** | Bounded heap (offset + count) | Bounded heap | **1** (single best value) | Bounded heap (offset + count) |
| **Tie-break** | Doc ID via `SearchResult_GetDocId` | Doc ID string compare | None | Doc ID on shard; arbitrary on coordinator |

### How `RLookupRow` Works (what the reducer receives)

The reducer's `Add(reducer, instance, srcrow)` receives `const RLookupRow *srcrow`. Understanding this structure is key to designing the in-group sorter.

**`RLookupRow`** is a **flat array** indexed by integer position:

```c
typedef struct {
  const RSSortingVector *sv;  // optional: sortable fields from the index
  RSValue **dyn;              // flat array of RSValue pointers
  size_t ndyn;                // count of non-NULL entries (not array length)
} RLookupRow;
```

**`RLookupKey`** provides the mapping from field name → array index:

```c
struct RLookupKeyHeader {
  uint16_t dstidx;    // index into RLookupRow.dyn[]
  uint16_t svidx;     // index into RSSortingVector (if RLOOKUP_F_SVSRC)
  RLookupKeyFlags flags;
  const char *path;
  const char *name;
  // ...
};
```

**Reading a value:** `RLookupRow_Get(key, row)` does:
1. Try `row->dyn[key->dstidx]` — if non-NULL, return it.
2. If `key->flags & RLOOKUP_F_SVSRC`, try `row->sv[key->svidx]`.
3. Otherwise return NULL.

So it's essentially `dyn[integer_index]` — O(1) lookup. No hash map, no string comparison.

### How `LOAD *` Populates the Row

This is critical because it determines what the reducer sees.

**JSON documents** (`RLookup_JSON_GetAll`):
- Fetches the entire document from the `$` (JSON root) path.
- Stores it as **one `RSValue_Map`** under a single `RLookupKey` for `"$"`.
- Result: `row->dyn[dollar_key->dstidx]` = an `RSValue_Map` containing the whole document.

**Hash documents** (`RLookup_HGETALL`):
- Scans all hash fields.
- Each field becomes a **separate `RSValue`** with its own `RLookupKey` and `dstidx`.
- Result: `row->dyn[field1_dstidx]` = value1, `row->dyn[field2_dstidx]` = value2, etc.

### What Happens After `Add()` — Row Wipe

After all reducers process a row, `SearchResult_Clear` → `RLookupRow_Wipe`:

```c
void RLookupRow_Wipe(RLookupRow *r) {
  for (size_t ii = 0; ii < array_len(r->dyn) && r->ndyn; ++ii) {
    RSValue **vpp = r->dyn + ii;
    if (*vpp) {
      RSValue_DecrRef(*vpp);   // <-- frees if refcount hits 0
      *vpp = NULL;
      r->ndyn--;
    }
  }
  r->sv = NULL;
}
```

Every RSValue in the row is **decref'd**. If the reducer didn't `IncrRef` a value, it's gone.

### Implications for the In-Group Sorter

Given the above, the in-group sorter:

1. **Holds `RLookupKey` pointers** — these are stored at parse time and remain valid for the entire lifetime of the reducer. They live in the `RLookup` struct, not the row. The reducer uses them on every `Add()` call to read values from the current row.

2. **The row data is ephemeral** — after `Add()` returns, `SearchResult_Clear` wipes the row and decrefs all RSValues. Any value the reducer wants to keep across `Add()` calls must be `RSValue_IncrRef`'d during `Add()`.

3. **Sort key access during `Add()`** — the reducer knows its sort keys' `RLookupKey` pointers from parse time. During `Add()`, it reads `RLookupRow_Get(sortkey[i], srcrow)` and `IncrRef`s the value into the heap entry. This is O(1) per sort key.

4. **Payload capture differs by type:**
   - `TOLIST @field` — single value: `RSValue_IncrRef(RLookupRow_Get(srckey, srcrow))`.
   - `TOLIST *` with JSON — the entire document is one `RSValue_Map` at the `"$"` key. Just `IncrRef` it — one value captures the whole document.
   - `TOLIST *` with Hash — each field is a separate entry in `dyn[]`. The reducer must iterate the `RLookup`'s key list, read each field, and build a new `RSValue_Map`. This requires the reducer to hold a pointer to the `RLookup` itself (available via `options->srclookup` at construction time), not just individual keys.

5. **For `TOLIST *`, the reducer must hold the `RLookup *`** — not just for payload capture, but also to know which keys exist and what their names are. At parse time, it stores `options->srclookup`. During each `Add()`, it iterates visible keys to enumerate loaded fields. This is essential for Hash documents and useful for JSON to locate the `"$"` key.

### Data Flow Diagram

```
Pipeline row (SearchResult)
│
├── RLookupRow.dyn[]
│     [0]: @relationshipName = "401K SEP"      (RSValue_String)
│     [1]: @opportunityId    = 693346585        (RSValue_Number)
│     [2]: @target           = "False"          (RSValue_String)
│     [3]: @bestByDate       = "2023-03-21"     (RSValue_String)
│     [4]: "$"               = { whole JSON }   (RSValue_Map, only for JSON LOAD *)
│     ...
│
├── SearchResult._doc_id = 42
│
▼
Reducer::Add(reducer, group_instance, &srcrow)
│
├── Read sort keys:  sortval[0] = IncrRef(RLookupRow_Get(target_key, srcrow))     → "False"
│                    sortval[1] = IncrRef(RLookupRow_Get(bestByDate_key, srcrow))  → "2023-03-21"
│
├── Read payload:    payload = IncrRef(RLookupRow_Get(dollar_key, srcrow))         → { whole JSON }
│                    (or for @field: IncrRef(RLookupRow_Get(srckey, srcrow)))
│
├── Read doc ID:     docId = ??? (not in RLookupRow — see open question below)
│
▼
Insert into heap entry { payload, sortval[], docId }
│
▼
SearchResult_Clear() → RLookupRow_Wipe() → DecrRef everything
(our IncrRef'd values survive)
```

### Doc ID: The Missing Piece

The `SearchResult` contains `_doc_id` (`t_docId`), but the reducer's `Add()` only receives `const RLookupRow *srcrow` — it doesn't get the `SearchResult` wrapper. So `_doc_id` is **not directly accessible** to the reducer.

Looking at `Grouper_rpAccum`:
```c
while ((rc = base->upstream->Next(base->upstream, res)) == RS_RESULT_OK) {
    invokeGroupReducers(g, SearchResult_GetRowDataMut(res));  // passes only &rowdata
    SearchResult_Clear(res);
}
```

The doc ID is in `res->_doc_id`, but `invokeGroupReducers` only passes `res->_row_data`. The reducer never sees the doc ID.

**Options to make doc ID available:**
1. **Inject into the row** — before calling `invokeGroupReducers`, write the doc ID as a hidden `RSValue_Number` into the `RLookupRow` under a reserved key. Lightweight, no interface changes.
2. **Widen the `Add()` signature** — pass both `RLookupRow *` and `t_docId`. Requires changing the `Reducer` interface (breaks all existing reducers).
3. **Pass the full `SearchResult *`** — similar to (2) but heavier.
4. **Accept arbitrary tie-break on standalone too** — skip doc ID tie-breaking entirely. Simpler but less deterministic.

Option (1) seems cleanest — the Grouper injects it before the reducer loop, and the TOLIST reducer reads it like any other field. Other reducers simply ignore it.

### Proposed Structure (refined)

Each heap entry — a flat struct, no `RLookupRow`:

```
┌──────────────────────────────────────────┐
│  payload: RSValue *                      │  ← what gets returned
│  sortvals: RSValue *[nsortkeys]          │  ← captured values, indexed by position
│  docId: t_docId                          │  ← for tie-breaking (shard only)
└──────────────────────────────────────────┘
```

The comparator indexes `entry->sortvals[i]` directly — no `RLookupKey` needed at compare time, since the sort key order is fixed at parse time. This is simpler than `cmpByFields` which does `RLookupRow_Get(key, row)` per comparison.

The per-group instance:

```
┌──────────────────────────────────┐
│  heap: mm_heap_t *               │  ← bounded to K = offset + count
│  dedup_dict: dict * (optional)   │  ← for default distinct behavior (skipped with ALLOWDUPS)
└──────────────────────────────────┘
```

### No Early Capping: All Reducers Must See All Rows

A critical constraint: **no reducer can cap the row stream before all reducers in the same GROUPBY have processed every row.**

The Grouper feeds each incoming row to all reducers for that group. A TOLIST reducer's bounded heap (size K = offset + count) only bounds what *that specific reducer instance* retains — it does **not** limit the rows fed to sibling reducers. This matters because:

1. **Multiple enhanced TOLIST reducers with different SORTBY/LIMIT** can coexist in the same GROUPBY. Each has its own heap with its own sort criteria and capacity. Reducer A's LIMIT of 3 must not prevent Reducer B (with LIMIT 10 and different sort keys) from seeing all rows.

2. **TOLIST coexists with scalar reducers** (COUNT, MAX, MIN, SUM, COUNT_DISTINCT, FIRST_VALUE, etc.). Scalar reducers require *every* row to produce correct results — COUNT must count all rows, MAX must see all values.

3. **This will become more important as more reducers gain in-group sorting.** If FIRST_VALUE or future reducers add their own SORTBY/LIMIT semantics, each instance must independently accumulate from the full input stream.

**Implication for the heap:** The bounded heap is a per-reducer-instance optimization that limits *memory* within one reducer, not an *input filter*. The Grouper loop remains:

```
for each row in group:
    for each reducer in group:
        reducer->Add(row)    // every reducer sees every row
```

The heap eviction inside `Add()` (discard the worst entry when full) is strictly local to that reducer instance. The row continues to the next reducer regardless.

**Verified empirically:** All combinations of multiple TOLIST + scalar reducers work correctly today (see [findings](../findings/multiple-reducers-coexistence-with-tolist.md)). The design must preserve this property.

---

### Reuse Opportunity — What Comes From Where

The in-group sorter is a synthesis of three existing, proven pieces:

**From FIRST_VALUE** (`src/aggregate/reducers/first_value.c`) — the data access and lifecycle pattern:
- **`RLookupRow_Get` + `IncrRef` capture in `Add()`** — proven pattern for reading fields from the ephemeral row and retaining them across calls. The sorter does this in a loop over N sort keys instead of one.
- **Separate return field vs sort field** — `retprop` ≠ `sortprop`. The sorter generalizes to `payload` vs `sortvals[]`.
- **`RSValue_Replace` for eviction** — DecrRef old + IncrRef new when a better value arrives. The sorter applies this to heap entry replacement.
- **Two-level struct pattern** — `FVReducer` (shared config) + `fvCtx` (per-group state). The sorter uses the same architecture with a heap replacing the single slot.
- **`BlkAlloc` for per-group instances** — same allocator strategy.
- **Null handling** — nulls lose, never beat a real value. Directly adopted.

**From RPSorter** (`src/result_processor.c`) — the multi-key comparison and heap infrastructure:
- **`mm_heap_t` bounded heap** — reused as-is, moved inside per-group state.
- **Multi-key comparison with `RSValue_Cmp`** — same algorithm, but simplified: `entry->sortvals[i]` replaces `RLookupRow_Get(key, &row)`.
- **ASC/DESC bitmap** (`sortAscMap`) — same `SORTASCMAP_GETASC` mechanism for multi-key direction control.

**From current TOLIST** (`src/aggregate/reducers/to_list.c`) — the dedup infrastructure:
- **`RSValueSet` dict type** — hash, compare, dup, destructor for `RSValue`. Reusable for the sorter's optional dedup dictionary.

**The main new work is:**
1. **Capture logic in `Add()`** — generalize FIRST_VALUE's single-field capture to snapshot payload + N sort keys (via `IncrRef`) + doc ID into a heap entry.
2. **Heap entry lifecycle** — extend FIRST_VALUE's `RSValue_Replace`/`DecrRef` pattern to all captured `RSValue *` pointers in each heap entry on eviction or free.
3. **Finalize** — extract from heap, sort, slice window, build output `RSValue` array from payloads.
4. **Doc ID injection** — make doc ID available to the reducer (option 1 above or alternative).
