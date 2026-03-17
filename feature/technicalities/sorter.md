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

## What the In-Group Sorter Needs

The TOLIST in-group sorter is fundamentally different from both existing sorters because:

| Aspect | RPSorter | Coord Heap | TOLIST In-Group |
|--------|----------|------------|-----------------|
| **Operates on** | `SearchResult *` (pipeline row) | `searchResult *` (parsed reply) | Accumulated entries within a single reducer instance |
| **Lifetime** | Exists as a pipeline node | Exists during coord merge | Lives inside the reducer's per-group state |
| **Data available** | Full `RLookupRow` with all fields | Parsed strings/numbers | Captured RSValues (snapshot from `Add()` calls) |
| **Multi-field sort** | Yes (up to 8) | No (single key) | Yes (up to 8, same as RPSorter) |
| **Tie-break** | Doc ID via `SearchResult_GetDocId` | Doc ID string compare | Doc ID on shard; arbitrary on coordinator |

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

### Reuse Opportunity

The **comparator logic** from `cmpByFields` can be simplified — instead of `RLookupRow_Get(key, &row)` per field, it's just `entry->sortvals[i]`. The `RSValue_Cmp` + ASC/DESC bitmap + null handling is identical.

The **heap infrastructure** (`mm_heap_t`) is directly reusable.

The main new work is:
1. **Capture logic in `Add()`** — snapshot payload + sort keys (via `IncrRef`) + doc ID into a heap entry.
2. **Heap entry lifecycle** — `DecrRef` all captured RSValues when an entry is evicted or freed.
3. **Finalize** — extract from heap, sort, slice window, build output `RSValue` array from payloads.
4. **Doc ID injection** — make doc ID available to the reducer (option 1 above or alternative).
