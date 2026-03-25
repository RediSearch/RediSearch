# COLLECT Reducer — Confluence Delta

Content to add to the Confluence design doc beyond what's already there (sections 1–4, 5.1–5.2).

---

## 6. Flow: Heap Pipeline (Shard & Coordinator)

Both shard and coordinator use the same heap-based accumulation engine. On each `Add()` call (one per document/entry in the group), the reducer captures the projected fields and sort keys, then inserts into a bounded min-max heap (`mm_heap_t`).

### 6.1 Add()

1. Capture sort key values from the source row.
2. **Dedup check** — compare against existing heap entries. If a duplicate is found, skip (avoids returning fewer results than requested due to post-hoc dedup).
3. Capture projected field values from the source row (`IncrRef` all `RSValue*` — they must survive the row wipe between documents).
4. Insert into the bounded heap.

Steps 3–4 only execute if the entry passes dedup. Sort key capture (step 1) is always needed for the dedup comparison.

### 6.2 Finalize()

1. Pop all entries from the heap via repeated `mmh_pop_max()` — yields entries in sorted order (best → worst). The heap is bounded to `K = offset + count`, so this pops at most `K` entries.
2. Apply `LIMIT`: skip first `offset` entries, return the next `count`.
3. For each surviving entry, build a flat KV array from `fieldNames[]` + `projected[]`.

No separate `qsort` step — the heap's pop operation produces sorted output directly.

### 6.3 Coordinator Merge

The coordinator runs the **same** heap pipeline. The difference is the input: instead of raw documents, each `Add()` receives entries from a shard's COLLECT output (the alias column contains an array of KV entries). The coordinator unwraps these entries and inserts them into its own bounded heap.

### 6.4 Tie-Breaking

TBD — needs decision on shard-side and coordinator-side tie-breaking strategy.

### 6.5 SORTBY / LIMIT Combinations

- **SORTBY present** → bounded heap (LIMIT explicit or default), sorted output.
- **SORTBY absent** → growing array, insertion order (capped if LIMIT present, unbounded otherwise).

---

## 7. Coordinator Merge — Unresolved Key Pattern

> **NOTE:** On the coordinator, sort keys injected by the distribution function (Section 5) are accessed via the **unresolved key opening** pattern — the same mechanism FT.HYBRID uses for `@__key`.
>
> 1. During distribution plan construction, sort key names are opened in the coordinator's `RLookup` with `RLOOKUP_OPT_ALLOWUNRESOLVED`. This creates `RLookupKey` entries marked `RLOOKUP_F_UNRESOLVED`.
> 2. When shard results arrive, RPNet deserializes the response and maps fields by name into the `RLookupRow`.
> 3. The coordinator's COLLECT `Add()` reads sort keys via `RLookupRow_Get(sortkey, srcrow)` — same API as the shard side.
>
> The coordinator's `Finalize()` builds output using only the user's original `num_fields`, stripping injected sort-key fields. The user sees exactly the fields they requested.

---

## 8. Optimization: Deferred Keyspace Access

When COLLECT has `*` in FIELDS, it needs to load the full document for each matching row. This is expensive if done eagerly for every document (most of which may not survive the heap).

### 8.1 Detection

At reducer creation time, a single flag check on the source lookup determines the optimization path:

```c
bool loadAllActive = (options->srclookup->_options & RLOOKUP_OPT_ALLLOADED) != 0;
```

- **Flag set** — An upstream `LOAD *` step already loads all document fields into every source row. COLLECT iterates them during `Add()`. No optimization possible (data is already loaded).
- **Flag not set** — COLLECT is the sole `*` consumer. Optimization is possible.

### 8.2 Constraint

This check is stable because the pipeline enforces that `LOAD` steps can only appear before any GROUPBY/APPLY/FILTER. No step after the GROUPBY can set `RLOOKUP_OPT_ALLLOADED` on the root lookup. **If a future change relaxes this ordering constraint, the optimization check must be revisited.**

### 8.3 The Tradeoff

The core tension is between **over-fetching** and **GIL acquisition count**:

| | LOAD * (no optimization) | Deferred loading |
|---|---|---|
| **What gets loaded** | Every document in every group | Only the K winners per group |
| **GIL acquisitions** | 1 (single RPLoader pass) | 1 + 1 (single batch after all groups accumulate) |
| **Wasted I/O** | High — most docs don't survive the heap | None — only winners are loaded |
| **Latency risk** | Predictable — one upfront cost | One extra GIL for the batch load |

**LOAD * (baseline):** One GIL acquisition loads all documents upfront. Simple, predictable, but wastes keyspace reads on documents that get evicted from the heap.

**Deferred:** Zero wasted reads, one additional GIL acquisition for a single batch load after all groups have accumulated their winners.

### 8.3 Approach: Batched Load

After all groups have accumulated (Accum phase drains upstream), collect all winner docIds across all COLLECT reducers into a shared `DocId_Set`, then perform a **single batch load** before yielding any groups.

- **Saves:** N - K keyspace reads per group (N = group size, K = limit). Deduplicates docIds across groups (a document appearing in multiple groups is loaded once).
- **Cost:** One additional GIL acquisition total.
- **Risk:** Document may have expired between Accum and the batch load phase.

Each COLLECT's `Finalize()` resolves `*` from the shared map — no I/O during yield.

> **Note:** A finer-grained optimization is possible — differentiating between sort key fields (needed during `Add()` for heap ordering) and other projected fields (only needed at output time). Sort keys that are already available (e.g., from the sorting vector or schema) wouldn't need deferred loading at all, and only the remaining projected fields would be deferred. However, this requires more complicated tracking of key usage — for example, knowing whether the Grouper is the sole consumer of a key or whether other pipeline steps also depend on it. Left out for now in favor of the simpler all-or-nothing `*` deferral.

---

## 9. Future Optimization: Post-Sort Dedup with Result Buffering

In this design, dedup happens **before** heap insertion (Section 6.1 step 2). This means a costly deep comparison runs for every result in every group — even for documents that would have been evicted by the heap anyway.

An alternative is to dedup **after** sorting: let duplicates compete in the heap normally, then dedup during `Finalize()`. The problem is that removing duplicates from the final sorted output can leave fewer than K results.

To recover, the Grouper's accumulation loop would need to **buffer processed search results** rather than discarding them immediately (`SearchResult_Clear` at `group_by.c:227`). When a post-sort dedup removes entries, COLLECT could trigger a re-sort pass over the buffered residuals to backfill the missing slots.

Left out for now.

---

## 10. Deduplication

### Assumption

For documents with the same payload, the sort keys will be the same.

### Approach

```
dedup_and_insert(heap, entry):
    // 1. Would this entry make it into the heap?
    if heap.full AND compare(entry, heap_worst()) <= 0:
        return  // worse than worst — no need to dedup

    // 2. Scan heap for duplicates
    for i in 1..heap.count:
        existing = heap.data[i]

        // 2a. Fast-path: compare sort keys (cheap scalar comparison)
        if sort_keys_differ(entry, existing):
            continue  // different rank — cannot be duplicate

        // 2b. Sort keys match — deep compare projected fields
        if deep_equal(entry.projected, existing.projected):
            return  // duplicate found — skip

    // 3. No duplicate — insert
    heap_insert_or_exchange(heap, entry)
```

### What's Missing in the Codebase

Deep equality for the `*` field requires comparing entire document payloads. The representation differs by document type, and both paths have gaps in `RSValue_CmpNC`:

**HASH documents** — `LOAD *` loads each hash field as a separate key in the `RLookupRow` (via `RLookup_HGETALL`). There is no single value for the whole document — COLLECT must **construct** one, e.g. a flat `RSValue_Array` of alternating name/value pairs. Today, `RSValue_CmpNC` for arrays only compares the **first element** (`compare_arrays_first`). A full element-wise `compare_arrays` already exists in `value.c` but is unused (marked TODO). Needs to be wired up for deep equality.

**JSON documents** — `LOAD *` stores the root JSON under the `$` key as an `RSValue_Trio`. The Trio wraps a serialized JSON string and an expanded `RSValueType_Map`. Comparing raw Trios compares the serialized strings — this is **not** reliable for equality because two semantically identical JSON objects can have different key ordering (e.g. `{"a":1,"b":2}` vs `{"b":2,"a":1}`). COLLECT must extract the inner `RSValueType_Map` and compare structurally. Today, `RSValue_CmpNC` returns 0 for **all** maps — every map is considered "equal" to every other. A recursive map comparison needs to be implemented (compare lengths, then compare each key-value pair). `RSValue_Hash` already handles maps recursively, so hash-based lookups work — only the equality side is missing.

| Component | HASH (`*` = array) | JSON (`*` = map) |
|-----------|--------------------|--------------------|
| Hash (`RSValue_Hash`) | Works | Works |
| Sort key comparison | Works (scalars) | Works (scalars) |
| Deep equality | `compare_arrays` exists but unused — needs wiring | Not implemented — Trio string comparison is unreliable (key ordering) |

**Optimization:** If `@__key` is one of the projected fields, dedup can be skipped entirely — documents with distinct keys are inherently unique.

---

## Open Questions

- **LIMIT default value:** When LIMIT is absent (with SORTBY present), what is the default? Needs product decision.
- **Tie-breaking:** Strategy for shard-side (doc ID? arbitrary?) and coordinator-side. Needs decision.
- **Null & missing field handling:** How should NULL/missing sort keys affect ordering? How should NULL projected fields appear in the output? Needs decision.
