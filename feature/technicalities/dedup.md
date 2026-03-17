# Deduplication Mechanism for Enhanced TOLIST

## How Dedup Works Today

### Core Mechanism

TOLIST (`src/aggregate/reducers/to_list.c`) uses a `dict` as a **set** — keys are `RSValue *`, values are `NULL`. Dedup relies on two functions:
- **Hash:** `RSValue_Hash(key, 0)` — FNV-1a, recursive by type.
- **Equality:** `RSValue_Equal(key1, key2)` — delegates to `RSValue_CmpNC`, then `== 0`.

On each `Add()`: read the field value → `dictAdd(values, v, NULL)`. For arrays, each element is inserted individually. `dictAdd` rejects duplicates (same hash + equal). On `Finalize()`: iterate the dict, collect keys into an output array. Order is non-deterministic.

### Cluster Behavior

TOLIST uses `distributeSingleArgSelf` — each shard runs TOLIST (producing a deduplicated array), and the coordinator runs TOLIST again on the shard arrays. The array branch in `tolistAdd` iterates elements into the coordinator's dict, giving cross-shard dedup for free.

### Current Hash/Equality by Type

| Type | Hash | Equality |
|------|------|----------|
| String/Number | FNV-1a on bytes | Byte/numeric compare |
| Array | Recursive over elements **in order** | **First element only** (`compare_arrays_first`) |
| Map | Recursive over entries **in storage order** | **Always "equal"** (returns 0) |
| Null | `hval + 1` | All nulls are equal |

---

## Why Map Comparison Matters

When a user writes `TOLIST *`, the reducer collects the **entire document** for each row in the group. Internally, a JSON document loaded via `LOAD *` is represented as a single `RSValue_Map` — the JSON object becomes a flat list of `(key, value)` pairs stored under the `"$"` key in the `RLookupRow`. For Hash documents, individual fields are assembled into an `RSValue_Map` at output time.

So when `TOLIST *` needs to deduplicate, it is comparing **maps**. The dedup dict calls `RSValue_Hash` and `RSValue_Equal` on these maps. And that is where the current infrastructure breaks down — because map hashing and equality were never properly implemented.

For `TOLIST @field` (single field), the payload is a scalar (string, number, etc.) — existing hash/equality works fine. The challenges below apply specifically to `TOLIST *`.

---

## Challenges

### Challenge 1: Map Equality — No Meaningful Comparison Exists

**Two layers to this problem:**

**Layer 1 — All maps are treated as equal.** `RSValue_CmpNC` returns `0` for maps unconditionally. PR #8622 (Rust port) returns `Err(CompareError::MapComparison)`, mapped to `0`/`true` — same behavior. Impact: `TOLIST *` dedup would keep **only one document per group** — the first JSON document inserted would cause every subsequent (different!) document to be rejected as a "duplicate."

**Layer 2 — The data structure doesn't support map semantics.** `RSValue_Map` is a **flat array of `(key, value)` pairs** — both in C (`RSValueMapBuilderEntry *entries` + `len`) and Rust (`Collection<(SharedRsValue, SharedRsValue)>` = `Box<[(K,V)]>`). No ordering guarantee, no key index, no O(1) lookup.

Consequences:
- **Equality** is O(n²) with unsorted entries (for each key in map1, linear scan map2). Sorting first → O(n log n), but requires temp copy or in-place mutation of shared data.
- **Hashing** must be order-independent, which the flat layout doesn't naturally give.
- **No key index** — "does map2 contain this key?" is a linear scan.

### Challenge 2: Map Hashing — Order-Dependent

`RSValue_Hash` walks entries in storage order and chains FNV-1a. Two maps with identical content but different entry order → different hashes. In cluster mode, shards may serialize the same document with different entry order, breaking coordinator dedup.

### Challenge 3: Array Equality — First Element Only (Resolved)

The C code compares only the first array element. **PR #8622 fixes this** with full lexicographic comparison in Rust. No additional work needed once it lands.

---

## Design Considerations

### What Gets Deduped

Dedup operates on the **payload** (the TOLIST output), not on sort keys. Two docs with identical sort keys but different content are not duplicates. Two docs with different sort keys but identical payload are duplicates.

### Dedup Timing: Before Sort Insertion

Dedup happens **before** heap insertion (consistent with current TOLIST). A duplicate is rejected regardless of its sort rank. This is memory-bounded but means a duplicate with a better sort position is silently dropped.

### Dedup Dict + Heap Interaction

When the heap evicts an entry, the evicted payload stays in the dedup dict (correctly — it was "seen"). The dict can grow to N (all distinct docs in the group) while the heap stays at K. Same memory profile as today's TOLIST, plus heap overhead.

### ALLOWDUPS Optimization

When `ALLOWDUPS`: no dict, no hashing, no equality checks. All entries go directly to the heap. Significantly cheaper. For `TOLIST *`, true content duplicates (same fields, different Redis keys) are extremely rare — most users won't need dedup.

### Cluster Coordinator

Enhanced TOLIST needs a **new distribution function** (replacing `distributeSingleArgSelf`) that forwards `ALLOWDUPS`, `SORTBY`, `LIMIT` to shards. The coordinator merges sorted per-shard lists, deduplicates across shards (if not ALLOWDUPS), re-ranks, and applies the final LIMIT window. Map challenges apply equally on the coordinator side.

---

## Options for Resolving Map Dedup

The core question: how do we implement proper dedup when the payload is a map (`TOLIST *`)? Both dedup-by-default and `ALLOWDUPS` must be supported per the PRD.

### Option A: Global Fix — Deep Map Equality + Order-Independent Hashing

Fix `RSValue_Equal` and `RSValue_Hash` for maps in the value infrastructure (Rust `comparison` module + `hash` module). This makes map comparison correct everywhere, not just TOLIST.

**Equality:** Sort entries by key, then compare pairwise. O(n log n) per comparison.
**Hashing:** Either commutative (XOR/wrapping-add per-pair hash, O(n)) or sort-then-chain (O(n log n), more robust).
**Fast path:** Hash comparison rejects most non-equal maps cheaply; deep equality only on hash collision.

| Aspect | Detail |
|--------|--------|
| Scope | Global — fixes `RSValue_Equal` + `RSValue_Hash` for all callers |
| Correctness | Full semantic equality for maps |
| Performance | O(n log n) per hash/compare, n = number of fields per document |
| Complexity | Medium-high — touches shared value infrastructure |
| Risk | Regression risk across GROUPBY, other reducers, anywhere maps flow through equality/hash |
| JSON docs | Straightforward — map already exists at `"$"` key |
| Hash docs | Must build map eagerly in `Add()` or use virtual compare on `RLookupRow` fields |

### Option B: TOLIST-Local — Map Comparison Only Inside the Reducer

Same algorithm as Option A (sort entries, compare pairwise, order-independent hash), but implemented as **TOLIST-specific functions** rather than changing the global `RSValue_Equal`/`RSValue_Hash`.

The TOLIST dedup dict would use custom `dictType` callbacks (`tolist_map_hash`, `tolist_map_equal`) that handle maps correctly, while leaving the global functions unchanged.

| Aspect | Detail |
|--------|--------|
| Scope | TOLIST-only — no changes to global value infrastructure |
| Correctness | Full semantic equality for maps within TOLIST |
| Performance | Same as Option A — O(n log n) per hash/compare |
| Complexity | Medium — new comparison code, but isolated |
| Risk | Zero regression risk outside TOLIST. Maps remain broken elsewhere (acceptable for now). |
| JSON docs | Same as Option A |
| Hash docs | Same as Option A |
| Downside | Code duplication — if map comparison is later needed elsewhere, it must be re-implemented or promoted to global |

---

## Summary

| Challenge | Status | Impact |
|-----------|--------|--------|
| Map equality (all maps "equal") | **Open** — needs resolution | Blocks `TOLIST *` dedup |
| Map hashing (order-dependent) | **Open** — coupled to map equality | Blocks correct dedup in cluster |
| Array equality (first element only) | **Resolved** by PR #8622 | No work needed |

| Option | Approach | Effort | Risk |
|--------|----------|--------|------|
| A | Global fix to `RSValue_Equal` + `RSValue_Hash` | Medium-High | Regression across all callers |
| B | TOLIST-local map comparison | Medium | None outside TOLIST |

Both options require the same algorithmic work (order-independent hashing + deep equality). The difference is **scope and blast radius**.

---

## Component Dependencies

```
Sorter (Technicality #1)
  └── Provides bounded heap + comparator + doc ID availability

Dedup (This document)
  ├── Gates insertion into the heap
  └── Requires map equality + order-independent hashing (Option A or B)

Coordinator Distribution (Technicality #3)
  ├── Must forward ALLOWDUPS to shards
  ├── Coordinator dedup across shards — same map challenges apply
  └── New distribution function needed (replaces distributeSingleArgSelf)
```
