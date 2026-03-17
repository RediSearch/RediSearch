# Coordinator Distribution for Enhanced TOLIST

## How It Works Today

`AGGPLN_Distribute` (`src/coord/dist_plan.cpp`) splits `FT.AGGREGATE GROUPBY` plans into a **remote plan** (sent to shards) and a **local plan** (runs on coordinator). Each reducer has a **distribution function** that maps it into a remote/local pair:

| Reducer | Shard → Coordinator | Merge Strategy |
|---------|---------------------|----------------|
| COUNT | COUNT → SUM | Sum of counts |
| SUM/MAX/MIN | Same → Same | Aggregate of aggregates |
| **TOLIST** | **TOLIST(field) → TOLIST(alias)** | **Concat + dedup** |
| AVG | COUNT + SUM → SUM + SUM + APPLY | total_sum / total_count |
| COUNT_DISTINCTISH | HLL → HLL_SUM | Merge HLL sketches |

TOLIST uses `distributeSingleArgSelf` — the simplest distribution: same reducer on both sides, single arg (`1 @field`).

### Data Flow: Shard → Coordinator

```
Shard TOLIST Finalize() → RSValue_Array → RESP → network
  ↓
Coordinator RPNet → MRReply_ToValue → RSValue_Array in RLookupRow
  ↓
Coordinator Grouper → groups by key → calls TOLIST Add() per shard row
  ↓
Coordinator TOLIST Add() → unpacks array → dictAdd() each element (cross-shard dedup)
```

The coordinator's `Add()` receives an **array** (the shard's output) and unpacks it element by element into its own dedup dict. RESP serialization preserves array/map element order transparently.

---

## What Needs to Change

### Straightforward: Arg Forwarding

`distributeSingleArgSelf` hardcodes `CHECK_ARG_COUNT(1)` and forwards only the field name. For enhanced TOLIST (`* ALLOWDUPS SORTBY 4 @rating DESC LIMIT 0 5`), write a `distributeToList` that forwards the full `ArgsCursor`. The serialization infrastructure already handles arbitrary reducer args.

### The Coordinator Merge Problem

The shard's enhanced TOLIST produces a **sorted, bounded** array. But the coordinator's `Add()` just sees a flat array — it unpacks elements into a dedup dict, losing all sort information.

The coordinator must: re-sort entries across shards using the same comparator, apply cross-shard dedup (if not ALLOWDUPS), and apply the final LIMIT window.

---

## Deep Dive: Sort-Aware Merge

### Problem: Sort Keys Are Lost After Shard `Finalize()`

On the shard, `HeapEntry` holds both **payload** and **sort key values** (`sortvals[]`). But `Finalize()` outputs only payloads — sort keys are discarded. The coordinator receives payloads with no sort metadata.

### Decision: Shard Must Embed Sort Keys in Its Output

The shard `Finalize()` must include sort key values alongside each payload. The coordinator unpacks them for merge/compare, then strips them before returning the final result.

**Precedent:** `FT.HYBRID` requires `__key` on the coordinator — shards don't return it unless the distribution function explicitly requests it. Same pattern: shard output includes metadata the coordinator needs but the user never sees.

**Wire format per entry (when SORTBY is present):**
```
Entry = [sortval_0, sortval_1, ..., sortval_N-1, payload]
```
- N is known from the forwarded SORTBY args (both sides parse the same clause).
- When SORTBY is absent (legacy `TOLIST @field`), entries remain bare payloads — fully backward compatible.
- The `RSValue → RESP → RSValue` path handles nested arrays/maps transparently — no serialization changes needed.

### Implementation: Non-Trivial Shard Output Change

Embedding sort keys in the shard response requires changes across the TOLIST reducer's `Finalize()` path:

1. **Shard `Finalize()` must build wrappers** — instead of emitting bare payloads from the heap, each entry becomes `[sortvals..., payload]`. The `HeapEntry` already holds `sortvals[]`, so the data is available; the change is in how it's serialized into the output array.
2. **The coordinator must know it's receiving wrappers** — the coordinator TOLIST needs a mode flag (or detects SORTBY in its args) to know entries need unwrapping. This is a new code path in `Add()`.
3. **Coordinator `Finalize()` must strip sort keys** — the final output array contains only payloads, not wrappers. Sort keys are internal transport metadata.
4. **Both sides must agree on N** — the number of sort keys. Since both parse the same forwarded SORTBY clause, this is implicit, but must be validated.

This isn't a one-line change — it touches the shard's output construction, the coordinator's input parsing, and introduces a new internal wire format convention between shard and coordinator TOLIST.

### Coordinator Merge Algorithm

The coordinator re-sorts per group using the **same heap + comparator** as the shard:

```
Coordinator Add() — called once per shard row for a given group:
  1. Read alias field → RSValue_Array (shard's output)
  2. For each element:
     a. Unwrap: extract sortvals[0..N-1] and payload
     b. Check dedup dict (if not ALLOWDUPS)
     c. Insert HeapEntry { payload, sortvals[] } into bounded heap (K = offset + count)

Coordinator Finalize():
  1. Extract from heap, sort in-place (same comparator as shard)
  2. Slice: skip offset, return count entries
  3. Output RSValue_Array of payloads only (sort keys stripped)
```

Sort-key extraction is O(1) positional on both sides — no map field scanning needed.

All payload types handled uniformly:

| Payload type | Coordinator behavior |
|---|---|
| `TOLIST *` (with SORTBY) | Unwrap, merge via heap, strip, output maps |
| `TOLIST @field` (with SORTBY) | Unwrap, merge via heap, strip, output scalars |
| `TOLIST @field` (no SORTBY) | Legacy path: dict concat + dedup, unchanged |

---

## Summary

| What needs to change | Difficulty |
|---------------------|------------|
| Arg forwarding (generalize `distributeSingleArgSelf`) | Straightforward |
| Shard embeds sort keys in output (wrapper format) | **Non-trivial** — new internal wire format, touches Finalize + Add |
| Coordinator unwraps + re-sorts via heap | Straightforward — reuses shard-side infra |
| Cross-shard dedup for maps (`TOLIST *`) | Depends on Dedup technicality |
| Backward compat (`TOLIST @field` without SORTBY) | Straightforward — no wrapper, unchanged |

## Component Dependencies

```
Sorter (Technicality #1)
  └── Coordinator reuses the SAME comparator + heap for re-sorting

Dedup (Technicality #2)
  └── Map equality/hashing challenges apply on BOTH shard and coordinator

Coordinator Distribution (This document)
  ├── Arg forwarding — straightforward
  ├── Sort-key embedding in shard output — non-trivial, needs implementation
  └── Coordinator unwrap + re-sort — straightforward (reuses shard infra)
```
