# Union Iterator: Flat vs Heap Strategy Selection

## Overview

The Union iterator merges results from multiple child iterators, producing doc IDs in
sorted order. Two internal strategies exist:

- **Flat**: Linear scan over all children per step — O(N) child operations.
- **Heap**: Min-heap over children — O(1) child operation + O(log N) comparisons per step.

This document summarizes the benchmark analysis that determines when each strategy is
optimal, and proposes a heuristic for selecting between them at construction time.

## How Each Strategy Works

### Flat (UnionQuickFlat / UnionFullFlat)

Every `read()` call touches **all N children**: it calls `skip_to()` or `read()` on each
one to find the next minimum doc ID.

- **Cost per step**: `N × C_child`

### Heap (UnionQuickHeap / UnionFullHeap)

Every `read()` call invokes `read()` on **only the heap root** (the child that just
produced the last minimum), then restores the heap invariant via sift-down using O(log N)
**cheap u64 comparisons** on already-buffered `current()` values.

- **Cost per step**: `1 × C_child + O(log N) × C_compare`

Where `C_child` is the cost of a single `read()`/`skip_to()` on a child iterator, and
`C_compare` is a u64 comparison (essentially free).

## Key Factor: Cost Per Child Operation

The deciding factor is `C_child` — how expensive each child `read()`/`skip_to()` call is.

| Child type | `C_child` | Why |
|---|---|---|
| `IdListSorted` | Very cheap | Pointer bump into contiguous `Vec<u64>` |
| Inverted index iterators (`Numeric`, `Tag`, `Term`, `Full`, etc.) | Expensive | Variable-length bitstream decoding, block boundary transitions, virtual dispatch via `Box<dyn RQEIterator>` |

When `C_child` is large, the heap's reduction from N child calls to 1 child call per
step creates enormous savings. When `C_child` is tiny, the heap's structural overhead
(indirection, sift-down logic) outweighs the savings at low child counts.

## Key Factor: Data Overlap

The second factor is whether child iterators produce **disjoint** or **overlapping** doc
ID ranges.

- **Disjoint**: Each doc ID appears in exactly one child. The heap root changes on every
  step, and flat must wastefully call `skip_to()` on N-1 children that have nothing to
  contribute. The heap avoids all those wasted calls.
- **Overlapping**: Multiple children share doc IDs. Flat's linear scan efficiently finds
  all matching children in one pass. The heap must perform multiple sift operations and
  child calls to collect duplicates, reducing its advantage.

## Benchmark Results Summary

Benchmarks were run with 100,000 IDs per child, sweeping child counts from 2 to 64.

### Synthetic iterators (IdListSorted — cheap `C_child`)

| Scenario | Heap crossover point |
|---|---|
| Disjoint, Quick mode | ~48 children |
| Disjoint, Full mode | ~24 children |
| Low Overlap, Quick/Full | Never (Flat always wins) |
| High Overlap, Quick/Full | Never (Flat always wins) |

### Real Numeric iterators (expensive `C_child`, disjoint by construction)

| Scenario | Heap crossover point | Max heap advantage |
|---|---|---|
| Disjoint, Quick mode | **2 children** | 1.8× @ 64 children |
| Disjoint, Full mode | **4 children** | **5.9× @ 64 children** |
| Low Overlap, Full mode | ~12 children | modest ~1.2×, inconsistent |
| Low Overlap, Quick mode | Never (Flat always wins) | — |
| High Overlap, Full mode | ~8 children | modest ~1.2×, inconsistent |
| High Overlap, Quick mode | Never (Flat always wins) | — |

### Real Term iterators (DocIdsOnly encoding — lightest non-numeric inverted index)

| Scenario | Heap crossover point | Max heap advantage |
|---|---|---|
| Disjoint, Quick mode | **~8 children** | 2.1× @ 64 children |
| Disjoint, Full mode | **4 children** | **6.7× @ 64 children** |
| Low Overlap, Quick mode | **Never (Flat always wins)** | Flat 3.3× faster @ 64 |
| Low Overlap, Full mode | ~8 children | modest ~1.1×, narrows with more children |

#### Term Low Overlap Quick Mode Detail

This is the most important result for non-numeric heuristic design. With low overlap
and Quick mode (the most common production path for Tag/Term/Prefix queries), **Flat
dominates at every child count**, and the gap *widens* as children increase:

| Children | Flat (ms) | Heap (ms) | Flat advantage |
|---|---|---|---|
| 8 | 9.65 | 14.85 | 1.5× |
| 16 | 11.11 | 23.59 | 2.1× |
| 32 | 12.74 | 34.70 | 2.7× |
| 64 | 13.35 | 44.73 | 3.4× |

Flat's time plateaus because overlapping children share doc IDs — Quick mode's
`skip_to(min+1)` causes most children to advance cheaply together, and many
terminate early. Meanwhile the Heap must maintain its invariant across all children,
paying O(log N) per step even when most children are redundant.

### Key Observations

1. **Disjoint + expensive iterators**: Heap wins massively, even at 2–4 children.
   This applies to both Numeric and Term iterators when data is truly disjoint.
2. **Overlap + Quick mode**: Flat always wins regardless of child cost, because Quick
   mode's `skip_to(min+1)` causes overlapping children to advance cheaply together.
3. **Overlap + Full mode + expensive iterators**: Heap has a small, inconsistent edge
   at moderate child counts (8–32). The advantage narrows as children increase.
4. **Cheap iterators**: Heap only wins at very high child counts (24–48+) with disjoint
   data. Any overlap neutralizes the advantage entirely.
5. **Non-numeric overlap is the norm**: Tag, Term, Prefix, and Fuzzy queries produce
   children from the same document space, resulting in significant overlap. The disjoint
   case is rare for these query types.

## Numeric Ranges: The Primary Use Case for the Heap

Numeric range queries are the strongest candidate for the heap because:

- **Structurally disjoint**: Numeric range tree leaves cover non-overlapping doc ID
  ranges by construction.
- **High child counts**: A wide-range query (e.g., `@price:[0, +inf]`) can produce
  40–100+ leaf range iterators as union children.
- **Expensive per-call cost**: Each child is a `Numeric` inverted index iterator with
  variable-length bitstream decoding.

All three factors align to maximize the heap's advantage.

## Proposed Union Wrapper: `new_union_iterator`

The Rust union should have a single factory function that mirrors and improves upon the
C `NewUnionIterator` + `UnionIteratorReducer` pattern. This function reduces the child
list, applies early-exit optimizations, and selects the optimal internal strategy.

### C Reference: `UnionIteratorReducer`

The existing C code (`src/iterators/union_iterator.c`) applies these reductions before
creating the union:

1. **Remove empty iterators** — Any child with type `EMPTY_ITERATOR` or NULL is dropped
   and freed.
2. **Wildcard shortcut (quick-exit only)** — If `quickExit` is true and any remaining
   child is a wildcard iterator (`IsWildcardIterator`), return that child directly and
   free all others. A wildcard matches every document, so in quick-exit mode (where we
   only need the first match per doc ID) the union of {wildcard, anything} ≡ wildcard.
3. **Single child passthrough** — If exactly one child remains after filtering, return
   it directly without wrapping in a union.
4. **Zero children** — Return an `Empty` iterator.

Only if none of these reductions apply does the C code proceed to allocate a
`UnionIterator` and choose between Flat/Heap based on `config->minUnionIterHeap`.

### Proposed Rust Signature

```rust
/// Inputs needed by the union factory to select the optimal strategy.
pub struct UnionConfig {
    /// Whether to return after the first matching child (quick exit / OR semantics)
    /// or aggregate all children with the same doc ID (full mode).
    pub quick_exit: bool,
    /// The query node type that produced this union (e.g., QN_NUMERIC, QN_TAG, QN_PREFIX).
    /// Used to infer data distribution characteristics.
    pub query_node_type: QueryNodeType,
    /// Weight assigned to the union result.
    pub weight: f64,
}

/// Create a union iterator with automatic strategy selection.
///
/// Applies reductions (empty removal, wildcard shortcut, single-child passthrough)
/// then selects Flat or Heap based on the query type and child count.
pub fn new_union_iterator<'index>(
    children: Vec<Box<dyn RQEIterator<'index> + 'index>>,
    config: UnionConfig,
) -> Box<dyn RQEIterator<'index> + 'index>
```

### Reduction Steps (in order)

```text
1. Remove all Empty children from the list.

2. If quick_exit AND any child is a WildcardIterator:
     → Return that wildcard child, drop all others.
     (Wildcard matches everything; in quick-exit mode the union is redundant.)

3. If zero children remain:
     → Return Empty iterator.

4. If exactly one child remains:
     → Return that child directly (no union wrapper needed).

5. Select strategy based on query type and child count:
     → See "Strategy Selection Heuristic" below.
```

### Strategy Selection Heuristic

```text
if query_node_type == QN_NUMERIC && num_children >= 4:
    use Heap
else:
    use Flat
```

#### Rationale

- **`QN_NUMERIC` with ≥ 4 children → Heap**: Numeric range queries produce **disjoint
  children with expensive decode cost** — the exact scenario where the heap dominates
  (up to 5.9× faster at 64 children). The threshold of 4 captures the Full-mode
  crossover. For Quick mode the heap wins even at 2, but 4 provides a safety margin.

- **Everything else → Flat**: For all other query types (`QN_PREFIX`, `QN_FUZZY`,
  `QN_TAG`, `QN_UNION`, etc.), the children typically have overlapping doc IDs.
  Benchmarks with real Term (DocIdsOnly) iterators show that **Flat always wins in
  Quick mode with overlap**, and the advantage grows with child count (up to 3.4× at 64
  children). Even in Full mode with overlap, the heap's advantage is modest (~10-15%)
  and narrows as children increase. Since Quick mode is the most common production path,
  and overlap is the norm for non-numeric queries, Flat is the correct default.

- **No general fallback threshold**: Real inverted index benchmarks with overlap show that
  this threshold actively hurts performance: at 20+ children with overlap and Quick mode,
  Flat is 2.5× faster than Heap. The only scenario where Heap helps non-numeric children
  is disjoint data — which is rare for Tag/Term/Prefix/Fuzzy queries.

- **Zero runtime cost**: The heuristic uses only information already available at union
  construction time — query node type and child count.

#### Future Extensions

The `query_node_type` check could be extended if other structurally-disjoint union
sources are identified. For example, if a new index type produces guaranteed-disjoint
child iterators with expensive decode cost, it could be added alongside `QN_NUMERIC`.
