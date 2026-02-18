# Union Iterator Design Document

This document describes the Rust union iterator implementation, including design rationale,
performance analysis, and benchmark results comparing C and Rust implementations.

## Overview

The union iterator yields documents appearing in ANY child iterator (OR semantics).
Two separate types are provided for different performance characteristics:

| Type | Min-Finding | Best For | Overhead |
|------|-------------|----------|----------|
| `UnionFlat` | O(n) scan | <20 children | Minimal |
| `UnionHeap` | O(log n) heap | >20 children | Heap allocation |

Both types support a `QUICK_EXIT` const generic:
- `QUICK_EXIT=false` (Full mode): Aggregates results from all matching children
- `QUICK_EXIT=true` (Quick mode): Returns after first match without aggregation

## Type Aliases

```rust
// Full mode variants (aggregate all matching children)
pub type UnionFullFlat<'index, I> = UnionFlat<'index, I, false>;
pub type UnionFullHeap<'index, I> = UnionHeap<'index, I, false>;

// Quick mode variants (return after first match)
pub type UnionQuickFlat<'index, I> = UnionFlat<'index, I, true>;
pub type UnionQuickHeap<'index, I> = UnionHeap<'index, I, true>;

// Backwards compatibility
pub type Union<'index, I> = UnionFullFlat<'index, I>;
```

## Implementation Files

| File | Description |
|------|-------------|
| `src/union.rs` | Main implementation (~920 lines) |
| `src/util/min_heap.rs` | Custom `DocIdMinHeap` (~415 lines) |

## Custom Heap: DocIdMinHeap

Rust's `std::collections::BinaryHeap` lacks two critical operations needed for
optimal union iterator performance. We implemented a custom `DocIdMinHeap` with:

### Key Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `push(doc_id, idx)` | O(log n) | Insert entry with sift-up |
| `pop()` | O(log n) | Remove minimum with sift-down |
| `peek()` | O(1) | View minimum without removal |
| `replace_root(doc_id, idx)` | O(log n) | **In-place replace** - C's `heap_replace` equivalent |
| `for_each_root(callback)` | O(k) | **Iterate equal roots** - C's `heap_cb_root` equivalent |

### Why Custom Heap?

**Problem with `BinaryHeap`:**
```rust
// BinaryHeap: pop() + push() = O(2 log n)
let (_, idx) = self.heap.pop().unwrap();
// ... advance child ...
self.heap.push((child.last_doc_id(), idx));
```

**Solution with `DocIdMinHeap`:**
```rust
// DocIdMinHeap: replace_root() = O(log n)
let (_, idx) = self.heap.peek().unwrap();
// ... advance child ...
self.heap.replace_root(child.last_doc_id(), idx);
```

### Performance Improvement

For `k` children matching the minimum doc_id and `n` total children:

| Operation | BinaryHeap | DocIdMinHeap | Improvement |
|-----------|------------|--------------|-------------|
| Advance k children | k × O(2 log n) | k × O(log n) | **2×** |
| Collect k matches | O(2k log n) | O(k) | **O(log n)×** |

## Benchmark Results

All benchmarks run with 100,000 documents per child iterator.

### 5 Children (Few Children - Flat Should Win)

| Scenario | C Flat | C Heap | Rust Flat | Rust Heap | Winner |
|----------|--------|--------|-----------|-----------|--------|
| **High Overlap** | 4.65 ms | 8.80 ms | **4.20 ms** | 12.01 ms | Rust Flat |
| **Disjoint** | **7.56 ms** | 13.09 ms | 9.11 ms | 14.10 ms | C Flat |

### 50 Children (Many Children - Heap Should Win for Disjoint)

| Scenario | C Flat | C Heap | Rust Flat | Rust Heap | Winner |
|----------|--------|--------|-----------|-----------|--------|
| **High Overlap** | 40.66 ms | 126.14 ms | **28.64 ms** | 143.33 ms | Rust Flat |
| **Disjoint** | 179.70 ms | **135.94 ms** | 552.30 ms | 137.89 ms | C Heap |

### Relative Performance (vs C Flat baseline)

| Scenario | C Flat | C Heap | Rust Flat | Rust Heap |
|----------|--------|--------|-----------|-----------|
| 5 children, High Overlap | 1.00× | 1.89× | **0.90×** | 2.58× |
| 5 children, Disjoint | 1.00× | 1.73× | 1.21× | 1.87× |
| 50 children, High Overlap | 1.00× | 3.10× | **0.70×** | 3.53× |
| 50 children, Disjoint | 1.00× | 0.76× | 3.07× | **0.77×** |

## Key Findings

### 1. Rust Heap Achieves Parity with C Heap

For many-children disjoint scenarios (where heap shines):
- C Heap: 135.94 ms
- Rust Heap: 137.89 ms
- **Difference: ~1.4%** - essentially parity

The custom `DocIdMinHeap` with `replace_root()` and `for_each_root()` successfully
matches the C heap implementation's performance.

### 2. Rust Flat Outperforms C Flat in High-Overlap Scenarios

- 5 children: Rust Flat is **10% faster** (4.20 ms vs 4.65 ms)
- 50 children: Rust Flat is **30% faster** (28.64 ms vs 40.66 ms)

This is likely due to Rust's better optimization of tight loops and iterator abstractions.

### 3. C Flat is Faster in Disjoint Scenarios

- 5 children: C is **17% faster** (7.56 ms vs 9.11 ms)
- 50 children: C is **3× faster** (179.70 ms vs 552.30 ms)

The Rust flat implementation has room for optimization in disjoint scenarios.

### 4. Crossover Point

The heap variant becomes beneficial with:
- **50+ children** AND
- **Low overlap** (disjoint or near-disjoint data)

For high-overlap data, flat is always faster regardless of child count.

## Usage Recommendations

| Scenario | Recommended Variant |
|----------|---------------------|
| Small unions (<20 children) | `UnionFullFlat` or `UnionQuickFlat` |
| Large unions (>20 children), high overlap | `UnionFullFlat` (still faster) |
| Large unions (>20 children), low overlap | `UnionFullHeap` or `UnionQuickHeap` |
| Need first result quickly | `UnionQuickFlat` or `UnionQuickHeap` |

## C Comparison: Heap Operations

### C Implementation (`src/iterators/union_iterator.c`)

The C union iterator uses a custom heap (`src/util/heap.h`) with:

| Operation | C Function | Complexity |
|-----------|------------|------------|
| View root | `heap_peek()` | O(1) |
| Remove root | `heap_poll()` | O(log n) |
| Replace root | `heap_replace()` | O(log n) |
| Iterate equal roots | `heap_cb_root()` | O(k) |

### Rust Equivalent

Our `DocIdMinHeap` provides equivalent operations:

| C Function | Rust Method | Notes |
|------------|-------------|-------|
| `heap_peek()` | `peek()` | Direct equivalent |
| `heap_poll()` | `pop()` | Direct equivalent |
| `heap_replace()` | `replace_root()` | In-place with single sift-down |
| `heap_cb_root()` | `for_each_root()` | Recursive traversal without modification |

## Implementation Details

### UnionFlat Algorithm

1. **Find minimum**: Scan all non-EOF children for minimum `last_doc_id` - O(n)
2. **Advance matching**: For each child at minimum, call `read()` to advance
3. **Build result**: Aggregate results from all children (Full mode) or first child (Quick mode)

### UnionHeap Algorithm

1. **Find minimum**: Peek heap root - O(1)
2. **Advance matching**: For each child at minimum:
   - Use `replace_root()` if child advances successfully - O(log n)
   - Use `pop()` if child reaches EOF - O(log n)
3. **Build result**: Use `for_each_root()` to iterate matching children - O(k)

### Quick Exit Optimization

Quick mode avoids expensive result aggregation:
- **Full mode**: Collects results from ALL matching children
- **Quick mode**: Returns immediately after finding ANY matching child

This is a significant optimization when only existence (not aggregation) matters.

## References

- Union iterator: `src/redisearch_rs/rqe_iterators/src/union.rs`
- Custom heap: `src/redisearch_rs/rqe_iterators/src/util/min_heap.rs`
- C union iterator: `src/iterators/union_iterator.c`
- C heap: `src/util/heap.h`, `src/util/heap.c`
- Benchmarks: `src/redisearch_rs/rqe_iterators_bencher/src/benchers/union.rs`

