# Numeric Index Porting Plan

## Overview

The `numeric_index` module implements a numeric range tree data structure used for secondary indexing of numeric fields in RediSearch. It provides efficient range queries over numeric values by organizing documents into a balanced tree of ranges.

## C Module Analysis

### Header File (`src/numeric_index.h`)

#### Exposed Types

1. **`NumericRange`** - A leaf-level storage unit containing:
   - `minVal`, `maxVal` (double) - range boundaries
   - `hll` (struct HLL) - HyperLogLog for cardinality estimation
   - `invertedIndexSize` (size_t) - memory tracking
   - `entries` (InvertedIndex*) - the actual numeric data

2. **`NumericRangeNode`** - Binary tree node containing:
   - `value` (double) - split point
   - `maxDepth` (int) - for balancing
   - `left`, `right` pointers - children
   - `range` (NumericRange*) - optional data (NULL for inner nodes without retained ranges)

3. **`NumericRangeTree`** - Root tree container with:
   - `root` (NumericRangeNode*)
   - Metadata: `numRanges`, `numLeaves`, `numEntries`, `invertedIndexesSize`
   - `lastDocId` (t_docId) - for duplicate detection
   - `revisionId` (uint32_t) - for concurrent modification detection
   - `uniqueId` (uint32_t) - tree identifier
   - `emptyLeaves` (size_t) - tracking for GC

4. **`NumericRangeTreeIterator`** - Stack-based DFS iterator
   - `nodesStack` (NumericRangeNode**) - array-based stack

5. **`NRN_AddRv`** - Return value for Add/Free operations with delta tracking

#### Public API

| Function | Description |
|----------|-------------|
| `NewNumericRangeTree()` | Create a new tree |
| `NumericRangeTree_Add()` | Add a (docId, value) pair |
| `NumericRangeTree_Find()` | Find ranges matching a filter |
| `NumericRangeTree_Free()` | Free the entire tree |
| `NumericRangeTree_TrimEmptyLeaves()` | GC helper |
| `NumericRange_GetCardinality()` | Get estimated cardinality |
| `NumericIndexType_MemUsage()` | Calculate memory usage |
| `NumericRangeTreeIterator_*` | Iterator functions |
| `NewNumericFilterIterator()` | Create query iterator |
| `openNumericOrGeoIndex()` | Open/create index on field |

### Dependencies Analysis

#### Types from Other Modules

1. **`InvertedIndex`** - Already ported to Rust in `inverted_index` crate
   - Used via FFI: `InvertedIndex_WriteNumericEntry`, `InvertedIndex_NumDocs`, `InvertedIndex_NumEntries`

2. **`struct HLL`** (from `hll/hll.h`) - Already ported to Rust in `hyperloglog` crate
   - C API: `hll_init`, `hll_destroy`, `hll_add`, `hll_count`
   - Rust equivalent: `HyperLogLog6` with `CFnvHasher`

3. **`NumericFilter`** (from `numeric_filter.h`) - C-only type describing query filters
   - Used for `Find` operation and iterator creation
   - Contains: min, max, inclusivity flags, ascending/descending, offset/limit

4. **`Vector`** (from `rmutil/vector.h`) - Legacy dynamic array
   - Used only in `NumericRangeTree_Find` return value
   - Should be replaced with Rust `Vec` in pure Rust implementation

5. **`IndexReader`** (from inverted_index) - Already in Rust
   - Used for reading entries during node splitting

6. **Global Config** - `RSGlobalConfig.numericTreeMaxDepthRange`
   - Controls when inner nodes stop retaining ranges
   - Will need FFI access or configuration injection

#### External Interactions

- `RedisSearchCtx`, `IndexSpec`, `FieldSpec` - Redis module context types
- `QueryIterator` - Query execution infrastructure
- Memory allocation via `rm_malloc`/`rm_free`

### Key Algorithms

1. **Adaptive Splitting**: Nodes split when cardinality (HLL-estimated) exceeds threshold based on depth
   - Threshold formula: `NR_MINRANGE_CARD << (depth * 2)` capped at `NR_MAXRANGE_CARD`
   - Or when entries exceed `NR_MAXRANGE_SIZE` and cardinality > 1

2. **Median-based Splitting**: Uses heap-based algorithm to find median for split point

3. **AVL-style Balancing**: Tree rebalances when depth difference exceeds `NR_MAX_DEPTH_BALANCE`

4. **Range Retention**: Inner nodes may retain their ranges for efficiency up to `numericTreeMaxDepthRange`

## Porting Strategy

### Phase 1: Pure Rust Implementation

Create a new `numeric_range_tree` crate with:

1. **Core Data Structures**
   ```rust
   pub struct NumericRange {
       min_val: f64,
       max_val: f64,
       hll: HyperLogLog6<CFnvHasher>,
       entries: EntriesTrackingIndex<Numeric>,  // Use existing Rust type
   }

   pub struct NumericRangeNode {
       value: f64,
       max_depth: i32,
       left: Option<Box<NumericRangeNode>>,
       right: Option<Box<NumericRangeNode>>,
       range: Option<NumericRange>,
   }

   pub struct NumericRangeTree {
       root: NumericRangeNode,
       // ... metadata
   }
   ```

2. **Key Considerations**
   - The `InvertedIndex` type is already implemented in Rust
   - The `HyperLogLog` type already exists with compatible precision (6 bits = 64 registers, ~13% error)
   - Tree operations should be idiomatic Rust using `Option<Box<_>>` for child pointers

### Phase 2: C Modifications (Pre-Porting)

Before porting, consider these C-side changes to ease transition:

1. **Encapsulate Field Access**: The `NumericRange` and `NumericRangeNode` structs have their fields directly accessed throughout. Consider:
   - Adding accessor functions if needed for gradual migration
   - Keeping the FFI layer thin since we're doing a full replacement

2. **Iterator Return Type**: `NumericRangeTree_Find` returns a `Vector*` which is a C-only type. The FFI layer will need to convert this.

### Phase 3: FFI Bridge

Create `numeric_range_tree_ffi` crate exposing:

```c
// Types
typedef struct NumericRangeTree NumericRangeTree;
typedef struct NumericRangeTreeIterator NumericRangeTreeIterator;
typedef struct NumericRange NumericRange;
typedef struct NRN_AddRv NRN_AddRv;

// Core API
NumericRangeTree* NewNumericRangeTree();
NRN_AddRv NumericRangeTree_Add(NumericRangeTree* t, t_docId docId, double value, int isMulti);
Vector* NumericRangeTree_Find(NumericRangeTree* t, const NumericFilter* nf);
void NumericRangeTree_Free(NumericRangeTree* t);
// ... etc
```

### Phase 4: Integration

1. Remove `numeric_index.c` and `numeric_index.h`
2. Update all C files that include `numeric_index.h` to use the new Rust header
3. Run full test suite

## Testing Strategy

### Existing C++ Tests

Located in `tests/cpptests/`:
- `test_cpp_range.cpp` - Range tree operations
- `test_cpp_iterator_index.cpp` - Iterator tests
- `test_cpp_forkgc.cpp` - GC tests involving numeric indices
- `test_cpp_llapi.cpp` - Low-level API tests
- `index_utils.cpp/h` - Test utilities

### Required Rust Tests

1. **Unit Tests** (in `numeric_range_tree/tests/`)
   - Node creation and basic operations
   - Range containment and overlap logic
   - HLL cardinality updates
   - Tree balancing after insertions
   - Median computation for splits
   - Memory tracking accuracy

2. **Property-Based Tests** (using `proptest`)
   - Invariants: balanced tree, correct min/max in ranges
   - Insert/find consistency
   - Cardinality estimation accuracy bounds

3. **Integration Tests**
   - End-to-end with InvertedIndex reads
   - Concurrent modification detection (revisionId)
   - GC trim operations

### Test Migration

Ensure all C++ tests have equivalent Rust tests before removing C implementation.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Memory layout differences | Use `repr(C)` where needed for FFI compatibility |
| Global config access | Pass config values via FFI or use a config injection pattern |
| Iterator protocol differences | Carefully match semantics; the current iterator is simple DFS |
| Performance regression | Benchmark critical paths before/after; optimize hot loops |
| Concurrent access patterns | Preserve revisionId semantics for iteration safety |

## File Structure

```
src/redisearch_rs/
├── numeric_range_tree/           # Pure Rust implementation
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── range.rs              # NumericRange
│   │   ├── node.rs               # NumericRangeNode
│   │   ├── tree.rs               # NumericRangeTree
│   │   └── iter.rs               # Iterator
│   └── tests/
│       ├── unit.rs
│       └── proptest.rs
└── c_entrypoint/
    └── numeric_range_tree_ffi/   # FFI wrapper
        ├── Cargo.toml
        └── src/
            └── lib.rs
```

## Open Questions

1. **Query Iterator Creation**: `NewNumericFilterIterator` creates query execution iterators. This involves significant integration with the query engine. Should we:
   - Keep this function in C, calling into Rust for data access?
   - Port it fully to Rust?

   **Recommendation**: Keep in C initially, port later when more query infrastructure is in Rust.

2. **`openNumericOrGeoIndex`**: This function interacts with `IndexSpec` and `FieldSpec` which are C types. It should remain as a thin C wrapper or be ported when those types are available in Rust.

3. **Vector Return Type**: `NumericRangeTree_Find` returns `Vector*`. Options:
   - Return a Rust-allocated array and provide a free function
   - Keep a small C shim that converts Rust `Vec` to C `Vector`

   **Recommendation**: Return a Rust-allocated slice with length, provide FFI functions to access elements and free.

## Next Steps

1. [ ] Create `numeric_range_tree` crate skeleton
2. [ ] Implement `NumericRange` with HLL integration
3. [ ] Implement `NumericRangeNode` with split logic
4. [ ] Implement `NumericRangeTree` with balancing
5. [ ] Write comprehensive unit tests
6. [ ] Create `numeric_range_tree_ffi` crate
7. [ ] Wire up C code to use Rust implementation
8. [ ] Run full test suite and benchmark
