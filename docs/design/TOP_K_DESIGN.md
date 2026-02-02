# Top-K Iterator Design Document

> **Status:** Draft - Seeking Feedback
> **Last Updated:** February 2026
> **Authors:** RediSearch Team

---

## Table of Contents

1. [TL;DR](#tldr)
2. [Problem Statement](#problem-statement)
3. [Background: Current C Implementations](#background-current-c-implementations)
   - [Hybrid Iterator](#hybrid-iterator-hybrid_readerc)
   - [Optimizer Iterator](#optimizer-iterator-optimizer_readerc)
4. [Analysis: Shared Patterns](#analysis-shared-patterns)
5. [Proposed Design](#proposed-design)
   - [Core Trait: ScoreSource](#core-trait-scoresource)
   - [TopK Iterator Struct](#topk-iterator-struct)
   - [Execution Modes](#execution-modes)
6. [Design Decisions](#design-decisions)
7. [Concrete Implementations](#concrete-implementations)
   - [VectorScoreSource (Hybrid)](#vectorscoresource-hybrid)
   - [NumericScoreSource (Optimizer)](#numericscoresource-optimizer)
8. [Open Questions for Review](#open-questions-for-review)
9. [Implementation Plan](#implementation-plan)
10. [References](#references)

---

## TL;DR

We're porting two C iterators (`HybridIterator` and `OptimizerIterator`) to Rust. After analysis, we discovered they share the same fundamental algorithm with three execution modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| **Unfiltered** | Iterate source directly, collect top-k | Pure KNN / Pure SORTBY |
| **Batches** | Get batch from source → intersect with child filter | Hybrid vector search / Filtered SORTBY |
| **Adhoc-BF** | Iterate child filter → lookup score per doc | Small filter selectivity |

**Proposed design:** A `ScoreSource` trait that abstracts the score provider, and a single generic `TopKIterator<S: ScoreSource>` that implements the shared collection/intersection/yield logic.

```rust
pub trait ScoreSource<'index> {
    fn next_batch(&mut self) -> Result<Option<ScoreBatch>, RQEIteratorError>;
    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64>;
    fn num_estimated(&self) -> usize;
    fn rewind(&mut self);  // Called by TopK when CollectionStrategy::Rewind is returned
    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;
    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy;
}

pub enum CollectionStrategy {
    Continue,        // Keep iterating current batch sequence
    SwitchToAdhoc,   // Rewind child, switch to adhoc brute-force mode
    SwitchToBatches, // Source rewinds itself, TopK rewinds child, restart batches
    Stop,            // Collection complete
}
```

This keeps the shared logic in one place while allowing each source (Vector/Numeric) to handle its domain-specific details.

---

## Problem Statement

RediSearch has two C iterators that perform "top-k with optional filtering":

1. **`HybridIterator`** - Vector similarity search with optional query filter
2. **`OptimizerIterator`** - Numeric SORTBY with optional query filter

Both are complex, have known bugs (especially the optimizer), and need to be ported to Rust. Rather than porting them as-is, we want to:

1. **Identify shared logic** and implement it once
2. **Fix known bugs** in the optimizer's retry heuristics
3. **Enable future optimizations** through clean abstractions

### Key Insight

Both iterators solve the same problem: *"Find the top-k documents by some score, optionally filtered by a query predicate."*

The only differences are:
- **Score source**: Vector distance vs. Numeric field value
- **Score ordering**: Vector always ascending (lower distance = better), Numeric configurable

---

## Background: Current C Implementations

### Hybrid Iterator (`hybrid_reader.c`)

Performs vector similarity search with optional pre-filtering.

**Modes:**

| Mode | When Used | Algorithm |
|------|-----------|-----------|
| `STANDARD_KNN` | No child filter | Iterate VecSim results directly |
| `HYBRID_BATCHES` | With child filter, large result set | Fetch VecSim batch → intersect with child |
| `HYBRID_ADHOC_BF` | With child filter, small result set | Iterate child → compute distance per doc |

**Key characteristics:**
- Child iterator is **optional** (KNN mode has no child)
- Uses VecSim library for batched results sorted by distance
- Can switch modes mid-execution based on heuristics
- Min-max heap for top-k collection

### Optimizer Iterator (`optimizer_reader.c`)

Optimizes `SORTBY numeric_field` queries with optional filtering.

**Algorithm:**
1. Get a range of documents from numeric index (sorted by numeric value)
2. Intersect with child filter
3. If not enough results found, expand range and retry
4. Yield results sorted by numeric value

**Key characteristics:**
- Child iterator is **required** (current implementation)
- Uses numeric range iterator as source
- Configurable sort order (ASC/DESC)
- Has retry logic with "success ratio" heuristics

**Known issues:**
- The current implementation uses a hacky union iterator
- Retry heuristics are fragile and hard-coded
- Complex state management leads to bugs
- TODO comment: `VALIDATE_MOVED` not properly handled
- Potential division by zero in `getSuccessRatio` when `lastLimitEstimate == 0`

### Corrected Understanding (Optimizer)

The optimizer should work with **sorted numeric ranges** where each "batch" is a subset of ranges ordered by numeric value. The current union-based hack should be replaced with proper range-based iteration.

---

## Analysis: Shared Patterns

After analyzing both implementations, we identified these shared patterns:

| Aspect | Hybrid | Optimizer | Shared? |
|--------|--------|-----------|---------|
| **Unfiltered mode** | ✅ KNN (no child) | ✅ Pure SORTBY (should exist) | ✅ |
| **Batches mode** | ✅ VecSim batches | ✅ Numeric range subsets | ✅ |
| **Adhoc-BF mode** | ✅ Distance lookup | ✅ Value lookup | ✅ |
| **Top-k heap** | ✅ Min-max heap | ✅ Min/max heap | ✅ |
| **Two-phase execution** | ✅ Collect → Yield | ✅ Collect → Yield | ✅ |
| **Intersection algorithm** | ✅ Alternating skip_to | ✅ Alternating skip_to | ✅ |
| **Output order** | By score (unsorted by ID) | By score (unsorted by ID) | ✅ |
| **Strategy switching** | ✅ Batches → Adhoc | ✅ Expand range → Retry | ✅ |

**Conclusion:** The core algorithm is identical. Only the score source differs.

---

## Proposed Design

### Core Trait: `ScoreSource`

A minimal trait that abstracts the score provider:

```rust
/// A batch of (doc_id, score) pairs, sorted by doc_id within the batch.
/// Batches are ordered by score across the iteration.
pub struct ScoreBatch {
    // Implementation detail - could be Vec, iterator adapter, etc.
    entries: Vec<(t_docId, f64)>,
}

/// A source of scores for top-k collection.
///
/// Implementations provide batches of results where:
/// - Each batch is sorted by doc_id (for efficient intersection)
/// - Batches are ordered by score (best scores come first)
pub trait ScoreSource<'index> {
    /// Get the next batch of results.
    ///
    /// Returns `Ok(Some(batch))` if more results are available.
    /// Returns `Ok(None)` if exhausted.
    /// Returns `Err(TimedOut)` if timeout reached.
    fn next_batch(&mut self) -> Result<Option<ScoreBatch>, RQEIteratorError>;

    /// Lookup the score for a single document (for adhoc-BF mode).
    ///
    /// Returns `None` if the document doesn't exist in the source.
    /// Takes `&mut self` because some implementations may need to acquire locks.
    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64>;

    /// Estimated total number of results.
    fn num_estimated(&self) -> usize;

    /// Rewind to the beginning (for strategy retry).
    fn rewind(&mut self);

    /// Build an RSIndexResult from a doc_id and score.
    ///
    /// Implementations can return different result types (MetricResult, AggregateResult, etc.)
    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;

    /// Decide whether to continue, switch strategy, or stop.
    ///
    /// Called after each batch is processed. Takes `&mut self` so the source
    /// can update internal parameters before returning `Rewind`.
    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy;
}

/// Strategy decision for collection phase.
pub enum CollectionStrategy {
    /// Continue with current mode.
    Continue,
    /// Rewind child and switch to adhoc brute-force mode.
    SwitchToAdhoc,
    /// Rewind child and restart batches mode.
    ///
    /// The source should adjust its internal parameters (e.g., expand numeric range)
    /// AND call `self.rewind()` BEFORE returning this variant. TopK will then call
    /// `child.rewind()` before resuming batch collection.
    SwitchToBatches,
    /// Stop collection (enough results or exhausted).
    Stop,
}
```

### TopK Iterator Struct

A single generic struct that handles all modes:

```rust
/// Execution mode for top-k collection.
pub enum TopKMode {
    /// No child filter - collect directly from source.
    Unfiltered,
    /// Get batches from source, intersect with child.
    Batches,
    /// Iterate child, lookup scores in source.
    AdhocBF,
}

/// A top-k iterator that collects the best k results by score.
///
/// Results are yielded sorted by score, NOT by document ID.
/// This iterator can only be used at the root of a query tree.
pub struct TopKIterator<'index, S: ScoreSource<'index>> {
    source: S,
    child: Option<Box<dyn RQEIterator<'index> + 'index>>,
    mode: TopKMode,
    heap: TopKHeap<ScoredResult>,
    k: usize,
    compare: fn(f64, f64) -> Ordering,  // Score comparison

    // Execution state
    phase: Phase,

    // Output state (for RQEIterator impl)
    current: Option<RSIndexResult<'index>>,
    last_doc_id: t_docId,
    at_eof: bool,

    // Metrics (for profiling)
    metrics: TopKMetrics,
}

enum Phase {
    NotStarted,
    Collecting,
    Yielding,
}

pub struct TopKMetrics {
    pub num_batches: usize,
    pub strategy_switches: usize,
    pub total_comparisons: usize,
}
```

### Execution Modes

#### Unfiltered Mode (No Child)

```rust
fn collect_unfiltered(&mut self) -> Result<(), RQEIteratorError> {
    while let Some(batch) = self.source.next_batch()? {
        for (doc_id, score) in batch {
            self.heap.maybe_insert(doc_id, score);
        }
        // Early termination check
        if matches!(self.source.collection_strategy(self.heap.len(), self.k),
                    CollectionStrategy::Stop) {
            break;
        }
    }
    Ok(())
}
```

#### Batches Mode (With Child, Intersection)

```rust
fn collect_batches(&mut self) -> Result<(), RQEIteratorError> {
    let child = self.child.as_mut().unwrap();

    'outer: loop {
        while let Some(batch) = self.source.next_batch()? {
            child.rewind();
            self.intersect_batch_with_child(&batch, child)?;

            match self.source.collection_strategy(self.heap.len(), self.k) {
                CollectionStrategy::Continue => {}
                CollectionStrategy::SwitchToAdhoc => {
                    child.rewind();  // Need full child iteration for score lookups
                    self.metrics.strategy_switches += 1;
                    return self.collect_adhoc();
                }
                CollectionStrategy::SwitchToBatches => {
                    // Source has already adjusted parameters and rewound itself.
                    // Just rewind the child and restart batch collection.
                    child.rewind();
                    self.metrics.strategy_switches += 1;
                    continue 'outer;
                }
                CollectionStrategy::Stop => break 'outer,
            }
        }
        // Source exhausted without requesting rewind - we're done
        break;
    }
    Ok(())
}

fn intersect_batch_with_child(
    &mut self,
    batch: &ScoreBatch,
    child: &mut dyn RQEIterator
) -> Result<(), RQEIteratorError> {
    let mut batch_iter = batch.iter();
    let mut batch_entry = batch_iter.next();
    let mut child_result = child.read()?;

    while let (Some((batch_id, score)), Some(child_res)) = (batch_entry, child_result.as_ref()) {
        match batch_id.cmp(&child_res.doc_id) {
            Ordering::Equal => {
                self.heap.maybe_insert(*batch_id, *score);
                batch_entry = batch_iter.next();
                child_result = child.read()?;
            }
            Ordering::Greater => {
                // Child behind - skip forward
                child_result = child.skip_to(*batch_id)?.map(|o| o.into_result());
            }
            Ordering::Less => {
                // Batch behind - advance batch
                batch_entry = batch_iter.next();
            }
        }
    }
    Ok(())
}
```

#### Adhoc-BF Mode (With Child, Score Lookup)

```rust
fn collect_adhoc(&mut self) -> Result<(), RQEIteratorError> {
    let child = self.child.as_mut().unwrap();

    while let Some(result) = child.read()? {
        if let Some(score) = self.source.lookup_score(result.doc_id) {
            self.heap.maybe_insert(result.doc_id, score);
        }

        // Check if we should stop (heap full)
        if matches!(self.source.collection_strategy(self.heap.len(), self.k),
                    CollectionStrategy::Stop) {
            break;
        }
    }
    Ok(())
}
```

---

## Design Decisions

### D1: Single Struct with Mode Enum vs. Separate Structs

**Decision:** Single struct with runtime mode selection.

**Rationale:**
- The mode is determined by heuristics at runtime, not compile time
- Strategy switching requires changing mode mid-execution
- The hot loop is within each mode's method - no branching overhead
- Simpler API for callers

**Alternative considered:** Const generics `TopK<S, const HAS_CHILD: bool>` - rejected because mode can change during execution.

### D2: Strategy Switching

**Decision:** Source controls when to switch strategies via `collection_strategy()` method.

```rust
pub enum CollectionStrategy {
    Continue,
    SwitchToAdhoc,   // Rewind child, switch to adhoc
    SwitchToBatches, // Rewind source+child, restart batches
    Stop,
}

// In ScoreSource trait (note: &mut self to allow internal state updates):
fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy;
```

**Rationale:** The source has domain knowledge to make this decision:
- Hybrid: Based on batch size vs. child selectivity heuristics → `SwitchToAdhoc`
- Optimizer: Based on success ratio → `SwitchToBatches` after expanding numeric range

**Mode switch flows:**

*SwitchToAdhoc:*
1. Source detects low batch selectivity
2. Returns `CollectionStrategy::SwitchToAdhoc`
3. TopK rewinds child (need full iteration for lookups)
4. TopK switches to `collect_adhoc()`

*SwitchToBatches:*
1. Source detects need to retry (e.g., low success ratio)
2. Source adjusts internal parameters (e.g., expands range)
3. Source calls `self.rewind()` to reset iteration state
4. Returns `CollectionStrategy::SwitchToBatches`
5. TopK rewinds child
6. TopK restarts batch collection with new parameters

### D3: Comparator Handling

**Decision:** Function pointer passed at construction.

**Rationale:**
- Comparison only happens during heap operations (not hot intersection loop)
- Function pointers can be inlined by LLVM in many cases
- Simpler than const generics for ASC/DESC

### D4: Timeout Handling

**Decision:** Errors propagate through `Result` returns.

**Rationale:**
- Fits existing `RQEIteratorError` pattern
- Source checks timeout internally in `next_batch()` and `lookup_score()`
- No additional trait methods needed

### D5: Profile Integration

**Decision:** TopK exposes `TopKMetrics` struct that Profile can query.

**Rationale:** Allows capturing domain-specific metrics (batch count, strategy switches) without Profile knowing about TopK internals.

### D6: Naming

**Decision:** Rename the iterators to reflect what they actually do.

| Old Name (C) | New Name (Rust) | Rationale |
|--------------|-----------------|-----------|
| `hybrid_reader` / `HybridIterator` | `VectorTopKIterator` | "Hybrid" is an internal implementation detail (hybrid search modes). The iterator performs **vector similarity top-k**. |
| `optimizer_reader` / `OptimizerIterator` | `NumericTopKIterator` | "Optimizer" is vague and misleading. The iterator performs **numeric field top-k** (for SORTBY). |

**Concrete types:**
- `VectorTopKIterator` = `TopKIterator<VectorScoreSource>`
- `NumericTopKIterator` = `TopKIterator<NumericScoreSource>`

**Rationale:**
- Current names describe *how* they work internally, not *what* they do
- New names clearly indicate the score source (Vector vs Numeric) and purpose (Top-K)
- Consistent naming pattern makes the relationship between the two obvious
- Type aliases provide convenient names while sharing implementation

---

## Concrete Implementations

### VectorScoreSource (Hybrid)

```rust
pub struct VectorScoreSource {
    index: VecSimIndex,
    query_vector: Vec<f32>,
    query_params: VecSimQueryParams,
    batch_iterator: Option<VecSimBatchIterator>,
    timeout_ctx: TimeoutCtx,
}

impl<'index> ScoreSource<'index> for VectorScoreSource {
    fn next_batch(&mut self) -> Result<Option<ScoreBatch>, RQEIteratorError> {
        if self.timeout_ctx.is_expired() {
            return Err(RQEIteratorError::TimedOut);
        }

        let batch_iter = self.batch_iterator.get_or_insert_with(|| {
            VecSimBatchIterator::new(&self.index, &self.query_vector, &self.query_params)
        });

        if !batch_iter.has_next() {
            return Ok(None);
        }

        let reply = batch_iter.next(self.compute_batch_size());
        Ok(Some(ScoreBatch::from_vecsim_reply(reply)))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        // Used in adhoc-BF mode - may need to acquire locks on tiered index
        let distance = self.index.get_distance_from(doc_id, &self.query_vector);
        if distance.is_nan() { None } else { Some(distance) }
    }

    fn num_estimated(&self) -> usize {
        self.index.size().min(self.query_params.k)
    }

    fn rewind(&mut self) {
        self.batch_iterator = None;
    }

    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index> {
        // Returns MetricResult or AggregateResult depending on query requirements
        RSIndexResult::metric(doc_id, score)
    }

    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy {
        // Heuristic: switch to adhoc if batch selectivity is low
        if heap_count >= k {
            CollectionStrategy::Stop
        } else if self.should_switch_to_adhoc(heap_count, k) {
            CollectionStrategy::SwitchToAdhoc
        } else {
            CollectionStrategy::Continue
        }
    }
}
```

### NumericScoreSource (Optimizer)

```rust
pub struct NumericScoreSource<'index> {
    numeric_index: &'index NumericIndex,
    ranges: NumericRangeIterator,  // Sorted by value
    current_range_idx: usize,
    range_batch_size: usize,
    ascending: bool,
    timeout_ctx: TimeoutCtx,
}

impl<'index> ScoreSource<'index> for NumericScoreSource<'index> {
    fn next_batch(&mut self) -> Result<Option<ScoreBatch>, RQEIteratorError> {
        if self.timeout_ctx.is_expired() {
            return Err(RQEIteratorError::TimedOut);
        }

        // Get next subset of ranges based on current batch size
        let batch = self.ranges.next_n(self.range_batch_size)?;
        if batch.is_empty() {
            return Ok(None);
        }

        // Flatten ranges into (doc_id, value) pairs, sorted by doc_id
        Ok(Some(ScoreBatch::from_numeric_ranges(batch)))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        // Used in adhoc-BF mode
        self.numeric_index.get_value(doc_id)
    }

    fn num_estimated(&self) -> usize {
        self.ranges.total_docs_estimate()
    }

    fn rewind(&mut self) {
        self.current_range_idx = 0;
        // Optionally adjust range_batch_size for retry
    }

    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index> {
        RSIndexResult::numeric(doc_id, score)
    }

    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy {
        if heap_count >= k {
            return CollectionStrategy::Stop;
        }

        // Check if we should expand range and retry
        let success_ratio = self.compute_success_ratio(heap_count);
        if success_ratio < self.min_success_ratio && self.can_expand_range() {
            // Adjust parameters and rewind BEFORE returning SwitchToBatches
            self.expand_range();
            self.retry_count += 1;
            self.rewind();  // Reset iteration state
            return CollectionStrategy::SwitchToBatches;
        }

        CollectionStrategy::Continue
    }
}
```

---

## Open Questions for Review

We'd appreciate feedback on the following design questions:

### Q1: Trait Object vs. Monomorphization

**Current design:** `TopKIterator<S: ScoreSource<'index>>` uses static dispatch (monomorphization).

**Question:** Should we use `Box<dyn ScoreSource>` instead for smaller binary size and faster compilation?

**Trade-off:**
- Static dispatch: Better inlining, zero vtable overhead, but larger binary
- Dynamic dispatch: Smaller binary, but vtable overhead on every trait method call

### Q2: Batch Representation

**Current design:** `ScoreBatch` is a `Vec<(t_docId, f64)>`.

**Question:** Should we use a more abstract iterator-based design?

```rust
pub trait ScoreBatch {
    fn next(&mut self) -> Option<(t_docId, f64)>;
    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)>;
}
```

**Trade-off:**
- Vec: Simple, good cache locality, but requires allocation
- Iterator: Zero-copy from VecSim, but more complex API

### Q3: Result Building

**Current design:** Source provides `build_result(doc_id, score) -> RSIndexResult` method.

**Question:** Is this the right level of abstraction? Should we have separate methods for different result types?

```rust
// Alternative: more explicit methods
fn build_metric_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;
fn build_aggregate_result(&self, doc_id: t_docId, score: f64, child: &RSIndexResult) -> RSIndexResult<'index>;
```

**Trade-off:**
- Single method: Simpler trait, source decides internally
- Multiple methods: More explicit, but TopK needs to know which to call

### Q4: Const Generics for Performance-Critical Paths

**Question:** Should we use const generics to eliminate branches in hot paths?

```rust
pub struct TopKIterator<'index, S: ScoreSource, const ASC: bool, const FILTERED: bool> { ... }
```

**Trade-off:**
- More combinations to compile (2x2 = 4 variants)
- But guaranteed branch elimination in hot loops

---

## Current Test Coverage Analysis

### Existing C++ Unit Tests

| Test File | Coverage |
|-----------|----------|
| `test_cpp_parse_hybrid.cpp` | Parameter parsing (BATCH_SIZE, policy selection) |
| `test_cpp_index.cpp` | Core hybrid iterator behavior: all 3 modes, rewind, empty child, wildcard optimization |
| `test_cpp_benchmark_vecsim.cpp` | BATCHES vs ADHOC_BF recall comparison, rewind between iterations |
| `test_cpp_hybridrequest.cpp` | HybridRequest initialization |
| `test_cpp_hybridmerger.cpp` | Result processing, timeout simulation, error handling |

### Existing Python Flow Tests

| Test File | Coverage |
|-----------|----------|
| `test_profile.py` | Mode selection validation via `FT.PROFILE` (all modes including BATCHES_TO_ADHOC_BF) |
| `test_optimizer.py` | OptimizerIterator: Q_OPT_HYBRID, Q_OPT_PARTIAL_RANGE modes |
| `test_vecsim.py` | Hybrid query end-to-end, mode heuristics |
| `test_hybrid_timeout.py` | Timeout handling (FAIL/RETURN policies) |
| `test_hybrid_filter.py` | FILTER clause behavior |
| `test_hybrid.py` | FT.HYBRID VSIM component |
| `test_aggregate.py` | SORTBY with numeric optimization |

### Coverage Matrix by Mode

| Mode | C++ Unit | Python Flow |
|------|----------|-------------|
| **Unfiltered** (STANDARD_KNN) | ✅ | ✅ |
| **Batches** (HYBRID_BATCHES) | ✅ | ✅ |
| **Adhoc-BF** (HYBRID_ADHOC_BF) | ✅ | ✅ |
| **Strategy switch** (BATCHES→ADHOC) | ❌ | ✅ |
| **Optimizer batches** | ❌ | ✅ |
| **Optimizer retry** (SwitchToBatches) | ❌ | ❌ |

### Coverage Gaps in Current Implementation

| Gap | Severity | Notes |
|-----|----------|-------|
| **No C++ unit tests for OptimizerIterator** | High | Only Python flow tests exist |
| **No strategy switch unit tests** | Medium | Only validated via profile output, not internal state |
| **No rewind-during-batch tests** | Medium | Rewind only tested between full iterations |
| **No rewind-after-timeout tests** | Low | Edge case |
| **No concurrent access tests** | Low | Single-threaded iterator design |
| **Optimizer retry heuristics** | High | No tests validate expand-range-and-retry logic |

### Verified Code Analysis

#### HybridIterator (`hybrid_reader.c`) - Minor Gaps

| Gap | Location | Notes |
|-----|----------|-------|
| No overflow test for batch size | Line 261 | Uses `float` cast which limits risk, but no explicit test |
| No all-NaN test | Line 181 | Code handles correctly (`isnan` check), just untested |

#### OptimizerIterator (`optimizer_reader.c`) - Significant Issues

| Issue | Location | Severity |
|-------|----------|----------|
| **No C++ unit tests** | - | High - Only Python flow tests exist |
| **Division by zero bug** | Line 31: `getSuccessRatio` divides by `lastLimitEstimate` | High - Can be 0 when `successRatio` is very small (line 75) |
| **TODO: VALIDATE_MOVED** | Line 43 | Medium - Only checks ABORTED, not MOVED |

### Hardcoded Heuristics (Magic Numbers)

| Heuristic | Value | Location | Question |
|-----------|-------|----------|----------|
| Max optimizer iterations | `3` | `optimizer_reader.c:215` | Why 3? Should it adapt? |
| Success ratio "give up" | `0.01` | `optimizer_reader.c:210` | Is 1% the right threshold? |
| Success ratio rewind | `< 1.0` | `optimizer_reader.c:218` | Always rewind if any miss? |

### Recommended Validations for Port

1. **Fix division by zero in Rust:**
   ```rust
   fn get_success_ratio(&self) -> f64 {
       if self.last_limit_estimate == 0 {
           return 0.0; // or 1.0, depending on desired behavior
       }
       self.results_collected_since_last as f64 / self.last_limit_estimate as f64
   }
   ```

2. **Assertions to add in Rust:**
   ```rust
   debug_assert!(batch_size > 0, "batch size must be positive");
   debug_assert!(last_limit_estimate > 0, "division by zero guard");
   ```

3. **Invariants to test:**
   - Top-k heap never exceeds k elements
   - Results are always sorted by score
   - Intersection produces subset of both inputs
   - Rewind restores to initial state

---

## Testing Plan for Rust Implementation

### Unit Tests (Rust)

#### 1. TopKHeap Tests
```rust
#[test] fn test_heap_insert_maintains_top_k();
#[test] fn test_heap_with_ascending_comparator();
#[test] fn test_heap_with_descending_comparator();
#[test] fn test_heap_pop_order();
#[test] fn test_heap_capacity_limit();
```

#### 2. ScoreSource Trait Tests (Mock Implementation)
```rust
#[test] fn test_mock_source_batch_iteration();
#[test] fn test_mock_source_lookup_score();
#[test] fn test_mock_source_rewind();
#[test] fn test_mock_source_collection_strategy_stop();
#[test] fn test_mock_source_collection_strategy_switch_to_adhoc();
#[test] fn test_mock_source_collection_strategy_switch_to_batches();
```

#### 3. TopKIterator - Unfiltered Mode
```rust
#[test] fn test_unfiltered_collects_top_k();
#[test] fn test_unfiltered_early_termination_on_stop();
#[test] fn test_unfiltered_handles_empty_source();
#[test] fn test_unfiltered_timeout_propagates();
#[test] fn test_unfiltered_yields_sorted_by_score();
```

#### 4. TopKIterator - Batches Mode
```rust
#[test] fn test_batches_intersects_with_child();
#[test] fn test_batches_rewinds_child_per_batch();
#[test] fn test_batches_switch_to_adhoc();
#[test] fn test_batches_switch_to_batches_rewinds_both();
#[test] fn test_batches_handles_empty_child();
#[test] fn test_batches_handles_disjoint_batch_and_child();
#[test] fn test_batches_timeout_mid_intersection();
```

#### 5. TopKIterator - Adhoc-BF Mode
```rust
#[test] fn test_adhoc_iterates_child_lookups_scores();
#[test] fn test_adhoc_skips_docs_not_in_source();
#[test] fn test_adhoc_early_termination();
#[test] fn test_adhoc_handles_child_eof();
```

#### 6. RQEIterator Trait Implementation
```rust
#[test] fn test_read_triggers_collection_on_first_call();
#[test] fn test_read_yields_results_after_collection();
#[test] fn test_skip_to_not_supported_returns_error();
#[test] fn test_rewind_resets_to_not_started();
#[test] fn test_at_eof_after_all_results_yielded();
#[test] fn test_current_returns_last_yielded();
```

#### 7. VectorScoreSource Tests
```rust
#[test] fn test_vector_source_creates_batch_iterator_lazily();
#[test] fn test_vector_source_batch_sorted_by_doc_id();
#[test] fn test_vector_source_lookup_score_returns_distance();
#[test] fn test_vector_source_rewind_clears_batch_iterator();
#[test] fn test_vector_source_switch_to_adhoc_heuristic();
#[test] fn test_vector_source_timeout_check();
```

#### 8. NumericScoreSource Tests
```rust
#[test] fn test_numeric_source_iterates_ranges_by_value();
#[test] fn test_numeric_source_ascending_vs_descending();
#[test] fn test_numeric_source_lookup_returns_field_value();
#[test] fn test_numeric_source_expand_range_on_low_success_ratio();
#[test] fn test_numeric_source_switch_to_batches_rewinds_self();
#[test] fn test_numeric_source_max_retry_limit();
```

### Integration Tests (Rust)

#### 9. End-to-End with Real Index Structures
```rust
#[test] fn test_vector_topk_with_inverted_index_child();
#[test] fn test_vector_topk_with_intersection_child();
#[test] fn test_numeric_topk_with_tag_filter();
#[test] fn test_numeric_topk_ascending_descending();
```

### Python Flow Tests (Behavioral Parity)

#### 10. Parity Tests
Ensure new Rust implementation produces identical results to C:

```python
# Run same queries against C and Rust implementations
def test_vector_topk_parity_unfiltered():
def test_vector_topk_parity_with_filter():
def test_vector_topk_parity_mode_switching():
def test_numeric_topk_parity_ascending():
def test_numeric_topk_parity_descending():
def test_numeric_topk_parity_with_filter():
```

#### 11. Profile Mode Validation
```python
def test_rust_vector_topk_profile_shows_correct_mode():
def test_rust_numeric_topk_profile_shows_correct_mode():
def test_rust_topk_profile_shows_strategy_switches():
```

### Property-Based Tests (Optional, High Value)

```rust
#[test] fn prop_topk_always_returns_k_or_fewer_results();
#[test] fn prop_topk_results_sorted_by_score();
#[test] fn prop_topk_results_subset_of_child_intersection();
#[test] fn prop_batches_and_adhoc_produce_same_results();
```

### Microbenchmarks (Rust)

Standard iterator benchmarks using `criterion`:

```rust
// TopKHeap operations
fn bench_heap_insert(c: &mut Criterion);
fn bench_heap_pop_all(c: &mut Criterion);

// TopKIterator modes
fn bench_unfiltered_10k_docs(c: &mut Criterion);
fn bench_batches_10k_docs_1k_child(c: &mut Criterion);
fn bench_adhoc_10k_child(c: &mut Criterion);

// Strategy switching overhead
fn bench_switch_batches_to_adhoc(c: &mut Criterion);
fn bench_switch_to_batches_with_rewind(c: &mut Criterion);

// Comparison with C (via FFI)
fn bench_rust_vs_c_unfiltered(c: &mut Criterion);
fn bench_rust_vs_c_batches(c: &mut Criterion);
```

### Test Priority

| Priority | Test Category | Rationale |
|----------|---------------|-----------|
| **P0** | Unfiltered mode | Simplest path, validates core heap logic |
| **P0** | Batches mode intersection | Most common hybrid path |
| **P0** | Parity tests | Must match C behavior |
| **P1** | Adhoc-BF mode | Less common but critical |
| **P1** | Strategy switching | New SwitchToBatches needs coverage |
| **P1** | NumericScoreSource | Currently undertested in C |
| **P2** | Timeout handling | Edge case |
| **P2** | Property-based | High value but more effort |

---

## Implementation Plan

1. **Phase 1: Core Infrastructure**
   - [ ] Implement `TopKHeap` utility
   - [ ] Define `ScoreSource` trait
   - [ ] Implement `TopKIterator` struct with `RQEIterator` impl
   - [ ] Unit tests: heap, mock source, all 3 modes

2. **Phase 2: Vector Source**
   - [ ] Implement `VectorScoreSource`
   - [ ] FFI bridge for C integration
   - [ ] Port hybrid iterator C++ tests to Rust
   - [ ] Parity tests against C implementation

3. **Phase 3: Numeric Source**
   - [ ] Implement `NumericScoreSource` with proper range iteration
   - [ ] FFI bridge for C integration
   - [ ] **New tests for retry/SwitchToBatches** (currently untested in C)
   - [ ] Parity tests against C implementation

4. **Phase 4: Integration & Profiling**
   - [ ] Profile iterator integration
   - [ ] Performance benchmarks (compare to C)
   - [ ] Property-based tests
   - [ ] Python flow test parity validation

---

## References

- [iterator_api.h](../../src/iterators/iterator_api.h) - C iterator API
- [lib.rs](../../src/redisearch_rs/rqe_iterators/src/lib.rs) - Rust `RQEIterator` trait
- [metric.rs](../../src/redisearch_rs/rqe_iterators/src/metric.rs) - Example of unsorted iterator
- [hybrid_reader.c](../../src/iterators/hybrid_reader.c) - Current C hybrid implementation
- [optimizer_reader.c](../../src/iterators/optimizer_reader.c) - Current C optimizer implementation
