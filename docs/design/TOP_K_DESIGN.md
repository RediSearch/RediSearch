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

We're porting two C iterators (`HybridIterator` and `OptimizerIterator`) to Rust. After analysis, we discovered they share the same collection/yield skeleton, with source-specific strategy switching:

| Mode | Description | Use Case |
|------|-------------|----------|
| **Unfiltered** | Iterate source directly, collect top-k | Pure KNN / Pure SORTBY |
| **Batches** | Get batch from source → intersect with child filter | Hybrid vector search / Filtered SORTBY |
| **Adhoc-BF** | Iterate child filter → lookup score per doc | Vector source only (small filter selectivity) |

**Proposed design:** A `ScoreSource` trait that abstracts the score provider, and a single generic `TopKIterator<S: ScoreSource>` that implements the shared collection/intersection/yield logic.

```rust
pub trait ScoreBatch {
    fn next(&mut self) -> Option<(t_docId, f64)>;
    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)>;
}

pub trait ScoreSource<'index> {
    type Batch: ScoreBatch;
    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError>;
    // Optional in practice: sources that never switch to adhoc can return None.
    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64>;
    fn num_estimated(&self) -> usize;
    fn rewind(&mut self);  // Called by TopK when CollectionStrategy::Rewind is returned
    fn build_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;
    fn collection_strategy(&mut self, heap_count: usize, k: usize) -> CollectionStrategy;
}

pub enum CollectionStrategy {
    Continue,        // Keep iterating current batch sequence
    SwitchToAdhoc,   // Rewind child, switch to adhoc brute-force mode (Vector in v1)
    SwitchToBatches, // Source rewinds itself, TopK rewinds child, restart batches
    Stop,            // Collection complete
}
```

This keeps the shared logic in one place while allowing each source (Vector/Numeric) to handle its domain-specific details.

**Delivery strategy:** We will integrate in stages to reduce risk:
1. Port vector and numeric behavior with clear source-specific semantics.
2. Keep shared Top-K collection/intersection/yield logic in one Rust component.
3. Defer deeper specialization (const generics, result-builder split) until after parity/perf validation.

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
- No adhoc score-lookup mode in current implementation (retry is range expansion + rewind)

**Known issues:**
- The current implementation uses a hacky union iterator
- Retry heuristics are fragile and hard-coded
- Complex state management leads to bugs
- TODO comment: `VALIDATE_MOVED` not properly handled
- Potential division by zero in `getSuccessRatio` when `lastLimitEstimate == 0`

### Corrected Understanding (Optimizer)

The optimizer should work with **sorted numeric ranges** where each "batch" is a subset of ranges ordered by numeric value. The current union-based hack should be replaced with proper range-based iteration.

Behavioral expectation for the Rust port:
- Query semantics should remain the same (same matching and top-k ordering rules for ASC/DESC).
- Internal control flow changes (clean range iteration + explicit retries) should not be externally visible, except for bug fixes.
- Known bug fixes are expected outcomes, not breaking changes (e.g., safer retry math and cleaner revalidation paths).

---

## Analysis: Shared Patterns

After analyzing both implementations, we identified these shared patterns:

| Aspect | Hybrid | Optimizer | Shared? |
|--------|--------|-----------|---------|
| **Unfiltered mode** | ✅ KNN (no child) | ✅ Pure SORTBY (should exist) | ✅ |
| **Batches mode** | ✅ VecSim batches | ✅ Numeric range subsets | ✅ |
| **Adhoc-BF mode** | ✅ Distance lookup | ❌ Not in current implementation (possible future extension) | ⚠️ Partial |
| **Top-k heap** | ✅ Used for filtered modes | ✅ Used for filtered modes | ✅ |
| **Two-phase execution** | ⚠️ Optional (unfiltered can stream directly) | ⚠️ Optional (unfiltered can stream directly) | ✅ |
| **Intersection algorithm** | ✅ Alternating skip_to | ✅ Alternating skip_to | ✅ |
| **Output order** | By score (unsorted by ID) | By score (unsorted by ID) | ✅ |
| **Strategy switching** | ✅ Batches → Adhoc | ✅ Expand range → Retry | ✅ |

**Conclusion:** The collection/intersection skeleton is shared, with source-specific strategies and an unfiltered direct-yield fast path.

---

## Proposed Design

### Core Trait: `ScoreSource`

A minimal trait that abstracts the score provider:

```rust
/// A cursor over a score-ordered batch.
///
/// The cursor yields entries in score order.
/// For filtered intersection, it must also support doc-id navigation via `skip_to`.
pub trait ScoreBatch {
    fn next(&mut self) -> Option<(t_docId, f64)>;
    /// Forward-only skip, equivalent to `RQEIterator::skip_to` semantics:
    /// - Returns the first entry with doc_id >= target from the current position.
    /// - Returns `None` if no such entry exists (cursor becomes exhausted).
    /// - If it returns `Some(entry)`, a subsequent `next()` returns the following entry.
    /// Implementations may use auxiliary state/indexes internally.
    fn skip_to(&mut self, target: t_docId) -> Option<(t_docId, f64)>;
}

/// A source of scores for top-k collection.
///
/// Implementations provide batches of results where:
/// - Each batch is ordered by score (best score first for that source/order)
/// - Batch cursors support `skip_to(doc_id)` for filtered intersection
pub trait ScoreSource<'index> {
    type Batch: ScoreBatch;

    /// Get the next batch of results.
    ///
    /// Returns `Ok(Some(batch))` if more results are available.
    /// Returns `Ok(None)` if exhausted.
    /// Returns `Err(TimedOut)` if timeout reached.
    ///
    /// Unfiltered contract:
    /// - Sources should emit a single final score-ordered batch (up to k docs), then `None`.
    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError>;

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
    /// No child filter - yield directly from source batch cursor.
    Unfiltered,
    /// Get batches from source, intersect with child.
    Batches,
    /// Iterate child, lookup scores in source.
    AdhocBF,
}

/// A top-k iterator that returns the best k results by score.
///
/// Results are yielded sorted by score, NOT by document ID.
/// This iterator can only be used at the root of a query tree.
pub struct TopKIterator<'index, S: ScoreSource<'index>> {
    source: S,
    child: Option<Box<dyn RQEIterator<'index> + 'index>>,
    mode: TopKMode,
    heap: TopKHeap<ScoredResult>,
    direct_batch: Option<S::Batch>, // Used by unfiltered direct-yield path
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
    YieldingDirect,
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
fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
    // In unfiltered mode, source returns a single final score-ordered batch.
    self.direct_batch = self.source.next_batch()?;
    self.phase = Phase::YieldingDirect;
    Ok(())
}

fn read_unfiltered_direct(&mut self) -> Result<Option<RSIndexResult<'index>>, RQEIteratorError> {
    let Some(batch) = self.direct_batch.as_mut() else {
        return Ok(None);
    };

    if let Some((doc_id, score)) = batch.next() {
        return Ok(Some(self.source.build_result(doc_id, score)));
    }

    // Optional sanity check: sources should be exhausted after the final batch in this mode.
    if cfg!(debug_assertions) {
        debug_assert!(matches!(self.source.next_batch()?, None));
    }
    self.direct_batch = None;
    Ok(None)
}
```

#### Batches Mode (With Child, Intersection)

```rust
fn collect_batches(&mut self) -> Result<(), RQEIteratorError> {
    let child = self.child.as_mut().unwrap();

    'outer: loop {
        while let Some(mut batch) = self.source.next_batch()? {
            child.rewind();
            self.intersect_batch_with_child(&mut batch, child)?;

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

fn intersect_batch_with_child<B: ScoreBatch>(
    &mut self,
    batch: &mut B,
    child: &mut dyn RQEIterator
) -> Result<(), RQEIteratorError> {
    let mut batch_entry = batch.next();
    let mut child_result = child.read()?;

    while let (Some((batch_id, score)), Some(child_res)) = (batch_entry, child_result.as_ref()) {
        match batch_id.cmp(&child_res.doc_id) {
            Ordering::Equal => {
                self.heap.maybe_insert(batch_id, score);
                batch_entry = batch.next();
                child_result = child.read()?;
            }
            Ordering::Greater => {
                // Child behind - skip forward
                child_result = child.skip_to(batch_id)?.map(|o| o.into_result());
            }
            Ordering::Less => {
                // Batch behind - advance batch
                batch_entry = batch.skip_to(child_res.doc_id).or_else(|| batch.next());
            }
        }
    }
    Ok(())
}
```

#### Adhoc-BF Mode (Vector Source in v1)

```rust
fn collect_adhoc(&mut self) -> Result<(), RQEIteratorError> {
    // Used by sources that support per-doc score lookup (Vector in v1).
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
- In v1, `NumericScoreSource` will not emit `SwitchToAdhoc`

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

**Decision:** Function pointer passed at construction, with deterministic tie-breaking by `doc_id` ascending (independent of ASC/DESC score direction).

**Rationale:**
- Comparison only happens during heap operations (not hot intersection loop)
- Function pointers can be inlined by LLVM in many cases
- Simpler than const generics for ASC/DESC
- Stable output ordering for equal scores

### D4: Unfiltered Fast Path

**Decision:** In `Unfiltered` mode, bypass heap collection and yield directly from a single final score-ordered batch cursor.

**Rationale:**
- Both vector and numeric sources can provide final sorted results directly in unfiltered queries.
- Avoids unnecessary heap maintenance and collection phase when there is no child intersection.
- Keeps heap-based logic focused on filtered modes (`Batches`, `AdhocBF`).

### D5: Timeout Handling

**Decision:** Errors propagate through `Result` returns.

**Rationale:**
- Fits existing `RQEIteratorError` pattern
- Source checks timeout internally in `next_batch()` and `lookup_score()`
- No additional trait methods needed

### D6: Profile Integration

**Decision:** TopK exposes `TopKMetrics` struct that Profile can query.

**Rationale:** Allows capturing domain-specific metrics (batch count, strategy switches) without Profile knowing about TopK internals.

### D7: Naming

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

### D8: Dispatch Strategy

**Decision:** Use static dispatch (`TopKIterator<S: ScoreSource<'index>>`) for the initial implementation.

**Rationale:**
- Matches existing RediSearch Rust style (prefer static dispatch where concrete types are known at iterator-tree build time).
- Keeps call sites simple and avoids vtable overhead in frequently invoked trait methods.
- If compile time or binary size becomes an issue, dynamic dispatch can be evaluated with benchmarks later.

### D9: Batch Cursor Ownership (v1)

**Decision:** Use owning batch cursors in v1.

**Rationale:**
- Simpler and safer lifetime model across Rust/C FFI boundaries.
- Reduces integration risk for initial rollout.
- Borrowed-cursor optimizations can be evaluated later using benchmark data.

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
    type Batch = VecSimScoreBatchCursor;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
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
        Ok(Some(VecSimScoreBatchCursor::new(reply)))
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
    ranges: NumericRangeIterator,  // Yielded in score order according to `ascending`
    current_range_idx: usize,
    range_batch_size: usize,
    ascending: bool, // Set at construction from SORTBY ASC/DESC
    timeout_ctx: TimeoutCtx,
}

impl<'index> NumericScoreSource<'index> {
    pub fn new(
        numeric_index: &'index NumericIndex,
        ranges: NumericRangeIterator,
        ascending: bool, // from query's SORTBY direction
        range_batch_size: usize,
        timeout_ctx: TimeoutCtx,
    ) -> Self {
        Self {
            numeric_index,
            ranges,
            current_range_idx: 0,
            range_batch_size,
            ascending,
            timeout_ctx,
        }
    }
}

impl<'index> ScoreSource<'index> for NumericScoreSource<'index> {
    type Batch = NumericRangeBatchCursor;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        if self.timeout_ctx.is_expired() {
            return Err(RQEIteratorError::TimedOut);
        }

        // Get next subset of ranges based on current batch size
        let batch = self.ranges.next_n(self.range_batch_size)?;
        if batch.is_empty() {
            return Ok(None);
        }

        Ok(Some(NumericRangeBatchCursor::new(batch)))
    }

    fn lookup_score(&mut self, doc_id: t_docId) -> Option<f64> {
        // Numeric adhoc lookup is not used in v1 strategy selection.
        // Keep API compatibility for possible future extension.
        let _ = doc_id;
        None
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

        // Numeric source does not switch to adhoc in v1.
        CollectionStrategy::Continue
    }
}
```

---

## Open Questions for Review

Most high-level API choices are now fixed for v1:
- Static dispatch for `TopKIterator<S: ScoreSource<'index>>`
- Iterator-first batch API (`ScoreBatch` cursor, not `Vec<(doc_id, score)>`)
- Vector supports `SwitchToAdhoc`; Numeric uses batches+retry only
- Equal-score tie-breaker is `doc_id` ascending
- Batch cursors are owning in v1

The remaining questions are intentionally deferred until after parity and baseline benchmarks.

### Q1: Result Building

**Current design:** Source provides `build_result(doc_id, score) -> RSIndexResult` method.

**Deferred question:** Is this the right level of abstraction, or should we split into result-type-specific methods?

```rust
// Alternative: more explicit methods
fn build_metric_result(&self, doc_id: t_docId, score: f64) -> RSIndexResult<'index>;
fn build_aggregate_result(&self, doc_id: t_docId, score: f64, child: &RSIndexResult) -> RSIndexResult<'index>;
```

**Trade-off:**
- Single method: Simpler trait, source decides internally
- Multiple methods: More explicit, but TopK needs to know which to call

### Q2: Const Generics for Performance-Critical Paths

**Deferred question:** Should we use const generics to eliminate branches in hot paths?

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
   - Top-k heap never exceeds k elements (filtered modes)
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
#[test] fn test_unfiltered_direct_yield_no_heap_collection();
#[test] fn test_unfiltered_consumes_single_final_batch();
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
| **P0** | Unfiltered mode | Simplest path, validates direct-yield fast path |
| **P0** | Batches mode intersection | Most common hybrid path |
| **P0** | Parity tests | Must match C behavior |
| **P1** | Adhoc-BF mode (Vector) | Less common but critical for hybrid queries |
| **P1** | Strategy switching | New SwitchToBatches needs coverage |
| **P1** | NumericScoreSource | Currently undertested in C |
| **P2** | Timeout handling | Edge case |
| **P2** | Property-based | High value but more effort |

---

## Implementation Plan

### Delivery Order

1. Build stable shared primitives and state machine.
2. Integrate vector path to parity (including BATCHES→ADHOC).
3. Integrate numeric path to parity (batches+retry only).
4. Validate parity/profile output, then optimize.

### Ticket Breakdown (Digestible Tasks)

| ID | Task | Dependencies | Done Criteria |
|----|------|--------------|---------------|
| `T1` | **Implement `TopKHeap` utility** with ASC/DESC comparator support and capacity guarantees. | - | Unit tests for insert/replace/pop/capacity pass. |
| `T2` | **Implement `TopKIterator` state machine skeleton** (`NotStarted`, `Collecting`, `Yielding`, `YieldingDirect`) and iterator lifecycle (`read`, `rewind`, `at_eof`, `current`). | `T1` | Iterator lifecycle tests pass; no mode-specific logic yet. |
| `T3` | **Implement unfiltered direct-yield path** (no heap collection, consume single final batch cursor directly). | `T2` | Unfiltered tests pass, no heap mutation in unfiltered mode. |
| `T4` | **Implement batches intersection engine** (alternating `read/skip_to`, child rewind per batch, strategy hooks). | `T2`, `T1` | Filtered intersection tests pass for disjoint/overlap/empty child cases. |
| `T5` | **Implement adhoc-BF collection path** in `TopKIterator` (generic path used by vector source in v1). | `T2`, `T1` | Adhoc mode tests pass; early stop semantics validated. |
| `T6` | **Implement `VectorScoreSource` batch cursor adapter** (`VecSimScoreBatchCursor`) and strategy logic (`Continue`, `SwitchToAdhoc`, `Stop`). | `T4`, `T5` | Vector unit tests + mode-switch tests pass; timeout behavior preserved. |
| `T7` | **Integrate vector iterator path end-to-end** behind existing query planning path. | `T3`, `T4`, `T5`, `T6` | Existing hybrid flow tests/parity checks pass with Rust path enabled. |
| `T8` | **Implement `NumericScoreSource` batch cursor adapter** (`NumericRangeBatchCursor`) with range expansion + retry via `SwitchToBatches`; no adhoc in v1. | `T4` | Numeric source tests pass; retry math includes division-by-zero guard. |
| `T9` | **Integrate numeric iterator path end-to-end** behind existing optimizer query planning path. | `T3`, `T4`, `T8` | Numeric flow/parity tests pass for ASC/DESC and filtered cases. |
| `T10` | **Revalidation/timeout correctness pass** for both sources (including rewind-after-timeout behavior and `VALIDATE_MOVED` handling policy). | `T7`, `T9` | Revalidation tests added; no regressions in timeout/retry behavior. |
| `T11` | **Profile/metrics integration** (`num_batches`, `strategy_switches`, optional retry counters) and profile output parity checks. | `T7`, `T9` | Profile tests pass; expected modes/switches visible and stable. |
| `T12` | **Cross-language parity suite** (Rust unit/integration + Python flow + selected C parity checks). | `T7`, `T9`, `T10`, `T11` | P0/P1 matrix is green in CI for both vector and numeric paths. |
| `T13` | **Performance and deferred design decisions** (const generics, result-builder split) guided by benchmarks. | `T12` | Benchmark report produced; follow-up design decisions documented with data. |

### Suggested Milestones

| Milestone | Scope | Tasks |
|-----------|-------|-------|
| `M1` | Shared core complete | `T1`-`T5` |
| `M2` | Vector parity complete | `T6`, `T7` |
| `M3` | Numeric parity complete | `T8`, `T9`, `T10` |
| `M4` | Validation + profiling complete | `T11`, `T12` |
| `M5` | Performance decisions complete | `T13` |

### Execution Notes

- Keep feature flags or guarded planner switches during rollout, so vector/numeric paths can be enabled independently.
- Land tests with each task (no deferred "big test PR" at the end).
- Avoid mixing behavior changes and performance changes in the same task.

---

## References

- [iterator_api.h](../../src/iterators/iterator_api.h) - C iterator API
- [lib.rs](../../src/redisearch_rs/rqe_iterators/src/lib.rs) - Rust `RQEIterator` trait
- [metric.rs](../../src/redisearch_rs/rqe_iterators/src/metric.rs) - Example of unsorted iterator
- [hybrid_reader.c](../../src/iterators/hybrid_reader.c) - Current C hybrid implementation
- [optimizer_reader.c](../../src/iterators/optimizer_reader.c) - Current C optimizer implementation
