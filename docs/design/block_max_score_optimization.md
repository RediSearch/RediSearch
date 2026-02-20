# Design: Block Max Score Optimization for InvertedIndex

## Overview

This document proposes an optimization to store **max score contribution** metadata at the `IndexBlock` level in inverted indexes. This enables iterators to **skip entire blocks** during query execution when it can be determined that no document in a block can contribute enough to the final score to affect the top-K results.

## Background

### Current Block Structure

Each `IndexBlock` currently stores:
- `first_doc_id`: The first document ID in the block
- `last_doc_id`: The last document ID in the block
- `num_entries`: Number of entries in the block
- `buffer`: Encoded entries (delta, frequency, field mask, offsets)

Blocks are ordered by document ID, enabling efficient `skip_to` operations via binary search on `last_doc_id`.

### Current Scoring Flow

1. **Indexing time**: Documents are indexed with term frequencies stored per entry
2. **Query time**:
   - IDF is computed globally: `log(1 + (totalDocs + 1) / termDocs)`
   - TF-IDF per document: `(freq / normalization) * IDF * docScore`
   - BM25: `IDF * ((freq * (k1 + 1)) / (freq + k1 * (1 - b + b * docLen/avgDocLen)))`
3. **Scoring happens after iteration**: The scorer function receives `RSIndexResult` with frequency data

### The Opportunity

During top-K retrieval, we often maintain a `minScore` threshold—the minimum score a document must achieve to enter the result heap. If we knew the **maximum possible score contribution** any document in a block could achieve, we could skip entire blocks where `maxBlockScore < minScore`.

### Key Insight: Term Contributions vs. Final Scores

A critical concept to understand is the distinction between a **term's score contribution** and a **document's final score**.

#### Single-Term Queries

For a query like `FT.SEARCH idx "redis" LIMIT 0 10`, the final document score equals the term's contribution:

$$
\text{FinalScore}_{\text{doc}} = \text{TermContribution}(\text{"redis"}, \text{doc})
$$

In this case, the block's max score directly bounds the final score. If `BlockMaxScore < minScore`, no document in that block can enter the top-K results.

#### Multi-Term Queries

For a query like `FT.SEARCH idx "redis database" LIMIT 0 10`, the final score is the **sum** of contributions from each term:

$$
\text{FinalScore}_{\text{doc}} = \text{TermContribution}(\text{"redis"}, \text{doc}) + \text{TermContribution}(\text{"database"}, \text{doc})
$$

Here, a single term's block max score does **not** bound the final score—a document might have a low "redis" contribution but a high "database" contribution.

#### How Block Skipping Still Works for Multi-Term Queries

The optimization leverages a key property: **we're iterating through one term's posting list at a time**. When processing the "redis" term:

1. We know the document must also match "database" (for AND queries) or might match it (for OR queries)
2. We can compute an **upper bound on the remaining contribution** from other terms
3. If `BlockMaxScore("redis") + MaxPossibleContribution(other terms) < minScore`, we can skip the block

For **intersection (AND) queries**, this is particularly powerful:
- We process the rarest term first (fewest documents)
- For each candidate document, we check if it exists in other terms' posting lists
- The block max score tells us: "Even if this document has the maximum possible 'redis' contribution from this block, can it possibly beat the threshold when combined with contributions from other terms?"

For **union (OR) queries**, the logic is similar but we track partial scores:
- Documents accumulate scores as we iterate through each term
- A block can be skipped if its max contribution, added to the document's current partial score, cannot exceed the threshold

#### Practical Example

Consider a top-10 query where the 10th-best document so far has score 5.0 (our threshold).

**Scenario A: Single-term query "redis"**
- Block 3 has `max_freq=2`, `min_doc_len=1000`, `max_doc_score=1.0`
- Computed `BlockMaxScore = 0.8`
- Since `0.8 < 5.0`, skip the entire block

**Scenario B: Multi-term AND query "redis database"**
- We're iterating through "redis" posting list
- Block 3 has `BlockMaxScore("redis") = 0.8`
- We know `MaxPossibleContribution("database") = 3.0` (from global term statistics)
- Upper bound for any doc in this block: `0.8 + 3.0 = 3.8`
- Since `3.8 < 5.0`, skip the entire block

**Scenario C: Multi-term AND query where we can't skip**
- Block 4 has `BlockMaxScore("redis") = 2.5`
- Upper bound: `2.5 + 3.0 = 5.5`
- Since `5.5 >= 5.0`, we must process this block (some documents might qualify)

This is why storing **per-term, per-block** metadata is valuable: it allows fine-grained skipping decisions even when the final score depends on multiple terms.

## Proposed Design

### New Block Metadata

Add scoring-related metadata fields to `IndexBlock`:

```rust
pub struct IndexBlock {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,

    // NEW: Block-level scoring metadata
    max_freq: u16,        // Maximum term frequency in this block
    max_doc_score: f32,   // Maximum document score in this block
    min_doc_len: u32,     // Minimum document length in this block

    buffer: Vec<u8>,
}
```

**Why store these three values?**

These three values (`max_freq`, `min_doc_len`, `max_doc_score`) are chosen because they provide the necessary building blocks to compute **upper bound scores** for all commonly used scoring functions:

- **TF-IDF and TFIDF.DOCNORM**: Use term frequency normalized by document length. `max_freq / min_doc_len` gives the maximum possible TF/DocLen ratio.
- **BM25**: Uses term frequency with saturation and length normalization. The same three values enable computing the BM25 upper bound by plugging in the best-case values.
- **DOCSCORE**: Simply uses the document score, so `max_doc_score` directly provides the block maximum.

By storing extrema (maximums and minimums) rather than averages, we ensure the computed upper bound is **never lower** than the actual maximum score in the block—a requirement for correctness. The specific values are:

- **max_freq**: Upper bound on term frequency (maximizes the TF component)
- **min_doc_len**: Lower bound on document length (minimizes the normalization denominator, maximizing the score)
- **max_doc_score**: Upper bound on the document score multiplier

**IDF** is computed at query time from index statistics. Since all blocks within an inverted index belong to the same term, they share the same IDF value.

See [Deriving Block Max Score for Each Scoring Function](#deriving-block-max-score-for-each-scoring-function) for the detailed formulas.

**Storage overhead:** 10 bytes per block (2 + 4 + 4)

**Estimated extra space per text field inverted index** (assuming ~100 documents per block):

Each block has a fixed overhead of ~48 bytes (`IndexBlock::STACK_SIZE`) plus a variable-size buffer (~800-1600 bytes for ~100 entries in a Full encoding). The 10 bytes of scoring metadata represents ~0.6-1.25% overhead per block.

| Documents | Blocks | Baseline Block Overhead | Extra Storage | % Increase |
|-----------|--------|-------------------------|---------------|------------|
| 100K      | ~1,000 | ~48 KB                  | ~10 KB        | ~21%       |
| 1M        | ~10,000 | ~480 KB                | ~100 KB       | ~21%       |
| 10M       | ~100,000 | ~4.8 MB               | ~1 MB         | ~21%       |
| 100M      | ~1,000,000 | ~48 MB              | ~10 MB        | ~21%       |

> **Note:** This overhead applies only to **text field** inverted indices. See [Index Type Applicability](#index-type-applicability) for details on which index types benefit from this optimization.

### Index Type Applicability

Not all inverted index types benefit from block-max score optimization. The scoring metadata (`max_freq`, `min_doc_len`, `max_doc_score`) is designed for **TF-based scoring functions** like BM25 and TF-IDF.

| Index Type | Encoding | Block-Max Score Benefit | Reason |
|------------|----------|-------------------------|--------|
| **Text fields** | Full, FreqsFields, FreqsOnly, etc. | ✅ **Full benefit** | Stores term frequency; TF-based scoring applies |
| **Tag fields** | DocIdsOnly | ❌ **No benefit** | No TF data stored; tags are binary matches |
| **Numeric fields** | Numeric, NumericFloatCompression | ❌ **No benefit** | Stores numeric values, not term frequencies |

#### Current State: Unified IndexBlock

Currently, all inverted index types share the same `IndexBlock` structure, which includes the scoring metadata fields. This means:

- **Tag and Numeric indices waste 10 bytes per block** on unused metadata
- The `max_freq == 0` sentinel indicates metadata is not tracked (e.g., for `DocIdsOnly` encoding)
- Block-max score computation returns `f64::MAX` for these blocks, disabling skipping

#### Optional Feature: Balancing Speedup vs. Memory Usage

Even for text fields, storing scoring metadata could be made **optional** via a configuration flag or index creation option. This allows users to choose their preferred trade-off:

| Configuration | Memory Overhead | Query Performance |
|---------------|-----------------|-------------------|
| **Enabled** (default) | +10 bytes/block (~21% increase) | Faster top-K queries via block skipping |
| **Disabled** | No overhead | Standard iteration (no skipping) |

**Use cases for disabling:**
- Memory-constrained environments where every byte counts
- Workloads dominated by non-scored queries (e.g., filtering, aggregations)
- Indexes with very few documents where skipping provides minimal benefit

This could be implemented as:
- A global configuration option (e.g., `FT.CONFIG SET BLOCK_MAX_SCORE_ENABLED`) (hard to change for existing indexes)
- A per-index option at creation time (e.g., `FT.CREATE ... NOBLOCK_MAX_SCORE`)

> **Note:** The [Alternative Storage Strategies](#alternative-storage-strategies) section describes other approaches (pre-computed scores, TF/DocLen pairs) with different memory vs. performance trade-offs. Once there is a decision to invest in this feature, these alternatives should be benchmarked to determine which strategy provides the best balance for different workloads.

#### Future Work: Type-Specific IndexBlock via Traits

A cleaner design would use Rust's trait system to define type-specific block structures:

```rust
// Base trait for all index blocks
trait IndexBlockBase {
    fn first_doc_id(&self) -> t_docId;
    fn last_doc_id(&self) -> t_docId;
    fn num_entries(&self) -> u16;
    fn buffer(&self) -> &[u8];
}

// Extended trait for blocks that support scoring metadata
trait ScoringMetadata: IndexBlockBase {
    fn max_freq(&self) -> u32;
    fn min_doc_len(&self) -> u32;
    fn max_doc_score(&self) -> f32;
}

// Text field blocks implement both traits
struct TextIndexBlock {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,
    max_freq: u32,
    max_doc_score: f32,
    min_doc_len: u32,
    buffer: Vec<u8>,
}

// Tag/Numeric blocks only implement the base trait (no scoring overhead)
struct MinimalIndexBlock {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,
    buffer: Vec<u8>,
}
```

**Benefits of this approach:**
- **Memory efficiency**: Tag and Numeric indices save 10 bytes per block
- **Type safety**: Compile-time enforcement of which operations are valid
- **Extensibility**: Easy to add new metadata for specific index types

**Implementation considerations:**
- Requires updating the `InvertedIndex<E>` generic to be parameterized by block type
- May require associated types in the `Encoder` trait
- RDB serialization/deserialization needs to handle different block types

> **TODO:** Create a tracking issue for implementing type-specific `IndexBlock` structures.

### Alternative Storage Strategies

The approach described above (storing `max_freq`, `min_doc_len`, `max_doc_score`) is one of several strategies used in the industry. This section discusses two alternative approaches that could be considered for future enhancements.

#### Alternative 1: Pre-computed Impact Scores

**Approach:** Store the pre-computed maximum score directly in each block, rather than the raw components.

```rust
pub struct IndexBlock {
    // ... existing fields ...

    // Pre-computed maximum score for this block
    precomputed_max_score: f32,
}
```

**How it works:**
- At indexing time, compute the maximum score for each block using the configured scoring function
- Store this single value per block
- At query time, compare directly against the threshold without computation

**Advantages:**
- **Fastest query-time evaluation**: No computation needed—just compare `precomputed_max_score >= minScore`
- **Minimal storage**: Only 4 bytes per block

**Disadvantages:**
- **Fixed scoring function**: The pre-computed score is tied to the scorer used at index time. Changing scorers (e.g., from TF-IDF to BM25) requires reindexing
- **Parameter sensitivity**: BM25 parameters (k1, b) are baked into the pre-computed score. Tuning these parameters requires reindexing
- **Less flexibility**: Cannot support multiple scoring functions from the same index

**When to use:** Best for deployments with a fixed, well-tuned scoring function that rarely changes.

---

#### Alternative 2: (TF, DocLen) Pairs

**Approach:** Store the actual (term_frequency, doc_length) pairs that could yield the maximum score, rather than just the extrema.

```rust
pub struct IndexBlock {
    // ... existing fields ...

    // Pairs of (tf, doc_len) that could yield max score
    // Pruned during indexing to only keep "dominant" pairs
    max_impact_pairs: SmallVec<[(u16, u32); 4]>,
    max_doc_score: f32,
}
```

**How it works:**
- During indexing, track all (tf, doc_len) pairs seen in the block
- Prune dominated pairs: if we have `(tf₁, dl₁)`, we can remove `(tf₂, dl₂)` where `tf₂ ≤ tf₁` AND `dl₂ ≥ dl₁` (since they're guaranteed to yield lower scores for any reasonable scoring function)
- At query time, compute the upper bound by evaluating the scoring function on each remaining pair and taking the maximum

**Example of pair pruning:**

Consider a block with these documents:

| DocID | TF | DocLen |
|-------|-----|--------|
| 1     | 3   | 100    |
| 2     | 5   | 200    |
| 3     | 4   | 80     |
| 4     | 2   | 50     |

Initial pairs: `[(3, 100), (5, 200), (4, 80), (2, 50)]`

Pruning analysis:
- `(3, 100)` is dominated by `(4, 80)` because 4 > 3 and 80 < 100 → remove `(3, 100)`
- `(5, 200)` is NOT dominated: it has the highest TF
- `(4, 80)` is NOT dominated: better TF/DocLen ratio than `(5, 200)`
- `(2, 50)` is NOT dominated: has the shortest doc length

Final pairs: `[(5, 200), (4, 80), (2, 50)]`

**Advantages:**
- **Tighter bounds**: Uses actual (tf, doc_len) combinations that exist in the block, rather than assuming the document with `max_freq` might also have `min_doc_len`
- **Flexible scoring**: Can compute bounds for any scoring function at query time
- **No reindexing needed**: Changing scoring functions or parameters doesn't require reindexing

**Disadvantages:**
- **Higher storage**: Typically 1-4 pairs per block, so 6-24 bytes vs. 6 bytes for (max_freq, min_doc_len)
- **Query-time computation**: Must evaluate the scoring function on each pair

**Why bounds are tighter:**

With our current approach (storing extrema), we compute:
```
BlockMaxScore = score(max_freq, min_doc_len, max_doc_score)
```

But `max_freq` and `min_doc_len` might come from different documents! The actual maximum score in the block could be significantly lower.

With (tf, doc_len) pairs, we compute:
```
BlockMaxScore = max(score(tf₁, dl₁), score(tf₂, dl₂), ...) × max_doc_score
```

Each pair represents an actual document, so the bound is tighter.

**When to use:** Best when bound tightness is critical and storage overhead is acceptable.

---

#### Comparison of Approaches

Each strategy offers a different trade-off between memory usage, implementation complexity, and query performance. The table below summarizes these trade-offs:

| Aspect | No Optimization | Current Proposal | Pre-computed Scores | (TF, DocLen) Pairs |
|--------|-----------------|------------------|---------------------|---------------------|
| **Memory per block** | 0 bytes | 10 bytes | 4 bytes | 10-28 bytes |
| **Write complexity** | None | Low (track min/max) | Medium (compute score) | High (pair pruning) |
| **Read complexity** | None | Low (compute bound) | None (direct compare) | Medium (evaluate pairs) |
| **Read speedup** | None (baseline) | Moderate | High | High |
| **Bound tightness** | N/A | Loose (upper bound) | Exact | Tight (upper bound) |
| **Scoring flexibility** | N/A | High | None | High |
| **Reindex on scorer change** | N/A | No | Yes | No |

**Legend:**
- **Write complexity**: Additional work during document indexing
- **Read complexity**: Additional work during query execution to compute/compare block scores
- **Read speedup**: Expected improvement in top-K query performance due to block skipping

> **Note:** These trade-offs should be validated with benchmarks on representative workloads before committing to a specific strategy. The optimal choice may vary depending on index size, query patterns, and memory constraints.

#### Recommendation

The **current proposal** (storing `max_freq`, `min_doc_len`, `max_doc_score`) offers a good balance:
- Flexible enough to support multiple scoring functions
- Simple to implement and maintain
- Reasonable storage overhead

For future optimization, consider:
1. **Hybrid approach**: Store both raw components AND pre-computed scores for the default scorer
2. **Adaptive pairs**: Use (tf, doc_len) pairs for high-value indexes where tighter bounds justify the overhead

### Deriving Block Max Score for Each Scoring Function

The key insight is that we want to find the **maximum possible score** any document in a block could achieve. To do this, we analyze each scoring formula and determine which combination of block metadata produces the highest score.

---

#### TF-IDF Scorer

**Document Score Formula:**
$$
\text{Score}_{\text{doc}} = \frac{\text{TF}}{\text{DocLen}} \times \text{IDF} \times \text{DocScore}
$$

**Derivation of Upper Bound:**

To maximize the score, we need to:
1. **Maximize TF** → use `max_freq` (highest term frequency in block)
2. **Minimize DocLen** → use `min_doc_len` (smallest document maximizes TF/DocLen ratio)
3. **Maximize DocScore** → use `max_doc_score` (highest document weight in block)
4. **IDF** is constant for all documents containing the term (computed at query time)

**Block Max Score Formula:**
$$
\text{BlockMaxScore}_{\text{TF-IDF}} = \frac{\text{max\_freq}}{\text{min\_doc\_len}} \times \text{IDF} \times \text{max\_doc\_score}
$$

**Why this is an upper bound (not exact):**
The document with `max_freq` may not be the same document with `min_doc_len` or `max_doc_score`. We're combining the best-case values from potentially different documents, so the actual maximum score in the block is ≤ this bound.

```rust
fn block_max_score_tfidf(block: &IndexBlock, idf: f64) -> f64 {
    (block.max_freq as f64 / block.min_doc_len as f64) * idf * block.max_doc_score as f64
}
```

---

#### BM25 Scorer

**Document Score Formula:**
$$
\text{Score}_{\text{doc}} = \text{IDF} \times \frac{\text{TF} \times (k_1 + 1)}{\text{TF} + k_1 \times \left(1 - b + b \times \frac{\text{DocLen}}{\text{avgDocLen}}\right)} \times \text{DocScore}
$$

Where:
- $k_1$ = term frequency saturation parameter (typically 1.2)
- $b$ = length normalization parameter (typically 0.75)
- $\text{avgDocLen}$ = average document length in the index

**Derivation of Upper Bound:**

The BM25 formula has a **saturation effect** on TF—higher frequencies have diminishing returns. Let's analyze each factor:

1. **TF component**: $\frac{\text{TF} \times (k_1 + 1)}{\text{TF} + k_1 \times (\ldots)}$

   This is monotonically increasing in TF (more occurrences = higher score), so use `max_freq`.

2. **Length normalization**: The denominator contains $k_1 \times (1 - b + b \times \frac{\text{DocLen}}{\text{avgDocLen}})$

   - Smaller DocLen → smaller denominator → higher score
   - Use `min_doc_len` to minimize the denominator

3. **DocScore**: Multiplicative factor, use `max_doc_score`

4. **IDF**: Constant for the term

**Block Max Score Formula:**
$$
\text{BlockMaxScore}_{\text{BM25}} = \text{IDF} \times \frac{\text{max\_freq} \times (k_1 + 1)}{\text{max\_freq} + k_1 \times \left(1 - b + b \times \frac{\text{min\_doc\_len}}{\text{avgDocLen}}\right)} \times \text{max\_doc\_score}
$$

```rust
fn block_max_score_bm25(
    block: &IndexBlock,
    idf: f64,
    avg_doc_len: f64,
    k1: f64,
    b: f64,
) -> f64 {
    let tf = block.max_freq as f64;
    let len_norm = 1.0 - b + b * (block.min_doc_len as f64 / avg_doc_len);
    let tf_saturation = (tf * (k1 + 1.0)) / (tf + k1 * len_norm);

    idf * tf_saturation * block.max_doc_score as f64
}
```

---

#### TFIDF.DOCNORM Scorer

**Document Score Formula:**
$$
\text{Score}_{\text{doc}} = \frac{\text{TF}}{\text{DocLen}} \times \text{IDF}
$$

This is TF-IDF without the DocScore multiplier.

**Block Max Score Formula:**
$$
\text{BlockMaxScore}_{\text{DOCNORM}} = \frac{\text{max\_freq}}{\text{min\_doc\_len}} \times \text{IDF}
$$

```rust
fn block_max_score_docnorm(block: &IndexBlock, idf: f64) -> f64 {
    (block.max_freq as f64 / block.min_doc_len as f64) * idf
}
```

---

#### DOCSCORE Scorer

**Document Score Formula:**
$$
\text{Score}_{\text{doc}} = \text{DocScore}
$$

This scorer ignores term frequency entirely.

**Block Max Score Formula:**
$$
\text{BlockMaxScore}_{\text{DOCSCORE}} = \text{max\_doc\_score}
$$

```rust
fn block_max_score_docscore(block: &IndexBlock) -> f64 {
    block.max_doc_score as f64
}
```

---

#### Summary: Scorer Support for Block Skipping

**Supported Scorers:**

| Scorer | Uses max_freq | Uses min_doc_len | Uses max_doc_score | Uses IDF |
|--------|---------------|------------------|--------------------| ---------|
| TF-IDF | ✓ | ✓ | ✓ | ✓ |
| BM25 | ✓ | ✓ | ✓ | ✓ |
| TFIDF.DOCNORM | ✓ | ✓ | ✗ | ✓ |
| DOCSCORE | ✗ | ✗ | ✓ | ✗ |

**Not Supported:**

| Scorer | Reason |
|--------|--------|
| DISMAX | Requires per-field block metadata (separate `max_freq` per field) |
| HAMMING | Distance-based scorer, not compatible with score upper bounds |
| Custom scorers | Unknown formula, cannot compute upper bound |

For unsupported scorers, block skipping is disabled and all blocks are processed normally.

### Block-Skipping in Iterators

The `skip_to` and `read` implementations can leverage this:

```rust
impl<'index, E> IndexReader<'index> for IndexReaderCore<'index, E> {
    fn skip_to_with_threshold(&mut self, doc_id: t_docId, min_score: f64) -> bool {
        // Find the target block
        let target_block_idx = self.find_block_containing(doc_id);

        // Skip blocks that can't contribute enough score
        while target_block_idx < self.ii.blocks.len() {
            let block_max = self.compute_block_max_score(target_block_idx);
            if block_max >= min_score {
                break;
            }
            target_block_idx += 1;
        }

        if target_block_idx >= self.ii.blocks.len() {
            return false;
        }

        self.set_current_block(target_block_idx);
        true
    }
}
```

### Use Cases

#### 1. Single Term Queries with LIMIT
When processing `FT.SEARCH idx "rare_term" LIMIT 0 10`, after finding 10 high-scoring documents, blocks with low `max_freq` can be skipped entirely.

#### 2. Intersection Queries
For `FT.SEARCH idx "common_term rare_term"`, the intersection iterator could skip blocks of `common_term` where the max score can't beat the current threshold.

#### 3. Union with Score Accumulation
When computing union scores, low-contribution blocks can be deferred or skipped.

## Implementation Plan

This section outlines a phased approach to implementing block-max score optimization. The plan prioritizes simplicity and correctness first, with tighter bounds as a future enhancement.

### Design Decisions

**Single Block Type Approach**: Rather than creating multiple block type variants (one with metadata, one without), we add the metadata fields to `IndexBlock` unconditionally. All new blocks will have valid metadata. This avoids combinatorial explosion of block types.

**Note on Persistence**: The inverted index data is NOT persisted in RDB - it is rebuilt from Redis keys on load. Therefore, no RDB versioning or backward compatibility handling is needed. Every time an index is rebuilt, the new metadata fields will be populated correctly.

---

### Phase 1: Block Metadata Infrastructure ✅ COMPLETE

**Goal**: Add scoring metadata fields to `IndexBlock` and ensure they are correctly initialized and serialized.

#### 1.1 Add Fields to `IndexBlock`

```rust
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexBlock {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    num_entries: u16,

    // NEW: Block-level scoring metadata
    // max_freq = 0 indicates metadata is not available (old index)
    max_freq: u16,
    max_doc_score: f32,
    min_doc_len: u32,

    buffer: Vec<u8>,
}
```

#### 1.2 Update `IndexBlock::new()`

```rust
impl IndexBlock {
    fn new(doc_id: t_docId) -> Self {
        Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            // Initialize with values that will be updated on first entry
            max_freq: 0,           // Will be set to actual max on first write
            max_doc_score: 0.0,    // Will be set to actual max on first write
            min_doc_len: u32::MAX, // Will be set to actual min on first write
            buffer: Vec::new(),
        }
    }

    /// Returns true if this block has valid scoring metadata
    pub fn has_scoring_metadata(&self) -> bool {
        // max_freq = 0 with num_entries > 0 would be invalid for a real block
        // (every entry has freq >= 1), so we use max_freq = 0 as sentinel
        self.max_freq > 0 || self.num_entries == 0
    }
}
```

#### 1.3 Update Block Writing ✅ IMPLEMENTED

Added `add_record_with_metadata()` method to `InvertedIndex`, `EntriesTrackingIndex`, and `FieldMaskTrackingIndex`:

```rust
/// Add a new record to the index with document metadata for block-max score optimization.
///
/// This method tracks `max_doc_score` and `min_doc_len` per block, enabling the block-max
/// score optimization to skip blocks that cannot contain top-K results.
pub fn add_record_with_metadata(
    &mut self,
    record: &RSIndexResult,
    doc_len: u32,
    doc_score: f32,
) -> std::io::Result<usize> {
    // ... internally updates block metadata:
    // block.max_freq = block.max_freq.max(record.freq);
    // block.min_doc_len = block.min_doc_len.min(doc_len);
    // block.max_doc_score = block.max_doc_score.max(doc_score);
}
```

The existing `add_record()` method remains unchanged for backward compatibility and only tracks `max_freq`.

#### 1.4 Serialization (Fork GC)

The `IndexBlock` struct uses serde for fork GC serialization (messagepack via `rmp_serde`), not for RDB persistence. The inverted index data is rebuilt from Redis keys on load, not persisted in RDB.

- Update the `Serialize`/`Deserialize` implementations to include the new fields
- The custom `Deserialize` impl for `IndexBlock` already exists (for tracking total blocks) - extend it to handle the new fields
- No RDB version changes needed

#### 1.5 Unit Tests

- [x] Test that `max_freq`, `min_doc_len`, `max_doc_score` are correctly tracked during indexing
- [x] Test `has_scoring_metadata()` returns correct values
- [x] Test serde round-trip preserves metadata (for fork GC)

---

### Phase 2: Score Computation Helpers ✅ COMPLETE

**Goal**: Create utilities to compute block-level score upper bounds for each supported scorer.

#### 2.1 BlockScorer Enum (Unified Design)

We use a unified enum that embeds scoring parameters in each variant. This provides:
- **Type safety**: Can't accidentally use BM25 parameters with TfIdf scorer
- **No wasted fields**: DocScore doesn't carry unused `avg_doc_len`
- **Clean API**: One thing to pass instead of separate context + type

```rust
/// Block scorer with embedded scoring parameters.
/// Each variant contains only the parameters it needs.
pub enum BlockScorer {
    /// TF-IDF scorer: Score = (TF / DocLen) × IDF × DocScore
    TfIdf { idf: f64 },
    /// BM25 scorer with length normalization
    Bm25 { idf: f64, avg_doc_len: f64, k1: f64, b: f64 },
    /// DOCSCORE scorer: Score = DocScore (ignores term frequency)
    DocScore,
}

impl BlockScorer {
    /// Create a TF-IDF scorer
    pub fn tfidf(idf: f64) -> Self {
        Self::TfIdf { idf }
    }

    /// Create a BM25 scorer with custom parameters
    pub fn bm25(idf: f64, avg_doc_len: f64, k1: f64, b: f64) -> Self {
        Self::Bm25 { idf, avg_doc_len, k1, b }
    }

    /// Create a BM25 scorer with default k1=1.2, b=0.75
    pub fn bm25_default(idf: f64, avg_doc_len: f64) -> Self {
        Self::bm25(idf, avg_doc_len, 1.2, 0.75)
    }

    /// Create a DOCSCORE scorer
    pub fn doc_score() -> Self {
        Self::DocScore
    }

    /// Compute the maximum possible score for any document in this block
    pub fn block_max_score(&self, block: &IndexBlock) -> f64 {
        if block.num_entries == 0 {
            return 0.0; // Empty block
        }
        if !block.has_scoring_metadata() {
            return f64::MAX; // No skipping possible
        }

        match self {
            Self::TfIdf { idf } => {
                let doc_score = if block.max_doc_score == 0.0 { 1.0 } else { block.max_doc_score as f64 };
                (block.max_freq as f64 / block.min_doc_len as f64) * idf * doc_score
            }
            Self::Bm25 { idf, avg_doc_len, k1, b } => {
                let tf = block.max_freq as f64;
                let len_norm = 1.0 - b + b * (block.min_doc_len as f64 / avg_doc_len);
                let tf_saturation = (tf * (k1 + 1.0)) / (tf + k1 * len_norm);
                let doc_score = if block.max_doc_score == 0.0 { 1.0 } else { block.max_doc_score as f64 };
                idf * tf_saturation * doc_score
            }
            Self::DocScore => block.max_doc_score as f64,
        }
    }
}

impl Default for BlockScorer {
    fn default() -> Self {
        Self::tfidf(1.0)
    }
}
```

#### 2.3 Unit Tests

- [x] Test TF-IDF upper bound computation
- [x] Test BM25 upper bound computation
- [x] Test DOCSCORE upper bound computation
- [x] Test that blocks without metadata return `f64::MAX`
- [x] Test that empty blocks return 0.0 (no documents to contribute)

---

### Phase 3: Iterator Interface Extension ✅ COMPLETE

**Goal**: Add threshold-aware skip methods to the `IndexReader` trait and implement block skipping.

#### 3.1 Extend `IndexReader` Trait

```rust
pub trait IndexReader<'index> {
    // ... existing methods ...

    /// Skip to the first block containing doc_id >= target, but also skip
    /// any blocks whose max score is below min_score.
    ///
    /// Returns `true` if positioned at a valid block, `false` if EOF.
    ///
    /// When `min_score = 0.0`, this behaves identically to `skip_to()`.
    fn skip_to_with_threshold(
        &mut self,
        doc_id: t_docId,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> bool;

    /// Get the max score for the current block, or `f64::MAX` if unknown.
    fn current_block_max_score(&self, scorer: &BlockScorer) -> f64;
}
```

#### 3.2 Implement in `IndexReaderCore`

```rust
impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReader<'index>
    for IndexReaderCore<'index, E>
{
    fn skip_to_with_threshold(
        &mut self,
        doc_id: t_docId,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> bool {
        // First, find the block containing doc_id (existing logic)
        let mut target_block_idx = self.find_block_containing(doc_id);

        // Then, skip blocks that can't contribute enough score
        while target_block_idx < self.ii.blocks.len() {
            let block = &self.ii.blocks[target_block_idx];
            let block_max = scorer.block_max_score(block);

            if block_max >= min_score {
                break;
            }
            target_block_idx += 1;
        }

        if target_block_idx >= self.ii.blocks.len() {
            return false;
        }

        self.set_current_block(target_block_idx);
        true
    }

    fn current_block_max_score(&self, scorer: &BlockScorer) -> f64 {
        if self.current_block_idx >= self.ii.blocks.len() {
            return 0.0;
        }
        let block = &self.ii.blocks[self.current_block_idx];
        scorer.block_max_score(block)
    }
}
```

#### 3.3 Default Implementation for Compatibility

For readers that don't support block skipping (e.g., filtered readers), provide a default:

```rust
/// Default implementation that falls back to regular skip_to
fn skip_to_with_threshold(
    &mut self,
    doc_id: t_docId,
    _min_score: f64,
    _scorer: &BlockScorer,
) -> bool {
    self.skip_to(doc_id)
}

fn current_block_max_score(&self, _scorer: &BlockScorer) -> f64 {
    f64::MAX // Conservative: never skip
}
```

#### 3.4 Wire Through FFI Layer

Update `inverted_index_ffi` to expose the new methods to C code.

#### 3.5 Unit Tests

- [x] Test `skip_to_with_threshold` skips low-score blocks
- [x] Test `skip_to_with_threshold` with `min_score = 0.0` behaves like `skip_to`
- [x] Test `current_block_max_score` returns correct values for each scorer type
- [x] Test `skip_to_with_threshold` returns false when all blocks below threshold
- [x] Test BM25 scorer with block skipping
- [x] Test DOCSCORE scorer with block skipping

---

### Phase 4: Iterator Layer Extension ✅ COMPLETE

**Goal**: Add a `read_with_threshold` method to the `RQEIterator` interface that automatically skips blocks whose max score is below the threshold.

Phase 3 added threshold-aware skipping to `IndexReader` (the low-level reader). However, query execution uses higher-level iterators (`InvIndIterator`, `RQEIterator`). This phase extends those interfaces with a clean API for threshold-aware reading.

#### 4.1 Extend `RQEIterator` Trait

Add the `read_with_threshold` method to the `RQEIterator` trait:

```rust
pub trait RQEIterator<'index> {
    // ... existing methods ...

    /// Read the next entry, skipping blocks whose max score is below `min_score`.
    ///
    /// This is the primary API for block-max score optimization. It combines
    /// reading with automatic block skipping:
    /// 1. If the current block's max score < min_score, skip to the next promising block
    /// 2. Read the next entry from the current (or newly positioned) block
    /// 3. Repeat until a valid entry is found or EOF
    ///
    /// # Arguments
    /// * `min_score` - Minimum score threshold. Blocks with max_score < min_score are skipped.
    /// * `scorer` - The scorer used to compute block max scores.
    ///
    /// # Returns
    /// * `Ok(Some(&mut RSIndexResult))` - Next entry from a block with max_score >= min_score
    /// * `Ok(None)` - No more entries (all remaining blocks are below threshold or EOF)
    /// * `Err(...)` - Error during iteration
    ///
    /// # Default Implementation
    /// Falls back to regular `read()`, ignoring the threshold (for iterators that
    /// don't support block-level scoring).
    fn read_with_threshold(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // Default: ignore threshold, just read
        self.read()
    }
}
```

#### 4.2 Extend `IndexReader` Trait

Add a helper method to check if we should skip the current block:

```rust
pub trait IndexReader<'index> {
    // ... existing methods from Phase 3 ...

    /// Advance to the next block that has max_score >= min_score.
    ///
    /// Returns `true` if positioned at a valid block, `false` if EOF.
    /// This is called internally by `read_with_threshold` when the current
    /// block's score is below threshold.
    fn advance_to_next_promising_block(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> bool {
        // Skip blocks until we find one with sufficient max score
        while self.current_block_max_score(scorer) < min_score {
            if !self.advance_to_next_block() {
                return false; // EOF
            }
        }
        true
    }
}
```

#### 4.3 Implement in `InvIndIterator<R>`

The Rust `InvIndIterator` implements the threshold-aware read:

```rust
impl<'index, R: IndexReader<'index>> RQEIterator<'index> for InvIndIterator<'index, R> {
    fn read_with_threshold(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        loop {
            // Check if current block is worth reading
            if self.reader.current_block_max_score(scorer) < min_score {
                // Skip to next promising block
                if !self.reader.advance_to_next_promising_block(min_score, scorer) {
                    self.at_eos = true;
                    return Ok(None);
                }
            }

            // Try to read from current block
            match self.reader.next_record(&mut self.result) {
                true => {
                    self.last_doc_id = self.result.doc_id;
                    return Ok(Some(&mut self.result));
                }
                false => {
                    // Current block exhausted, move to next
                    if !self.reader.advance_to_next_block() {
                        self.at_eos = true;
                        return Ok(None);
                    }
                    // Loop will check if new block passes threshold
                }
            }
        }
    }
}
```

#### 4.4 Implement for Wrapper Iterators

Wrapper iterators (e.g., `Term`, `Numeric`) delegate to their inner iterator:

```rust
impl<'index, R: TermReader<'index>> RQEIterator<'index> for Term<'index, R> {
    fn read_with_threshold(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read_with_threshold(min_score, scorer)
    }
}
```

#### 4.5 Update `Box<dyn RQEIterator>` Implementation

Ensure dynamic dispatch works correctly:

```rust
impl<'index> RQEIterator<'index> for Box<dyn RQEIterator<'index> + 'index> {
    fn read_with_threshold(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (**self).read_with_threshold(min_score, scorer)
    }
}
```

#### 4.6 Usage Example

```rust
// Top-K query with block skipping
let mut iterator = index.iterator();
let mut heap = MinHeap::with_capacity(k);
let mut min_score = 0.0;
let scorer = BlockScorer::tfidf(idf);

// Use read_with_threshold - iterator handles block skipping internally
while let Some(result) = iterator.read_with_threshold(min_score, &scorer)? {
    let doc_meta = &doc_table[&result.doc_id];
    let score = compute_score(result, doc_meta, &scorer);

    if heap.push(result.doc_id, score) {
        // Heap improved, update threshold for next iteration
        min_score = heap.min_score();
    }
}
```

#### 4.7 Unit Tests

- [x] Test `advance_to_next_promising_block` skips low-score blocks
- [x] Test `advance_to_next_promising_block` returns false at EOF
- [x] Test `advance_to_next_promising_block` with zero threshold
- [x] Test `read_with_threshold` implementation in `InvIndIterator`
- [x] Test wrapper iterators (`Numeric`, `Term`) delegate correctly
- [x] Test default implementation falls back to regular `read()`

---

### Phase 5: Rust Microbenchmarks ✅ COMPLETE

**Goal**: Validate the block-skipping optimization with realistic Top-K query simulation using Criterion benchmarks.

This phase creates benchmarks in the `inverted_index_bencher` crate that simulate end-to-end Top-K query execution, comparing performance with and without block skipping. The benchmarks use the iterator layer (from Phase 4) to measure realistic query performance.

#### 5.1 Benchmark Infrastructure

Add new files to the existing `inverted_index_bencher` crate:

```
src/redisearch_rs/inverted_index_bencher/
├── Cargo.toml                          # Add criterion dependency (already exists)
├── benches/
│   └── block_max_score.rs              # NEW: Criterion benchmark file
└── src/
    └── benchers/
        └── block_max_score.rs          # NEW: Benchmark helpers and data generators
```

#### 5.2 Data Generators

Create helpers to generate realistic test data with various distributions:

```rust
// src/benchers/block_max_score.rs

use rand::distributions::{Distribution, Uniform};
use rand_distr::Zipf;

/// Document metadata stored in a simulated DocTable
#[derive(Clone, Copy)]
pub struct DocMetadata {
    pub doc_len: u32,
    pub doc_score: f32,
}

/// Distribution types for generating test data
pub enum DataDistribution {
    /// All values roughly equal
    Uniform,
    /// Power-law distribution (few high values, many low)
    Zipfian,
    /// High-scoring documents clustered in early blocks
    ClusteredHighScores,
    /// High-scoring documents spread evenly across blocks
    ScatteredHighScores,
}

/// Generates test data for benchmarking
pub struct BenchmarkDataGenerator {
    distribution: DataDistribution,
    rng: rand::rngs::StdRng,
}

impl BenchmarkDataGenerator {
    pub fn new(distribution: DataDistribution, seed: u64) -> Self;

    /// Generate term frequency based on distribution
    pub fn sample_term_freq(&mut self) -> u32;

    /// Generate document length (typically 50-5000 tokens)
    pub fn sample_doc_len(&mut self) -> u32;

    /// Generate document score (typically 1.0, occasionally boosted)
    pub fn sample_doc_score(&mut self) -> f32;
}
```

#### 5.3 Benchmark Setup

Create a setup struct that populates both a DocTable and InvertedIndex:

```rust
use std::collections::HashMap;
use inverted_index::{InvertedIndex, FreqsOnly, IndexFlags};

/// Complete benchmark setup with DocTable and InvertedIndex
pub struct TopKBenchmarkSetup {
    /// Simulated document table: doc_id -> metadata
    pub doc_table: HashMap<t_docId, DocMetadata>,
    /// The inverted index with block-level scoring metadata
    pub index: InvertedIndex<FreqsOnly>,
    /// Pre-computed IDF for the term
    pub idf: f64,
    /// Average document length (for BM25)
    pub avg_doc_len: f64,
    /// Total number of documents indexed
    pub num_docs: usize,
}

impl TopKBenchmarkSetup {
    /// Create a new benchmark setup with the given number of documents
    ///
    /// # Arguments
    /// * `num_docs` - Number of documents to index
    /// * `distribution` - Distribution for generating TF, doc_len, doc_score
    /// * `seed` - Random seed for reproducibility
    pub fn new(num_docs: usize, distribution: DataDistribution, seed: u64) -> Self {
        let mut generator = BenchmarkDataGenerator::new(distribution, seed);
        let mut doc_table = HashMap::with_capacity(num_docs);
        let flags = IndexFlags::STORE_FREQS;
        let mut index = InvertedIndex::<FreqsOnly>::new(flags);

        for doc_id in 1..=num_docs as t_docId {
            let doc_len = generator.sample_doc_len();
            let doc_score = generator.sample_doc_score();
            let tf = generator.sample_term_freq();

            doc_table.insert(doc_id, DocMetadata { doc_len, doc_score });

            // Create record and add to index
            // Note: add_record_with_metadata tracks min_doc_len and max_doc_score per block
            let record = RSIndexResult::virt().doc_id(doc_id).frequency(tf);
            index.add_record_with_metadata(&record, doc_len, doc_score).unwrap();
        }

        let idf = (1.0 + (num_docs as f64 + 1.0) / (num_docs as f64)).ln();
        let avg_doc_len = doc_table.values().map(|m| m.doc_len as f64).sum::<f64>() / num_docs as f64;

        Self { doc_table, index, idf, avg_doc_len, num_docs }
    }
}
```

#### 5.4 Top-K Query Simulation

Implement two query variants to compare:

```rust
use std::collections::BinaryHeap;
use std::cmp::Reverse;

/// Result entry for the min-heap
#[derive(PartialEq, PartialOrd)]
struct HeapEntry {
    score: f64,
    doc_id: t_docId,
}

/// Run Top-K query WITHOUT block skipping (baseline)
pub fn query_top_k_baseline(
    setup: &TopKBenchmarkSetup,
    k: usize,
    scorer: &BlockScorer,
) -> Vec<(t_docId, f64)> {
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k);
    let mut iterator = setup.index.iterator();

    while let Ok(Some(result)) = iterator.read() {
        let doc_meta = &setup.doc_table[&result.doc_id];
        let score = compute_score(result.freq, doc_meta, scorer, setup.idf);

        if heap.len() < k {
            heap.push(Reverse(HeapEntry { score, doc_id: result.doc_id }));
        } else if score > heap.peek().unwrap().0.score {
            heap.pop();
            heap.push(Reverse(HeapEntry { score, doc_id: result.doc_id }));
        }
    }

    // Extract results sorted by score descending
    heap.into_sorted_vec().into_iter().map(|Reverse(e)| (e.doc_id, e.score)).collect()
}

/// Run Top-K query WITH block skipping using read_with_threshold
pub fn query_top_k_with_skipping(
    setup: &TopKBenchmarkSetup,
    k: usize,
    scorer: &BlockScorer,
) -> Vec<(t_docId, f64)> {
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k);
    let mut iterator = setup.index.iterator();
    let mut min_score = 0.0;

    // Use read_with_threshold - iterator handles block skipping internally
    while let Ok(Some(result)) = iterator.read_with_threshold(min_score, scorer) {
        let doc_meta = &setup.doc_table[&result.doc_id];
        let score = compute_score(result.freq, doc_meta, scorer, setup.idf);

        if heap.len() < k {
            heap.push(Reverse(HeapEntry { score, doc_id: result.doc_id }));
        } else if score > heap.peek().unwrap().0.score {
            heap.pop();
            heap.push(Reverse(HeapEntry { score, doc_id: result.doc_id }));
        }

        // Update threshold after each heap modification
        if heap.len() == k {
            min_score = heap.peek().unwrap().0.score;
        }
    }

    heap.into_sorted_vec().into_iter().map(|Reverse(e)| (e.doc_id, e.score)).collect()
}

/// Compute score based on scorer type
fn compute_score(freq: u32, doc_meta: &DocMetadata, scorer: &BlockScorer, idf: f64) -> f64 {
    match scorer {
        BlockScorer::TfIdf { idf } => {
            (freq as f64 / doc_meta.doc_len as f64) * idf * doc_meta.doc_score as f64
        }
        BlockScorer::Bm25 { idf, avg_doc_len, k1, b } => {
            let tf = freq as f64;
            let doc_len = doc_meta.doc_len as f64;
            let numerator = tf * (k1 + 1.0);
            let denominator = tf + k1 * (1.0 - b + b * doc_len / avg_doc_len);
            idf * (numerator / denominator) * doc_meta.doc_score as f64
        }
        BlockScorer::DocScore => doc_meta.doc_score as f64,
    }
}
```

#### 5.5 Criterion Benchmark File

Create the benchmark file that uses the helpers:

```rust
// benches/block_max_score.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use inverted_index_bencher::benchers::block_max_score::*;

fn bench_top_k_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_query");

    // Test different index sizes
    for num_docs in [10_000, 100_000] {
        // Test different distributions
        for distribution in [DataDistribution::Uniform, DataDistribution::Zipfian] {
            let setup = TopKBenchmarkSetup::new(num_docs, distribution.clone(), 42);
            let scorer = BlockScorer::tfidf(setup.idf);
            let k = 10;

            let dist_name = match distribution {
                DataDistribution::Uniform => "uniform",
                DataDistribution::Zipfian => "zipfian",
                _ => "other",
            };

            group.bench_with_input(
                BenchmarkId::new("baseline", format!("{num_docs}_{dist_name}")),
                &(&setup, &scorer, k),
                |b, (setup, scorer, k)| {
                    b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
                },
            );

            group.bench_with_input(
                BenchmarkId::new("with_skipping", format!("{num_docs}_{dist_name}")),
                &(&setup, &scorer, k),
                |b, (setup, scorer, k)| {
                    b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
                },
            );
        }
    }

    group.finish();
}

fn bench_different_k_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_varying_k");

    let setup = TopKBenchmarkSetup::new(100_000, DataDistribution::Zipfian, 42);
    let scorer = BlockScorer::tfidf(setup.idf);

    for k in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("baseline", k),
            &(&setup, &scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_skipping", k),
            &(&setup, &scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
            },
        );
    }

    group.finish();
}

fn bench_different_scorers(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_k_scorers");

    let setup = TopKBenchmarkSetup::new(100_000, DataDistribution::Zipfian, 42);
    let k = 10;

    let scorers = [
        ("tfidf", BlockScorer::tfidf(setup.idf)),
        ("bm25", BlockScorer::bm25(setup.idf, setup.avg_doc_len, 1.2, 0.75)),
        ("docscore", BlockScorer::doc_score()),
    ];

    for (name, scorer) in &scorers {
        group.bench_with_input(
            BenchmarkId::new("baseline", name),
            &(&setup, scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_baseline(setup, *k, scorer)))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_skipping", name),
            &(&setup, scorer, k),
            |b, (setup, scorer, k)| {
                b.iter(|| black_box(query_top_k_with_skipping(setup, *k, scorer)))
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_top_k_query,
    bench_different_k_values,
    bench_different_scorers,
);
criterion_main!(benches);
```

#### 5.6 Cargo.toml Updates

Add the benchmark to `inverted_index_bencher/Cargo.toml`:

```toml
[[bench]]
name = "block_max_score"
harness = false
```

#### 5.7 Running the Benchmarks

```bash
cd src/redisearch_rs
cargo bench -p inverted_index_bencher --bench block_max_score
```

#### 5.8 Expected Metrics

| Metric | Description | How to Measure |
|--------|-------------|----------------|
| **Query latency** | Time to complete Top-K query | Criterion's built-in timing |
| **Speedup ratio** | `baseline_time / with_skipping_time` | Compare benchmark results |
| **Skip rate** | % of blocks skipped | Add instrumentation to iterator |

#### 5.9 Expected Results

Based on the design, we expect:

| Distribution | K=10 Skip Rate | K=100 Skip Rate | K=1000 Skip Rate |
|--------------|----------------|-----------------|------------------|
| Zipfian | 60-80% | 40-60% | 20-40% |
| Uniform | 5-15% | 2-10% | <5% |
| Clustered High | 70-90% | 50-70% | 30-50% |

**Key insights to validate:**
- Zipfian distributions benefit most (power-law means few high-score blocks)
- Smaller K benefits more (threshold rises faster)
- Overhead when no skipping is minimal (just threshold comparisons)

---

### Phase 6: Integration Testing

**Goal**: Ensure block skipping produces identical results to full scan.

#### 6.1 Correctness Tests

Create comprehensive tests that:
1. Index a set of documents with known scores
2. Run top-K queries with block skipping enabled
3. Run the same queries with block skipping disabled
4. Verify identical results

```rust
#[test]
fn test_block_skipping_correctness() {
    // Create index with documents having varying TF, doc_len, doc_score
    let index = create_test_index_with_varied_scores();

    // Query with skipping
    let results_with_skip = query_top_k(&index, "term", 10, true);

    // Query without skipping
    let results_without_skip = query_top_k(&index, "term", 10, false);

    assert_eq!(results_with_skip, results_without_skip);
}
```

#### 6.2 Edge Case Tests

- [ ] Empty blocks
- [ ] Single-entry blocks
- [ ] All blocks skipped (no results)
- [ ] No blocks skipped (all pass threshold)
- [ ] Threshold exactly equals block max score

#### 6.3 RDB Compatibility Tests

- [ ] Load old RDB → no skipping (sentinel values)
- [ ] Save new RDB → load → skipping works
- [ ] Mixed old/new blocks after partial reindex

---

### Phase 7: End-to-End Benchmarks (Future)

**Goal**: Validate performance improvements with realistic query workloads after full integration.

This phase runs after the optimization is fully integrated into the query execution pipeline.

#### 7.1 Full Query Benchmarks

```rust
fn bench_full_query_execution(c: &mut Criterion) {
    // Load a realistic test dataset
    let index = load_wikipedia_sample_index();

    c.bench_function("top_10_query_no_skip", |b| {
        b.iter(|| execute_query(&index, "redis database", 10, SkipMode::Disabled))
    });

    c.bench_function("top_10_query_with_skip", |b| {
        b.iter(|| execute_query(&index, "redis database", 10, SkipMode::Enabled))
    });
}
```

#### 7.2 Realistic Workload Metrics

- **Query latency reduction**: p50, p95, p99 latency with vs. without skipping
- **Throughput improvement**: Queries per second
- **Skip rate in practice**: Actual % of blocks skipped on real queries
- **Result quality**: Verify identical results with and without skipping

---

### Future Enhancements

Once the basic implementation is validated, consider:

1. **Tighter Bounds with (TF, DocLen) Pairs**: Store actual (tf, doc_len) pairs instead of just extrema for tighter upper bounds (see [Alternative 2](#alternative-2-tf-doclen-pairs))

2. **Pre-computed Scores**: For deployments with fixed scorers, store pre-computed max scores for zero query-time computation

3. **Dynamic Threshold Adjustment**: Track running statistics during query execution to dynamically adjust skip thresholds

4. **Multi-term Optimization**: Extend block skipping to intersection and union queries

## Memory Overhead Analysis

- **Per block**: +10 bytes total
  - `max_freq: u16` = 2 bytes
  - `max_doc_score: f32` = 4 bytes
  - `min_doc_len: u32` = 4 bytes
- **Typical block size**: ~100 entries, ~800-1600 bytes of buffer
- **Overhead**: ~0.6-1.25% increase in block memory
- **With 1M blocks**: ~10MB additional memory

## Backward Compatibility

### No RDB Changes Required

The inverted index data is **not persisted in RDB**. It is rebuilt from Redis keys (the actual document data) when loading. Therefore:
- No RDB version changes are needed
- No migration path is required
- Every time an index is rebuilt, the new metadata fields will be populated correctly

### Fork GC Serialization

The `IndexBlock` struct uses serde for fork GC serialization (messagepack). The new fields will be included in the serialization automatically via the `Serialize`/`Deserialize` derives.

### Gradual Rollout
- Feature can be disabled via config flag
- All indexes work correctly (no optimization when disabled, but correct results)
- Optimization activates automatically when enabled

## Future Extensions

### Tighter Bounds with (TF, DocLen) Pairs

As discussed in [Alternative Storage Strategies](#alternative-storage-strategies), storing actual (tf, doc_len) pairs instead of just extrema can provide tighter upper bounds. This could be implemented as an optional enhancement for high-value indexes where the storage overhead (10-28 bytes vs. 10 bytes per block) is justified by improved skip rates.

### Hybrid Storage: Raw Components + Pre-computed Scores

A hybrid approach could offer the best of both worlds:
- Store raw components (`max_freq`, `min_doc_len`, `max_doc_score`) for flexibility
- Additionally store pre-computed max scores for the most common scorers (BM25, TF-IDF)

```rust
pub struct IndexBlock {
    // Flexible components (current proposal)
    max_freq: u16,
    max_doc_score: f32,
    min_doc_len: u32,

    // Optional: Pre-computed for common scorers
    // Updated at index time using the default scorer
    precomputed_max_bm25: Option<f32>,
}
```

This allows:
- **Fast path**: Use pre-computed score when query uses the default scorer
- **Flexible path**: Compute from raw components when using a different scorer

Trade-off: Additional 4 bytes per block for each pre-computed scorer.

### Additional Block Statistics
Consider storing more block-level stats for advanced optimizations:
- `distinct_fields`: Bitmask of fields present in block (for field-specific queries)

### Approximate Block Skip
For very large indexes, maintain a skip list or B-tree of (docId, maxScore) pairs for O(log n) threshold-based seeking.

### Dynamic Score Thresholds
Track running statistics during query execution to dynamically adjust skip thresholds.

## Testing Strategy

1. **Unit tests**: Verify max_freq is correctly tracked during indexing
2. **Correctness tests**: Ensure block skipping produces identical results to full scan
3. **Performance benchmarks**: Measure speedup on top-K queries with varying selectivity
4. **RDB compatibility**: Test loading old/new format files

## Open Questions

1. Should we also track `min_freq` for more precise bounds?
2. How to handle multi-value fields where freq accumulates across values?
3. Should block skipping be exposed as a query hint (e.g., `OPTIMIZER BLOCK_SKIP`)?
4. Impact on numeric and geo index blocks?

---

## Appendix: Illustrative Example

This section provides a concrete walkthrough of how block-level max score optimization works.

### Setup: 20 Documents, 4 Blocks

Consider an index with 20 documents containing the term **"redis"**. Documents are stored in blocks of 5 entries each.

#### Document Content

Each document is a blog post or article. The term frequency (TF) is how many times "redis" appears in the text.

| DocID | Document Content (excerpt) | TF |
|-------|----------------------------|-----|
| 1     | "Getting started with **Redis**. **Redis** is fast. Learn **Redis** today." | 3 |
| 2     | "Database comparison: PostgreSQL vs **Redis**" | 1 |
| 3     | "**Redis** caching guide: **Redis** keys, **Redis** values, **Redis** TTL, **Redis** clusters" | 5 |
| 4     | "How **Redis** and Memcached compare. **Redis** wins." | 2 |
| 5     | "NoSQL databases including **Redis**" | 1 |
| 6     | "**Redis** deep dive: **Redis** Streams, **Redis** Pub/Sub, **Redis** Lua, **Redis** modules, **Redis** Cluster, **Redis** Sentinel, **Redis** persistence, **Redis** replication" | 8 |
| 7     | "Scaling with **Redis**. **Redis** horizontal scaling." | 2 |
| 8     | "Cloud databases: AWS offers **Redis**" | 1 |
| 9     | "**Redis** performance tuning: optimize **Redis**, benchmark **Redis**" | 3 |
| 10    | "**Redis** vs MongoDB. **Redis** for caching." | 2 |
| 11    | "Introduction to **Redis**" | 1 |
| 12    | "Why we chose **Redis**" | 1 |
| 13    | "**Redis** at scale. Enterprise **Redis**." | 2 |
| 14    | "Databases overview: **Redis**" | 1 |
| 15    | "Caching with **Redis**" | 1 |
| 16    | "**Redis** Search: full-text search in **Redis**, vector search in **Redis**, **Redis** queries" | 4 |
| 17    | "Complete **Redis** handbook: **Redis** basics, **Redis** advanced, **Redis** ops, **Redis** security, **Redis** monitoring, **Redis** debugging" | 6 |
| 18    | "**Redis** and Python. **Redis**-py library." | 2 |
| 19    | "Key-value stores like **Redis**" | 1 |
| 20    | "**Redis** in production: **Redis** tips, **Redis** tricks" | 3 |

#### Document Metadata

| DocID | Term Frequency (TF) | Doc Length (words) | Doc Score |
|-------|---------------------|---------------------|-----------|
| 1     | 3                   | 100                 | 1.0       |
| 2     | 1                   | 50                  | 0.8       |
| 3     | 5                   | 200                 | 1.0       |
| 4     | 2                   | 80                  | 0.9       |
| 5     | 1                   | 60                  | 0.7       |
| 6     | 8                   | 150                 | 1.0       |
| 7     | 2                   | 90                  | 0.6       |
| 8     | 1                   | 70                  | 0.5       |
| 9     | 3                   | 100                 | 0.8       |
| 10    | 2                   | 85                  | 0.9       |
| 11    | 1                   | 60                  | 0.4       |
| 12    | 1                   | 55                  | 0.5       |
| 13    | 2                   | 90                  | 0.6       |
| 14    | 1                   | 70                  | 0.3       |
| 15    | 1                   | 65                  | 0.5       |
| 16    | 4                   | 120                 | 1.0       |
| 17    | 6                   | 180                 | 0.9       |
| 18    | 2                   | 75                  | 0.7       |
| 19    | 1                   | 50                  | 0.4       |
| 20    | 3                   | 100                 | 0.8       |

**Note:** Doc Score is a user-provided relevance weight (e.g., based on page rank, freshness, or manual boost).

#### Block Structure with Scoring Metadata

Each block tracks three values computed from its documents:

| Block | DocID Range | Entries | max_freq | min_doc_len | max_doc_score |
|-------|-------------|---------|----------|-------------|---------------|
| 0     | 1 - 5       | 5       | **5**    | **50**      | **1.0**       |
| 1     | 6 - 10      | 5       | **8**    | **70**      | **1.0**       |
| 2     | 11 - 15     | 5       | **2**    | **55**      | **0.6**       |
| 3     | 16 - 20     | 5       | **6**    | **50**      | **1.0**       |

**How these values are derived:**

| Block | Documents | max_freq derivation | min_doc_len derivation | max_doc_score derivation |
|-------|-----------|---------------------|------------------------|--------------------------|
| 0     | 1-5       | max(3,1,5,2,1) = 5  | min(100,50,200,80,60) = 50 | max(1.0,0.8,1.0,0.9,0.7) = 1.0 |
| 1     | 6-10      | max(8,2,1,3,2) = 8  | min(150,90,70,100,85) = 70 | max(1.0,0.6,0.5,0.8,0.9) = 1.0 |
| 2     | 11-15     | max(1,1,2,1,1) = 2  | min(60,55,90,70,65) = 55   | max(0.4,0.5,0.6,0.3,0.5) = 0.6 |
| 3     | 16-20     | max(4,6,2,1,3) = 6  | min(120,180,75,50,100) = 50 | max(1.0,0.9,0.7,0.4,0.8) = 1.0 |

**Key observation:** Block 2 has the worst scoring potential because:
- Low `max_freq` (2) → documents mention "redis" infrequently
- High `min_doc_len` (55) → no very short documents to boost TF normalization
- Low `max_doc_score` (0.6) → no high-priority documents

### TF-IDF Scoring Formula

The TF-IDF score for a document is:

$$
\text{Score} = \frac{\text{TF}}{\text{DocLen}} \times \text{IDF} \times \text{DocScore}
$$

Where:
- **TF** = Term frequency in the document
- **DocLen** = Document length (normalization factor)
- **IDF** = Inverse Document Frequency = $\log_2(1 + \frac{N + 1}{n})$
- **N** = Total documents in index (assume 1000)
- **n** = Documents containing the term (20)

**IDF Calculation:**
$$
\text{IDF} = \log_2\left(1 + \frac{1001}{20}\right) = \log_2(51.05) \approx 5.67
$$

### Computing Block Upper Bounds

To compute the **maximum possible score** from a block, we use the per-block metadata:

$$
\text{BlockMaxScore} = \frac{\text{max\_freq}}{\text{min\_doc\_len}} \times \text{IDF} \times \text{max\_doc\_score}
$$

Using the **actual per-block values** (not global assumptions):

| Block | max_freq | min_doc_len | max_doc_score | BlockMaxScore Calculation           | BlockMaxScore |
|-------|----------|-------------|---------------|-------------------------------------|---------------|
| 0     | 5        | 50          | 1.0           | (5 / 50) × 5.67 × 1.0               | **0.567**     |
| 1     | 8        | 70          | 1.0           | (8 / 70) × 5.67 × 1.0               | **0.648**     |
| 2     | 2        | 55          | 0.6           | (2 / 55) × 5.67 × 0.6               | **0.124**     |
| 3     | 6        | 50          | 1.0           | (6 / 50) × 5.67 × 1.0               | **0.680**     |

**Notice how Block 2's upper bound is much lower (0.124)** because:
- It uses the actual `min_doc_len=55` (not a global minimum)
- It uses the actual `max_doc_score=0.6` (not assuming 1.0)

This tighter bound makes block skipping more effective!

### Query Execution: Top-3 Results

**Query:** `FT.SEARCH idx "redis" LIMIT 0 3`

#### Without Block Skipping (Current Behavior)

The iterator reads all 20 documents, computes scores, and maintains a min-heap of size 3:

| DocID | TF | DocLen | DocScore | Score = (TF/DocLen) × 5.67 × DocScore |
|-------|----|--------|----------|---------------------------------------|
| 1     | 3  | 100    | 1.0      | 0.170                                 |
| 2     | 1  | 50     | 0.8      | 0.091                                 |
| 3     | 5  | 200    | 1.0      | 0.142                                 |
| 4     | 2  | 80     | 0.9      | 0.128                                 |
| 5     | 1  | 60     | 0.7      | 0.066                                 |
| **6** | 8  | 150    | 1.0      | **0.302**                             |
| 7     | 2  | 90     | 0.6      | 0.076                                 |
| 8     | 1  | 70     | 0.5      | 0.040                                 |
| 9     | 3  | 100    | 0.8      | 0.136                                 |
| 10    | 2  | 85     | 0.9      | 0.120                                 |
| 11    | 1  | 60     | 0.4      | 0.038                                 |
| 12    | 1  | 55     | 0.5      | 0.052                                 |
| 13    | 2  | 90     | 0.6      | 0.076                                 |
| 14    | 1  | 70     | 0.3      | 0.024                                 |
| 15    | 1  | 65     | 0.5      | 0.044                                 |
| **16**| 4  | 120    | 1.0      | **0.189**                             |
| **17**| 6  | 180    | 0.9      | **0.170**                             |
| 18    | 2  | 75     | 0.7      | 0.106                                 |
| 19    | 1  | 50     | 0.4      | 0.045                                 |
| 20    | 3  | 100    | 0.8      | 0.136                                 |

**Top 3 Results:** Doc 6 (0.302), Doc 16 (0.189), Doc 17 (0.170)

**Documents processed:** 20

#### With Block Skipping (Proposed Optimization)

**Step 1:** Process Block 0 (docs 1-5)
- BlockMaxScore = 0.567 (always process first block to fill heap)
- Best score found: 0.170 (Doc 1)
- Heap: [Doc 1: 0.170, Doc 3: 0.142, Doc 4: 0.128]
- `minScore` threshold: 0.128 (lowest in heap)

**Step 2:** Check Block 1 (docs 6-10)
- BlockMaxScore = 0.648 > minScore (0.128) → **Process block**
- Find Doc 6 with score 0.302
- Heap: [Doc 6: 0.302, Doc 1: 0.170, Doc 3: 0.142]
- `minScore` threshold: 0.142

**Step 3:** Check Block 2 (docs 11-15)
- BlockMaxScore = **0.124** < minScore (0.142) → **SKIP ENTIRE BLOCK!** 🎉
- Heap unchanged
- `minScore` threshold: 0.142

**Step 4:** Check Block 3 (docs 16-20)
- BlockMaxScore = 0.680 > minScore (0.142) → **Process block**
- Find Doc 16 (0.189) and Doc 17 (0.170)
- Heap: [Doc 6: 0.302, Doc 16: 0.189, Doc 17: 0.170]
- `minScore` threshold: 0.170

**Final Top 3:** Doc 6 (0.302), Doc 16 (0.189), Doc 17 (0.170) ✓

### Why Block 2 Was Skipped

Block 2's upper bound (0.124) is computed from its **actual metadata**:

```
BlockMaxScore = (max_freq / min_doc_len) × IDF × max_doc_score
              = (2 / 55) × 5.67 × 0.6
              = 0.124
```

Since 0.124 < 0.142 (current minScore), **no document in Block 2 can possibly enter the top-3**, so we skip all 5 documents without decoding them.

### Summary

| Metric | Without Optimization | With Optimization |
|--------|---------------------|-------------------|
| Blocks read | 4 | 3 (Block 2 skipped) |
| Documents decoded | 20 | 15 |
| Score computations | 20 | 15 |
| Block metadata checks | 0 | 4 |

The optimization is most effective when:
1. **High-scoring documents appear early** → raises `minScore` quickly
2. **Blocks have unfavorable scoring metadata** → low `max_freq`, high `min_doc_len`, or low `max_doc_score`
3. **Large K values** → more opportunities to establish stable thresholds
4. **Heterogeneous document collections** → natural variation in term frequencies, document lengths, and scores across blocks
