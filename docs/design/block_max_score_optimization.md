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

### Phase 1: Add Block Metadata Fields (Rust)

1. Add fields to `IndexBlock` struct:
   - `max_freq: u16`
   - `max_doc_score: f32`
   - `min_doc_len: u32`
2. Update `IndexBlock::new()` to initialize with neutral values:
   - `max_freq = 0`
   - `max_doc_score = 0.0`
   - `min_doc_len = u32::MAX`
3. Update serialization/deserialization (RDB compatibility)

### Phase 2: Update Block Writing

In `InvertedIndex::write_entry()`, update block metadata when adding entries:

```rust
fn write_entry(
    &mut self,
    entry: &E,
    doc_score: f32,
    doc_len: u32,
    same_doc: bool,
) -> std::io::Result<usize> {
    // ... existing code ...

    // Update block scoring metadata
    let block = self.blocks.last_mut().unwrap();
    block.max_freq = block.max_freq.max(entry.freq as u16);
    block.max_doc_score = block.max_doc_score.max(doc_score);
    block.min_doc_len = block.min_doc_len.min(doc_len);

    // ... rest of existing code ...
}
```

### Phase 3: Add Score Computation Helpers

Create utility functions to compute score upper bounds:
- `compute_tfidf_upper_bound(block, idf)` → uses `max_freq`, `min_doc_len`, `max_doc_score`
- `compute_bm25_upper_bound(block, idf, avg_doc_len)` → uses all three fields with BM25 formula

### Phase 4: Extend Iterator Interface

Add threshold-aware methods to `IndexReader` trait:
```rust
trait IndexReader<'index> {
    // Existing methods...

    /// Skip to doc_id, but also skip blocks that can't contribute >= min_score
    fn skip_to_with_score_threshold(
        &mut self,
        doc_id: t_docId,
        min_score: f64,
        score_params: &ScoreParams,
    ) -> bool;
}
```

### Phase 5: Integration with Query Execution

Modify the result accumulator to pass `minScore` thresholds down to iterators.

## Memory Overhead Analysis

- **Per block**: +10 bytes total
  - `max_freq: u16` = 2 bytes
  - `max_doc_score: f32` = 4 bytes
  - `min_doc_len: u32` = 4 bytes
- **Typical block size**: ~100 entries, ~800-1600 bytes of buffer
- **Overhead**: ~0.6-1.25% increase in block memory
- **With 1M blocks**: ~10MB additional memory

## Backward Compatibility

### RDB Versioning
- Increment RDB version for inverted index format
- Old RDB files: blocks load with `max_freq = u16::MAX` (conservative, no skipping)
- New RDB files: blocks include actual `max_freq`

### Gradual Rollout
- Feature can be disabled via config flag
- Old indexes work correctly (no optimization, but correct results)
- Optimization activates automatically after reindex

## Future Extensions

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
