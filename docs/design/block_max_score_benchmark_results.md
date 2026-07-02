# Block-Max Score Optimization Benchmark Results

This document presents benchmark results for the block-max score optimization,
which allows skipping entire blocks during Top-K queries when their maximum
possible score is below the current threshold.

## Current Implementation Limitations

The current block metadata tracks:
- `max_freq`: Maximum term frequency in the block
- `min_doc_len`: Minimum document length in the block
- `max_doc_score`: Maximum document score in the block

**Important**: These bounds are **loose upper bounds**, not tight bounds. The issue is that
`max_freq`, `min_doc_len`, and `max_doc_score` may come from **different documents** within
the same block. For example:
- Document A might have `max_freq = 100` but `doc_len = 500`
- Document B might have `min_doc_len = 50` but `freq = 5`

The block max score computation assumes the best case: a document with `max_freq` AND
`min_doc_len` AND `max_doc_score` simultaneously, which may not exist.

### Potential Improvement: Track Actual Maximum Score

Instead of storing individual components (`max_freq`, `min_doc_len`, `max_doc_score`) and
combining them later, we could compute and store the **actual maximum score** during indexing:

```rust
struct BlockMetadata {
    // Existing fields for other purposes...
    max_freq: u32,
    min_doc_len: u32,
    max_doc_score: f32,

    // NEW: The actual maximum score seen in this block
    // Computed at index time for each document added
    actual_max_score: f32,
}
```

During indexing, for each document added to the block:
```rust
let doc_score = compute_score(freq, doc_len, doc_score, idf, avg_doc_len, ...);
block.actual_max_score = block.actual_max_score.max(doc_score);
```

This gives a **tight upper bound** because it's the real maximum score of any document
in the block, not an optimistic combination of best-case values from different documents.

**Trade-off**: This requires knowing the scoring function at index time, or storing
multiple precomputed max scores (one per scorer type: TF-IDF, BM25, DocScore).

## Benchmark Configuration

- **Index sizes**: 10K, 100K, 1M documents
- **Distributions**: Uniform (random), Zipfian (power law - few high values, many low)
- **Scorers**: TF-IDF, BM25, DocScore
- **K values**: 10, 100, 1000

## Results: 10K Documents

| Distribution | Scorer | K | Blocks Skipped | Baseline | With Skipping | Speedup |
|--------------|--------|---|----------------|----------|---------------|---------|
| uniform | tfidf | 10 | 24.0% | 94µs | 88µs | **1.07x** |
| uniform | bm25 | 10 | 23.0% | 105µs | 100µs | **1.05x** |
| uniform | docscore | 10 | 0.0% | 83µs | 100µs | 0.83x |
| uniform | tfidf | 100 | 0.0% | 106µs | 128µs | 0.83x |
| uniform | bm25 | 100 | 0.0% | 117µs | 142µs | 0.82x |
| uniform | docscore | 100 | 0.0% | 84µs | 101µs | 0.83x |
| uniform | tfidf | 1000 | 0.0% | 164µs | 200µs | 0.82x |
| uniform | bm25 | 1000 | 0.0% | 165µs | 220µs | 0.75x |
| uniform | docscore | 1000 | 0.0% | 98µs | 116µs | 0.84x |
| zipfian | tfidf | 10 | 0.0% | 94µs | 116µs | 0.81x |
| zipfian | bm25 | 10 | 3.0% | 106µs | 125µs | 0.85x |
| zipfian | docscore | 10 | 75.0% | 84µs | 26µs | **3.2x** |
| zipfian | tfidf | 100 | 0.0% | 108µs | 131µs | 0.82x |
| zipfian | bm25 | 100 | 0.0% | 121µs | 144µs | 0.84x |
| zipfian | docscore | 100 | 19.0% | 94µs | 98µs | 0.96x |
| zipfian | tfidf | 1000 | 0.0% | 162µs | 199µs | 0.81x |
| zipfian | bm25 | 1000 | 0.0% | 164µs | 207µs | 0.79x |
| zipfian | docscore | 1000 | 0.0% | 125µs | 145µs | 0.86x |

## Results: 100K Documents

| Distribution | Scorer | K | Blocks Skipped | Baseline | With Skipping | Speedup |
|--------------|--------|---|----------------|----------|---------------|---------|
| uniform | tfidf | 10 | 72.0% | 1.11ms | 371µs | **3.0x** |
| uniform | bm25 | 10 | 71.8% | 1.21ms | 412µs | **2.9x** |
| uniform | docscore | 10 | 0.0% | 996µs | 1.20ms | 0.83x |
| uniform | tfidf | 100 | 18.9% | 1.15ms | 1.11ms | **1.04x** |
| uniform | bm25 | 100 | 17.2% | 1.24ms | 1.23ms | **1.01x** |
| uniform | docscore | 100 | 0.0% | 997µs | 1.20ms | 0.83x |
| uniform | tfidf | 1000 | 0.0% | 1.38ms | 1.66ms | 0.83x |
| uniform | bm25 | 1000 | 0.0% | 1.48ms | 1.77ms | 0.84x |
| uniform | docscore | 1000 | 0.0% | 1.03ms | 1.22ms | 0.84x |
| zipfian | tfidf | 10 | 12.6% | 1.15ms | 1.18ms | 0.97x |
| zipfian | bm25 | 10 | 15.6% | 1.22ms | 1.25ms | 0.98x |
| zipfian | docscore | 10 | 95.4% | 1.01ms | 56µs | **18x** |
| zipfian | tfidf | 100 | 0.0% | 1.14ms | 1.37ms | 0.83x |
| zipfian | bm25 | 100 | 1.6% | 1.25ms | 1.51ms | 0.83x |
| zipfian | docscore | 100 | 73.6% | 1.05ms | 334µs | **3.1x** |
| zipfian | tfidf | 1000 | 0.0% | 1.43ms | 1.70ms | 0.84x |
| zipfian | bm25 | 1000 | 0.0% | 1.53ms | 1.81ms | 0.85x |
| zipfian | docscore | 1000 | 12.6% | 1.18ms | 1.23ms | 0.96x |

## Results: 1M Documents

| Distribution | Scorer | K | Blocks Skipped | Baseline | With Skipping | Speedup |
|--------------|--------|---|----------------|----------|---------------|---------|
| uniform | tfidf | 10 | 87.7% | 25.6ms | 2.4ms | **10.7x** |
| uniform | bm25 | 10 | 88.0% | 31.4ms | 2.6ms | **12.1x** |
| uniform | docscore | 10 | 0.0% | 23.6ms | 25.7ms | 0.92x |
| uniform | tfidf | 100 | 68.5% | 28.0ms | 7.8ms | **3.6x** |
| uniform | bm25 | 100 | 71.8% | 28.3ms | 7.7ms | **3.7x** |
| uniform | docscore | 100 | 0.0% | 26.5ms | 27.0ms | 0.98x |
| uniform | tfidf | 1000 | 15.4% | 32.2ms | 29.2ms | **1.1x** |
| uniform | bm25 | 1000 | 12.5% | 28.8ms | 29.6ms | 0.97x |
| uniform | docscore | 1000 | 0.0% | 23.7ms | 28.2ms | 0.84x |
| zipfian | tfidf | 10 | 56.9% | 26.0ms | 12.2ms | **2.1x** |
| zipfian | bm25 | 10 | 48.3% | 29.2ms | 15.9ms | **1.8x** |
| zipfian | docscore | 10 | 99.4% | 27.0ms | 97µs | **278x** |
| zipfian | tfidf | 100 | 12.8% | 27.0ms | 27.0ms | 1.0x |
| zipfian | bm25 | 100 | 15.3% | 27.1ms | 30.0ms | 0.90x |
| zipfian | docscore | 100 | 95.0% | 27.5ms | 744µs | **37x** |
| zipfian | tfidf | 1000 | 0.0% | 27.4ms | 31.2ms | 0.88x |
| zipfian | bm25 | 1000 | 1.0% | 27.6ms | 30.0ms | 0.92x |
| zipfian | docscore | 1000 | 71.8% | 24.2ms | 7.0ms | **3.5x** |

## Key Takeaways

### 1. Optimization Scales with Index Size

At 10K documents, the overhead of checking block scores often outweighs the benefit of
skipping. At 1M documents, the speedups become dramatic because skipping a block saves
reading 100 documents instead of just 100.

### 2. Best Cases (>10x speedup)

| Scenario | Speedup | Blocks Skipped |
|----------|---------|----------------|
| 1M/zipfian/docscore/k10 | **278x** | 99.4% |
| 1M/zipfian/docscore/k100 | **37x** | 95.0% |
| 1M/uniform/bm25/k10 | **12x** | 88.0% |
| 1M/uniform/tfidf/k10 | **11x** | 87.7% |

### 3. Worst Cases (slowdown)

- Small indexes (10K) with 0% blocks skipped: ~0.75-0.85x
- Large k values with 0% blocks skipped: ~0.83-0.88x

The overhead comes from:
- Computing block max scores
- Checking thresholds at block boundaries
- Additional branching in the read loop

### 4. DocScore is Special

DocScore uses only `max_doc_score` for its block max score, which is an **exact upper bound**
(not a loose approximation). When scores are skewed (Zipfian distribution), it can skip
almost all blocks because:
- 10% of docs have high scores (1.5-3.0)
- 90% of docs have score = 1.0
- Once we find the top-k high-scoring docs, threshold rises above 1.0
- All blocks with `max_doc_score = 1.0` can be skipped

### 5. Why TF-IDF/BM25 Don't Skip as Much with Zipfian

With Zipfian distribution, a few documents have very high frequencies. These high-frequency
documents set `max_freq` high in their blocks. Even if most documents in the block have
low scores, the loose upper bound (computed from `max_freq` + `min_doc_len` + `max_doc_score`
from potentially different documents) remains high.

### 6. Rule of Thumb

The optimization helps when `blocks_skipped > ~20%` and hurts when `blocks_skipped ≈ 0%`.

## Future Work: Tighter Bounds

The current loose bounds could be tightened by:

1. **Store precomputed max scores**: For each scorer type, compute and store the actual
   maximum score in the block at index time. This requires either:
   - Knowing the scoring function at index time, or
   - Storing multiple max scores (one per scorer: TF-IDF, BM25, DocScore)

2. **Adaptive skipping**: Disable block-max skipping when the threshold is too low
   to skip any blocks (e.g., during early iterations before top-k heap fills up).

3. **Hybrid approach**: Use loose bounds for initial filtering, then verify with
   tighter bounds only for borderline blocks.
