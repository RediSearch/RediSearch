# Design: Sparse Vector Search in RediSearch

## Scope

This document focuses exclusively on the **core indexing and search components** for sparse vector search:
- Index data structures (inverted index, posting lists)
- Similarity computation (dot product scoring)
- Query processing (iterator merging, score accumulation)

**Out of scope:**
- How sparse vectors are stored in the keyspace (HASH fields, JSON paths, etc.)
- Query syntax and command API (e.g., `FT.SEARCH` extensions)
- Client library integration

These topics will be addressed in separate design documents.

## Overview

This document explores the feasibility of implementing sparse vector search in RediSearch by leveraging existing Full-Text Search (FTS) infrastructure. The core idea is to repurpose inverted index structures—replacing the Trie-based term lookup with a hashmap of `dimension_id -> posting list (InvertedIndex)`—and to use scoring mechanisms analogous to TF-IDF for computing similarity (e.g., cosine similarity or dot product).

## Background

### What Are Sparse Vectors?

Sparse vectors are high-dimensional vectors where most elements are zero, with only a small number of non-zero values. They are represented efficiently as key-value pairs: `{dimension_id: weight}`.

**Example:**
```
dense  = [0.2, 0.3, 0.5, 0.7, ...]  # Hundreds of floats
sparse = {331: 0.5, 14136: 0.7}     # Only non-zero dimensions
```

### Sparse vs Dense Vectors

| Feature | Sparse Vectors | Dense Vectors |
|---------|---------------|---------------|
| Dimensionality | Very high (vocabulary size, e.g., 30K+) | Fixed, moderate (768-1536) |
| Non-zero elements | Few (10-200 per document) | All elements non-zero |
| Interpretability | High (dimensions map to tokens/terms) | Low (opaque embeddings) |
| Exact matching | ✓ Supported | ✗ Not supported |
| Semantic similarity | Limited (learned models like SPLADE) | ✓ Strong |
| Memory efficiency | ✓ Very efficient | Less efficient |

### Use Cases for Sparse Vectors

1. **Hybrid search**: Combining keyword matching (sparse) with semantic search (dense)
2. **Domain-specific search**: Medical, legal, or technical domains with specialized terminology
3. **Interpretable retrieval**: Understanding why documents match queries
4. **Exact term matching**: Guaranteeing results contain specific keywords
5. **Recommendation systems**: User preferences and item attributes can be represented as sparse vectors (e.g., user-item interaction matrices, feature-based item representations), enabling efficient similarity-based recommendations

## Industry Approaches to Sparse Vector Search

### Common Implementation: Inverted Index

The industry-standard approach for sparse vector search uses **inverted indexes**, which is essentially what the proposal suggests. This is a **well-established and validated pattern**.

**How it works:**
- Each non-zero dimension ID acts as a "term"
- For each dimension, maintain a posting list of documents containing that dimension, along with their weights
- Similarity computation (dot product) is performed by intersecting posting lists and summing `query_weight × doc_weight`

### Learned Sparse Representations (SPLADE)

Modern sparse vector search often uses **learned sparse embeddings** like SPLADE (Sparse Lexical and Expansion Model):

- Trained neural models that produce sparse vectors from text
- Enable **term expansion**: adding semantically related terms not in the original text
- Weights are learned importance scores, not just term frequencies
- Still indexed using inverted indexes, but with richer representations

## Critical Analysis of the Proposed Approach

### ✓ What Makes Sense

1. **Inverted index structure is correct**: The proposal to use `dimension_id -> posting list` mirrors the industry-standard approach. This is fundamentally sound.

2. **Reusing existing infrastructure**: RediSearch's `InvertedIndex` implementation is mature and optimized. Leveraging it reduces development effort and inherits existing optimizations (block-based storage, compression, delta encoding).

3. **Hashmap vs Trie**: Using a hashmap for dimension lookup instead of a Trie is appropriate because:
   - Dimension IDs are integers, not strings requiring prefix matching
   - O(1) lookup is preferable to O(k) Trie traversal
   - No autocomplete/prefix-search requirements for dimension IDs

### ⚠️ Points Requiring Clarification

#### 1. Computational Pattern: TF-IDF Analogy

The proposal draws an analogy between TF-IDF scoring and sparse vector similarity computation. This analogy is **conceptually accurate** in terms of the computational pattern:

**TF-IDF scoring pattern:**
```
score(q, d) = Σ TF(t,d) × IDF(t)  for each query term t in document d
```

**Sparse vector dot product pattern:**
```
similarity(q, d) = Σ q[i] × d[i]  for each dimension i where both are non-zero
```

**Why the analogy holds:**
- Both iterate through an inverted index (terms or dimensions)
- Both accumulate per-document scores by multiplying query-side and document-side weights
- Both benefit from the same optimizations (early termination, score upper bounds, WAND)

**Key difference:** TF-IDF weights are derived from term frequency statistics, while sparse vector weights come from the vector representation itself (either learned via SPLADE or computed via other methods).

**Recommendation:** The similarity metric for sparse vectors is dot product. The TF-IDF analogy correctly identifies that the **scoring infrastructure** (iterators, accumulators, top-K selection) can be reused.

#### 2. Handling of Vector Weights

RediSearch's current FTS inverted index stores term frequencies (integers), but sparse vectors need float weights. Fortunately, the **Numeric encoder already exists** and stores `(doc_id, float)` pairs efficiently. This encoder can be reused directly for sparse vector dimensions.

#### 3. Normalization Considerations

For cosine similarity, vectors must be normalized (unit length). Options:
- **Pre-normalize at indexing time**: Store normalized vectors, compute dot product at query time
- **Store norms separately**: Store unnormalized vectors, divide by norms during scoring
- **Support multiple metrics**: Dot product (no normalization) and cosine (with normalization)

### ⚠️ Potential Challenges

#### 1. Vocabulary Size / Dimension Space

- SPLADE uses BERT vocabulary (~30,522 dimensions)
- Custom sparse representations might have larger dimension spaces
- The hashmap approach handles this well, but memory for the hashmap itself should be considered

#### 2. Query-Time Efficiency

Sparse queries typically have 10-100 non-zero dimensions. For each:
- Look up the posting list (O(1) with hashmap)
- Iterate through matching documents
- Accumulate similarity scores

This is efficient, but for very common dimensions (high document frequency), posting lists can be large. Consider:
- Block-max WAND (Weak AND) for early termination
- Score upper-bound pruning

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Sparse Vector Field                              │
├─────────────────────────────────────────────────────────────────────┤
│  HashMap<dimension_id, InvertedIndex>                               │
│                                                                      │
│  dimension_0 ──→ [doc_1: 0.5, doc_3: 0.7, doc_8: 0.2, ...]         │
│  dimension_42 ─→ [doc_1: 0.3, doc_5: 0.9, ...]                      │
│  dimension_99 ─→ [doc_2: 0.1, doc_3: 0.4, doc_8: 0.6, ...]         │
│  ...                                                                 │
├─────────────────────────────────────────────────────────────────────┤
│  Metadata:                                                           │
│  - Total documents indexed                                           │
│  - Optional: per-document norms (for cosine similarity)             │
└─────────────────────────────────────────────────────────────────────┘
```

### Query Processing

```
Query: {dim_5: 0.8, dim_42: 0.3, dim_99: 0.5}
                │
                ▼
┌───────────────────────────────────┐
│  1. For each query dimension:     │
│     - Lookup posting list         │
│     - Create iterator             │
└───────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│  2. Merge iterators (DAAT/TAAT)   │
│     - Document-at-a-time: process │
│       all dims for each doc       │
│     - Term-at-a-time: process all │
│       docs for each dim           │
└───────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│  3. Score accumulation            │
│     score[doc] += q[dim] × d[dim] │
└───────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│  4. Top-K selection               │
│     - Heap-based selection        │
│     - Optional: early termination │
└───────────────────────────────────┘
```

## Implementation Considerations

### Reusing InvertedIndex with Numeric Encoder

The existing `InvertedIndex` already supports a `Numeric` encoder (see `src/redisearch_rs/inverted_index/src/numeric.rs`) that stores `(doc_id, float_value)` pairs with efficient encoding:

- **Compact encoding**: Uses 1-16 bytes per entry depending on delta and value size
- **Integer optimization**: Small integers (0-7) encoded in header byte only
- **Float compression**: Supports f32/f64 with optional lossy f32 compression
- **Delta encoding**: Document IDs stored as deltas for compression

This encoder is **exactly what sparse vectors need** for storing dimension weights per document.

### What Already Exists

| Component | Status | Notes |
|-----------|--------|-------|
| Numeric encoder/decoder | ✓ Exists | Stores `(doc_id, float)` pairs efficiently |
| InvertedIndex structure | ✓ Exists | Block-based storage, GC, compression |
| Index iterators | ✓ Exists | Can iterate through posting lists |
| Score accumulation | ✓ Exists | TF-IDF scoring infrastructure |

### New Components Needed

1. **SparseVectorIndex structure**: HashMap wrapper around `dimension_id → InvertedIndex` mapping. This replaces the Trie used for text terms.

2. **Query iterator for sparse vectors**: A new iterator type that:
   - Takes a sparse query vector as input
   - Creates sub-iterators for each query dimension
   - Accumulates dot product scores across dimensions
   - Returns top-K results

3. **Sparse vector field type**: New field type in schema definition (similar to VECTOR but for sparse representation).

### Similarity Metrics

| Metric | Formula | Use Case |
|--------|---------|----------|
| Dot Product | `Σ q[i] × d[i]` | Default; works with any sparse representation |
| Cosine | `(Σ q[i] × d[i]) / (‖q‖ × ‖d‖)` | When magnitude shouldn't affect similarity |

## Comparison with Existing Solutions

### RediSearch FTS vs Sparse Vector Search

| Aspect | FTS | Sparse Vectors |
|--------|-----|----------------|
| Term lookup | Trie (prefix support) | HashMap (O(1) lookup) |
| Values stored | Frequencies (int) | Weights (float) |
| Scoring | TF-IDF, BM25 | Dot product, Cosine |
| Query type | Boolean (AND/OR) | Vector similarity (top-K) |
| Use case | Keyword search | Learned representations |

### Industry Sparse Vector Implementation

Industry approach aligns with this proposal:
- Uses inverted index structure
- Exact search (no approximation)
- Dot product as primary similarity metric

## Recommendations

1. **Proceed with the inverted index approach**: It's industry-validated and leverages RediSearch's existing strengths.

2. **Use HashMap for dimension lookup**: More appropriate than Trie for integer dimension IDs.

3. **Leverage Numeric encoder path**: Reuse existing float storage infrastructure in InvertedIndex.

4. **Support dot product as primary metric**: This is the standard for sparse vectors.

5. **Plan for hybrid search**: Enable combining sparse and dense vector results (e.g., via RRF fusion).
