# Design: Sparse Vector Search in RediSearch

## Scope

This document covers sparse vector search end to end:
- Index data structures (inverted index, posting lists)
- Similarity metrics and normalization
- Query processing (candidate generation, scoring, top-K selection)
- Field creation and query syntax
- Implementation plan

**Out of scope:**
- Client library integration
- Approximate (non-exact) sparse retrieval algorithms

## Overview

Sparse vector search reuses RediSearch's full-text search (FTS) infrastructure. Each field keeps a hashmap of `dimension_id → posting list (InvertedIndex)`, in place of the trie used for text terms. A query is the union of its dimensions' posting lists, scored by dot product inside a top-K iterator. This is the same computational pattern as TF-IDF over an OR query.

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

### Learned Sparse Representations (SPLADE)

Modern sparse vector search often uses **learned sparse embeddings** like SPLADE (Sparse Lexical and Expansion Model):

- Trained neural models that produce sparse vectors from text
- Enable **term expansion**: adding semantically related terms not in the original text
- Weights are learned importance scores, not just term frequencies, and are non-negative
- Trained for dot-product similarity
- Indexed with inverted indexes, like text terms

### Industry Practice

Sparse vector search is commonly implemented with an **inverted index**: each non-zero dimension acts as a term, and its posting list holds `(doc_id, weight)` pairs. A query scores every document that shares at least one dimension with it. Systems differ in whether retrieval is exact or approximate (e.g., dropping low-weight query dimensions or posting entries). This design is exact and exhaustive: every document in the union is scored.

## Index Structure

### Inverted Index per Field

- **Dimension space**: the field declares its dimensionality `DIM` at creation, as dense vector fields do. Valid dimension IDs are `0 … DIM−1`, stored as `u32`, so `DIM` is at most 2³². For learned models `DIM` is the model's vocabulary size (e.g., 30522 for BERT-based SPLADE); for hashed vocabularies it is the hash space.
- **Hashmap over trie**: dimension IDs are integers, not strings. No prefix matching is needed, so O(1) hashmap lookup is preferred to O(k) trie traversal.
- **Hashmap over an array of size `DIM`**: memory grows with the dimensions that actually occur, not with `DIM`. That keeps a large hash space (e.g., 2³²) cheap, and costs little for small vocabularies.
- **Numeric encoder reuse**: the existing `Numeric` encoder (`src/redisearch_rs/inverted_index/src/codec/numeric.rs`) stores `(doc_id, float)` pairs with delta-encoded doc IDs. Weights are converted to `f32` before encoding, so the float-compressing variant stores each one losslessly in 4 bytes.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Sparse Vector Field                             │
├──────────────────────────────────────────────────────────────────────────┤
│  HashMap<dimension_id, InvertedIndex<Numeric>>                           │
│                                                                          │
│  dimension_0  ─→ [doc_1: 0.5, doc_3: 0.7, doc_8: 0.2]                    │
│  dimension_42 ─→ [doc_1: 0.3, doc_5: 0.9]                                │
│  dimension_99 ─→ [doc_2: 0.1, doc_3: 0.4, doc_8: 0.6]                    │
├──────────────────────────────────────────────────────────────────────────┤
│  Field attributes: DIM, metric                                           │
│  Stats: documents, dimensions, entries, memory                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### Rust Data Model

Crate: `src/redisearch_rs/sparse_vector_index/`; C ABI in `src/redisearch_rs/c_entrypoint/sparse_vector_index_ffi/`.

```rust
/// One field's sparse vector index.
pub struct SparseVectorIndex {
    dimensions: FxHashMap<u32, EntriesTrackingIndex<NumericFloatCompression>>,
    metric: SparseMetric,
    dim: u32,
    num_docs: u64,
    memory_usage: usize,
}

pub enum SparseMetric {
    InnerProduct,
    /// Weights are L2-normalized before they are written.
    Cosine,
}
```

### C Integration

New field type in `src/field_spec.h`:

```c
typedef enum {
  INDEXFLD_T_FULLTEXT = 0x01,
  INDEXFLD_T_NUMERIC = 0x02,
  INDEXFLD_T_GEO = 0x04,
  INDEXFLD_T_TAG = 0x08,
  INDEXFLD_T_VECTOR = 0x10,
  INDEXFLD_T_GEOMETRY = 0x20,
  INDEXFLD_T_SPARSE_VECTOR = 0x40,
} FieldType;
```

`INDEXFLD_NUM_TYPES` is 7. The `FieldSpec` union holds the opaque Rust index:

```c
struct {
  struct SparseVectorIndex *sparseVecIndex;
} sparseVecOpts;
```

C-side accessors (`src/sparse_vector_index.h`), following `TagIndex_Open` and `openNumericOrGeoIndex`:

```c
typedef struct SparseVectorIndex SparseVectorIndex;

SparseVectorIndex *openSparseVectorIndex(FieldSpec *fs, bool create_if_missing);
void SparseVectorIndex_Free(SparseVectorIndex *idx);

size_t SparseVectorIndex_NumDocs(const SparseVectorIndex *idx);
size_t SparseVectorIndex_NumDimensions(const SparseVectorIndex *idx);
size_t SparseVectorIndex_MemoryUsage(const SparseVectorIndex *idx);
```

`FieldSpec_Cleanup` calls `SparseVectorIndex_Free`. `IndexSpec` gets an `Index_HasSparseVec` flag, set when a sparse vector field is added.

### Input Validation

Applied at indexing time to every vector. A vector that fails is counted as an indexing failure for that document:
- Dimension IDs must be strictly increasing (which also rules out duplicates).
- Weights must be finite (no NaN or infinity).
- Zero weights are dropped, not stored.
- Every dimension ID must be below the field's `DIM`. With strictly increasing IDs, this also caps the number of non-zero entries per vector at `DIM`.
- Under `COSINE`, a vector whose norm is zero is rejected.

Query vectors go through the same checks, and a failing query vector is a query error. This includes a dimension ID at or above `DIM`, and a zero norm under `COSINE`.

## Similarity Metrics

| Metric | Formula | Use case |
|--------|---------|----------|
| `IP` (dot product) | `Σ q[i] × d[i]` | Default; what SPLADE-style models are trained for |
| `COSINE` | `(Σ q[i] × d[i]) / (‖q‖ × ‖d‖)` | When vector magnitude should not affect similarity |

L2 (Euclidean) distance is not supported; see [Alternatives Considered](#alternatives-considered).

### The Metric Is Fixed at Schema Time

Sparse fields follow the same contract as dense `VECTOR` fields:

- For dense vectors, `DISTANCE_METRIC` is a **mandatory** attribute for every algorithm (`FLAT`, `HNSW`, `SVS-VAMANA`) in `FT.CREATE` (`src/spec.c`). The `KNN` query clause has no metric parameter, so the metric **cannot be overridden at query time**.
- Dense `COSINE` is not a separate scoring kernel. For float types VecSim maps `VecSimMetric_Cosine` to the inner-product distance function (`spaces/spaces.cpp`). Its `CosinePreprocessor` normalizes every vector at **insertion** and every query vector at **query time** (`spaces/computer/preprocessors.h`), and the stored blob is the normalized vector.
- The reported score is a distance, `1 − ⟨q, d⟩`, for both `IP` and `COSINE`.

For sparse fields, `DISTANCE_METRIC IP | COSINE` is declared on the field. Under `COSINE`, each weight is divided by the document's L2 norm before it is written to the posting lists, and the query weights are divided by the query norm. Scoring is then always a plain dot product.

### How Norms Affect Scores

Write `cos(q, d) = ⟨q, d⟩ / (‖q‖ · ‖d‖)`.

| Norm | Effect on ranking | Effect on score value |
|------|-------------------|-----------------------|
| Query norm `‖q‖` | **None**: it is the same constant for every document, under both `IP` and `COSINE` | Scales every score. This matters when the score is combined linearly with another score (e.g., hybrid `LINEAR` fusion), not for rank-based fusion such as RRF |
| Document norm `‖d‖`, under `IP` | Documents with larger-magnitude vectors are favoured, all else equal. For SPLADE this is intended: the model is trained with dot product, so the magnitude carries relevance signal | Unbounded |
| Document norm `‖d‖`, under `COSINE` | Magnitude is removed, so only the direction counts. Applying cosine to SPLADE-style vectors generally *hurts* quality, because it discards a signal the model learned | Bounded to `[−1, 1]` (`[0, 1]` with non-negative weights) |

Consequences:

1. **The document norm needs the whole vector.** `‖d‖` sums over *all* non-zero dimensions of `d`, including those not in the query, so it cannot be recovered from the posting lists a query touches. It is computed at indexing time and used once, at write time.
2. **The metric is fixed at creation.** Raw weights are not kept under `COSINE`, so switching between `IP` and `COSINE` means creating a new index. That is the same trade-off dense vectors make.
3. **Updates cannot leave a stale norm.** An update gets a new doc ID and is normalized from scratch.

## Query Processing

### Candidate Set: Union of the Query Dimensions

Per document, the dot product `Σ q[i] · d[i]` has non-zero terms only for `i ∈ supp(q) ∩ supp(d)`; a dimension the document lacks contributes 0. Across documents, every document that shares **at least one** dimension with the query has a non-zero score and can be in the top-K. The exact candidate set is therefore the **union** of the query dimensions' posting lists:

```
Query: {a: 1.0, b: 1.0, c: 1.0}
Doc 1: {a: 10.0}                    # dot = 10.0  ← top-1
Doc 2: {a: 0.1, b: 0.1, c: 0.1}     # dot = 0.3
```

Doc 1 has only one of the three query dimensions and is still the best match.

| Metric | Score for a candidate | Exact top-K candidate set | Notes |
|--------|-----------------------|---------------------------|-------|
| `IP` | `Σ q_i d_i` over `supp(q) ∩ supp(d)` | Union of the query dimensions' posting lists | Docs with no overlap score 0 and are skipped |
| `COSINE` | Dot product over the overlap, with pre-normalized weights | Union | Identical to `IP` after normalization |

### Mapping onto the Existing Iterators and Scorers

| Concept | Full-text search today | Sparse vector search |
|---------|------------------------|----------------------|
| Term → posting list | Trie lookup → `InvertedIndex<FreqsOffsets…>` | Hashmap lookup → `InvertedIndex<Numeric>` |
| Merge operator for "any term matches" | `Union` of term iterators (`a \| b \| c`) | `Union` of dimension iterators |
| Per-child payload | Term frequency | Document weight `d_i` |
| Per-child query-side weight | Term IDF × query weight | Query weight `q_i` |
| Per-document score | Scorer walks the aggregate result's children: `Σ tf · idf` (TF-IDF) | `Σ q_i · d_i` over the children present at that doc |
| When the score is computed | After iteration, in the scorer result processor | **Inside the iterator**, which keeps only the top K in a bounded heap |
| Extra filter (`@tag:{x}`) | `Intersection(filter, union)` | Same: the filter restricts candidates; only the sparse child contributes score |

A plain `Union<InvIndIterator<Numeric>>` followed by a dot-product scorer computes the same results. Scoring and top-K selection still live inside the iterator, for two reasons. Only K results leave it, instead of every document in the union flowing through the result-processor pipeline. And it takes the same top-K path as dense KNN, so filters, `FT.HYBRID` and the coordinator treat both field types the same way.

### Architecture

```
FT.SEARCH / FT.HYBRID ── query parser (KNN on a SPARSEVECTOR field)
          │
          ▼
  TopKIterator<SparseScoreSource>          (existing top_k crate: modes, heap, timeout,
          │                                 suspend/resume, profile)
          ├─ Unfiltered:  union + dot product + heap → one batch of top-K
          └─ Adhoc:       filter child drives; lookup_score(doc) = Σ q_i · skip_to(doc) on each dimension
          │
          ▼
  SparseVectorIndex  (per field)
```

1. **Indexing.** The document value is parsed and [validated](#input-validation), normalized under `COSINE`, and its entries are added to the posting lists. This happens under the spec write lock, like the other field types.
2. **Query: `SparseScoreSource` implementing `top_k::ScoreSource`.** This reuses the orchestration of the dense vector path:
   - `all_results_unfiltered_batch`: traverse the union of the query dimensions, accumulating the dot product per document into a `TopKHeap(k)`, and return the top-K as an owned batch sorted by doc ID. Owning the batch satisfies the `ScoreBatch` rule that a batch holds no reference into the index.
   - `lookup_score`: the adhoc scan visits doc IDs in increasing order, so each dimension reader only moves forward with `skip_to`. This gives an exact filtered search, with no recall loss from post-filtering.
   - `batch_strategy`: with a filter, switch to adhoc immediately. A heuristic choice between pushing the filter into the union traversal and adhoc scanning is later work.
   - Revalidation: dimension readers follow the existing inverted-index iterator revalidation contract when the lock is released and GC runs.
3. **Score reporting.** The score is reported as the distance `1 − ⟨q, d⟩`, consistent with dense `IP`/`COSINE`, so `AS <name>`/`YIELD_SCORE_AS`, `SORTBY` and hybrid fusion behave the same for dense and sparse fields.
4. **FFI.** `sparse_vector_index_ffi` exposes create/free/add/stats and an iterator constructor returning a `QueryIterator*`. The C side calls it from the field-type dispatch in indexing and in query evaluation.
5. **GC.** A scanner under `src/redisearch_rs/index_gc/src/scanners/` removes deleted doc IDs from each dimension and drops empty dimensions.
6. **Persistence.** Only the field attributes are saved in the spec RDB. Posting lists are rebuilt from the keyspace on load, as for tag, numeric and dense fields. A new field type in the spec RDB is a persistence-format change, so this feature goes through the full spec-driven review rather than the unstable-features gate.
7. **Cluster.** Each shard returns its top-K and the coordinator merges them by score, the existing dense KNN path. Ties are broken by key.

### Query Edge Cases

**Missing dimensions in documents** contribute 0 to the score:

```
Query: {dim_10: 0.5, dim_20: 0.3, dim_999: 0.8}
Doc A: {dim_10: 1.0, dim_20: 0.5}  # No dim_999
Doc B: {dim_10: 0.2}               # No dim_20, no dim_999

Dot product scores:
- Doc A: 0.5×1.0 + 0.3×0.5 + 0.8×0 = 0.65
- Doc B: 0.5×0.2 + 0.3×0   + 0.8×0 = 0.10
```

**Query dimensions not in the index** have no posting list and contribute 0 to every score. The ranking is determined by the dimensions that do exist.

**A query with only non-existent dimensions** has no posting list to iterate and returns an empty result. This matches text search, where a term that occurs in no document returns no results.

**Ties** are broken by document ID within a shard (lower ID first), and by key when the coordinator merges shard results, because doc IDs are local to each shard.

## Syntax

### Field Creation

Attributes follow a count of arguments, as for dense `VECTOR` fields: `SPARSEVECTOR <nargs> <attribute> <value> …`.

```
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA
    title    TEXT
    category TAG
    splade   SPARSEVECTOR 4 DIM 30522 DISTANCE_METRIC IP
```

| Attribute | Required | Values | Meaning |
|-----------|----------|--------|---------|
| `DIM` | Yes | Integer, 1 to 2³² | Size of the dimension space; valid dimension IDs are `0 … DIM−1`. Fixed at creation, as for dense vectors |
| `DISTANCE_METRIC` | Yes | `IP` \| `COSINE` | Fixed at creation, as for dense vectors |
| `TYPE` | No | `FLOAT32` (default) | Weight type; room for `FLOAT16`/`UINT8` quantization |

### Document Value

- **HASH**: a binary blob of little-endian `(u32 dim_id, f32 weight)` pairs with strictly increasing `dim_id`. Binary like dense vectors, with no text parsing on the write path.
- **JSON**: `{"indices": [331, 14136], "values": [0.5, 0.7]}`, the shape most sparse-vector clients produce.

```
HSET doc:1 title "running shoes" category footwear splade <blob>
JSON.SET doc:2 $ '{"splade": {"indices": [331, 14136], "values": [0.5, 0.7]}}'
```

### Query

The query vector uses the same blob format as the HASH value, passed as a parameter.

```
# Top-10 by dot product
FT.SEARCH idx "*=>[KNN 10 @splade $q AS dist]" PARAMS 2 q <blob> SORTBY dist DIALECT 2

# Filtered: exact top-10 among footwear documents
FT.SEARCH idx "(@category:{footwear})=>[KNN 10 @splade $q AS dist]" PARAMS 2 q <blob> DIALECT 2

# Hybrid: full-text + sparse, fused with RRF
FT.HYBRID idx SEARCH "@title:shoes" VSIM @splade $q KNN 2 K 10 COMBINE RRF 2 CONSTANT 60 PARAMS 2 q <blob>
```

Range queries (`VECTOR_RANGE`) and dense + sparse fusion in a single `FT.HYBRID` are not part of the first version. The latter needs a decision on how `FT.HYBRID` composes two vector clauses.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| Trie for dimension lookup | Dimension IDs are integers with no prefix queries; a hashmap is O(1) |
| Array of posting lists indexed by dimension ID | Memory grows with `DIM` rather than with the dimensions in use, which is prohibitive for large hash spaces |
| A cap on non-zero entries per vector instead of `DIM` | `DIM` matches how sparse models are described (vocabulary size), catches vectors from the wrong model, and already bounds the vector size |
| Intersection of the query dimensions as the candidate set | Misses documents that lack some query dimension but still score highest (see [Candidate Set](#candidate-set-union-of-the-query-dimensions)) |
| Metric-agnostic index, metric chosen per query, norms stored in DocumentMetadata | DocumentMetadata is per document, not per field, so it cannot hold a norm for each sparse field, and it adds memory to every document of every index. Dense vectors already fix the metric at creation |
| L2 (Euclidean) distance | `‖q‖² + ‖d‖² − 2⟨q, d⟩` is finite for documents with no overlapping dimension, which can rank first, so the candidate set is the whole corpus and posting lists alone cannot answer the query |
| Weighted Jaccard | Needs each document's full weight mass, like a norm; no demand identified |
| Scoring in the result-processor scorer, as for TF-IDF | Every document in the union would flow through the result-processor pipeline instead of only the top K, and the query would not share the dense KNN top-K path |
| Serializing posting lists to RDB | Indexes are rebuilt from the keyspace on load; persisting them adds a format commitment with no benefit |

## Implementation Plan

Each step is roughly one reviewable PR.

| # | Step | Main locations | Status | Done when |
|---|------|----------------|--------|-----------|
| 0 | Proposal and design review, spec delta for the new field type and query semantics | `docs/design/`, GitHub issue | In progress | Maintainer agreement on metrics, syntax, union semantics |
| 1 | Storage crate: `f32` weights, input validation, `COSINE` normalization | `sparse_vector_index/` | Partial: crate with `dim_id → posting list` exists; validation and normalization missing | Rust unit tests + property test against a brute-force dot product |
| 2 | Schema: field type, parse `SPARSEVECTOR <nargs> …` (`DIM`, `DISTANCE_METRIC`, `TYPE`), `FT.INFO` output, spec RDB save/load of the field attributes, free on drop | `spec.c`, `field_spec.[ch]`, `info/` | Partial: `INDEXFLD_T_SPARSE_VECTOR` and the `FieldSpec` union member exist | Flow tests for create / info / invalid args / RDB reload |
| 3 | Indexing: HASH blob and JSON parsing, add on write, delete through DocTable, FFI create/add/free | `document.c`, `sparse_vector_index_ffi/` | Not started | Flow tests: index, update, delete, expire, rejection of dimension IDs `≥ DIM` |
| 4 | Query: `SparseScoreSource` over the union, wired into `TopKIterator`, parser dispatch for `KNN` on a sparse field, `AS` score | `top_k`, `query.c`, `iterators_ffi` | Not started | Results equal a brute-force reference; filtered and unfiltered flow tests |
| 5 | GC scanner | `index_gc/src/scanners/` | Not started | GC flow tests; memory returns after delete |
| 6 | Cluster and `FT.HYBRID`: coordinator merge, `VSIM` on a sparse field, `FT.PROFILE` output | `coord/`, `hybrid/` | Not started | Cluster flow tests; hybrid RRF/LINEAR tests |
| 7 | Benchmarks: SPLADE-style corpus (e.g. MS MARCO) for latency and memory | `tests/benchmarks/` | Not started | Benchmark YAML in CI |

Later work: weight quantization, range queries, dense + sparse in one `FT.HYBRID`, approximate algorithms.
