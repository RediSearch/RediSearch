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

Sparse vector search reuses RediSearch's full-text search (FTS) infrastructure. Each field keeps a hashmap of `dimension_id → posting list (InvertedIndex)`, in place of the trie used for text terms. A sparse vector query behaves like a full-text OR query with `LIMIT`: it is the union of its dimensions' posting lists, each matching document gets a dot-product score, and the result-processor sorter keeps the best `LIMIT` documents. This is the same computational pattern as TF-IDF over an OR query.

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

1. **Learned sparse retrieval**: Indexing and querying the output of learned sparse models such as SPLADE (see [Learned Sparse Representations](#learned-sparse-representations-splade)). Their weights and term expansion cannot be expressed through full-text search, which derives weights from term frequencies.
2. **Recommendation systems**: User preferences and item attributes can be represented as sparse vectors (e.g., user-item interaction matrices, feature-based item representations), enabling efficient similarity-based recommendations

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
│  Stats: dimensions, entries, memory                                      │
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

size_t SparseVectorIndex_NumDimensions(const SparseVectorIndex *idx);
size_t SparseVectorIndex_NumEntries(const SparseVectorIndex *idx);
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

For sparse fields, `DISTANCE_METRIC IP | COSINE` is declared on the field. Under `COSINE`, each weight is divided by the document's L2 norm before it is written to the posting lists, and the query weights are divided by the query norm. Scoring is then always a plain dot product. Unlike dense vectors, the sparse score is reported as a similarity (higher is better), because it ranks like a full-text score; see [Score](#score).

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

### KNN as a Term Search with LIMIT

Dense KNN and sparse KNN look alike to the user but run very differently:

| | Dense KNN (`VECTOR`) | Sparse KNN (`SPARSEVECTOR`) |
|---|---|---|
| Index | VecSim (FLAT, HNSW, SVS-VAMANA) | One inverted index per dimension |
| Can produce results in score order | Yes (VecSim batches) | No: posting lists are in doc-ID order |
| Iterator | `TopKIterator` over a VecSim `ScoreSource`; yields exactly K results | A union of posting-list iterators; yields every document sharing a dimension with the query |
| Where the top K is chosen | Inside the iterator | In the result-processor sorter, from `LIMIT` |
| Closest existing analogue | — | A full-text OR query ranked by score with `LIMIT` |

So sparse KNN reuses the full-text query machinery, not the dense top-K machinery. It maps term for term onto a full-text OR query:

| Concept | Full-text search today | Sparse vector search |
|---------|------------------------|----------------------|
| Term → posting list | Trie lookup → `InvertedIndex<FreqsOffsets…>` | Hashmap lookup → `InvertedIndex<Numeric>` |
| Leaf iterator | `InvIndIterator` per term | `InvIndIterator` per query dimension, with result weight `q_i` |
| Merge operator for "any term matches" | `Union` of term iterators (`a \| b \| c`) | `Union` of dimension iterators |
| Per-child payload | Term frequency | Document weight `d_i` |
| Per-document score | Scorer walks the aggregate result's children: `Σ weight · tf · idf` (TF-IDF, `ext/default.c`) | `Σ q_i · d_i` over the dimension children present at that doc |
| Top K | Sorter heap of size offset + limit (`RPSorter_NewByScore`) | Same |
| Extra filter (`@tag:{x}`) | `Intersection(filter, union)` | Same: the filter restricts candidates; only the sparse node contributes the sparse score |

Retrieval stays exact: every document in the union is scored, and the sorter keeps the true best `LIMIT`.

### Iterator Tree

For `@splade:[SPARSE $q]` with `q = {12: 0.8, 40: 0.3, 77: 0.5}`, filtered by `@category:{footwear}`:

```
Intersection                                   ← only when the query has other clauses
├── TagIterator(@category:{footwear})
└── SparseVectorIterator(@splade)              ← yields Metric(score = Σ q_i · d_i)
    └── Union (QUICK_EXIT = false)             ← UnionFlat or UnionHeap by child count
        ├── InvIndIterator<Numeric>(dim 12)    weight 0.8
        ├── InvIndIterator<Numeric>(dim 40)    weight 0.3
        └── InvIndIterator<Numeric>(dim 77)    weight 0.5
```

1. **Leaves.** Query evaluation (`src/query.c`) looks up each query dimension in the field's hashmap and opens an `InvIndIterator` over its posting list. The iterator's result weight is the query weight `q_i`, as a term iterator's weight is the term's query weight. Dimensions with no posting list are skipped; if none remain, the node becomes an empty iterator.
2. **Union.** The union runs with `QUICK_EXIT = false`, so each result aggregates *every* child positioned on that doc, which the sum needs. The existing `minUnionIterHeap` threshold picks `UnionHeap` for typical SPLADE queries (tens of dimensions) and `UnionFlat` for short ones.
3. **`SparseVectorIterator`.** A thin wrapper over the union and the only new iterator. On each `read`/`skip_to`, it sums `child.weight × child.value` over the union's children and yields a single `Metric` result carrying that score. A new metric kind is added next to vector distance. Reducing at the node keeps the sparse score separate from other clauses: inside an `Intersection` with text or tag children, the score comes from this node alone and is not mixed with their TF-IDF contributions.
4. **Filters and composition.** The node is an ordinary query node, so it combines with text, tag, numeric and geo clauses through the existing `Intersection`, `Union`, `Not` and `Optional` iterators. A filter therefore restricts the candidate set exactly, with no recall loss.
5. **Lifecycle.** Timeouts, profiling (`FT.PROFILE`) and revalidation after the spec lock is released come from the existing iterators. Deleted documents still present in posting lists before GC are dropped by the query-iterator result processor (`DocTable_Borrow`), as for term queries.

### Score

- The `Metric` result is exposed like a vector distance. `=>{$YIELD_SCORE_AS: <name>}` yields it as a field for `RETURN` and `SORTBY`.
- **The sparse score is the default document score.** When a query contains a sparse node and no `SCORER` is given, the document score is the sum of the sparse `Metric` values in the result. The default score ordering with `LIMIT` then gives the top K with no extra arguments, in `FT.SEARCH` and in the `SEARCH` clause of `FT.HYBRID`. The scorer needs no configuration: the metric is fixed at field creation and already applied in the computed score.
- **In a query that mixes a sparse node with other clauses** (e.g. `@title:shoes @splade:[SPARSE $q]`), the default score is the sparse score alone, and the other clauses act as filters. Text and sparse scores are on different scales, so summing them is not meaningful; combining them is what `FT.HYBRID` fusion is for.
- **An explicit `SCORER` overrides the default**, e.g. `SCORER BM25STD` to rank by the text clauses instead. The sparse score stays available through `$YIELD_SCORE_AS`.
- The score is a similarity, higher is better: the dot product under `IP`, and the cosine under `COSINE` (pre-normalized weights).

### Architecture

1. **Indexing.** The document value is parsed and [validated](#input-validation), normalized under `COSINE`, and its entries are added to the posting lists. This happens under the spec write lock, like the other field types.
2. **Query parsing.** A new query node for `@field:[SPARSE $param]` in the DIALECT 2 parser (`src/query_parser/v2/`). Query evaluation builds the [iterator tree](#iterator-tree).
3. **FFI.** `sparse_vector_index_ffi` exposes create/free/add/stats and a constructor that returns the node's iterator tree as a `QueryIterator*`. The C side calls it from the field-type dispatch in indexing and in query evaluation.
4. **GC.** A scanner under `src/redisearch_rs/index_gc/src/scanners/` removes deleted doc IDs from each dimension and drops empty dimensions.
5. **Persistence.** Only the field attributes are saved in the spec RDB. Posting lists are rebuilt from the keyspace on load, as for tag, numeric and dense fields. A new field type in the spec RDB is a persistence-format change, so this feature goes through the full spec-driven review rather than the unstable-features gate.
6. **Cluster.** Each shard returns its best offset + limit results by score, and the coordinator merges them, the existing `FT.SEARCH` sorted-by-score path. Ties are broken by key.

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

The query vector uses the same blob format as the HASH value, passed as a parameter. Sparse KNN is written as a query node, and `K` is the `LIMIT`:

```
# Top-10 by dot product
FT.SEARCH idx "@splade:[SPARSE $q]" LIMIT 0 10 PARAMS 2 q <blob> DIALECT 2

# Same, returning the score as a field and sorting on it
FT.SEARCH idx "@splade:[SPARSE $q]=>{$YIELD_SCORE_AS: score}" SORTBY score DESC LIMIT 0 10 PARAMS 2 q <blob> DIALECT 2

# Filtered: exact top-10 among footwear documents
FT.SEARCH idx "@category:{footwear} @splade:[SPARSE $q]" LIMIT 0 10 PARAMS 2 q <blob> DIALECT 2

# Dense + sparse, fused with RRF
FT.HYBRID idx SEARCH "@splade:[SPARSE $q]" VSIM @embedding $v KNN 2 K 10 COMBINE RRF 2 CONSTANT 60 PARAMS 4 q <sparse_blob> v <dense_blob>
```

`total_results` is the number of documents sharing at least one dimension with the query, as for a full-text OR query, not `K`.

Range queries (a minimum score) are not part of the first version.

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
| Top-K iterator like dense KNN (`TopKIterator` with a sparse `ScoreSource`) | `TopKIterator`'s modes are built around sources that produce results in score order (VecSim batches, the numeric range tree); posting lists are in doc-ID order, so only its plain-union and filter-driven modes would apply, which the ordinary iterator tree already provides. Its heap would still need a per-candidate deleted-document check, as the numeric source does (`DocValidity`), so it saves little over the sorter, and it would not get `LIMIT`/offset, cursors or `FT.AGGREGATE` for free |
| Summing the dimension children directly in the scorer, as TF-IDF does | In a query that combines the sparse node with text or numeric clauses, the scorer cannot tell the sparse children from the others; reducing in `SparseVectorIterator` keeps the sparse score separate |
| Requiring an explicit `SCORER SPARSE` | The scorer has nothing to configure, and omitting it would silently rank sparse queries with the text scorer, which scores them as zero |
| Reusing the `=>[KNN k @field $q]` syntax | Dense KNN returns exactly `k` results and treats the left side as a pre-filter. Sparse retrieval returns the whole union and takes the top from `LIMIT`; a query node states that behaviour and composes with other clauses like a term |
| Serializing posting lists to RDB | Indexes are rebuilt from the keyspace on load; persisting them adds a format commitment with no benefit |

## Implementation Plan

Each step is roughly one reviewable PR.

| # | Step | Main locations | Status | Done when |
|---|------|----------------|--------|-----------|
| 0 | Proposal and design review, spec delta for the new field type and query semantics | `docs/design/`, GitHub issue | In progress | Maintainer agreement on metrics, syntax, union semantics |
| 1 | Storage crate: `f32` weights, input validation, `COSINE` normalization | `sparse_vector_index/` | Done | Rust unit tests + property test against a brute-force dot product |
| 2 | Schema: field type, parse `SPARSEVECTOR <nargs> …` (`DIM`, `DISTANCE_METRIC`, `TYPE`), `FT.INFO` output, spec RDB save/load of the field attributes, free on drop | `spec.c`, `field_spec.[ch]`, `info/` | Done (spec encoding version 30) | Flow tests for create / info / invalid args / RDB reload |
| 3 | Indexing: HASH blob and JSON parsing, add on write, delete through DocTable, FFI create/add/free | `document.c`, `sparse_vector_index_ffi/` | Done | Flow tests: index, update, delete, expire, rejection of dimension IDs `≥ DIM` |
| 4 | Query: `@field:[SPARSE $q]` parser node, iterator tree (dimension `InvIndIterator`s, `Union`, `SparseVectorIterator`), sparse `Metric` kind, sparse score as the default document score, `$YIELD_SCORE_AS` | `query_parser/v2/`, `query.c`, `sparse_vector_index/`, `query_eval/`, `ext/default.c` | Done | Top `LIMIT` equals a brute-force reference; filtered, combined-with-text and unfiltered flow tests |
| 5 | GC scanner | `index_gc/src/scanners/` | Done (fork and thread policies; emptied dimensions are removed) | GC flow tests; memory returns after delete |
| 6 | Cluster and `FT.HYBRID`: coordinator merge, a sparse `SEARCH` clause fused with dense `VSIM`, `FT.PROFILE` output | `coord/`, `hybrid/` | Partial: `FT.HYBRID` and `FT.PROFILE` work standalone; cluster merge not yet tested | Cluster flow tests; dense + sparse RRF/LINEAR tests |
| 7 | Benchmarks: SPLADE-style corpus (e.g. MS MARCO) for latency and memory | `tests/benchmarks/` | Not started | Benchmark YAML in CI |

Later work: weight quantization, range queries, approximate algorithms, and [disk (Redis Flex) support](#future-work-disk-redis-flex-integration).

## Future Work: Disk (Redis Flex) Integration

In the first version, sparse vector fields are RAM-only. `FT.CREATE` on a Flex index rejects `SPARSEVECTOR` through `SearchDisk_MarkUnsupportedFieldIfDiskEnabled`, as it does for `GEOSHAPE`. Disk support lives in RediSearchDisk (`redisearch_disk/`, Rust over SpeedB) and follows the pattern its tag and numeric indexes already use.

### Storage

- **One column family per sparse field**, named like the tag families (`tag_<field_index>`), e.g. `sparse_<field_index>`. It gets the shared block cache and the per-index column-family sizing.
- **Key**: `[dim_id: u32 BE][block_max_doc_id: u64 BE]`. All blocks of a dimension are contiguous and ordered by doc ID, so a prefix extractor on `dim_id` gives one prefix scan per query dimension. This mirrors numeric, whose keys carry the bucket prefix and the `block_max_doc_id` suffix (`InvertedIndexKey`).
- **Value**: a structure-of-arrays block, with all doc IDs first and then all `f32` weights, little-endian. This is the numeric block layout (`index_spec/inverted_index/numeric/block.rs`) with `f32` in place of `f64`, so the generic block traits, binary search by doc ID and archive format can be reused.
- **Appends** go through a merge operator built on `GenericMergeOperator`, as for tags and numerics.

### Write Path

- New `IndexDiskAPI` entry: `indexSparseVector(index, batch, docId, fieldIndex, dims, weights, n)`. It writes every entry of one document into the same `SearchDiskWriteBatchHandle` as the document's doc-table record, so a document's postings commit atomically with it. Validation and `COSINE` normalization stay on the C/RediSearch side and are shared with RAM mode.
- `SearchDisk_DescribeFields` passes the sparse field's attributes, at least `DIM` and the metric, to `openIndexSpec` so the column family can be opened with the right options.

### Query Path

- New `IndexDiskAPI` entry: `newSparseDimensionIterator(index, fieldIndex, dimId, weight, snapshot, status)`. It returns a `QueryIterator*` over one dimension's posting list and reads under a `RedisSearchDiskSnapshot`, like `newTermIterator`/`newTagIterator`.
- The rest of the [iterator tree](#iterator-tree) — the union and `SparseVectorIterator` — is the same as in RAM mode; only the leaves differ.
- With a filter, the intersection's `skip_to` maps onto key seeks within each dimension's prefix.
- **Cost.** The exhaustive union reads every block of every query dimension. On disk that is I/O proportional to the total posting length of the query's dimensions, served by the block cache when hot. It needs to be measured on a SPLADE-scale corpus before it ships.

### GC, Counters and Persistence

- **GC** runs in compaction, as for tags: a compaction filter drops entries whose doc ID is in `DeletedIds` and removes emptied blocks. No sparse scanner from `index_gc` runs in disk mode.
- **Counters** (entries per field, dimensions in use) are kept through a compaction-counter factory like `TagCompactionCounterFactory`, so `FT.INFO` stays correct as compaction removes entries.
- **Persistence differs from RAM mode.** Posting lists live in SpeedB and are not rebuilt from the keyspace. The spec RDB carries only the field attributes, and the disk-only state (`max_doc_id`, deleted IDs, record counts) travels in `IndexSpecRdbState`, which needs no sparse-specific additions beyond any new counters.
- **Disk usage and metrics**: register the column family with the disk-usage callback registry so `FT.INFO` and the Flex metrics report it.

### Plan

| # | Step | Location |
|---|------|----------|
| D1 | Reject `SPARSEVECTOR` on Flex indexes until disk support lands | RediSearch `spec.c` |
| D2 | Column family, key and block format, merge operator, compaction filter and counters | RediSearchDisk `index_spec/inverted_index/sparse*` |
| D3 | `indexSparseVector` and `newSparseDimensionIterator` in `IndexDiskAPI`, wired from the C dispatch | RediSearch `search_disk_api.h`, `search_disk.c`; RediSearchDisk `lib.rs` |
| D4 | Flow tests on Flex: index, query, filter, delete + compaction, restart and RDB reload, replication | RediSearchDisk `flow-tests/` |
| D5 | Benchmarks: latency and I/O per query against RAM mode | RediSearchDisk `benchmarks/` |
