# TopKQuery Implementation Plan for HNSWDiskIndex

## Overview

This document describes the changes needed to implement the `topKQuery` method for the `HNSWDiskIndex` class (backend disk-based HNSW index). The implementation follows the pattern from the in-memory HNSW index but adds support for optional reranking using full-precision vectors from disk storage.

**Scope**: This document covers only the `HNSWDiskIndex` implementation. The tiered index (`TieredHNSWDiskIndex`) implementation is a separate task.

## Key Concepts

1. **Rerank Flag**: The disk index supports a `rerank` flag (from `VecSimDiskContext`) that triggers reranking of search results using full-precision vectors from disk instead of quantized vectors from memory.

2. **Distance Computation Modes**:
   - During search: Uses quantized vectors (`computeDistanceQuantized_Full`)
   - During reranking: Uses full-precision vectors from disk (`computeDistanceFull_Full`)

## Implementation Phases

The implementation is split into two phases:

1. **Phase 1**: Implement `topKQuery()` - basic search without reranking
2. **Phase 2**: Add reranking support

---

## Phase 1: Backend Index `topKQuery()` (Basic Search)

### Files to Modify

- `vecsim_disk/src/algorithms/hnsw/hnsw_disk.h`

### 1.1 Implement `topKQuery` Method

Replace the stub implementation with a real search:

```cpp
template <typename DataType, typename DistType>
VecSimQueryReply* HNSWDiskIndex<DataType, DistType>::topKQuery(
    const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const {
    
    auto rep = new VecSimQueryReply(this->allocator);
    this->lastMode = STANDARD_KNN;
    
    if (curElementCount_ == 0 || k == 0) {
        return rep;
    }
    
    // Get runtime parameters
    void* timeoutCtx = queryParams ? queryParams->timeoutCtx : nullptr;
    size_t query_ef = efRuntime_;
    if (queryParams && queryParams->hnswRuntimeParams.efRuntime != 0) {
        query_ef = queryParams->hnswRuntimeParams.efRuntime;
    }
    
    // Preprocess query (if needed)
    auto processed_query_ptr = this->preprocessQuery(queryBlob);
    const void* processed_query = processed_query_ptr.get();
    
    // Find entry point for bottom layer search
    idType bottom_layer_ep = searchBottomLayerEP(processed_query, timeoutCtx, &rep->code);
    if (rep->code != VecSim_QueryReply_OK || bottom_layer_ep == INVALID_ID) {
        return rep;
    }
    
    // Search bottom layer with ef parameter
    auto top_candidates = searchLayer<true>(bottom_layer_ep, processed_query, 0, 
                                            std::max(query_ef, k));
    
    // Convert candidates to results (take top k)
    // Results are in max-heap order, need to reverse for ascending score order
    size_t result_count = std::min(k, top_candidates.size());
    rep->results.resize(result_count);
    
    // Pop from heap (worst first) and fill in reverse
    size_t total_size = top_candidates.size();
    for (size_t i = 0; i < total_size - result_count; ++i) {
        top_candidates.pop();
    }
    for (auto it = rep->results.rbegin(); it != rep->results.rend(); ++it) {
        auto [dist, id] = top_candidates.pop_top();
        it->score = dist;
        it->id = getLabelByInternalId(id);
    }
    
    return rep;
}
```

### 1.2 Add `searchBottomLayerEP` Method

This method finds the entry point for the bottom layer by greedily searching through upper layers.

**Note**: The entry point's level is stored in `maxLevel_` (a class member), not retrieved via `getElementLevel()`. The `safeGetEntryPointState()` method returns both the entry point ID and `maxLevel_` together, ensuring thread-safe access.

```cpp
template <typename DataType, typename DistType>
idType HNSWDiskIndex<DataType, DistType>::searchBottomLayerEP(
    const void* query_data, void* timeoutCtx, VecSimQueryReply_Code* rc) const {

    *rc = VecSim_QueryReply_OK;

    // Get entry point and max level atomically (thread-safe)
    auto [currObj, maxLevel] = safeGetEntryPointState();
    if (currObj == INVALID_ID) {
        return INVALID_ID;
    }

    DistType currDist = computeDistance<true>(query_data, currObj);

    // Greedy search through upper layers (from maxLevel down to level 1)
    for (levelType level = maxLevel; level > 0; --level) {
        greedySearchLevel<true>(query_data, level, currObj, currDist, timeoutCtx, rc);
        if (*rc != VecSim_QueryReply_OK) {
            return INVALID_ID;
        }
    }

    return currObj;
}
```

### 1.3 Required Helper Methods

Ensure the following methods exist in `HNSWDiskIndex`:

- `getLabelByInternalId(idType)` - Get external label from internal ID
- `safeGetEntryPointState()` - Get entry point ID and maxLevel atomically (already exists)

### 1.4 Testing Phase 1

After Phase 1, the backend index should be able to perform basic KNN search using quantized vectors. Test by:

1. Creating an `HNSWDiskIndex` with some vectors
2. Calling `topKQuery()` and verifying results are returned in correct order
3. Verifying timeout handling works correctly

---

## Phase 2: Backend Index Reranking Support

### Files to Modify

- `vecsim_disk/src/algorithms/hnsw/hnsw_disk.h`

### 2.1 Store Rerank Flag in Backend Index

The `HNSWDiskIndex` should store the rerank flag from `VecSimDiskContext`:

```cpp
template <typename DataType, typename DistType>
class HNSWDiskIndex : public VecSimIndexAbstract<DataType, DistType>, public VecSimIndexTombstone {
private:
    bool rerankEnabled_;  // Whether to rerank results using full-precision vectors from disk
    // ... existing members ...

public:
    // Constructor should accept rerank flag from VecSimDiskContext
    HNSWDiskIndex(const VecSimParamsDisk* params, ...)
        : ..., rerankEnabled_(params->diskContext->rerank) {}
```

### 2.2 Add `rerankResults` Method

Add a method to rerank results using full-precision vectors from disk:

```cpp
template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::rerankResults(
    const void* queryBlob, VecSimQueryReply* results) const {

    for (auto& result : results->results) {
        // Get internal ID from label
        idType internalId = getInternalIdByLabel(result.id);
        if (internalId != INVALID_ID) {
            // Recompute distance using full-precision vector from disk
            result.score = computeDistanceFull_Full(queryBlob, internalId);
        }
    }

    // Re-sort by new scores
    std::sort(results->results.begin(), results->results.end(),
              [](const auto& a, const auto& b) { return a.score < b.score; });
}
```

### 2.3 Additional Helper Method for Reranking

- `getInternalIdByLabel(labelType)` - Get internal ID from label (needed for reranking)

### 2.4 Update `topKQuery` to Call Reranking

Modify the `topKQuery` method from Phase 1 to call reranking when enabled:

```cpp
template <typename DataType, typename DistType>
VecSimQueryReply* HNSWDiskIndex<DataType, DistType>::topKQuery(
    const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const {

    auto rep = new VecSimQueryReply(this->allocator);
    this->lastMode = STANDARD_KNN;

    if (curElementCount_ == 0 || k == 0) {
        return rep;
    }

    // ... search logic (from Phase 1) ...

    // After populating results, apply reranking if enabled
    if (rerankEnabled_) {
        rerankResults(queryBlob, rep);
    }

    return rep;
}
```

### 2.5 Testing Phase 2

After Phase 2, the backend index should support optional reranking. Test by:

1. Creating an `HNSWDiskIndex` with `rerank=false` - results should use quantized distances
2. Creating an `HNSWDiskIndex` with `rerank=true` - results should use full-precision distances from disk
3. Comparing result ordering between reranked and non-reranked queries
4. Verifying that reranking reads from disk storage correctly

## Data Flow Diagram

```text
              HNSWDiskIndex::topKQuery(queryBlob, k, params)
                              │
                              ▼
              ┌───────────────────────────────────┐
              │  searchBottomLayerEP()            │
              │  - Get entry point and maxLevel   │
              │  - Greedy search through upper    │
              │    layers to find best entry      │
              │    point for bottom layer         │
              └───────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────────┐
              │  searchLayer() at level 0         │
              │  - Beam search with ef parameter  │
              │  - Uses quantized vectors (SQ8)   │
              │  - Returns top candidates         │
              └───────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────────┐
              │  Convert candidates to results    │
              │  - Take top k from candidates     │
              │  - Map internal IDs to labels     │
              └───────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────────┐
              │  If rerankEnabled_:               │
              │  rerankResults()                  │
              │  - Read full vectors from disk    │
              │  - Recompute exact distances      │
              │  - Re-sort by new scores          │
              └───────────────────────────────────┘
                              │
                              ▼
                         Return results
```

## Key Considerations

### Thread Safety

- Reranking in `HNSWDiskIndex::rerankResults()` needs proper locking when accessing disk storage
- The storage access should be thread-safe (storage already handles this)

### Performance

- Reranking involves disk I/O for each result - expensive operation
- Only rerank the final k results, not the full ef candidates
- Consider caching frequently accessed vectors or batch reading from disk

---

## Summary

| Phase | File | Changes |
|-------|------|---------|
| Phase 1 | `hnsw_disk.h` | Implement `topKQuery()`, `searchBottomLayerEP()`, helper methods |
| Phase 2 | `hnsw_disk.h` | Add `rerankEnabled_`, `rerankResults()`, `getInternalIdByLabel()` |
