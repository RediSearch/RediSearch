# Design: New Vector Index Metrics

## Overview

Add two new metrics to the INFO section for vector indexes:
1. **HNSW Direct Insertions** - Count of vectors inserted directly into HNSW by the main thread (bypassing the flat buffer)
2. **Total Flat Buffer Size** - Sum of all flat buffer sizes across tiered vector indexes

## Background

In tiered HNSW indexes, vectors are normally inserted into a flat buffer first, then asynchronously moved to the HNSW index by background workers. However, in two scenarios, vectors are inserted directly into HNSW by the main thread:

1. **WriteInPlace mode** - When `VecSim_WriteInPlace` mode is active, all insertions go directly to HNSW
2. **Full flat buffer** - When the flat buffer reaches its limit (`flatBufferLimit`), new vectors are inserted directly into HNSW

Tracking these direct insertions helps monitor indexing behavior and identify potential performance bottlenecks.

The flat buffer size metric provides visibility into the pending work for background indexing threads.

## Architecture

### Data Flow

```
VecSimIndex::statisticInfo()     →  VecSimIndexStatsInfo (C++ struct in VectorSimilarity)
        ↓
VecSimIndex_StatsInfo()          →  C API wrapper
        ↓
IndexSpec_GetVectorIndexStats()  →  Populates VectorIndexStats (RediSearch C struct)
        ↓
IndexSpec_GetVectorIndexesStats()→  Aggregates across all vector fields in an index
        ↓
IndexesInfo_TotalInfo()          →  Aggregates across all indexes into TotalIndexesInfo
        ↓
AddToInfo_*()                    →  Renders to INFO command output
```

### Key Files

| File | Purpose |
|------|---------|
| `deps/VectorSimilarity/src/VecSim/vec_sim_common.h` | `VecSimIndexStatsInfo` struct definition |
| `deps/VectorSimilarity/src/VecSim/vec_sim_tiered_index.h` | `statisticInfo()` implementation for tiered indexes |
| `deps/VectorSimilarity/src/VecSim/algorithms/hnsw/hnsw_tiered.h` | Tiered HNSW implementation (where direct insertions occur) |
| `src/info/vector_index_stats.h` | `VectorIndexStats` struct and getter/setter mappings |
| `src/info/vector_index_stats.c` | Getter/setter implementations and aggregation |
| `src/info/field_spec_info.c` | `IndexSpec_GetVectorIndexStats()` - bridges VecSim to RediSearch |
| `src/info/indexes_info.h` | `TotalIndexesFieldsInfo` struct for aggregated stats |
| `src/info/indexes_info.c` | `IndexesInfo_TotalInfo()` - aggregates across all indexes |
| `src/info/info_redis/info_redis.c` | `AddToInfo_*()` - renders to INFO output |

## Implementation Plan

### Phase 1: VectorSimilarity Changes

#### 1.1 Add counter to TieredHNSWIndex
In `hnsw_tiered.h`, add a counter field:
```cpp
std::atomic<size_t> directHNSWInsertions{0};
```

Increment in `addVector()` when inserting directly to HNSW (both WriteInPlace and full-buffer cases).

#### 1.2 Extend VecSimIndexStatsInfo
In `vec_sim_common.h`:
```c
typedef struct {
    size_t memory;
    size_t numberOfMarkedDeleted;
    size_t directHNSWInsertions;  // NEW: Direct insertions to HNSW by main thread
    size_t flatBufferSize;        // NEW: Current flat buffer size
} VecSimIndexStatsInfo;
```

#### 1.3 Update statisticInfo() implementations
In `vec_sim_tiered_index.h`:
```cpp
VecSimIndexStatsInfo statisticInfo() const override {
    return VecSimIndexStatsInfo{
        .memory = this->getAllocationSize(),
        .numberOfMarkedDeleted = this->getNumMarkedDeleted(),
        .directHNSWInsertions = 0,  // Base class returns 0
        .flatBufferSize = this->frontendIndex->indexSize(),
    };
}
```

In `hnsw_tiered.h`, override to include the counter:
```cpp
VecSimIndexStatsInfo statisticInfo() const override {
    auto stats = VecSimTieredIndex<DataType, DistType>::statisticInfo();
    stats.directHNSWInsertions = this->directHNSWInsertions.load();
    return stats;
}
```

### Phase 2: RediSearch Changes

#### 2.1 Extend VectorIndexStats
In `vector_index_stats.h`:
```c
typedef struct VectorIndexStats {
    size_t memory;
    size_t marked_deleted;
    size_t direct_hnsw_insertions;  // NEW
    size_t flat_buffer_size;        // NEW
} VectorIndexStats;
```

#### 2.2 Add getter/setter functions
In `vector_index_stats.c`, add:
- `VectorIndexStats_GetDirectHNSWInsertions()` / `VectorIndexStats_SetDirectHNSWInsertions()`
- `VectorIndexStats_GetFlatBufferSize()` / `VectorIndexStats_SetFlatBufferSize()`

Update mapping arrays and `VectorIndexStats_Agg()`.

#### 2.3 Update VectorIndexStats_Metrics array
```c
static char* const VectorIndexStats_Metrics[] = {
    "memory",
    "marked_deleted",
    "direct_hnsw_insertions",
    "flat_buffer_size",
    NULL
};
```

#### 2.4 Update IndexSpec_GetVectorIndexStats()
In `field_spec_info.c`:
```c
VectorIndexStats IndexSpec_GetVectorIndexStats(FieldSpec *fs) {
    // ... existing code ...
    stats.direct_hnsw_insertions = info.directHNSWInsertions;
    stats.flat_buffer_size = info.flatBufferSize;
    return stats;
}
```

#### 2.5 Extend TotalIndexesFieldsInfo
In `indexes_info.h`:
```c
typedef struct {
    size_t total_vector_idx_mem;
    size_t total_mark_deleted_vectors;
    size_t total_direct_hnsw_insertions;  // NEW
    size_t total_flat_buffer_size;        // NEW
} TotalIndexesFieldsInfo;
```

#### 2.6 Update aggregation in IndexesInfo_TotalInfo()
In `indexes_info.c`:
```c
info.fields_stats.total_direct_hnsw_insertions += vec_info.direct_hnsw_insertions;
info.fields_stats.total_flat_buffer_size += vec_info.flat_buffer_size;
```

#### 2.7 Add to INFO output
In `info_redis.c`:
```c
RedisModule_InfoAddFieldULongLong(ctx, "vector_direct_hnsw_insertions", 
    total_info->fields_stats.total_direct_hnsw_insertions);
RedisModule_InfoAddFieldULongLong(ctx, "vector_flat_buffer_size",
    total_info->fields_stats.total_flat_buffer_size);
```

## INFO Output

The new metrics will appear in the `search_indexes` section:
```
# search_indexes
...
used_memory_vector_index:12345678
vector_direct_hnsw_insertions:42
vector_flat_buffer_size:1000
```

And in per-field statistics (FT.INFO):
```
field statistics:
  - identifier: vec_field
    attribute: vec_field
    memory: 12345678
    marked_deleted: 0
    direct_hnsw_insertions: 42
    flat_buffer_size: 500
```

## Testing

1. **Unit tests**: Verify counter increments in WriteInPlace mode and full-buffer scenarios
2. **Integration tests**: Verify INFO output contains new metrics with correct values
3. **Cluster tests**: Verify aggregation works correctly across shards

## Notes

- The `directHNSWInsertions` counter is cumulative (never reset)
- The `flatBufferSize` is a point-in-time snapshot
- For non-tiered indexes, both new metrics return 0
- For SVS tiered indexes, `directHNSWInsertions` returns 0 (SVS doesn't have this concept)

