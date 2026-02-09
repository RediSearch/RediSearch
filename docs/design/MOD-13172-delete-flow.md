# MOD-13172: Implement the Delete Flow

## Status: ✅ Complete

**Scope**: Internal `HNSWDiskIndex` delete mechanism - provides building blocks for MOD-13797.

**Implemented** (MOD-13172):
- `HNSWDiskIndex::markDelete()` - marks element deleted, removes from labelToIdLookup_
- `HNSWDiskIndex::replaceEntryPoint()` - selects new entry point when current is deleted
- Helper methods: `getInternalIdByLabel()`, `getElementMaxLevel()`, `recycleId()`, `decrementElementCount()`
- 19 unit tests in `test_hnsw_disk.cpp`
- Entry point replacement call in `executeDeleteInitJob()` (see note below)

**Note on Scope Overlap**:
The entry point replacement refactor added one line to `executeDeleteInitJob()` in `hnsw_disk_tiered.h`:
```cpp
hnsw_index->replaceEntryPoint();
```
This is technically in MOD-13797's file, but it's a minimal change to wire up the MOD-13172 building block.
The rest of `executeDeleteInitJob()` (neighbor collection, repair job submission) remains MOD-13797 scope.

**Remaining for MOD-13797** (tiered scope):
- `TieredHNSWDiskIndex::deleteVector()` - full implementation
- `executeDeleteInitJob()` - neighbor collection, repair job submission
- `executeDeleteFinalizeJob()` - disk cleanup, ID recycling
- `runGC()` - synchronous garbage collection
- Tiered unit tests, flow tests (`test_delete.py`)

---

## Background

### Design References
- **MVP1 Design**: [Vector Search on Disk - MVP1 Design](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5734727816)
- **Low-Level Design**: [HNSW Disk Low-Level Design](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5806129198)
- **Related Ticket**: MOD-13529 (HNSW disk Add Vector helper functions) - merged via PR #117

### Label-to-ID Mapping

The MVP1 Design document explicitly specifies `labelToIdMap` as part of the in-memory metadata:

```cpp
class HNSW_Disk {
  // In-memory metadata about elements
  shared_mutex lock;
  vecsim_stl::unordered_map<labelType, idType> labelToIdMap;  // Explicitly specified
  vecsim_stl::vector<DiskElementMetaData> idToMetaData;
  vecsim_stl::vector<idType> holes;
  ...
}
```

**Storage decision for MVP1**: RAM-only structure, consistent with RAM HNSW (`HNSWIndex_Single`).
- Provides O(1) label-to-ID lookup required for delete operations
- Serialized/deserialized during RDB save/load (handled separately, not part of this ticket)
- Can be rebuilt from `idToMetaData` if needed

---

## Problem Statement

The HNSW disk index needs a complete delete flow implementation. Currently, `deleteVector()` and `markDelete()` are stubs. The delete flow must:
1. Mark elements as deleted
2. Replace entry point if needed
3. Trigger repair jobs for affected neighbors
4. Clean up disk storage and recycle IDs

---

## DoD Coverage

**Ticket DoD**: "The **internal (not tiered)** algorithm can delete a batch of vectors. We should have, for each vector: In-memory metadata and quantized vector, Disk object per level of outgoing edges, Disk object per level of incoming edges, Disk object of full vector"

### Scope Clarification

The key phrase is **"internal (not tiered)"**. This ticket implements the `HNSWDiskIndex` internal delete mechanism - the building blocks that the tiered layer will use. The tiered coordination (`TieredHNSWDiskIndex::deleteVector()`, job execution, `runGC()`) is handled by MOD-13797.

### What MOD-13172 Implements

| DoD Item | Implementation | Verification |
|----------|----------------|--------------|
| In-memory metadata | `markDelete()` sets `DISK_DELETE_MARK` flag in `idToMetaData[id]`, removes from `labelToIdMap` | `MarkDeleteSetsFlag` test |
| In-memory quantized vector | Remains in `this->vectors` until ID is recycled and overwritten by new insert | `RecycleIdReusesHole` test |
| Helper methods for disk cleanup | `getElementMaxLevel()`, `recycleId()`, `decrementElementCount()` - provide building blocks for MOD-13797 | Unit tests for each helper |

### What MOD-13797 Will Implement (Deferred)

| DoD Item | Implementation | Ticket |
|----------|----------------|--------|
| Disk outgoing edges (per level) | `executeDeleteFinalizeJob()` calls `storage->del_outgoing_edges(id, level)` | MOD-13797 |
| Disk incoming edges (per level) | `executeDeleteFinalizeJob()` calls `storage->del_incoming_edges(id, level)` | MOD-13797 |
| Disk full vector | `executeDeleteFinalizeJob()` calls `storage->del_vector(id)` | MOD-13797 |

**Note on "batch"**: The DoD refers to deleting multiple disk objects per vector (vector + edges at all levels), not multiple vectors in a single API call.

---

## Architecture Overview

### Delete Semantics

The delete flow is **asynchronous** (mark-then-cleanup pattern):
- **Immediate effect**: Element is marked deleted and immediately invisible to searches
- **Deferred cleanup**: Disk storage deletion and ID recycling happen asynchronously via background jobs
- **Synchronous flush via `runGC()`**: Forces execution of ready finalize jobs (see Stage 5)

### Threading Model

| Operation | Thread | Synchronous? | Ticket |
|-----------|--------|--------------|--------|
| `markDelete(label)` | Main thread | Yes | **MOD-13172** ✅ |
| `replaceEntryPoint()` | Worker thread (in `executeDeleteInitJob`) | No | **MOD-13172** ✅ |
| `executeDeleteInitJob()` | Worker thread | No | MOD-13797 (skeleton exists) |
| `executeDeleteFinalizeJob()` | Worker thread | No | MOD-13797 |
| `runGC()` | Caller's thread | Yes | MOD-13797 |

Entry point replacement is in worker thread (not main thread) for consistency with insert flow.

### Delete Flow (Full Architecture)

```
deleteLabelFromHNSW(label)          # [MOD-13797]
    └─> markDelete(label)           # [MOD-13172] ✅
        └─> pendByCurrentlyRunning  # [MOD-13797]
            └─> executeDeleteInitJob        # [MOD-13797] (worker thread)
                ├─> replaceEntryPoint()     # [MOD-13172] ✅
                ├─> getElementMaxLevel(id)  # [MOD-13172] ✅
                ├─> Get incoming edges      # [MOD-13797] TODO
                └─> submitRepairs(...)      # [MOD-13797] TODO
                    └─> executeDeleteFinalizeJob    # [MOD-13797] TODO
                            ├─> recycleId(id)           # [MOD-13172] ✅
                            └─> decrementElementCount() # [MOD-13172] ✅
```

---

## Lock Ordering

**Rule**: Never hold one mutex while acquiring another. All operations in this design acquire and release each mutex before acquiring the next.

### Mutexes

| Mutex | Class | Protects |
|-------|-------|----------|
| `metadataMutex_` | HNSWDiskIndex | `idToMetaData`, `nodeLocks_` |
| `entryPointMutex_` | HNSWDiskIndex | `entryPoint_`, `maxLevel_` |
| `vectorsMutex_` | HNSWDiskIndex | `this->vectors` |
| `holesMutex_` | HNSWDiskIndex | `holes_` |
| `labelToIdMapMutex_` | HNSWDiskIndex | `labelToIdMap` (NEW) |
| `pending_finalize_guard` | TieredHNSWDiskIndex | `pending_finalize_jobs` (NEW) |

### Key Design Decisions

1. **`replaceEntryPoint()`**: Reads entry point state, releases lock, scans metadata, releases lock, then updates entry point. Uses double-check pattern to handle concurrent modifications.

2. **`runGC()`**: Collects ready jobs under `pending_finalize_guard`, releases lock, then executes jobs. This avoids holding the lock during potentially slow disk operations.

3. **`executeDeleteFinalizeJob()`**: Removes from tracking first (under lock), then does cleanup (no lock). This prevents `runGC()` from trying to execute the same job.

---

## Implementation Stages

### Running Tests

```bash
# C++ unit tests (vecsim_disk)
./build.sh test-vecsim

# Python flow tests (requires Redis with SpeedB)
./build.sh test-flow --redis-lib-path /path/to/redis/src
./build.sh test-flow --redis-lib-path /path/to/redis/src --test test_delete.py::test_name
```

---

### Stage 1: Acceptance Tests (Write First)

**Goal**: Define expected behavior before implementation. Tests will fail initially.

**File**: `flow-tests/test_delete.py` (new file)

| Test | Description |
|------|-------------|
| `test_delete_single_vector` | Add vector, delete by label, verify `indexSize` decreases, vector not in search results |
| `test_delete_nonexistent_label` | Delete unknown label, verify no error, index unchanged |
| `test_delete_and_search` | Add 10 vectors, delete 1, verify remaining 9 searchable |
| `test_delete_and_reinsert` | Delete vector, add new vector with same label, verify new vector searchable |
| `test_delete_all_vectors` | Add vectors, delete all, verify empty index |

**File**: `vecsim_disk/tests/unit/test_hnsw_disk_delete.cpp` (new file or extend existing)

**Label-to-ID Map Tests:**
| Test | Description |
|------|-------------|
| `LabelToIdMapBasic` | Add vectors, verify `labelToIdMap` populated correctly |
| `LabelToIdMapAfterDelete` | Delete vector, verify label removed from map |
| `GetInternalIdByLabelFound` | Verify returns correct ID for existing label |
| `GetInternalIdByLabelNotFound` | Verify returns `INVALID_ID` for unknown label |

**markDelete Tests:**
| Test | Description |
|------|-------------|
| `MarkDeleteReturnsCorrectId` | Verify `markDelete()` returns internal ID |
| `MarkDeleteNonExistent` | Verify `markDelete()` returns empty for unknown label |
| `MarkDeleteTwiceReturnsEmpty` | Delete same label twice, second call returns empty (label already removed from map) |
| `MarkDeleteSetsFlag` | Verify `isMarkedDeleted(id)` returns true after `markDelete()` |

**Helper Method Tests (for tiered integration):**
| Test | Description |
|------|-------------|
| `GetElementMaxLevel` | Add vector at level N, verify `getElementMaxLevel()` returns N |
| `DecrementElementCount` | Add vectors, call `decrementElementCount()`, verify `indexSize()` decreases |
| `RecycleIdReusesHole` | Delete vector, add new vector, verify ID is reused from `holes_` |

**Entry Point Replacement Tests:**
| Test | Description |
|------|-------------|
| `ReplaceEntryPointSelectsNeighbor` | Delete entry point, verify neighbor becomes new entry point |
| `ReplaceEntryPointScansLevel` | Delete entry point with no valid neighbors, verify scan finds replacement |
| `ReplaceEntryPointDecreasesLevel` | Delete only element at top level, verify `maxLevel_` decreases |
| `ReplaceEntryPointEmptyIndex` | Delete last element, verify `entryPoint_ = INVALID_ID` |

**Delete Finalize Tests (without runGC):**
| Test | Description |
|------|-------------|
| `ExecuteDeleteFinalizeDeletesStorage` | Manually call `executeDeleteFinalizeJob()`, verify disk keys deleted |
| `ExecuteDeleteFinalizeRecyclesId` | Verify ID added to `holes_` after finalize |
| `ExecuteDeleteFinalizeDecrementsCount` | Verify `indexSize()` decreases after finalize |

**runGC Tests:**
| Test | Description |
|------|-------------|
| `RunGCExecutesReadyJobs` | Delete vector (no repairs needed), call `runGC()`, verify finalize executed |
| `RunGCSkipsPendingJobs` | Delete vector with pending repairs, verify `runGC()` doesn't execute prematurely |

---

### Stage 2: Label-to-ID Infrastructure

**Goal**: Add `labelToIdMap` and wire it into insert flow.

**File**: `vecsim_disk/src/algorithms/hnsw/hnsw_disk.h`

1. Add member variables (~line 282, after `holes_`):
```cpp
// --- Label to ID Mapping ---
// Maps external labels to internal IDs for O(1) lookup during delete.
// RAM-only for MVP1; serialized during RDB save/load.
vecsim_stl::unordered_map<labelType, idType> labelToIdMap;
mutable std::shared_mutex labelToIdMapMutex_;
```

2. Add lookup method:
```cpp
idType getInternalIdByLabel(labelType label) const {
    std::shared_lock<std::shared_mutex> lock(labelToIdMapMutex_);
    auto it = labelToIdMap.find(label);
    return (it != labelToIdMap.end()) ? it->second : INVALID_ID;
}
```

3. Update `addVector()` to populate map after `initElementMetadata()`:
```cpp
{
    std::unique_lock<std::shared_mutex> lock(labelToIdMapMutex_);
    labelToIdMap[label] = newId;
}
```

4. Add helper methods for tiered integration:
```cpp
levelType getElementMaxLevel(idType id) const {
    std::shared_lock<std::shared_mutex> lock(metadataMutex_);
    return idToMetaData[id].maxLevel;
}

void decrementElementCount() {
    curElementCount_.fetch_sub(1, std::memory_order_relaxed);
}
```

Note: `recycleId()` already exists in `hnsw_disk.h`.

**Verification**: Unit tests for `labelToIdMap` and helper methods should pass.

---

### Stage 3: Implement `markDelete()`

**Goal**: Mark element as deleted, remove from label map. Does NOT replace entry point.

**File**: `vecsim_disk/src/algorithms/hnsw/hnsw_disk.h`

**Steps**:
1. Atomically remove from `labelToIdLookup_` and get the internal ID (prevents TOCTOU race)
2. Set `DISK_DELETE_MARK` flag
3. Return the deleted ID

**Key design decisions**:
- `labelToIdLookup_` is the authoritative "already deleted" check - if not in map, already deleted
- The delete mark is for graph operations that traverse by internal ID
- **Entry point replacement is NOT done here** - it's done in `executeDeleteInitJob()` (worker thread)

**Verification**: `MarkDelete*` unit tests should pass.

---

### Stage 4: Implement `replaceEntryPoint()`

**Goal**: Handle deletion of entry point by selecting a new one.

**File**: `vecsim_disk/src/algorithms/hnsw/hnsw_disk.h`

**Selection strategy**:
1. Try neighbors of current entry point at top level
2. If no valid neighbor, scan all elements at top level
3. If no element at top level, decrease `maxLevel_` and recurse
4. If index becomes empty, set `entryPoint_ = INVALID_ID`

**Locking strategy**: Acquires `entryPointMutex_` exclusively for the entire operation. This is simpler than fine-grained locking and avoids race conditions.

**Verification**: Entry point replacement tests should pass.

---

### Stage 5: Complete Delete Job Execution and `runGC()`

**Goal**: Implement the async job handlers and synchronous GC mechanism.

**File**: `vecsim_disk/src/algorithms/hnsw/hnsw_disk_tiered.h`

#### 5.1 Finalize Job Tracking

Add tracking for pending finalize jobs (similar to RAM's `idToSwapJob`):
- `pending_finalize_jobs`: maps deleted ID → finalize job shared_ptr
- `pending_finalize_guard`: mutex protecting the map

#### 5.2 `executeDeleteInitJob()`

1. **Replace entry point if needed** (first, before any other work):
   ```cpp
   hnsw_index->replaceEntryPoint();
   ```
2. Get deleted element's max level
3. Collect all incoming neighbors at all levels from storage
4. Create finalize job and track it in `pending_finalize_jobs`
5. Submit repair jobs for each neighbor, linking them to the finalize job
6. If no repairs needed, job is ready immediately

**Note**: Entry point replacement is done first to minimize the window where a deleted node is the entry point. The existing `replaceEntryPoint()` acquires its own lock internally.

#### 5.3 `executeDeleteFinalizeJob()`

Cleanup work:
1. Delete vector data from disk storage
2. Delete outgoing/incoming edges at all levels
3. Recycle ID for reuse (`recycleId()`)
4. Decrement element count

#### 5.4 `runGC()`

Execute ready finalize jobs synchronously:
1. Collect jobs where `use_count() == 1` (only tracking reference remains = all repairs done)
2. Remove from tracking map
3. Execute cleanup on caller's thread (no lock held during execution)

**Verification**: All acceptance tests should pass, including `runGC()` tests.

---

### Stage 6: Edge Cases and Hardening

**Goal**: Handle edge cases and ensure robustness.

| Case | Handling |
|------|----------|
| Delete last element | `replaceEntryPoint()` sets `entryPoint_ = INVALID_ID` |
| Delete during search | Deleted elements skipped via `isMarkedDeleted()` check |
| Concurrent deletes | `labelToIdMapMutex_` and node locks prevent races |
| Delete non-existent | `markDelete()` returns empty vector, no-op |

**Additional unit tests**:
- `DeleteLastElement` - verify index becomes empty
- `DeleteThenSearch` - verify search works after deletes
