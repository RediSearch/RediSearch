# MOD-13797: Tiered Delete Flow for Disk-Based HNSW Vector Indexes

**Ticket**: [MOD-13797](https://redislabs.atlassian.net/browse/MOD-13797)

---

## 1. Background

MOD-13172 implemented the internal delete building blocks in `HNSWDiskIndex`: `markDelete()`, `replaceEntryPointOnDelete()`, `getElementMaxLevel()`, `recycleId()`, `decrementElementCount()`, and storage deletion methods.

**MOD-13797 implements the tiered coordination layer** that orchestrates these building blocks via async jobs.

---

## 2. Complete Tiered Delete Flow

### 2.1 Entry Point: `deleteVector(label)`

The main thread entry point determines where the label exists and routes accordingly.

```
deleteVector(label):
    1. Acquire flatIndexGuard (exclusive)

    2. Check where label exists:

       CASE A: Label in flat buffer
         - frontendIndex->deleteVector(label)
         - Insert job will fail validation when it runs (label gone from flat)
         - Return 1

       CASE B: Label in HNSW index
         - Release flatIndexGuard (no longer needed)
         - Call deleteLabelFromHNSW(label)  // uses HNSW's internal locks
         - Return 1

       CASE C: Label doesn't exist
         - Return 0

    3. Release locks (RAII)
```

**Complexity**: O(1) - hash lookups only

### 2.2 Delete Init Job: `executeDeleteInitJob()`

The init job runs on a worker thread after all currently-running jobs complete (via auto-submit pattern).

```
executeDeleteInitJob(delete_job):
    deleted_id = delete_job->deleted_id

    1. Replace entry point if deleted_id is the current entry point
       hnsw_index->replaceEntryPointOnDelete(deleted_id)

    2. Get deleted element's max level
       max_level = hnsw_index->getElementMaxLevel(deleted_id)

    3. Collect incoming neighbors at all levels
       all_neighbors = []
       for level in 0..max_level:
           incoming = storage->get_incoming_edges(deleted_id, level)
           for neighbor_id in incoming:
               all_neighbors.append((neighbor_id, level))

    4. Create finalize job with auto-submit pattern
       finalize_job = createAutoSubmitJob<DeleteDiskJob>(
           DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, deleted_id)

    5. Submit repair jobs linked to finalize job
       submitRepairs(all_neighbors, finalize_job)
       // Each repair holds shared_ptr to finalize_job
       // When all repairs complete, finalize job auto-submits
```

**No ConsistencySharedGuard needed**: This job only reads from disk storage and submits other jobs. No in-memory mutation occurs.

**Complexity**: O(neighbors × levels) for disk reads

### 2.3 Repair Jobs: `executeRepairJob()` ✅ Already Implemented

Repair jobs fix outgoing edges of neighbors by removing references to deleted nodes. Calls `hnsw_index->repairNode(node_id, level)`.

**Deduplication**: `pending_repairs` map ensures one repair job per (node_id, level) pair.

### 2.4 Delete Finalize Job: `executeDeleteFinalizeJob()`

The finalize job runs after all repair jobs complete (auto-submit pattern). It performs final cleanup.

```
executeDeleteFinalizeJob(delete_job):
    // REQUIRES: ConsistencySharedGuard for fork safety
    consistency_guard = ConsistencySharedGuard(getConsistencyMutex())

    deleted_id = delete_job->deleted_id
    max_level = hnsw_index->getElementMaxLevel(deleted_id)

    1. Remove deleted_id from incoming edges of all outgoing neighbors
       for level in 0..max_level:
           outgoing = storage->get_outgoing_edges(deleted_id, level)
           batch_ops = []
           for neighbor_id in outgoing:
               batch_ops.append((neighbor_id, level, deleted_id, Delete))
           storage->batch_merge_incoming_edges(batch_ops)

    2. Delete disk storage keys
       storage->del_vector(deleted_id)
       for level in 0..max_level:
           storage->del_outgoing_edges(deleted_id, level)
           storage->del_incoming_edges(deleted_id, level)

    3. Recycle the internal ID with tail trimming
       hnsw_index->recycleId(deleted_id)
       // If deleted_id == nextId_ - 1, decrement nextId_ instead of adding to holes_
       // Also trims any consecutive holes from the tail
       // Call shrinkUnusedCapacity() when nextId_ <= maxElements_ - blockSize

    4. Decrement element count
       hnsw_index->decrementElementCount()
```

**ConsistencySharedGuard required**: `recycleId()` modifies `holes_` and may call `shrinkUnusedCapacity()` (in-memory mutations). Must be protected for fork safety.

**Ordering constraint**: `getElementMaxLevel()` must be called BEFORE `recycleId()`. Once `recycleId()` is called, the ID may be immediately reused by a concurrent insert, invalidating the metadata.

**Complexity**: O(levels) for disk deletes, O(1) for recycleId

---

## 3. Thread Safety Analysis

### 3.1 Lock Ordering

All operations must follow this lock order to prevent deadlocks:

```
flatIndexGuard → HNSW internal locks
                  ├── labelLookupMutex_
                  ├── metadataMutex_
                  ├── entryPointMutex_
                  ├── vectorsMutex_
                  └── holesMutex_
```

### 3.2 Operation Lock Requirements

| Operation | Thread | ConsistencySharedGuard | flatIndexGuard | HNSW Locks |
|-----------|--------|------------------------|----------------|------------|
| `deleteVector()` | Main | ❌ Not needed | ✅ Exclusive (released before HNSW) | None on main thread |
| `executeDeleteInitJob()` | Worker | ❌ Not needed | ❌ Not needed | metadataMutex_, entryPointMutex_ |
| `executeDeleteFinalizeJob()` | Worker | ✅ Required | ❌ Not needed | holesMutex_, metadataMutex_ |

### 3.3 Race Condition Prevention

**Delete vs Insert (same label)**:
- `deleteVector()` holds `flatIndexGuard` exclusively to check where label exists
- If label in flat buffer: removed immediately, insert job will fail validation
- If label in HNSW: `markDelete()` sets `DISK_DELETE_MARK` flag and removes label from `labelToIdLookup_`
- Insert job checks label existence under flat lock before transition

**Delete vs Delete (same label)**:
- Deletes are serialized on the main thread, so no concurrent delete race
- `markDelete()` checks `DISK_DELETE_MARK` flag - if already set, returns empty (no-op)
- Only one delete init job created per label

**Delete vs Search (marking)**:
- Deleted elements marked with `DISK_DELETE_MARK`
- Search skips marked elements in results (existing behavior)
- Search can still traverse through deleted elements
- Label is removed from `labelToIdLookup_` immediately in `markDelete()`

**Delete vs Search (ID recycling)**:
- Delete init job waits for all currently running jobs (including queries) via `pendDeleteInitJobByCurrentlyRunning()`
- By the time finalize job runs, all queries that started before the delete have completed
- No additional synchronization needed for ID recycling

**Concurrent Repairs**:
- `pending_repairs` map deduplicates repair jobs
- Multiple deletes affecting same neighbor share one repair
- Repair job holds shared_ptr to all dependent finalize jobs and currently running jobs

---

## 4. Fork/Replication Safety

Operations that modify in-memory state (`executeInsertJob`, `executeDeleteFinalizeJob`) must hold `ConsistencySharedGuard`. Before fork, main thread acquires exclusive lock, waiting for these operations to complete.

**Why `deleteVector()` doesn't need ConsistencySharedGuard:**
- Main thread only sets `DISK_DELETE_MARK` flag (metadata write protected by `metadataMutex_`)
- Label removal from `labelToIdLookup_` is a simple map erase (no structural mutation)
- No complex in-memory structural mutations on main thread

**Why `executeDeleteInitJob()` doesn't need ConsistencySharedGuard:**
- Uses internal locks (`metadataMutex_`, `entryPointMutex_`)
- Entry point is RAM-only; replica gets correct state via data sync

Disk operations (SpeedB writes) are safe during fork - SpeedB handles its own fork safety.

---

## 5. Implementation Checklist

### 5.1 Implementation Status

All methods are now implemented:

- **`deleteVector(label)`**: Routes to flat buffer or HNSW based on label location
- **`executeDeleteInitJob()`**: Replaces entry point if needed, collects neighbors, submits repair jobs
- **`executeDeleteFinalizeJob()`**: Removes edges, deletes storage keys, recycles ID
- **`recycleId(id)`**: Includes tail trimming optimization
- **Repair jobs**: `RepairDiskJob`, `executeRepairJob()`, `submitRepairs()`, `repairNode()`
- **Job infrastructure**: `DeleteDiskJob`, `pendDeleteInitJobByCurrentlyRunning()`, `deleteLabelFromHNSW()`
- **Building blocks** (MOD-13172): `markDelete()`, `replaceEntryPointOnDelete(deleted_id)`, `getElementMaxLevel()`, `decrementElementCount()`, storage deletion methods

**Note**: `markDelete()` already removes the label from `labelToIdLookup_`, so `executeDeleteFinalizeJob()` does not call `removeLabel()` separately.

---

## 6. Hole Management Strategy

### 7.1 RAM vs Disk Approach

**RAM HNSW** uses "swap with last": moves the last element's data to the deleted slot, keeping IDs contiguous. No holes.

**Disk HNSW** cannot swap because:
- Disk storage is keyed by internal ID (rewriting keys is expensive)
- Concurrent readers might access the "last" element during swap
- Tiered architecture shares IDs between frontend and backend

Therefore, Disk HNSW uses a **holes-based approach**: deleted IDs are recycled on next insert.

### 7.2 Tail Trimming

- **Tail delete** (`id == nextId_ - 1`): Decrement `nextId_`, sort `holes_` ascending, pop consecutive tail holes, shrink capacity if a full block is unused.
- **Non-tail delete**: Add `id` to `holes_` for reuse.

### 7.3 Memory Waste Analysis

**Worst case**: Delete 500K vectors from middle of 1M index, tail ID never deleted.
- `holes_` vector: ~4 MB
- `idToMetaData`/`nodeLocks_` can't shrink: ~36 MB
- **Total: ~40 MB** (~8-30% overhead)

**Mitigation**: Holes are reused on insert. Waste is temporary unless no new inserts occur.

**Note**: We considered using `std::set` for O(log n) lookups, but decided against it. The sort-and-pop approach is simple and handles batch delete scenarios where many consecutive holes may exist.

---

## 7. Test Plan

### 7.1 Acceptance Tests (Flow Tests)

Add to `flow-tests/test_vecsim_disk.py`:

**Basic Delete:**
- [ ] Delete single vector, verify not in search results
- [ ] Delete non-existent label (no error)
- [ ] Delete all vectors, verify empty results
- [ ] Delete same label twice (idempotent)

**Delete + Insert:**
- [ ] Delete then insert same label (ID reuse)
- [ ] Insert after delete (different label)
- [ ] Delete all vectors, then insert new vector (verify index recovers correctly)
- [ ] Delete during insert (same label) - verify element ends up deleted

**Entry Point:**
- [ ] Delete entry point, verify search still works
- [ ] Delete all but one vector

**Graph Integrity:**
- [ ] Search accuracy after multiple deletes
- [ ] Delete multiple connected nodes, verify remaining found
- [ ] Concurrent deletes of connected nodes (verify no crashes, graph remains searchable)
- [ ] Delete entry point when all its neighbors are also deleted

**Metrics:**
- [ ] Index size decrements after delete (FT.INFO)
- [ ] Memory usage after many deletes (verify holes_ doesn't grow unbounded)

### 7.2 Unit Tests

Add to `vecsim_disk/tests/unit/test_hnsw_disk.cpp`:

**Delete Flow:**
- [ ] `DeleteVectorFromFlatBuffer`
- [ ] `DeleteVectorFromHNSW`
- [ ] `DeleteVectorNonExistent`
- [ ] `DeleteVectorIdempotent`
- [ ] `DeleteVectorEntryPointReplacement`

**Hole Management:**
- [ ] `RecycleIdTailDecrementsNextId`
- [ ] `RecycleIdMiddleAddsToHoles`
- [ ] `RecycleIdTailTriggersShrink`
- [ ] `RecycleIdTailNoShrinkPartialBlock`
- [ ] `RecycleIdMixedPattern`

**Note:** Concurrency tests are not required for MOD-13797. Race conditions are handled by design (lock ordering, pend mechanism). MOD-13798 will add query concurrency tests.

---

## Appendix: Sequence Diagram

```
Main Thread                    Worker Thread 1              Worker Thread 2
     │                              │                            │
     │ deleteVector(label)          │                            │
     │──────────────────────────────│                            │
     │ acquire flatIndexGuard       │                            │
     │ check label location         │                            │
     │ [if HNSW] release flatGuard  │                            │
     │ markDelete(label) → id       │                            │
     │ pendDeleteInitJob(id, label) │                            │
     │                              │                            │
     │                              │ executeDeleteInitJob(id)   │
     │                              │ replaceEntryPointOnDelete(id)│
     │                              │ get_incoming_edges()       │
     │                              │ create finalize_job        │
     │                              │ submitRepairs(neighbors)   │
     │                              │                            │
     │                              │                            │ executeRepairJob(n1, L0)
     │                              │                            │ repairNode(n1, L0)
     │                              │                            │ (finalize_job ref--)
     │                              │                            │
     │                              │ executeRepairJob(n2, L0)   │
     │                              │ repairNode(n2, L0)         │
     │                              │ (finalize_job ref--)       │
     │                              │                            │
     │                              │ [all repairs done]         │
     │                              │ finalize_job auto-submits  │
     │                              │                            │
     │                              │ executeDeleteFinalizeJob() │
     │                              │ acquire ConsistencyShared  │
     │                              │ del_vector(), del_edges()  │
     │                              │ recycleId(id)              │
     │                              │ decrementElementCount()    │
     │                              │ release ConsistencyShared  │
     ▼                              ▼                            ▼
```
