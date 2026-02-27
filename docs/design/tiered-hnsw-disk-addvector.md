# TieredHNSWDiskIndex::addVector Design

**Branch:** `dorer-tiered-add-vector`
**Created:** 2026-02-06
**Last Updated:** 2026-02-17 (Unified insertion patterns: removed storeVector, addVector uses tiered pattern)
**References:**
- [HNSW Disk Low-Level Design](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5806129198)
- [Vector Search on Disk - MVP1 Design](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5734727816)

---

## 1. Overview

Implements `addVector` for the tiered disk-based HNSW index. The tiered architecture uses a flat buffer (brute-force index) as a frontend for incoming vectors, with background workers asynchronously indexing vectors into the HNSW disk index.

## 2. Design Constraints

| Constraint | Value | Notes |
|------------|-------|-------|
| Write Mode | `VecSim_WriteAsync` only | No `WriteInPlace` support for disk indexes |
| Multi-value | Not supported | Single-value only for MVP; enforced at factory level |
| Buffer Full | Continue adding | Throttling mechanism to be implemented later |

### Multi-value Rejection

Multi-value indexes (`hnsw_params.multi == true`) are explicitly rejected at index creation time in `TieredHNSWDiskFactory::NewIndex()`. The factory returns `nullptr` if multi-value is requested.

**Rationale:** The `addVector` implementation assumes single-value semantics (one vector per label). It uses `dynamic_cast<BruteForceIndex_Single<...>*>` to get the ID of an existing label for overwrite. With a multi-value frontend (`BruteForceIndex_Multi`), this cast would return `nullptr` and crash. Rather than adding complex multi-value handling throughout the codebase, we enforce single-value at creation time per the MVP1 design spec.

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TieredHNSWDiskIndex                          │
├─────────────────────────────────────────────────────────────────┤
│  frontendIndex (BruteForce)     │  backendIndex (HNSWDisk)      │
│  - Fast insertion               │  - Graph-based search         │
│  - Linear search                │  - SQ8 vectors in memory      │
│  - Temporary storage            │  - FP32 vectors on disk       │
└─────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Job Queue (External)                        │
│  - InsertDiskJob: moves vector from flat → HNSW                 │
│  - RepairDiskJob: fixes node connections after overflow         │
│  - DeleteDiskJob: handles deletion cleanup                      │
└─────────────────────────────────────────────────────────────────┘
```

## 4. Data Flow

### 4.1 Main Thread (addVector) - Zero Disk I/O, Minimal Work

**ASSUMPTION:** Caller guarantees no duplicate labels. `addVector()` will never be called with a label that already exists in either the flat buffer or the HNSW index. This assumption is enforced with assertions.

```
addVector(blob, label):
    1. Check flat buffer limit (log warning if full, throttling TBD)

    2. Assert no duplicates (contract enforcement):
       - flat_lock = unique_lock(flatIndexGuard)  // RAII for exception safety
       - ASSERT: label not in flat buffer
       - ASSERT: label not in HNSW index

    3. Insert into flat buffer:
       - frontendIndex->addVector(blob, label)
       - FP32 is stored in flat buffer (preprocessed for cosine metric)

    4. Create and submit insert job:
       - Create InsertDiskJob with {label}  // No state captured on main thread!
       - Track job in labelToInsertJobs map
       - flat_lock.unlock()  // Explicit unlock before submit (handle with proper RAII lock guards)
       - submitDiskJob(job)

    5. Return 1 (always a new vector per contract)
```

**NOTE:** Following the RAM HNSW pattern, NO state is captured on the main thread. All ID allocation, SQ8 storage, and entry point capture happens in the worker thread via the three-phase pattern: `indexVector()` → `allocateAndRegister()` → `storeVectorConnections()`.

### 4.2 Worker Thread (executeInsertJob) - Simplified Flow

**ARCHITECTURE:** The insertion is split into phases with the tiered layer owning preprocessing and lock ordering:

1. **Copy phase**: Copy FP32 from flat buffer (so we can release flat lock)
2. **Preprocess phase**: FP32 → SQ8 conversion in tiered layer (outside locks)
3. **Search phase** (`indexVector()`): Read-only graph traversal, no locks
4. **Atomic transition**: Under flat lock, validate + remove from flat + allocate in HNSW
5. **Storage phase** (`storeVectorConnections()`): Graph connections under consistency lock

```text
executeInsertJob(job):
    1. Copy FP32 from flat buffer:
       - flatIndexGuard.lock_shared()
       - Check if job invalidated (deleted before we start) → return if so
       - Copy FP32 data to local buffer
       - flatIndexGuard.unlock_shared()

    2. PREPROCESS in tiered layer (outside any lock):
       - processedBlobs = hnsw_index->preprocess(fp32DataCopy)
       - querySQ8 = processedBlobs.getQueryBlob()
       - storageSQ8 = processedBlobs.getStorageBlob()

    3. SEARCH PHASE - indexVector(querySQ8):
       - Generates random level internally (thread-safe)
       - Pure graph search using generated level and query blob
       - No locks held during this potentially expensive operation
       - Returns IndexVectorResult containing:
         * elementMaxLevel (generated internally)
         * levelSelections (pre-computed neighbor choices)
         * currEntryPoint, currMaxLevel (captured state)
         * indexWasEmpty flag

    4. ACQUIRE CONSISTENCY GUARD (outermost lock for fork safety):
       - ConsistencySharedGuard consistency_guard

    5. ATOMIC TRANSITION under flat lock:
       - flatIndexGuard.lock()
       - if (!frontendIndex->isLabelExists(label)) → return (deleted during search)
       - frontendIndex->deleteVector(label)  // Remove from flat
       - internalId = hnsw_index->allocateAndRegister(label, indexResult.elementMaxLevel, storageSQ8)
       - flatIndexGuard.unlock()

    6. STORAGE PHASE - storeVectorConnections():
       - Write FP32 to disk
       - Apply pre-computed graph connections
       - Update entry point if needed
       - Unmark IN_PROCESS flag
       - Returns nodesToRepair

    7. Submit repair jobs for overflowed nodes

    - consistency_guard released automatically
```

**Key design decisions:**
- **Tiered layer owns preprocessing**: Blobs are created once, reused for both search and storage
- **Tiered layer owns lock ordering**: consistency_guard acquired before flat_lock
- **Minimal flat lock hold time**: Only validation + remove + allocate (all fast memory ops)
- **No labelToInsertJob map needed**: Delete invalidation handled by flat buffer check

### 4.3 Storage APIs in HNSWDiskIndex

The `HNSWDiskIndex` class provides APIs for the tiered layer. Both tiered and standalone (`addVector`) use the same pattern:

| Method | Purpose | Used By |
|--------|---------|---------|
| `preprocess()` | FP32 → SQ8 conversion | Both tiered layer and `addVector` |
| `indexVector()` | Generate level + read-only graph search | Both tiered layer and `addVector` |
| `allocateAndRegister()` | Allocate ID + store SQ8 + register metadata | Tiered layer (under flat lock) |
| `storeVectorConnections()` | Write FP32 + graph connections | Both tiered layer and `addVector` |
| `addVector()` | Unified flow using tiered pattern | Unit tests, VecSimInterface compliance |

**Note:** The previous `storeVector()` function has been removed. `addVector()` now uses the same tiered pattern internally (`indexVector()` + inline allocation + `storeVectorConnections()`).

**Tiered Layer API:**

- `preprocess(fp32Data)`: Converts FP32 to SQ8. Returns `ProcessedBlobs` with query and storage blobs.
- `indexVector(querySQ8)`: Generates random level internally, performs read-only search phase. Returns `IndexVectorResult` with level and pre-computed neighbor selections.
- `allocateAndRegister(label, maxLevel, storageSQ8)`: Allocates ID, stores SQ8, registers metadata. Returns internal ID. Uses `indexResult.elementMaxLevel` for the level.
- `storeVectorConnections(internalId, fp32Data, querySQ8, indexResult)`: Writes FP32 to disk, applies graph connections. Returns nodes to repair.

**Standalone `addVector()` Implementation:**

The `addVector()` function (for tests and VecSimInterface compliance) uses the same tiered pattern:

```cpp
int addVector(blob, label):
    // Step 1: Preprocess outside critical section
    processedBlobs = preprocess(blob)
    querySQ8 = processedBlobs.getStorageBlob()

    // Step 2: Read-only search phase
    indexResult = indexVector(querySQ8)

    // Step 3: Atomic label check + allocation (inline, not allocateAndRegister)
    {
        labelLock = unique_lock(labelLookupMutex_)
        if (labelExists) deleteVector(label)  // Overwrite handling
        internalId = allocateIdAndStoreVector(querySQ8)
        initElementMetadata(internalId, label, indexResult.elementMaxLevel)
        curElementCount_++
        labelToIdLookup_[label] = internalId
    }

    // Step 4: Consistency lock
    consistency_guard = ConsistencySharedGuard()

    // Step 5: Store FP32 + connect to graph
    nodesToRepair = storeVectorConnections(internalId, blob, querySQ8, indexResult)

    // Step 6: Process repair jobs synchronously
    for (node, level) in nodesToRepair:
        repairNode(node, level)

    return label_exists ? 0 : 1
```

**Note:** `addVector()` cannot use `allocateAndRegister()` directly because it needs atomic label check + registration. The tiered layer handles this differently by holding the flat lock during the atomic transition.

**IndexVectorResult struct:**

```cpp
struct IndexVectorResult {
    levelType elementMaxLevel;
    vecsim_stl::vector<LevelNeighborSelection> levelSelections;  // Pre-computed neighbors
    idType entryPointAtSearchTime;   // Entry point snapshot at search time
    levelType maxLevelAtSearchTime;  // Max level snapshot at search time
    bool indexWasEmpty;

    // Note: No template parameter - struct doesn't depend on DistType
    // Note: ProcessedBlobs removed - tiered layer manages blobs separately
};
```

**LevelNeighborSelection struct:**

```cpp
struct LevelNeighborSelection {
    levelType level;
    vecsim_stl::vector<idType> selectedNeighbors;  // Neighbors selected by heuristic
    // Note: No template parameter - struct doesn't depend on DistType
    // Note: closestNeighbor removed - it's traversal state, not selection result
};
```

Both APIs handle `DataBlocksContainer::addElement()`'s sequential ID constraint (`id == element_count`) by holding `vectorsMutex_` during both `allocateId()` and `addElement()`.

### 4.4 Thread Safety: Sequential ID Constraint for Fresh IDs

**Problem:** `DataBlocksContainer::addElement()` requires IDs to be inserted sequentially (asserts `id == element_count`). With parallel job execution, if IDs were allocated on the main thread but stored on worker threads, fresh IDs could be processed out-of-order.

**Solution: allocateAndRegister() Under Flat Lock**

The `allocateAndRegister()` function handles ID allocation atomically under the tiered layer's flat lock:

```cpp
// Called by tiered layer under flatIndexGuard (exclusive lock)
idType allocateAndRegister(labelType label, levelType elementMaxLevel, const void* storageSQ8) {
    idType internalId;
    {
        std::unique_lock<std::shared_mutex> vectorsLock(vectorsMutex_);
        internalId = allocateId();

        if (internalId < this->vectors->size()) {
            this->vectors->updateElement(internalId, storageSQ8);  // Recycled ID
        } else {
            this->vectors->addElement(storageSQ8, internalId);     // Fresh ID
        }
    }

    // Initialize metadata (marks as DISK_IN_PROCESS)
    initElementMetadata(internalId, label, elementMaxLevel);
    curElementCount_.fetch_add(1, std::memory_order_relaxed);

    return internalId;
}
```

**Why This Works:**

Since the tiered layer holds the flat lock during `allocateAndRegister()`, and jobs execute one at a time through this critical section, fresh IDs are always allocated in order. The flat lock serializes the allocation, even though the search phases run in parallel.

**Key Benefits:**
- **Fresh entry point**: Captured during `indexVector()`, right before storage
- **Simpler job struct**: Just `label` - no captured state needed
- **Tiered layer owns coordination**: Preprocessing, level generation, and lock ordering

## 5. Job Structure

```cpp
// Simplified job struct - matches RAM HNSW pattern
// No state captured on main thread - everything happens in worker
// No allocatedId needed - insertExecutionGuard ensures no orphaned data
struct InsertDiskJob : public AsyncDiskJob {
    labelType label;       // User-provided label

    InsertDiskJob(std::shared_ptr<VecSimAllocator> allocator, labelType label_,
                  JobCallback callback, VecSimIndex* index_)
        : AsyncDiskJob(allocator, DISK_HNSW_INSERT_VECTOR_JOB, callback, index_),
          label(label_) {}
};
```

**Key Design:**

- `isIndexed` field removed - no overwrite handling per no-duplicate-labels contract
- Job only stores `label` - worker thread looks up data by label from flat buffer
- Delete invalidation handled by flat buffer existence check (no special coordination needed)

**Comparison with RAM HNSW job struct:**

| Field | Disk `InsertDiskJob` | RAM `HNSWInsertJob` | Purpose |
|-------|---------------------|---------------------|---------|
| `label` | ✓ | ✓ | User-provided label |
| `id` | N/A | ✓ | Flat buffer ID (disk looks up by label instead) |
| `state` | **Removed** | N/A | Was captured on main thread; now done in worker |
| `isIndexed` | **Removed** | N/A | Was for invalidation cleanup; no longer needed |

## 6. Thread Safety

### Lock Ordering (to prevent deadlock)

1. `ConsistencyMutex` (fork safety - outermost)
2. `flatIndexGuard` (flat buffer access)
3. HNSW internal locks (metadata, vectors, entry point)

**Note:** The tiered layer owns the lock ordering. Consistency guard is always acquired before flat lock.

### Key Synchronization Points

- **Flat buffer access**: Protected by `flatIndexGuard` (shared_mutex)
- **Atomic transition (flat → HNSW)**: Under exclusive `flatIndexGuard`
- **HNSW internal operations**: Internal locks handle concurrency
- **Fork safety**: `ConsistencySharedGuard` acquired before any writes

### Delete Invalidation (Simplified)

Delete invalidation is handled naturally by the flat buffer existence check:

1. **Delete runs first**: Removes label from flat buffer
2. **Insert job checks**: `isLabelExists(label)` returns false → job exits early
3. **No special coordination needed**: No `insertExecutionGuard` or job tracking maps

This works because the atomic transition (step 6 in executeInsertJob) validates the label still exists before allocating in HNSW. If delete ran during the search phase, the validation fails and the job exits cleanly.

### RAII Lock Management

All lock acquisitions use RAII wrappers (`std::unique_lock`, `std::shared_lock`, `ConsistencySharedGuard`) to ensure exception safety.

## 7. Fork Safety (ConsistencyMutex)

When Redis forks for replication or snapshotting, both disk (SpeedB) and in-memory data structures must be in a consistent state. The `ConsistencyMutex` (introduced in PR 142) provides a global coordination mechanism.

### 7.1 Mechanism

- **Global shared mutex**: `vecsim_disk::getConsistencyMutex()` returns a `std::shared_mutex`
- **Async jobs**: Acquire **shared lock** via `ConsistencySharedGuard` while modifying in-memory structures
- **Main thread**: Acquires **exclusive lock** via `VecSimDisk_AcquireConsistencyLock()` before fork

Multiple async jobs can hold the shared lock concurrently (they don't block each other). The exclusive lock blocks all async modifications.

### 7.2 Lock Placement in Job Executors

| Job Type | Holds Consistency Lock? | Reason |
|----------|------------------------|--------|
| Insert job (`executeInsertJob`) | Yes (shared) | Modifies flat buffer and HNSW in-memory structures |
| Repair job (`executeRepairJob`) | No | Only writes to SpeedDB (disk-only) |
| Delete finalize job (`executeDeleteFinalizeJob`) | Yes (shared) | Modifies `holes_` free list (to be implemented) |

### 7.3 executeInsertJob Lock Timing

The consistency lock is acquired in the tiered layer **before the atomic transition** (before flat lock). This ensures all in-memory modifications are protected for fork safety.

```text
executeInsertJob(job):
    1. Copy FP32 from flat buffer (shared lock on flatIndexGuard)

    2-3. SEARCH PHASE (NO consistency lock):
       - preprocess() → SQ8 blobs
       - indexVector() → generate level, graph search, pre-computed neighbors

    4. ACQUIRE ConsistencySharedGuard <-- Lock acquired HERE (outermost)

    5. ATOMIC TRANSITION (exclusive flatIndexGuard):
       - Validate label still exists
       - Remove from flat buffer
       - allocateAndRegister(indexResult.elementMaxLevel) → allocate ID, store SQ8, register metadata

    6. STORAGE PHASE - storeVectorConnections():
       - Write FP32 to disk
       - Apply graph connections
       - Update entry point, unmark IN_PROCESS

    7. Submit repair jobs

    - consistency_guard released automatically (RAII)
```

**Key insight:** The search phase is completely lock-free and read-only. The tiered layer owns lock ordering (consistency → flat), ensuring proper coordination with fork operations.

### 7.4 API

```cpp
// Async jobs use this RAII guard for shared access
namespace vecsim_disk {
    class ConsistencySharedGuard {
        std::shared_lock<std::shared_mutex> lock_;
    public:
        ConsistencySharedGuard() : lock_(getConsistencyMutex()) {}
    };
}

// Main thread uses C API for exclusive access before fork
extern "C" {
    void VecSimDisk_AcquireConsistencyLock();  // Blocks until all shared locks released
    void VecSimDisk_ReleaseConsistencyLock();  // Releases exclusive lock
}
```

## 8. No-Duplicate-Labels Contract

**ASSUMPTION:** Caller guarantees that `addVector()` will never be called with a label that already exists in either the flat buffer or the HNSW index. This contract is enforced with a combined assertion:

```cpp
assert(!this->frontendIndex->isLabelExists(label) && !hnsw_index->isLabelExists(label) &&
       "Duplicate label - caller must ensure uniqueness across both indices");
```

**Benefits of this assumption:**
1. No overwrite handling needed - simpler control flow
2. No job invalidation logic - jobs always complete successfully
3. No `isIndexed` flag needed - no cleanup decisions required
4. No `invalidatePendingJobForLabel()` function needed
5. Return value is always 1 (new vector)

**Previous overwrite handling (removed):** This section previously documented overwrite scenarios and job invalidation. That code has been removed per the no-duplicate-labels assumption.

## 9. Error Handling

| Scenario | Handling |
|----------|----------|
| Buffer full | Log warning, continue adding (throttling TBD) |
| Storage error in storeVector | Propagate error, don't create job |
| Job execution failure | Log error, job is lost (acceptable for MVP) |

## 10. Build Configuration

**Critical:** VectorSimilarity and vecsim_disk MUST be built with the same `BUILD_TESTS` macro setting to avoid ODR (One Definition Rule) violations.

The following classes have `#ifdef BUILD_TESTS` conditional members that affect object layout:
- `VecSimTieredIndex` (base class)
- `BruteForceIndex` and `BruteForceIndex_Single`

**Solution:** `CMakeLists.txt` sets `VECSIM_BUILD_TESTS=ON` before including RediSearch, ensuring consistent class layout.

**VecSimDisk_FreeIndex:** Must preserve allocator reference before deleting the index (matching `VecSimIndex_Free` behavior) because tiered indexes are created with placement new using a custom allocator.

## 11. Memory Management for VecsimBaseObject Classes

### 11.1 The Problem

`VecsimBaseObject::operator delete` has a **use-after-destroy bug** that prevents using the standard `delete` operator on derived classes (including `AsyncDiskJob`, `InsertDiskJob`, `RepairDiskJob`, etc.).

When `delete obj` is called:
1. The **destructor runs first** → destroys `obj->allocator` (a `shared_ptr` member)
2. **`operator delete(void*, size_t)`** is called
3. `operator delete` casts the pointer back to `VecsimBaseObject*` and calls `obj->allocator->deallocate()`
4. **BUG**: `obj->allocator` is already destroyed! This causes undefined behavior.

**Important Note**: Even though the underlying `VecSimAllocator` object is kept alive by other `shared_ptr` references (e.g., the index holds one), the **member variable** `obj->allocator` is a `shared_ptr` that gets destroyed when the destructor runs.

### 11.2 The Solution: `SafeVecSimDeleter` and `make_vecsim_shared_ptr`

We use a custom deleter template and helper function to bypass `operator delete`:

```cpp
/// Safe deleter that captures allocator before destruction.
template <std::derived_from<VecsimBaseObject> T>
struct SafeVecSimDeleter {
    void operator()(T* p) const {
        if (!p) return;
        auto alloc = p->allocator;  // Capture BEFORE destruction
        std::destroy_at(p);          // Call destructor (C++17 idiomatic)
        alloc->free_allocation(p);   // Free with captured allocator
    }
};

/// Wrap a raw pointer with safe deletion.
template <std::derived_from<VecsimBaseObject> T>
std::shared_ptr<T> make_vecsim_shared_ptr(T* raw) {
    return std::shared_ptr<T>(raw, SafeVecSimDeleter<T>{});
}

/// Allocate and construct with safe deletion.
template <std::derived_from<VecsimBaseObject> T, typename... Args>
std::shared_ptr<T> make_vecsim_shared_ptr(std::shared_ptr<VecSimAllocator> allocator, Args&&... args) {
    T* raw = new (allocator) T(allocator, std::forward<Args>(args)...);
    return make_vecsim_shared_ptr(raw);
}
```

This bypasses `operator delete` entirely, avoiding the use-after-destroy bug.

### 11.3 Usage in Job Lifecycle

| Scenario | Method | Description |
|----------|--------|-------------|
| Creating insert jobs | `make_vecsim_shared_ptr<InsertDiskJob>(alloc, ...)` | Returns `shared_ptr<InsertDiskJob>` with safe deleter |
| Creating repair jobs | `make_vecsim_shared_ptr<RepairDiskJob>(alloc, ...)` | Returns `shared_ptr<RepairDiskJob>` with safe deleter |
| Creating auto-submit jobs | `createAutoSubmitJob<DeleteDiskJob>(...)` | Returns `shared_ptr<AsyncDiskJob>` that auto-submits when ref count → 0 |
| Wrapping existing raw pointer | `make_vecsim_shared_ptr(raw_ptr)` | Wraps raw pointer with safe deletion |

### 11.4 PendingJobDeleter (Auto-Submit)

For auto-submit jobs (like delete finalize jobs that should run after all repair jobs complete), the `TieredHNSWDiskIndex::PendingJobDeleter` handles two code paths:

1. **Index alive**: Wrap job with safe deleter and submit to queue
2. **Index destroyed**: Can't submit anywhere, so clean up directly

```cpp
struct PendingJobDeleter {
    TieredHNSWDiskIndex* index;
    std::atomic<bool>* destroyed_flag;

    void operator()(AsyncDiskJob* job) const {
        if (job && index && destroyed_flag && !destroyed_flag->load()) {
            auto job_ptr = make_vecsim_shared_ptr(job);
            index->submitDiskJob(job_ptr);
        } else if (job) {
            SafeVecSimDeleter<AsyncDiskJob>{}(job);
        }
    }
};
```

Usage via `createAutoSubmitJob<T>()` method in `TieredHNSWDiskIndex`:

```cpp
template <typename JobType, typename... Args>
std::shared_ptr<AsyncDiskJob> createAutoSubmitJob(Args&&... args) {
    JobType* job = new (this->allocator) JobType(std::forward<Args>(args)...);
    return std::shared_ptr<AsyncDiskJob>(job, PendingJobDeleter{this, &is_destroyed});
}
```

### 11.5 Reference

See also:
- `VecSimDisk_FreeIndex()` in `vecsim_disk_api.cpp` - same pattern for index cleanup
- `VecsimBaseObject::operator delete` in `deps/RediSearch/deps/VectorSimilarity/src/VecSim/memory/vecsim_base.cpp`

## 12. Concurrency & Race Condition Handling

The async job flow has some concurrency considerations. With the no-duplicate-labels assumption, many previous race conditions are eliminated.

### 12.1 Empty-Index Concurrent Initialization (Still Relevant)

**Problem:** When multiple threads insert concurrently on an empty index, they all capture `currEntryPoint == INVALID_ID` and skip graph insertion. After one thread becomes the entry point, other threads may remain disconnected, creating orphaned graph components.

**Solution:** In `storeVectorConnections()` and `addVector()`, when `currEntryPoint == INVALID_ID`:
1. Re-check under **exclusive lock** (`entryPointMutex_`)
2. If `entryPoint_` is still `INVALID_ID`: this thread becomes entry point (no graph insertion)
3. If another thread established an entry point: use it for insertion

This ensures exactly one thread initializes the entry point, and all concurrent threads connect to it.

### 12.2 Race Conditions Eliminated by No-Duplicate-Labels Assumption

The following race conditions documented in earlier versions are **no longer relevant**:

1. **Graph corruption on invalidation**: Jobs cannot be invalidated, so no cleanup decisions needed
2. **Label registration before validity check**: Jobs are always valid (enforced by assertion)
3. **TOCTOU race in invalidation**: No invalidation flow exists

These were handled by the `isIndexed` flag and `invalidatePendingJobForLabel()` function, both of which have been removed.

## 13. Future Enhancements

- **Throttling**: When flat buffer is full, signal Redis to throttle writes
- **Multi-value support**: Track multiple jobs per label, handle `BruteForceIndex_Multi` frontend (currently rejected at factory level)
- **WriteInPlace mode**: Direct synchronous insertion (if needed)
- **Delete flow (MOD-13797)**: Implement full delete flow with repair jobs and ID recycling after neighbors are repaired
