/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

// Disk-based HNSW index. Inherits from VecSimIndexAbstract so all standard
// VecSimIndex_* operations work via polymorphism.

#include "VecSim/vec_sim_index.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/info_iterator_struct.h"
#include "VecSim/tombstone_interface.h"
#include "VecSim/utils/updatable_heap.h"
#include "storage/hnsw_storage.h"
#include "factory/components/disk_components_factory.h"
#include "utils/vecsim_stl_ext.h"
#include "graph_node_type.h"

#include <cmath>
#include <optional>
#include <random>
#include <shared_mutex>

#include "utils/consistency_lock.h"

// Test helpers are in tests/unit/ directory - included via CMake include path when BUILD_TESTS is defined
#ifdef BUILD_TESTS
#include "hnsw_disk_test_helpers.h"
#else
// When BUILD_TESTS is not defined, this macro expands to nothing
#define HNSW_DISK_TEST_METHODS
#endif

template <typename DistType>
using candidatesMaxHeap = vecsim_stl::max_priority_queue<DistType, idType>;
template <typename DistType>
using candidatesList = vecsim_stl::vector<std::pair<DistType, idType>>;

using elementFlags = uint8_t;

// Element flags for metadata (used with atomic operations)
// Redefined the hnsw.h Flags to allow more flags in the future.
enum ElementFlags : elementFlags {
    DISK_DELETE_MARK = 0x01, // Element is logically deleted but still in graph
    DISK_IN_PROCESS = 0x02,  // Element is being inserted into the graph
};

/**
 * @brief Result of resolveEntryPoint() - determines graph insertion strategy.
 *
 * When inserting into a potentially empty index with concurrent writers, we need to
 * atomically determine whether this thread becomes the entry point or should connect
 * to an existing entry point (possibly established by another thread after our snapshot).
 */
struct EntryPointResolution {
    idType entryPointToUse;  // Entry point to use for graph insertion (INVALID_ID if becameEntryPoint)
    levelType maxLevelToUse; // Max level corresponding to entryPointToUse
    bool becameEntryPoint;   // True if this thread initialized the entry point (skip graph insertion)
};

/**
 * @brief Pre-computed neighbor selection for a single level.
 *
 * Used to split insertElementToGraph() into search phase (read-only) and
 * write phase (disk I/O). The search phase populates this structure,
 * then the consistency lock is acquired, then the write phase uses it.
 */
struct LevelNeighborSelection {
    levelType level;
    vecsim_stl::vector<idType> selectedNeighbors; // Neighbors selected by heuristic

    explicit LevelNeighborSelection(const std::shared_ptr<VecSimAllocator>& alloc)
        : level(0), selectedNeighbors(alloc) {}

    LevelNeighborSelection(levelType lvl, vecsim_stl::vector<idType>&& neighbors)
        : level(lvl), selectedNeighbors(std::move(neighbors)) {}
};

/**
 * @brief Result from indexVector() containing pre-computed graph search state.
 *
 * This struct holds the pre-computed state from the read-only search phase,
 * allowing validation to occur between search and storage. The caller can
 * check if the job is still valid before proceeding with storeVector().
 *
 * Note: ProcessedBlobs are NOT stored here - they are managed by the caller
 * (tiered layer) and passed separately to indexVector() and storeVector().
 * This allows preprocessing once and reusing for both functions.
 */
struct IndexVectorResult {
    vecsim_stl::vector<LevelNeighborSelection> levelSelections; // Pre-computed neighbor selections
    idType entryPointAtSearchTime;                              // Entry point snapshot captured during indexVector()
    levelType elementMaxLevel;                                  // Random level for this element
    levelType maxLevelAtSearchTime;                             // Max level snapshot captured during indexVector()
    bool indexWasEmpty;                                         // True if index was empty at search time

    explicit IndexVectorResult(const std::shared_ptr<VecSimAllocator>& alloc)
        : levelSelections(alloc), entryPointAtSearchTime(INVALID_ID), elementMaxLevel(0), maxLevelAtSearchTime(0),
          indexWasEmpty(false) {}

    // Default move operations
    IndexVectorResult(IndexVectorResult&&) noexcept = default;
    IndexVectorResult& operator=(IndexVectorResult&&) noexcept = default;
    IndexVectorResult(const IndexVectorResult&) = delete;
    IndexVectorResult& operator=(const IndexVectorResult&) = delete;
};

// Note: INVALID_ID and HNSW_INVALID_LEVEL are already defined in vec_sim_common.h

/**
 * @brief Element metadata stored in memory.
 *
 * Stores per-element information needed for graph operations:
 * - label: User-provided label for the element
 * - flags: Bit flags (DISK_DELETE_MARK, DISK_IN_PROCESS) - use atomic operations for thread safety
 * - maxLevel: Maximum level this element exists on in the HNSW graph
 *
 * Note: Per-node locks are stored separately in a std::deque<std::atomic_bool> (nodeLocks_)
 * because std::atomic_bool is not movable, and this struct is stored in a vector that may resize.
 * The deque is used because it doesn't move existing elements when growing.
 */
#pragma pack(1)
struct DiskElementMetaData {
    labelType label = INVALID_LABEL;
    ElementFlags flags = DISK_IN_PROCESS;
    levelType maxLevel = 0;

    DiskElementMetaData() noexcept = default;
    explicit DiskElementMetaData(labelType l, levelType level) noexcept : label(l), maxLevel(level) {}
};
#pragma pack() // restore default packing

// RAII lock guard for per-node spinlocks stored in nodeLocks_ deque
class ElementLockGuard {
    std::atomic_bool& lock_flag_;

    static void lock(std::atomic_bool& flag) noexcept {
        bool expected = false;
        while (!flag.compare_exchange_weak(expected, true, std::memory_order_acquire)) {
            expected = false;
        }
    }
    static void unlock(std::atomic_bool& flag) noexcept { flag.store(false, std::memory_order_release); }

public:
    explicit ElementLockGuard(std::atomic_bool& flag) noexcept : lock_flag_(flag) { lock(lock_flag_); }
    ~ElementLockGuard() noexcept { unlock(lock_flag_); }
    ElementLockGuard(const ElementLockGuard&) = delete;
    ElementLockGuard& operator=(const ElementLockGuard&) = delete;
};

/**
 * @brief Thread-local random number generator with configurable seed.
 *
 * Uses constexpr if to select seeding strategy at compile time:
 * - Seed = 0 (default): Uses std::random_device for production randomness
 * - Seed != 0: Uses fixed seed for test reproducibility
 *
 * @tparam Seed The seed value. 0 means use random_device.
 * @return Reference to thread-local std::mt19937 generator.
 */
template <unsigned int Seed = 0>
inline std::mt19937& getThreadLocalGenerator() {
    if constexpr (Seed == 0) {
        static thread_local std::mt19937 generator(std::random_device{}());
        return generator;
    } else {
        static thread_local std::mt19937 generator(Seed);
        return generator;
    }
}

template <typename DataType, typename DistType>
class HNSWDiskIndex : public VecSimIndexAbstract<DataType, DistType>, public VecSimIndexTombstone {
public:
    // Constructor for factory use - takes ownership of storage and components
    HNSWDiskIndex(const VecSimParamsDisk* params, const AbstractIndexInitParams& abstractInitParams,
                  const DiskIndexComponents<DataType, DistType>& components,
                  std::unique_ptr<HNSWStorage<DataType>> storage);
    ~HNSWDiskIndex() override = default;

    // Distance calculation methods - zero overhead via compile-time mode selection
    // Names match DistanceMode enum: Full, QuantizedVsFull, Quantized
    // calcFullDistance: Used for reranking with full precision vectors from disk
    // calcQuantizedVsFullDistance: Used during query traversal (quantized stored vs full query)
    // calcQuantizedDistance: Used during insertion graph traversal (quantized↔quantized in memory)
    DistType calcFullDistance(const void* full1, const void* full2) const noexcept;
    DistType calcQuantizedVsFullDistance(const void* quantized, const void* full) const noexcept;
    DistType calcQuantizedDistance(const void* quantized1, const void* quantized2) const noexcept;

    // VecSimIndexInterface - stubs for MOD-13164
    int addVector(const void* blob, labelType label) override;
    int deleteVector(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void* blob) const override;
    size_t indexSize() const override { return curElementCount_.load(std::memory_order_relaxed); }
    size_t indexCapacity() const override { return maxElements_.load(std::memory_order_relaxed); }
    size_t indexLabelCount() const override {
        std::shared_lock<std::shared_mutex> lock(labelLookupMutex_);
        return labelToIdLookup_.size();
    }

    vecsim_stl::set<labelType> getLabelsSet() const override {
        std::shared_lock<std::shared_mutex> lock(labelLookupMutex_);
        vecsim_stl::set<labelType> labels(this->allocator);
        for (const auto& [label, id] : labelToIdLookup_) {
            labels.insert(label);
        }
        return labels;
    }

    /**
     * @brief Check if current capacity is full.
     *
     * @return true if nextId_ >= maxElements_ and holes is empty
     */
    [[nodiscard]] bool isCapacityFull() const;

    VecSimQueryReply* topKQuery(const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const override;
    VecSimQueryReply* rangeQuery(const void* queryBlob, double radius, VecSimQueryParams* queryParams) const override;

    VecSimIndexBasicInfo basicInfo() const override;
    VecSimIndexDebugInfo debugInfo() const override;
    VecSimDebugInfoIterator* debugInfoIterator() const override;

    VecSimBatchIterator* newBatchIterator(const void*, VecSimQueryParams*) const override { return nullptr; }
    bool preferAdHocSearch(size_t, size_t, bool) const override { return true; }

#ifdef BUILD_TESTS
    void fitMemory() override {}
    size_t indexMetaDataCapacity() const override { return maxElements_.load(std::memory_order_relaxed); }
    void getDataByLabel(labelType, std::vector<std::vector<DataType>>&) const override {}
    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType) const override { return {}; }
#endif

    size_t getEf() const { return efRuntime_; }
    void setEf(size_t ef) { efRuntime_ = ef; }

    HNSWStorage<DataType>* getStorage() const { return storage_.get(); }

    // --- Query Statistics (Test Only) ---
#ifdef BUILD_TESTS
    // Get cumulative count of visited nodes in upper layers (greedySearchLevel, levels > 0)
    size_t getNumVisitedNodesUpperLayers() const { return numVisitedNodesUpperLayers_.load(std::memory_order_relaxed); }
    // Get cumulative count of visited nodes in bottom layer (searchLayer, level 0)
    size_t getNumVisitedNodesBottomLayer() const { return numVisitedNodesBottomLayer_.load(std::memory_order_relaxed); }
    // Reset visited node counters (useful for testing)
    void resetVisitedNodesCounters() {
        numVisitedNodesUpperLayers_.store(0, std::memory_order_relaxed);
        numVisitedNodesBottomLayer_.store(0, std::memory_order_relaxed);
    }
#endif

    const char* getQuantizedDataByInternalId(idType internal_id) const;

    // =========================================================================
    // Delete Flow Helper Methods (for tiered integration)
    // =========================================================================

    // Thread-safe read of element's maxLevel.
    levelType getElementMaxLevel(idType id) const {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        return idToMetaData[id].maxLevel;
    }

    // Decrement element count after deletion. Thread-safe.
    void decrementElementCount() { curElementCount_.fetch_sub(1, std::memory_order_relaxed); }

    // Return ID to pool for reuse. Call after markDelete() and disk cleanup.
    void recycleId(idType id);

    // =========================================================================
    // Test Accessors - Public accessors for testing
    // =========================================================================
    // Test methods are defined in hnsw_disk_test_helpers.h via macro
    // This keeps the main header clean while providing full test access
    HNSW_DISK_TEST_METHODS

    vecsim_stl::vector<idType> markDelete(labelType label) override;

    // --- Locking ---
    /**
     * @brief Acquire lock on a node for edge modifications.
     *
     * Used during connectNewElement and repairNode
     * to protect read-modify-write operations on a node's outgoing edges.
     *
     * @param id Internal ID of the node to lock
     * @return RAII lock guard that releases on destruction
     */
    ElementLockGuard lockNode(idType id) const;

    // --- Tiered Index API ---
    // These methods are used by TieredHNSWDiskIndex

    /**
     * @brief Check if a label exists in the index.
     *
     * @param label User-provided label to check
     * @return true if label exists, false otherwise
     */
    bool isLabelExists(labelType label) const {
        std::shared_lock<std::shared_mutex> lock(labelLookupMutex_);
        return labelToIdLookup_.find(label) != labelToIdLookup_.end();
    }

    /**
     * @brief Store a new vector in the index (Phase 1 of tiered insertion).
     *
     * This function:
     * @brief Search/traversal phase for async insertion (read-only).
     *
     * Performs all read-only operations needed for insertion:
     * 1. Generates random level for this element
     * 2. Captures current entry point state
     * 3. Traverses graph to find best neighbors at each level
     * 4. Pre-computes neighbor selections (no disk writes)
     *
     * The result is used by allocateAndRegister() + storeVectorConnections() to complete insertion.
     * This split enables checking if the insertion is still valid (e.g., not deleted)
     * between the search and storage phases.
     *
     * NOTE: The caller must preprocess the vector to quantized format (via preprocess())
     * before calling this function. This allows the caller to manage blobs and reuse
     * them for storeVectorConnections().
     *
     * @param quantizedData Preprocessed query blob (quantized format) for distance computation
     * @return IndexVectorResult containing all pre-computed state
     */
    [[nodiscard]] IndexVectorResult indexVector(const void* quantizedData) const;

    /**
     * @brief Atomically allocate ID, store SQ8 blob, and register metadata.
     *
     * This is the atomic transition point where a vector moves from flat buffer
     * to HNSW disk index. Called while tiered layer holds flatIndexGuard.
     *
     * Operations performed:
     * 1. Allocate internal ID (from holes or increment counter)
     * 2. Store SQ8 blob in vectors container
     * 3. Initialize element metadata (label, flags, maxLevel)
     * 4. Increment element count
     * 5. Register label-to-ID mapping
     *
     * The element is marked as DISK_IN_PROCESS, making it invisible to queries
     * until storeVectorConnections() completes the insertion.
     *
     * @param label User-provided label for this vector
     * @param maxLevel Pre-generated random level for this element
     * @param storageSQ8 Preprocessed storage blob (SQ8 format)
     * @return Allocated internal ID
     */
    [[nodiscard]] idType allocateAndRegister(labelType label, levelType maxLevel, const void* storageSQ8);

    /**
     * @brief Complete graph insertion: write FP32 to disk and connect to graph.
     *
     * Called after allocateAndRegister() when flat lock has been released.
     * This is the expensive part that should NOT hold the flat lock.
     *
     * Operations performed:
     * 1. Write FP32 to disk storage
     * 2. Connect to graph using pre-computed levelSelections
     * 3. Update entry point if this element has a higher level
     * 4. Unmark DISK_IN_PROCESS, making the element visible to queries
     *
     * @param internalId ID returned from allocateAndRegister()
     * @param fullData Original full-precision data (needed for disk write)
     * @param quantizedData Preprocessed query blob (quantized format) for fallback graph insertion
     * @param indexResult Pre-computed state from indexVector() (moved in)
     * @return List of nodes needing repair (overflowed neighbors)
     */
    [[nodiscard]] GraphNodeList storeVectorConnections(idType internalId, const void* fullData,
                                                       const void* quantizedData, IndexVectorResult&& indexResult);

    // --- Graph Repair ---
    /**
     * @brief Repair a node's connections after neighbors have been deleted or when node overflows.
     *
     * Collects non-deleted neighbors and neighbors-of-deleted-neighbors as candidates,
     * then re-runs the heuristic to select the best connections.
     *
     * @param id The node to repair
     * @param level The graph level to repair connections at
     */
    void repairNode(idType id, levelType level) const;

protected:
    // =========================================================================
    // Protected Flagging API
    // =========================================================================
    // Uses shared lock to protect against concurrent resize
    template <ElementFlags FLAG>
    void markAs(idType internalId) {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        __atomic_fetch_or(&idToMetaData[internalId].flags, FLAG, __ATOMIC_RELAXED);
    }
    template <ElementFlags FLAG>
    void unmarkAs(idType internalId) {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        __atomic_fetch_and(&idToMetaData[internalId].flags, static_cast<ElementFlags>(~FLAG), __ATOMIC_RELAXED);
    }
    template <ElementFlags FLAG>
    bool isMarkedAs(idType internalId) const {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        return isMarkedAsUnsafe<FLAG>(internalId);
    }

private:
    // Private helper - assumes metadataMutex_ lock is already held.
    // Does NOT perform bounds checking.
    template <ElementFlags FLAG>
    bool isMarkedAsUnsafe(idType internalId) const {
        return __atomic_load_n(&idToMetaData[internalId].flags, __ATOMIC_RELAXED) & FLAG;
    }

    /**
     * @brief Resolve entry point for graph insertion, handling concurrent empty-index inserts.
     *
     * When multiple threads insert concurrently on an empty index, they may all capture
     * entryPointAtSearchTime == INVALID_ID. This helper uses an exclusive lock to atomically
     * determine whether this thread initializes the entry point or connects to an
     * existing one established by another thread.
     *
     * @param internalId The internal ID of the element being inserted
     * @param elementMaxLevel The max level of the element being inserted
     * @param capturedEntryPoint Entry point snapshot from indexVector() or storeVector()
     * @param capturedMaxLevel Max level snapshot from indexVector() or storeVector()
     * @return EntryPointResolution indicating whether this thread became entry point and what to use
     */
    EntryPointResolution resolveEntryPoint(idType internalId, levelType elementMaxLevel, idType capturedEntryPoint,
                                           levelType capturedMaxLevel) {
        // Fast path: if we captured a valid entry point, use it
        if (capturedEntryPoint != INVALID_ID) {
            return {capturedEntryPoint, capturedMaxLevel, false};
        }

        // Slow path: index was empty at capture time, need to check under lock
        std::unique_lock<std::shared_mutex> epLock(entryPointMutex_);
        if (entryPoint_ == INVALID_ID) {
            // This thread initializes the entry point for an empty index
            entryPoint_ = internalId;
            maxLevel_ = elementMaxLevel;
            return {INVALID_ID, 0, true}; // Caller should skip graph insertion
        }
        // Another thread established entry point after our snapshot - use it
        return {entryPoint_, maxLevel_, false};
    }

    // =========================================================================
    // Member Variables
    // =========================================================================

    // --- Lock Ordering Hierarchy (to prevent deadlocks) ---
    // When acquiring multiple locks, always acquire in this order:
    //   1. labelLookupMutex_  (outermost - protects label-to-ID mapping)
    //   2. vectorsMutex_      (protects vectors container)
    //   3. metadataMutex_     (protects idToMetaData and nodeLocks_)
    //   4. entryPointMutex_   (protects entry point state)
    //   5. holesMutex_        (innermost - protects recycled ID list)
    //
    // Note: Most code paths acquire only a single lock. The hierarchy above
    // must be followed when adding new code that acquires multiple locks.

    // --- Index Parameters ---
    size_t M_;  // Max neighbors per node at levels > 0
    size_t M0_; // Max neighbors at level 0 (= 2 * M)
    size_t efConstruction_;
    size_t efRuntime_;
    std::string indexName_;

    // --- Index State ---
    // curElementCount_: Actual number of vectors in the index.
    //                   Incremented in addVector(), decremented in deleteVector().
    std::atomic<size_t> curElementCount_{0};
    // maxElements_: Pre-allocated capacity (grows by blockSize in growByBlock()).
    //               Always >= nextId_. Used for bounds checking and resize decisions.
    std::atomic<size_t> maxElements_{0};

    // --- Element Metadata ---
    // Index data (mutable for locking in const methods)
    mutable vecsim_stl::vector<DiskElementMetaData> idToMetaData;

    // Per-node spinlocks for protecting edge modifications.
    // Stored in a deque (not vector) because std::atomic_bool is not movable,
    // and deque doesn't move existing elements when growing.
    // Must be kept in sync with idToMetaData (same indices).
    mutable vecsim_stl::deque<std::atomic_bool> nodeLocks_;

    // Protects idToMetaData and nodeLocks_ from concurrent access during resize.
    // Shared lock for reads and writes to existing elements (isMarkedAs, lockNode, initElementMetadata).
    // Exclusive lock only for structural changes (growByBlock resize).
    mutable std::shared_mutex metadataMutex_;

    // --- Label-to-ID Reverse Lookup (as per design doc) ---
    // Maps user-provided labels to internal IDs for O(1) label existence checks,
    // overwrites, and deletions. The forward mapping (ID → label) is in idToMetaData.
    vecsim_stl::unordered_map<labelType, idType> labelToIdLookup_;
    mutable std::shared_mutex labelLookupMutex_; // Protects labelToIdLookup_

    // --- Storage ---
    std::unique_ptr<HNSWStorage<DataType>> storage_;

    // --- Reranking ---
    // Whether to rerank results using full-precision vectors from disk.
    // When enabled, topKQuery will recompute distances using FP32 vectors
    // after the initial search with quantized vectors.
    bool rerankEnabled_;

    // --- Query Statistics (Test Only) ---
#ifdef BUILD_TESTS
    // Cumulative counters for visited nodes during queries.
    // These are mutable because they are updated during const query operations.
    // Upper layers: nodes visited during greedySearchLevel (levels > 0)
    mutable std::atomic<size_t> numVisitedNodesUpperLayers_{0};
    // Bottom layer: nodes visited during searchLayer at level 0
    mutable std::atomic<size_t> numVisitedNodesBottomLayer_{0};
#endif

    // --- Level Generation ---
    double mult_; // = 1 / ln(M), for level generation

    // --- ID Allocation ---
    // nextId_: Next fresh ID to allocate (monotonically increasing).
    //          Only used when holes_ is empty. After deletions, nextId_ > curElementCount_.
    idType nextId_{0};
    mutable std::mutex holesMutex_; // Protects holes_
    // holes_: Recycled IDs from deleted elements. Populated in deleteVector().
    vecsim_stl::vector<idType> holes_;

    // --- Entry Point State ---
    mutable std::shared_mutex entryPointMutex_;                 // Protects entryPoint_ and maxLevel_
    idType entryPoint_{INVALID_ID};                             // Current entry point ID
    levelType maxLevel_{std::numeric_limits<levelType>::max()}; // Maximum level in the graph

    // --- Vector Container Protection ---
    // Protects this->vectors (DataBlocksContainer) from concurrent access.
    // Shared lock for reads (getElement), exclusive lock for writes (addElement).
    mutable std::shared_mutex vectorsMutex_;

    // =========================================================================
    // Private Methods
    // =========================================================================

    // --- Level Generation ---
    /**
     * @brief Generate a random level for a new element.
     *
     * Uses exponential distribution: level = floor(-ln(uniform(0,1)) * mult_)
     * Level 0 is most common, higher levels are exponentially rarer.
     *
     * @tparam Seed Random seed. 0 (default) uses random_device, non-zero for test reproducibility.
     * @return Random level (0 to ~16 typically)
     */
    template <unsigned int Seed = 0>
    levelType getRandomLevel() const;

    // --- Graph Traversal Helpers ---
    vecsim_stl::vector<idType> getNeighbors(idType nodeId, levelType level) const;
    void setNeighbors(idType nodeId, levelType level, const vecsim_stl::vector<idType>& neighbors) const;

    // --- ID Allocation Methods ---
    /**
     * @brief Allocate a new internal ID for an element.
     *
     * First checks the holes vector for recycled IDs, otherwise uses nextId_.
     * Thread-safe.
     *
     * @return Newly allocated internal ID
     */
    [[nodiscard]] idType allocateId();

    /**
     * @brief Allocate an ID and store quantized vector atomically.
     *
     * Combines allocateId() with vector storage under vectorsMutex_.
     * Handles both fresh IDs (addElement) and recycled IDs (updateElement).
     *
     * @param quantizedBlob The quantized vector data to store
     * @return Newly allocated internal ID with vector already stored
     */
    [[nodiscard]] idType allocateIdAndStoreVector(const void* quantizedBlob);

    // --- Capacity Management ---
    /**
     * @brief Grow both idToMetaData and nodeLocks_ by blockSize.
     *
     * Called when capacity is full before allocating a new ID.
     * Uses double-checked locking: re-checks if growth is still needed after
     * acquiring metadataMutex_ to avoid redundant growth from concurrent callers.
     *
     * @param triggeringId The ID that triggered the growth request. Growth is
     *                     skipped if this ID is already within current capacity.
     */
    void growByBlock(idType triggeringId);

    /**
     * @brief Shrink both idToMetaData and nodeLocks_ by blockSize.
     *
     * Called when there's excess capacity (e.g., after many deletions).
     * Uses double-checked locking: re-checks if shrinking is still safe after
     * acquiring metadataMutex_ to avoid unsafe shrinking from concurrent callers.
     *
     * Shrinking is only performed if:
     * - Current capacity >= blockSize
     * - nextId_ <= (maxElements_ - blockSize) (at least one full unused block at the tail)
     *
     * This allows shrinking to zero capacity when the index is truly empty
     * (nextId_ == 0 and current capacity == blockSize).
     */
    void shrinkByBlock();

    /**
     * @brief Initialize metadata and lock for an element.
     *
     * Works for both fresh IDs and recycled IDs from the holes pool.
     * Capacity is pre-allocated by growByBlock(); this just resets the lock for recycled IDs.
     *
     * @param id The allocated ID (from allocateId())
     * @param label User-provided label
     * @param level Maximum level for this element
     */
    void initElementMetadata(idType id, labelType label, levelType level);

    // --- Entry Point Management ---
    /**
     * @brief Get current entry point and max level safely.
     *
     * Thread-safe read under shared lock.
     *
     * @return Graph node type containing entry point and max level
     */
    GraphNodeType safeGetEntryPointState() const;

    /**
     * @brief Conditionally update entry point if new element has higher level.
     *
     * Thread-safe with exclusive lock. Only updates if the new element has a higher
     * level than the current entry point (or if there is no entry point yet).
     *
     * Called from indexVector AFTER the element is connected to the graph,
     * ensuring concurrent queries never start from an unconnected node.
     *
     * @param newId ID of the new element
     * @param newLevel Maximum level of the new element
     */
    void replaceEntryPoint(idType newId, levelType newLevel);

    /**
     * @brief Replace entry point when current entry point is deleted.
     *
     * Selection strategy:
     * 1. Try neighbors of deleted entry point at top level
     * 2. If no valid neighbor, scan all elements at top level
     * 3. If no element at top level, decrease maxLevel_ and recurse
     * 4. If index becomes empty, set entryPoint_ = INVALID_ID
     *
     * Holds entryPointMutex_ exclusively for the entire operation to avoid
     * race conditions with concurrent deletes. Cost is minimal since entry
     * point replacement is rare (only when deleting the entry point itself).
     */
    void replaceEntryPointOnDelete();

    // --- Distance Computation ---
    /**
     * @brief Compute distance between a query vector and a stored element
     *
     * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
     *
     * When quantized_query=true: SQ8↔SQ8 distance (both vectors from memory)
     * When quantized_query=false: FP32↔SQ8 distance (full query vs quantized stored)
     */
    template <bool quantized_query = true>
    inline DistType computeDistance(const void* query, idType elementId) const;
    // Quantized stored vs full precision query
    DistType computeDistanceQuantized_Full(const void* fullQuery, idType elementId) const;
    // Quantized stored vs quantized query
    DistType computeDistanceQuantized_Quantized(const void* quantizedQuery, idType elementId) const;
    // Full precision stored vs full precision query (for reranking from disk)
    DistType computeDistanceFull_Full(const void* fullQuery, idType elementId) const;

    // --- Element State Queries ---
    /**
     * @brief Check if an element is marked as DISK_IN_PROCESS.
     *
     * Thread-safe check using atomic flag operations on in-memory metadata.
     * Elements being inserted are marked DISK_IN_PROCESS and should be skipped
     * during search to avoid reading incomplete data.
     *
     * @param id Internal ID to check
     * @return true if element is DISK_IN_PROCESS
     */
    bool isInProcess(idType id) const;

    /**
     * @brief Check if an element is marked as deleted.
     *
     * Thread-safe check using atomic flag operations on in-memory metadata.
     * Includes bounds checking - returns false for out-of-bounds IDs.
     *
     * @param id Internal ID to check
     * @return true if element is marked deleted
     */
    bool isMarkedDeleted(idType id) const;

    // --- Search Algorithms ---
    /**
     * @brief Greedy search at a single level to find the closest element.
     *
     * Used for upper level traversal (L > 0). Starts from entry point and
     * greedily moves to closer neighbors until no improvement is possible.
     *
     * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
     * @tparam running_query true if running a query (checks timeout, allows deleted elements)
     *
     * When running_query=true: Checks for timeout, can return deleted element
     * When running_query=false: No timeout check, must return non-deleted element
     *
     * @param query Pointer to query vector data (SQ8 if quantized_query=true, FP32 otherwise)
     * @param level The level to search at
     * @param[in,out] currObj Current best element ID (updated to closest found)
     * @param[in,out] currDist Distance to current best (updated accordingly)
     * @param timeoutCtx Timeout context (only used when running_query=true)
     * @param rc Result code output (only used when running_query=true)
     */
    template <bool quantized_query, bool running_query>
    void greedySearchLevel(const void* query, levelType level, idType& currObj, DistType& currDist,
                           void* timeoutCtx = nullptr, VecSimQueryReply_Code* rc = nullptr) const;

    template <bool quantized_query, bool running_query>
    candidatesMaxHeap<DistType> searchLayer(idType ep_id, const void* data_point, levelType layer, size_t ef,
                                            void* timeoutCtx = nullptr, VecSimQueryReply_Code* rc = nullptr) const;

    /**
     * @brief Find the entry point for bottom layer search.
     *
     * Greedily traverses upper layers (from maxLevel down to level 1) to find the best
     * entry point for the bottom layer beam search.
     *
     * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
     *
     * @param query_data Query vector (SQ8 if quantized_query=true, FP32 otherwise)
     * @param timeoutCtx Timeout context for early termination
     * @param rc Result code output (VecSim_QueryReply_OK or VecSim_QueryReply_TimedOut)
     * @return Entry point ID for bottom layer search, or INVALID_ID on error/timeout
     */
    template <bool quantized_query>
    idType searchBottomLayerEP(const void* query_data, void* timeoutCtx, VecSimQueryReply_Code* rc) const;

    /**
     * @brief Rerank candidates using full-precision vectors from disk.
     *
     * Recomputes distances for all candidates using FP32 vectors stored on disk
     * instead of the quantized vectors used during search. Returns a new heap
     * with reranked distances.
     *
     * @param queryBlob Full precision query vector (FP32)
     * @param candidates Max-heap of candidates from search (internal IDs)
     * @param timeoutCtx Timeout context for early termination
     * @param rc Result code output (VecSim_QueryReply_OK or VecSim_QueryReply_TimedOut)
     * @return Vector of all candidates with recomputed distances, as (distance, id) pairs
     */
    vecsim_stl::vector<std::pair<DistType, idType>> rerankCandidates(const void* queryBlob,
                                                                     const candidatesMaxHeap<DistType>& candidates,
                                                                     void* timeoutCtx, VecSimQueryReply_Code* rc) const;

    /**
     * @brief Process a candidate during beam search.
     *
     * Explores neighbors of a candidate node and updates the search heaps.
     * Used by searchLayer for level 0 search with ef parameter.
     *
     * @tparam quantized_query true if query vector is quantized, false if full-precision
     * @param curNodeId Current node being processed
     * @param data_point Query vector (SQ8 if quantized_query=true, FP32 otherwise)
     * @param layer Current level being searched
     * @param ef Beam width
     * @param visited_set Set of already visited nodes
     * @param top_candidates Max-heap of best candidates found (farthest at top)
     * @param candidate_set Min-heap of nodes to explore (closest at top, via negation)
     * @param lowerBound Current distance threshold for adding candidates
     */
    template <bool quantized_query>
    void processCandidate(idType curNodeId, const void* data_point, levelType layer, size_t ef,
                          vecsim_stl::unordered_set<idType>& visited_set, candidatesMaxHeap<DistType>& top_candidates,
                          candidatesMaxHeap<DistType>& candidate_set, DistType& lowerBound) const;

    // --- Neighbor Selection Heuristic ---
    /**
     * @brief Filter candidates using diversity heuristic (Algorithm 4 in HNSW paper).
     *
     * Selects at most M neighbors that are diverse (not too close to each other).
     *
     * @param top_candidates Candidates to filter (modified in place)
     * @param M Maximum number of neighbors to keep
     */
    void getNeighborsByHeuristic2(candidatesList<DistType>& top_candidates, size_t M) const;

    /**
     * @brief Filter candidates and record removed ones.
     */
    void getNeighborsByHeuristic2(candidatesList<DistType>& top_candidates, size_t M,
                                  vecsim_stl::vector<idType>& removed_candidates) const;

    /**
     * @brief Internal implementation of heuristic filtering.
     */
    template <bool record_removed>
    void getNeighborsByHeuristic2_internal(candidatesList<DistType>& top_candidates, size_t M,
                                           vecsim_stl::vector<idType>* removed_candidates) const;

    // --- Graph Modification ---
    /**
     * @brief Select neighbors for a level without writing to disk (search phase).
     *
     * Takes raw candidates from searchLayer and applies the heuristic to select
     * the best neighbors. Does NOT write edges to disk - that's done by connectNewElement().
     *
     * @param top_candidates Candidates from searchLayer (moved from)
     * @param level The graph level
     * @return Pair of (neighbor selection, closest neighbor for next level entry point)
     */
    std::pair<LevelNeighborSelection, idType> selectNeighborsForLevel(candidatesMaxHeap<DistType>&& top_candidates,
                                                                      levelType level) const;

    /**
     * @brief Search the graph and select neighbors for each level (shared search logic).
     *
     * This is the common search phase used by both indexVector() and insertElementToGraph().
     * It traverses from the entry point, doing greedy search on upper levels, then
     * searchLayer + selectNeighborsForLevel on each level from max_common_level down to 0.
     *
     * @param quantizedData Quantized vector data for distance computation
     * @param elementMaxLevel Maximum level for the new element
     * @param entryPoint Entry point to start traversal from
     * @param globalMaxLevel Current max level of the graph
     * @return Vector of neighbor selections, one per level (from max_common_level down to 0)
     */
    [[nodiscard]] vecsim_stl::vector<LevelNeighborSelection> searchGraphForNeighbors(const void* quantizedData,
                                                                                     levelType elementMaxLevel,
                                                                                     idType entryPoint,
                                                                                     levelType globalMaxLevel) const;

    /**
     * @brief Connect a new element to pre-selected neighbors (write phase).
     *
     * Takes the pre-computed neighbor selection from selectNeighborsForLevel() and
     * writes all edges to disk. This is the write-phase counterpart to the search phase.
     *
     * @param new_node_id The new element being inserted
     * @param selection Pre-computed neighbor selection from selectNeighborsForLevel()
     * @return List of nodes needing repair (overflowed neighbors)
     */
    GraphNodeList connectNewElement(idType new_node_id, const LevelNeighborSelection& selection) const;

    /**
     * @brief Insert element into the graph (Phase 2 of insertion).
     *
     * Uses searchGraphForNeighbors() to find neighbors, then connects the element.
     * Used as a fallback when the index was empty at search time but another thread
     * initialized the entry point before we could become the entry point ourselves.
     *
     * @param element_id Internal ID of the new element
     * @param element_max_level Maximum level for this element
     * @param entry_point Entry point to start traversal from
     * @param global_max_level Current max level of the graph
     * @param quantizedData Quantized vector data for distance computation
     * @return List of nodes needing repair (caller is responsible for repairing them)
     */
    [[nodiscard]] GraphNodeList insertElementToGraph(idType element_id, levelType element_max_level, idType entry_point,
                                                     levelType global_max_level, const void* quantizedData) const;

    /**
     * @brief Connect a new element to the graph based on resolved entry point state.
     *
     * Helper for storeVectorConnections() that handles three cases with early returns:
     * 1. becameEntryPoint=true: First element, no connections needed
     * 2. indexWasEmpty=true: Another thread initialized EP, do full search now
     * 3. Normal case: Use pre-computed levelSelections from indexVector()
     *
     * @param internalId The internal ID of the element being inserted
     * @param indexResult Pre-computed state from indexVector() (neighbor selections)
     * @param quantizedData Quantized vector for distance computation (used in case 2)
     * @param resolution Entry point resolution result from resolveEntryPoint()
     * @return List of nodes needing repair
     */
    [[nodiscard]] GraphNodeList connectToGraph(idType internalId, const IndexVectorResult& indexResult,
                                               const void* quantizedData, const EntryPointResolution& resolution) const;

    /**
     * @brief Collect repair candidates for a node.
     *
     * Gathers candidates from:
     * 1. Current non-deleted neighbors
     * 2. Non-deleted neighbors of deleted neighbors (to maintain connectivity)
     *
     * @param nodeId The node being repaired (used to exclude self from candidates)
     * @param outgoingEdges Current outgoing edges of the node
     * @param level Graph level (needed to fetch neighbor edges from storage)
     * @param[out] deletedNeighbors Populated with IDs of deleted neighbors found in outgoingEdges
     * @return Vector of candidate IDs (without distances - distances computed later only if needed)
     */
    vecsim_stl::vector<idType> collectRepairCandidates(idType nodeId, const vecsim_stl::vector<idType>& outgoingEdges,
                                                       levelType level,
                                                       vecsim_stl::vector<idType>& deletedNeighbors) const;

    /**
     * @brief Update incoming edges after repair.
     *
     * Maintains bidirectional edge consistency by:
     * - For removed neighbors (in original but not in new): delete nodeId from their incoming edges
     * - For new neighbors (in new but not in original): add nodeId to their incoming edges
     * - For deleted neighbors: delete nodeId from their incoming edges (to prevent redundant repairs)
     *
     * @param nodeId The node that was repaired
     * @param originalEdgesSet Set of original outgoing edges before repair (for O(1) lookup)
     * @param newNeighbors New neighbors after heuristic selection
     * @param removedCandidates Candidates rejected by the heuristic
     * @param deletedNeighbors Neighbors that were marked as deleted
     * @param level Graph level
     */
    void updateIncomingEdgesAfterRepair(idType nodeId, const vecsim_stl::unordered_set<idType>& originalEdgesSet,
                                        const vecsim_stl::vector<idType>& newNeighbors,
                                        const vecsim_stl::vector<idType>& removedCandidates,
                                        const vecsim_stl::vector<idType>& deletedNeighbors, levelType level) const;

    // --- Label Lookup Helpers ---
    /**
     * @brief Look up internal ID by label.
     *
     * @param label User-provided label to look up
     * @return The internal ID if found, or std::nullopt if label doesn't exist
     */
    std::optional<idType> getIdByLabel(labelType label) const {
        std::shared_lock<std::shared_mutex> lock(labelLookupMutex_);
        auto it = labelToIdLookup_.find(label);
        if (it == labelToIdLookup_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    /**
     * @brief Remove a label from the lookup.
     *
     * Called during vector deletion.
     *
     * @param label User-provided label to remove
     * @return Number of elements removed (0 or 1)
     */
    size_t removeLabel(labelType label) {
        std::unique_lock<std::shared_mutex> lock(labelLookupMutex_);
        return labelToIdLookup_.erase(label);
    }

    // Helper to access the disk calculator with proper type for templated calcDistance<Mode>()
    const DiskDistanceCalculator<DistType>* getDiskCalculator() const {
        return static_cast<const DiskDistanceCalculator<DistType>*>(this->getIndexCalculator());
    }

    // --- Deleted Operations ---
    HNSWDiskIndex(const HNSWDiskIndex&) = delete;
    HNSWDiskIndex& operator=(const HNSWDiskIndex&) = delete;
};

// Template Implementation

template <typename DataType, typename DistType>
HNSWDiskIndex<DataType, DistType>::HNSWDiskIndex(const VecSimParamsDisk* params,
                                                 const AbstractIndexInitParams& abstractInitParams,
                                                 const DiskIndexComponents<DataType, DistType>& components,
                                                 std::unique_ptr<HNSWStorage<DataType>> storage)
    : VecSimIndexAbstract<DataType, DistType>(abstractInitParams, components),
      indexName_(params->diskContext->indexName, params->diskContext->indexNameLen),
      idToMetaData(abstractInitParams.allocator), nodeLocks_(abstractInitParams.allocator),
      labelToIdLookup_(abstractInitParams.allocator), storage_(std::move(storage)),
      rerankEnabled_(params->diskContext->rerank), holes_(abstractInitParams.allocator) {
    const HNSWParams& hnswParams = params->indexParams->algoParams.hnswParams;
    // Apply defaults for zero values (uses the public VectorSimilarity definitions)
    M_ = hnswParams.M ? hnswParams.M : HNSW_DEFAULT_M;
    // Validate M (same as RAM HNSW)
    if (M_ <= 1) {
        throw std::runtime_error("HNSW index parameter M cannot be 1 or 0");
    }
    M0_ = 2 * M_; // As per HNSW paper, level 0 has 2M neighbors
    if (M0_ > UINT16_MAX) {
        throw std::runtime_error("HNSW index parameter M is too large: argument overflow");
    }
    efConstruction_ = hnswParams.efConstruction ? hnswParams.efConstruction : HNSW_DEFAULT_EF_C;
    efRuntime_ = hnswParams.efRuntime ? hnswParams.efRuntime : HNSW_DEFAULT_EF_RT;
    mult_ = 1.0 / std::log(static_cast<double>(M_));
}

template <typename DataType, typename DistType>
template <unsigned int Seed>
levelType HNSWDiskIndex<DataType, DistType>::getRandomLevel() const {
    // Use template helper for thread-local RNG (Seed=0 means random_device)
    auto& generator = getThreadLocalGenerator<Seed>();
    // Use (0.0, 1.0] range to avoid log(0) which is undefined.
    // Distribution is stateless after construction, only the generator needs thread-local state.
    static std::uniform_real_distribution<double> distribution(std::nextafter(0.0, 1.0), 1.0);
    double r = -std::log(distribution(generator)) * mult_;
    return static_cast<levelType>(r);
}

template <typename DataType, typename DistType>
bool HNSWDiskIndex<DataType, DistType>::isCapacityFull() const {
    // Capacity is full when we have no holes and nextId_ >= maxElements_
    std::lock_guard<std::mutex> lock(holesMutex_);
    return holes_.empty() && nextId_ >= maxElements_.load(std::memory_order_relaxed);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::growByBlock(idType triggeringId) {
    std::unique_lock<std::shared_mutex> metaLock(metadataMutex_);

    // Double-check after acquiring lock: another thread may have already grown capacity
    size_t currentMax = maxElements_.load(std::memory_order_relaxed);
    if (triggeringId < currentMax) {
        return; // Capacity already sufficient
    }

    size_t newCapacity = currentMax + this->blockSize;

    // Pre-allocate idToMetaData and nodeLocks_ to avoid frequent reallocations
    idToMetaData.resize(newCapacity);
    nodeLocks_.resize(newCapacity);

    maxElements_.store(newCapacity, std::memory_order_release);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::shrinkByBlock() {
    std::unique_lock<std::shared_mutex> metaLock(metadataMutex_);
    std::lock_guard<std::mutex> holesLock(holesMutex_);

    size_t currentMax = maxElements_.load(std::memory_order_relaxed);
    if (currentMax == 0) {
        return; // Another thread may have already shrunk to zero.
    }
    assert(currentMax % this->blockSize == 0 && "shrinkByBlock: capacity must be block-aligned");
    assert(currentMax >= this->blockSize && "shrinkByBlock: non-zero capacity must be at least one block");

    // Double-check after acquiring lock: ensure we still have enough unused capacity
    // Only shrink if we have at least one full block of unused capacity at the end.
    // Use subtraction to avoid overflow in (nextId + blockSize).
    // Note: shrinking to zero capacity is allowed when index is truly empty.
    // Hold holesMutex_ while reading nextId_ and shrinking to avoid racing allocateId(),
    // which increments nextId_ under holesMutex_.
    size_t nextIdValue = nextId_;
    if (nextIdValue > currentMax - this->blockSize) {
        return; // Not safe to shrink - IDs might be in use or about to be used
    }

    size_t newCapacity = currentMax - this->blockSize;

    // Shrink idToMetaData and nodeLocks_ to release memory
    idToMetaData.resize(newCapacity);
    nodeLocks_.resize(newCapacity);

    maxElements_.store(newCapacity, std::memory_order_release);
}

template <typename DataType, typename DistType>
idType HNSWDiskIndex<DataType, DistType>::allocateId() {
    bool needGrow = false;
    idType newId;

    {
        std::lock_guard<std::mutex> lock(holesMutex_);

        // First check for recycled IDs
        if (!holes_.empty()) {
            idType id = holes_.back();
            holes_.pop_back();
            return id;
        }

        // No holes available, need a fresh ID
        newId = nextId_++;

        // Check if we need to grow capacity
        // maxElements_ is atomic, so this read is safe without metadataMutex_
        needGrow = (newId >= maxElements_.load(std::memory_order_acquire));
    }

    // Call growByBlock() outside of holesMutex_ to avoid nested lock acquisition
    // (growByBlock acquires metadataMutex_ exclusively)
    if (needGrow) {
        growByBlock(newId);
    }
    return newId;
}

template <typename DataType, typename DistType>
idType HNSWDiskIndex<DataType, DistType>::allocateIdAndStoreVector(const void* quantizedBlob) {
    std::unique_lock<std::shared_mutex> vectorsLock(vectorsMutex_);
    idType internalId = allocateId();

    // internalId must be either a recycled slot (< size) or the next sequential slot (== size).
    // If internalId > size, it indicates a gap in IDs which is a logic error in allocateId().
    assert(internalId <= this->vectors->size() &&
           "internalId must be either a recycled slot (< size) or the next sequential slot (== size)");

    // Check if this is a recycled ID by comparing to container size.
    // If ID < container size, the slot exists and we update it.
    // If ID == container size, this is a fresh ID and we append.
    if (internalId < this->vectors->size()) {
        this->vectors->updateElement(internalId, quantizedBlob);
    } else {
        this->vectors->addElement(quantizedBlob, internalId);
    }

    return internalId;
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::recycleId(idType id) {
    // TODO(delete PR) [MOD-13172]: Smarter hole management (if the id is last, also trim any holes from the left)
    // NOTE: If recycleId() starts triggering shrinkByBlock(), ensure holes_ does not retain
    // IDs >= new capacity after shrinking. Otherwise allocateId() may pop an out-of-bounds ID.
    std::lock_guard<std::mutex> lock(holesMutex_);
    holes_.push_back(id);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::initElementMetadata(idType id, labelType label, levelType level) {
    std::shared_lock<std::shared_mutex> lock(metadataMutex_);
    assert(id < maxElements_.load(std::memory_order_relaxed) && "initElementMetadata: id exceeds capacity");
    // Direct assignment - capacity was pre-allocated by growByBlock()
    idToMetaData[id] = DiskElementMetaData(label, level);
    // Reset the lock for recycled IDs (fresh IDs are already false from resize)
    nodeLocks_[id].store(false, std::memory_order_relaxed);
}

template <typename DataType, typename DistType>
GraphNodeType HNSWDiskIndex<DataType, DistType>::safeGetEntryPointState() const {
    std::shared_lock<std::shared_mutex> lock(entryPointMutex_);
    return {entryPoint_, maxLevel_};
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::replaceEntryPoint(idType newId, levelType newLevel) {
    std::unique_lock<std::shared_mutex> lock(entryPointMutex_);
    if (entryPoint_ == INVALID_ID || newLevel > maxLevel_) {
        entryPoint_ = newId;
        maxLevel_ = newLevel;
    }
}

template <typename DataType, typename DistType>
template <bool quantized_query>
inline DistType HNSWDiskIndex<DataType, DistType>::computeDistance(const void* query, idType elementId) const {
    if constexpr (quantized_query) {
        return computeDistanceQuantized_Quantized(query, elementId);
    } else {
        return computeDistanceQuantized_Full(query, elementId);
    }
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::computeDistanceQuantized_Full(const void* fullQuery,
                                                                          idType elementId) const {
    // Get quantized vector from memory (used for query traversal)
    // Acquire shared lock to protect against concurrent addElement() resizing
    std::shared_lock<std::shared_mutex> vectorsLock(vectorsMutex_);
    const void* storedQuantized = getQuantizedDataByInternalId(elementId);
    assert(storedQuantized && "storedQuantized should not be null");

    // calcQuantizedVsFullDistance expects (quantized, full)
    return calcQuantizedVsFullDistance(storedQuantized, fullQuery);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::computeDistanceQuantized_Quantized(const void* quantizedQuery,
                                                                               idType elementId) const {
    // Get quantized vector from memory (used for insertion graph traversal)
    // Acquire shared lock to protect against concurrent addElement() resizing
    std::shared_lock<std::shared_mutex> vectorsLock(vectorsMutex_);
    const void* storedQuantized = getQuantizedDataByInternalId(elementId);
    assert(storedQuantized && "storedQuantized should not be null");

    return calcQuantizedDistance(quantizedQuery, storedQuantized);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::computeDistanceFull_Full(const void* fullQuery, idType elementId) const {
    // Get full precision vector from disk (used for reranking)
    // Use inputBlobSize which is the full precision size (dim * sizeof(DataType))
    const size_t vectorSize = this->getInputBlobSize();
    vecsim_stl::vector<char> buffer(vectorSize, this->allocator);
    storage_->get_vector(elementId, buffer.data(), vectorSize);

    // calcFullDistance expects (full, full)
    return calcFullDistance(buffer.data(), fullQuery);
}

template <typename DataType, typename DistType>
vecsim_stl::vector<idType> HNSWDiskIndex<DataType, DistType>::getNeighbors(idType nodeId, levelType level) const {
    vecsim_stl::vector<idType> result(this->allocator);
    storage_->get_outgoing_edges(nodeId, level, result);
    return result;
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::setNeighbors(idType nodeId, levelType level,
                                                     const vecsim_stl::vector<idType>& neighbors) const {
    storage_->put_outgoing_edges(nodeId, level, neighbors);
}

template <typename DataType, typename DistType>
bool HNSWDiskIndex<DataType, DistType>::isInProcess(idType id) const {
    std::shared_lock<std::shared_mutex> lock(metadataMutex_);
    if (id >= idToMetaData.size()) {
        return true; // Unknown ID treated as in-process (not ready)
    }
    return isMarkedAsUnsafe<DISK_IN_PROCESS>(id);
}

template <typename DataType, typename DistType>
bool HNSWDiskIndex<DataType, DistType>::isMarkedDeleted(idType id) const {
    std::shared_lock<std::shared_mutex> lock(metadataMutex_);
    return id < idToMetaData.size() && isMarkedAsUnsafe<DISK_DELETE_MARK>(id);
}

template <typename DataType, typename DistType>
ElementLockGuard HNSWDiskIndex<DataType, DistType>::lockNode(idType id) const {
    // Acquire shared lock to safely access nodeLocks_.
    // Once we have the reference, it's stable because deque doesn't move elements.
    std::shared_lock<std::shared_mutex> lock(metadataMutex_);
    assert(id < nodeLocks_.size() && "Invalid node ID for locking");
    return ElementLockGuard(nodeLocks_[id]);
}

/**
 * Process a candidate node during beam search.
 *
 * Explores all neighbors of curNodeId, computing distances and updating
 * the search heaps. This is the core of the beam search algorithm.
 */
template <typename DataType, typename DistType>
template <bool quantized_query>
void HNSWDiskIndex<DataType, DistType>::processCandidate(idType curNodeId, const void* data_point, levelType layer,
                                                         size_t ef, vecsim_stl::unordered_set<idType>& visited_set,
                                                         candidatesMaxHeap<DistType>& top_candidates,
                                                         candidatesMaxHeap<DistType>& candidate_set,
                                                         DistType& lowerBound) const {

    // Get neighbors of current node at this level
    vecsim_stl::vector<idType> neighbors = getNeighbors(curNodeId, layer);

    for (idType candidate_id : neighbors) {
        // Skip already visited nodes
        if (visited_set.find(candidate_id) != visited_set.end()) {
            continue;
        }
        visited_set.insert(candidate_id);

        // Skip elements still being inserted
        if (isInProcess(candidate_id)) {
            continue;
        }

        // Compute distance using appropriate function based on context
        DistType cur_dist = computeDistance<quantized_query>(data_point, candidate_id);

        // Add to candidates if within bounds or we don't have enough results yet
        if (lowerBound > cur_dist || top_candidates.size() < ef) {
            // Always add to candidate set for exploration
            candidate_set.emplace(-cur_dist, candidate_id);

            // Only add non-deleted elements to results
            if (!isMarkedDeleted(candidate_id)) {
                top_candidates.emplace(cur_dist, candidate_id);
            }

            // Trim results to ef size
            if (top_candidates.size() > ef) {
                top_candidates.pop();
            }

            // Update lower bound if we have results
            if (!top_candidates.empty()) {
                lowerBound = top_candidates.top().first;
            }
        }
    }
}

/**
 * Beam search at a single level to find ef closest elements.
 *
 * This is the core search algorithm for HNSW at level 0.
 *
 * Algorithm:
 * 1. Initialize with entry point
 * 2. Maintain two heaps:
 *    - top_candidates (max-heap): Best ef results (farthest at top for easy trimming)
 *    - candidate_set (min-heap via negation): Nodes to explore (closest first)
 * 3. Process candidates until the closest unexplored is farther than farthest result
 *
 * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
 * @tparam running_query true if running a query (checks timeout, counts visited nodes)
 */
template <typename DataType, typename DistType>
template <bool quantized_query, bool running_query>
candidatesMaxHeap<DistType> HNSWDiskIndex<DataType, DistType>::searchLayer(idType ep_id, const void* data_point,
                                                                           levelType layer, size_t ef, void* timeoutCtx,
                                                                           VecSimQueryReply_Code* rc) const {
    // TODO: Consider using tag-based visited set for better performance
    vecsim_stl::unordered_set<idType> visited_set(std::min(ef * 10, size_t(10000)), this->allocator);

    candidatesMaxHeap<DistType> top_candidates(this->allocator);
    candidatesMaxHeap<DistType> candidate_set(this->allocator);

    DistType lowerBound;
    if (!isMarkedDeleted(ep_id)) {
        lowerBound = computeDistance<quantized_query>(data_point, ep_id);
        top_candidates.emplace(lowerBound, ep_id);
        candidate_set.emplace(-lowerBound, ep_id);
    } else {
        // Entry point is deleted - still explore from it but don't add to results
        lowerBound = std::numeric_limits<DistType>::max();
        candidate_set.emplace(-lowerBound, ep_id);
    }

    visited_set.insert(ep_id);

    // Main search loop
    while (!candidate_set.empty()) {
        // Check for timeout only when running a query
        if constexpr (running_query) {
            if (VECSIM_TIMEOUT(timeoutCtx)) {
                *rc = VecSim_QueryReply_TimedOut;
#ifdef BUILD_TESTS
                // Count visited nodes before returning
                numVisitedNodesBottomLayer_.fetch_add(visited_set.size(), std::memory_order_relaxed);
#endif
                return top_candidates;
            }
        }

        std::pair<DistType, idType> curr_el_pair = candidate_set.top();

        // Early termination: closest unexplored is farther than farthest result
        if ((-curr_el_pair.first) > lowerBound && top_candidates.size() >= ef) {
            break;
        }
        candidate_set.pop();

        // Process this candidate's neighbors
        processCandidate<quantized_query>(curr_el_pair.second, data_point, layer, ef, visited_set, top_candidates,
                                          candidate_set, lowerBound);
    }

#ifdef BUILD_TESTS
    // Count visited nodes for query statistics
    if constexpr (running_query) {
        numVisitedNodesBottomLayer_.fetch_add(visited_set.size(), std::memory_order_relaxed);
    }
#endif

    return top_candidates;
}

// =============================================================================
// Phase 2.2: Greedy Search Level Implementation
// =============================================================================

/**
 * Greedy search at a single level to find the closest element to the query vector.
 *
 * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
 * @tparam running_query true if running a query (checks timeout, allows deleted elements, counts visited)
 *
 * When running_query=true: Checks for timeout, can return deleted element, counts visited nodes
 * When running_query=false: No timeout check, must return non-deleted element
 *
 * Algorithm:
 * 1. Start from the current best element (currObj)
 * 2. Get all outgoing edges (neighbors) at this level from disk
 * 3. For each neighbor, compute distance and update best if closer
 * 4. Repeat until no improvement is found
 *
 * Performance notes:
 * - Disk reads for edges and vectors are the bottleneck
 * - Skips DISK_IN_PROCESS elements to avoid reading incomplete data
 * - Template allows compile-time optimization based on query type
 */
template <typename DataType, typename DistType>
template <bool quantized_query, bool running_query>
void HNSWDiskIndex<DataType, DistType>::greedySearchLevel(const void* query, levelType level, idType& currObj,
                                                          DistType& currDist, void* timeoutCtx,
                                                          VecSimQueryReply_Code* rc) const {
    bool changed;

    // Track best non-deleted candidate when running_query=false.
    // When running_query=false, we need a non-deleted entry point for the next level.
    // Initialize to INVALID_ID so we can detect if we found any non-deleted candidate.
    // If the initial currObj is not deleted, use it as the starting best candidate.
    idType bestNonDeletedCand = INVALID_ID;
    DistType bestNonDeletedDist = std::numeric_limits<DistType>::max();

    // Check if the starting point is non-deleted
    if constexpr (!running_query) {
        if (!isMarkedDeleted(currObj)) {
            bestNonDeletedCand = currObj;
            bestNonDeletedDist = currDist;
        }
    }

    // Track visited nodes to prevent infinite loops when graph has cycles
    // or when all reachable neighbors are deleted/in-process
    vecsim_stl::unordered_set<idType> visited(this->allocator);
    visited.insert(currObj);

    do {
        if constexpr (running_query) {
            if (VECSIM_TIMEOUT(timeoutCtx)) {
                *rc = VecSim_QueryReply_TimedOut;
                currObj = INVALID_ID;
                return;
            }
        }

        changed = false;

        // Get neighbors of current node at this level from disk
        vecsim_stl::vector<idType> neighbors = getNeighbors(currObj, level);

        // Check each neighbor
        for (idType candidateId : neighbors) {
            // Skip already visited nodes to prevent infinite loops
            if (visited.find(candidateId) != visited.end()) {
                continue;
            }
            visited.insert(candidateId);

            // Skip elements that are still being inserted (DISK_IN_PROCESS)
            // This avoids reading incomplete vector data
            if (isInProcess(candidateId)) {
                continue;
            }

            const DistType dist = computeDistance<quantized_query>(query, candidateId);

            // Track best non-deleted candidate only when running_query=false.
            // When running_query=true, deleted elements are OK as intermediate steps.
            if constexpr (!running_query) {
                if (!isMarkedDeleted(candidateId) && dist < bestNonDeletedDist) {
                    bestNonDeletedCand = candidateId;
                    bestNonDeletedDist = dist;
                }
            }

            if (dist >= currDist) {
                continue; // No improvement for traversal
            }

            currDist = dist;
            currObj = candidateId;
            changed = true;
        }
    } while (changed);

    // When running_query=false, return non-deleted element as entry point for next level.
    // When running_query=true, return whatever element we found (can be deleted).
    if constexpr (!running_query) {
        // If we found a non-deleted candidate, use it. Otherwise, fall back to
        // the best traversal result (which may be deleted). This is a best-effort
        // approach when all reachable nodes are deleted.
        if (bestNonDeletedCand != INVALID_ID) {
            currObj = bestNonDeletedCand;
            currDist = bestNonDeletedDist;
        }
        // If bestNonDeletedCand is INVALID_ID, we keep currObj/currDist as-is
        // (the best traversal result, even if deleted). The caller must handle this.
    }
#ifdef BUILD_TESTS
    // Count visited nodes for query statistics (upper layers only)
    if constexpr (running_query) {
        numVisitedNodesUpperLayers_.fetch_add(visited.size(), std::memory_order_relaxed);
    }
#endif
}

// =============================================================================
// Search Bottom Layer Entry Point
// =============================================================================

/**
 * Find the entry point for bottom layer search by traversing upper layers.
 *
 * This method starts from the global entry point and greedily searches through
 * upper layers (from maxLevel down to level 1) to find the best entry point
 * for the bottom layer (level 0) beam search.
 *
 * @tparam quantized_query true if query vector is quantized (SQ8), false if full-precision (FP32)
 *
 * @param query_data Full precision query vector (FP32)
 * @param timeoutCtx Timeout context for early termination
 * @param rc Result code output (VecSim_QueryReply_OK or VecSim_QueryReply_TimedOut)
 * @return Entry point ID for bottom layer search, or INVALID_ID if index is empty or timeout
 */
template <typename DataType, typename DistType>
template <bool quantized_query>
idType HNSWDiskIndex<DataType, DistType>::searchBottomLayerEP(const void* query_data, void* timeoutCtx,
                                                              VecSimQueryReply_Code* rc) const {
    *rc = VecSim_QueryReply_OK;

    // Get entry point and max level atomically (thread-safe)
    auto [currObj, maxLevel] = safeGetEntryPointState();
    if (currObj == INVALID_ID) {
        return INVALID_ID;
    }

    // Compute initial distance to entry point (quantized query vs quantized stored)
    DistType currDist = computeDistance<quantized_query>(query_data, currObj);

    // Greedy search through upper layers (from maxLevel down to level 1)
    // quantized_query=false (FP32 query), running_query=true (check timeout, allow deleted)
    for (levelType level = maxLevel; level > 0; --level) {
        greedySearchLevel<quantized_query, true>(query_data, level, currObj, currDist, timeoutCtx, rc);
        if (*rc != VecSim_QueryReply_OK) {
            return INVALID_ID;
        }
    }

    return currObj;
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::getNeighborsByHeuristic2(candidatesList<DistType>& top_candidates,
                                                                 const size_t M) const {
    if (top_candidates.size() < M) {
        return;
    }
    getNeighborsByHeuristic2_internal<false>(top_candidates, M, nullptr);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::getNeighborsByHeuristic2(candidatesList<DistType>& top_candidates,
                                                                 const size_t M,
                                                                 vecsim_stl::vector<idType>& removed_candidates) const {
    getNeighborsByHeuristic2_internal<true>(top_candidates, M, &removed_candidates);
}

template <typename DataType, typename DistType>
template <bool record_removed>
void HNSWDiskIndex<DataType, DistType>::getNeighborsByHeuristic2_internal(
    candidatesList<DistType>& top_candidates, const size_t M, vecsim_stl::vector<idType>* removed_candidates) const {
    if (top_candidates.size() < M) {
        return;
    }

    candidatesList<DistType> return_list(this->allocator);
    vecsim_stl::vector<const void*> cached_vectors(this->allocator);
    return_list.reserve(M);
    cached_vectors.reserve(M);
    if constexpr (record_removed) {
        removed_candidates->reserve(top_candidates.size());
    }

    // Sort the candidates by their distance (we don't mind the secondary order (the internal id))
    std::sort(top_candidates.begin(), top_candidates.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Acquire shared lock for the entire loop since we cache pointers from vectors container
    std::shared_lock<std::shared_mutex> vectorsLock(vectorsMutex_);

    auto current_pair = top_candidates.begin();
    for (; current_pair != top_candidates.end() && return_list.size() < M; ++current_pair) {
        DistType candidate_to_query_dist = current_pair->first;
        bool good = true;
        // Get SQ8 vector from in-memory container
        const void* curr_vector = this->vectors->getElement(current_pair->second);
        if (!curr_vector) {
            // Element not found in memory - skip this candidate
            if constexpr (record_removed) {
                removed_candidates->push_back(current_pair->second);
            }
            continue;
        }

        // A candidate is "good" to become a neighbour, unless we find
        // another item that was already selected to the neighbours set which is closer
        // to both q and the candidate than the distance between the candidate and q.
        // Uses SQ8↔SQ8 distance since both vectors are quantized.
        for (size_t i = 0; i < return_list.size(); i++) {
            DistType candidate_to_selected_dist = calcQuantizedDistance(cached_vectors[i], curr_vector);
            if (candidate_to_selected_dist < candidate_to_query_dist) {
                if constexpr (record_removed) {
                    removed_candidates->push_back(current_pair->second);
                }
                good = false;
                break;
            }
        }
        if (good) {
            cached_vectors.push_back(curr_vector);
            return_list.push_back(*current_pair);
        }
    }

    if constexpr (record_removed) {
        for (; current_pair != top_candidates.end(); ++current_pair) {
            removed_candidates->push_back(current_pair->second);
        }
    }

    top_candidates.swap(return_list);
}

template <typename DataType, typename DistType>
std::pair<LevelNeighborSelection, idType>
HNSWDiskIndex<DataType, DistType>::selectNeighborsForLevel(candidatesMaxHeap<DistType>&& top_candidates,
                                                           levelType level) const {
    // Maximum neighbors allowed at this level
    const size_t max_M_cur = level ? M_ : M0_;

    // Copy candidates to list for heuristic processing.
    candidatesList<DistType> top_candidates_list(this->allocator);
    top_candidates_list.insert(top_candidates_list.end(), top_candidates.begin(), top_candidates.end());

    // Use heuristic to filter candidates to at most max_M_cur neighbors.
    // The heuristic sorts by distance and selects diverse neighbors.
    getNeighborsByHeuristic2(top_candidates_list, max_M_cur);

    // After heuristic filtering, the closest selected neighbor is at the front.
    assert(!top_candidates_list.empty() && "top_candidates_list empty after heuristic - all candidates invalid");
    idType closestNeighbor = top_candidates_list.front().second;

    // Extract selected neighbor IDs
    vecsim_stl::vector<idType> selectedNeighbors(this->allocator);
    selectedNeighbors.reserve(top_candidates_list.size());
    for (const auto& [dist, id] : top_candidates_list) {
        selectedNeighbors.push_back(id);
    }

    return {LevelNeighborSelection(level, std::move(selectedNeighbors)), closestNeighbor};
}

template <typename DataType, typename DistType>
GraphNodeList HNSWDiskIndex<DataType, DistType>::connectNewElement(idType new_node_id,
                                                                   const LevelNeighborSelection& selection) const {
    GraphNodeList nodesToRepair(this->allocator);

    const levelType level = selection.level;
    const auto& new_node_neighbors = selection.selectedNeighbors;
    const size_t max_M_cur = level ? M_ : M0_;

    // Write new node's outgoing edges
    setNeighbors(new_node_id, level, new_node_neighbors);

    // For each neighbor that new_node points to, update that neighbor's incoming edges
    // to reflect that new_node now points to it. Use batch for efficiency.
    vecsim_stl::vector<std::tuple<idType, levelType, idType, EdgeOperation>> incoming_ops(this->allocator);
    incoming_ops.reserve(new_node_neighbors.size());
    for (idType neighbor_id : new_node_neighbors) {
        incoming_ops.emplace_back(neighbor_id, level, new_node_id, EdgeOperation::Append);
    }
    storage_->batch_merge_incoming_edges(incoming_ops);

    // Set new_node's incoming edges upfront - all neighbors will add back-edges to us
    storage_->put_incoming_edges(new_node_id, level, new_node_neighbors);

    // For each selected neighbor, add back-edge to new_node
    for (idType selected_neighbor : new_node_neighbors) {
        // Only lock the neighbor - new_node_id doesn't need locking because:
        // 1. It's marked DISK_IN_PROCESS, so other threads skip it in searchLayer/processCandidate
        // 2. No concurrent insertion will select it as a neighbor
        // 3. Only this thread accesses new_node's edges during insertion
        auto neighbor_lock = lockNode(selected_neighbor);

        // Read current neighbors from storage
        vecsim_stl::vector<idType> neighbor_neighbors = getNeighbors(selected_neighbor, level);

        // Add new_node_id to neighbor's outgoing edges
        neighbor_neighbors.push_back(new_node_id);
        setNeighbors(selected_neighbor, level, neighbor_neighbors);

        // If neighbor overflows, mark it as needing repair
        if (neighbor_neighbors.size() > max_M_cur) {
            nodesToRepair.emplace_back(selected_neighbor, level);
        }
        // Lock released automatically when going out of scope
    }

    return nodesToRepair;
}

// =============================================================================
// VecSimIndexInterface Stubs
// =============================================================================

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcFullDistance(const void* full1, const void* full2) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::Full>(full1, full2, this->dim);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcQuantizedVsFullDistance(const void* quantized,
                                                                        const void* full) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::QuantizedVsFull>(quantized, full, this->dim);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcQuantizedDistance(const void* quantized1,
                                                                  const void* quantized2) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::Quantized>(quantized1, quantized2, this->dim);
}

// =============================================================================
// Vector Addition Implementation
// =============================================================================

template <typename DataType, typename DistType>
vecsim_stl::vector<LevelNeighborSelection>
HNSWDiskIndex<DataType, DistType>::searchGraphForNeighbors(const void* quantizedData, levelType elementMaxLevel,
                                                           idType entryPoint, levelType globalMaxLevel) const {
    idType curr_element = entryPoint;

    // Compute initial distance to entry point using quantized↔quantized
    DistType cur_dist = computeDistanceQuantized_Quantized(quantizedData, curr_element);

    // Determine the level at which we start connecting (min of element's level and global max)
    levelType max_common_level;
    if (elementMaxLevel < globalMaxLevel) {
        max_common_level = elementMaxLevel;

        // Greedy search through upper levels to find best entry point for lower levels.
        // quantized_query=true, running_query=false (no timeout, need non-deleted)
        // We use int for the loop variable to safely decrement past zero.
        for (int level = static_cast<int>(globalMaxLevel); level > static_cast<int>(elementMaxLevel); --level) {
            greedySearchLevel<true, false>(quantizedData, static_cast<levelType>(level), curr_element, cur_dist);
        }
    } else {
        max_common_level = globalMaxLevel;
    }

    // Search and select neighbors for each level
    vecsim_stl::vector<LevelNeighborSelection> levelSelections(this->allocator);
    levelSelections.reserve(static_cast<size_t>(max_common_level) + 1);

    for (int level = static_cast<int>(max_common_level); level >= 0; --level) {
        // Search this level to find best neighbors
        // quantized_query=true, running_query=false (no timeout)
        auto top_candidates =
            searchLayer<true, false>(curr_element, quantizedData, static_cast<levelType>(level), efConstruction_);

        // If the entry point was marked deleted between iterations, we may receive an empty
        // candidates set. In this case, we keep using the previous curr_element for the next level.
        // This matches the RAM HNSW behavior and is intentional.
        if (!top_candidates.empty()) {
            auto [selection, closestNeighbor] =
                selectNeighborsForLevel(std::move(top_candidates), static_cast<levelType>(level));
            // Use closest neighbor as entry point for next level search
            curr_element = closestNeighbor;
            levelSelections.push_back(std::move(selection));
        }
    }

    return levelSelections;
}

template <typename DataType, typename DistType>
IndexVectorResult HNSWDiskIndex<DataType, DistType>::indexVector(const void* quantizedData) const {
    IndexVectorResult result(this->allocator);

    // Generate random level internally (thread-safe)
    result.elementMaxLevel = getRandomLevel();

    // Capture entry point state at search time (read-only snapshot)
    auto epState = safeGetEntryPointState();
    result.entryPointAtSearchTime = epState.id;
    result.maxLevelAtSearchTime = epState.level;
    result.indexWasEmpty = (result.entryPointAtSearchTime == INVALID_ID);

    // If index is not empty, perform graph search to find best neighbors
    if (!result.indexWasEmpty) {
        result.levelSelections = searchGraphForNeighbors(quantizedData, result.elementMaxLevel,
                                                         result.entryPointAtSearchTime, result.maxLevelAtSearchTime);
    }

    return result;
}

template <typename DataType, typename DistType>
idType HNSWDiskIndex<DataType, DistType>::allocateAndRegister(labelType label, levelType maxLevel,
                                                              const void* storageSQ8) {
    // Step 1: Allocate ID and store SQ8 atomically under vectorsMutex_
    idType internalId = allocateIdAndStoreVector(storageSQ8);

    // Step 2: Initialize metadata (marked as IN_PROCESS by default)
    initElementMetadata(internalId, label, maxLevel);

    // Step 3: Increment element count
    curElementCount_.fetch_add(1, std::memory_order_relaxed);

    // Step 4: Register label-to-ID mapping
    {
        std::unique_lock<std::shared_mutex> lock(labelLookupMutex_);
        labelToIdLookup_[label] = internalId;
    }

    return internalId;
}

template <typename DataType, typename DistType>
GraphNodeList HNSWDiskIndex<DataType, DistType>::storeVectorConnections(idType internalId, const void* fullData,
                                                                        const void* quantizedData,
                                                                        IndexVectorResult&& indexResult) {
    // Step 1: Write full-precision data to disk (always happens regardless of graph state)
    storage_->put_vector(internalId, fullData, this->getInputBlobSize());

    // Step 2: Resolve entry point - handles concurrent empty-index inserts
    auto resolution = resolveEntryPoint(internalId, indexResult.elementMaxLevel, indexResult.entryPointAtSearchTime,
                                        indexResult.maxLevelAtSearchTime);

    // Step 3: Connect to graph
    GraphNodeList nodesToRepair = connectToGraph(internalId, indexResult, quantizedData, resolution);

    // Step 4: Update entry point if this element has a higher level
    replaceEntryPoint(internalId, indexResult.elementMaxLevel);

    // Step 5: Unmark the element as IN_PROCESS - it's now fully indexed and searchable
    unmarkAs<DISK_IN_PROCESS>(internalId);

    return nodesToRepair;
}

template <typename DataType, typename DistType>
GraphNodeList HNSWDiskIndex<DataType, DistType>::connectToGraph(idType internalId, const IndexVectorResult& indexResult,
                                                                const void* quantizedData,
                                                                const EntryPointResolution& resolution) const {
    GraphNodeList nodesToRepair(this->allocator);

    // Case 1: We are the first element (became entry point) - no connections needed
    if (resolution.becameEntryPoint) {
        return nodesToRepair;
    }

    // Invariant: indexVector() never populates levelSelections when index is empty
    assert((!indexResult.indexWasEmpty || indexResult.levelSelections.empty()) &&
           "indexWasEmpty should imply levelSelections is empty");

    // Case 2: Index was empty at search time, but another thread initialized entry point
    // We need to do full search now since levelSelections is empty
    if (indexResult.indexWasEmpty) {
        return insertElementToGraph(internalId, indexResult.elementMaxLevel, resolution.entryPointToUse,
                                    resolution.maxLevelToUse, quantizedData);
    }

    // Case 3: Normal case - use pre-computed selections from indexVector()
    for (const auto& selection : indexResult.levelSelections) {
        GraphNodeList levelRepairs = connectNewElement(internalId, selection);
        nodesToRepair.insert(nodesToRepair.end(), levelRepairs.begin(), levelRepairs.end());
    }
    return nodesToRepair;
}

template <typename DataType, typename DistType>
GraphNodeList HNSWDiskIndex<DataType, DistType>::insertElementToGraph(idType element_id, levelType element_max_level,
                                                                      idType entry_point, levelType global_max_level,
                                                                      const void* quantizedData) const {
    // Search phase: use shared helper to find neighbors at each level
    vecsim_stl::vector<LevelNeighborSelection> levelSelections =
        searchGraphForNeighbors(quantizedData, element_max_level, entry_point, global_max_level);

    // Write phase: connect the element using pre-computed selections
    // Caller is responsible for holding consistency lock during this phase.
    GraphNodeList allNodesToRepair(this->allocator);
    for (const auto& selection : levelSelections) {
        GraphNodeList nodesToRepair = connectNewElement(element_id, selection);
        allNodesToRepair.insert(allNodesToRepair.end(), nodesToRepair.begin(), nodesToRepair.end());
    }

    return allNodesToRepair;
}

template <typename DataType, typename DistType>
int HNSWDiskIndex<DataType, DistType>::addVector(const void* blob, labelType label) {
    // Main entry point for adding a vector.
    // Used for tests and VecSimInterface compliance.
    // Uses the same tiered pattern as production: indexVector() + allocateAndRegister() + storeVectorConnections()
    // but with atomic label check/registration for single-index use.

    // Step 1: Preprocess vector outside critical section for better concurrency
    // For metrics like cosine, query and storage blobs differ - use the correct one for each operation.
    ProcessedBlobs processedBlobs = this->preprocess(blob);
    const void* quantizedQuery = processedBlobs.getQueryBlob();
    const void* quantizedStorage = processedBlobs.getStorageBlob();

    // Step 2: Read-only search phase - captures entry point and pre-computes neighbor selections
    IndexVectorResult indexResult = indexVector(quantizedQuery);

    // Step 3: Atomic label check + allocation + registration
    // We must hold the lock from check through allocation + label registration to prevent
    // multiple threads from inserting the same label concurrently.
    idType internalId;
    bool label_exists = false;
    {
        std::unique_lock<std::shared_mutex> labelLock(labelLookupMutex_);
        label_exists = (labelToIdLookup_.find(label) != labelToIdLookup_.end());
        if (label_exists) {
            // Per VecSimInterface contract: overwrite = delete old + insert new
            // TODO(MOD-13164): deleteVector is currently a stub.
            deleteVector(label);
        }

        // Allocate ID and store quantized data atomically
        internalId = allocateIdAndStoreVector(quantizedStorage);
        initElementMetadata(internalId, label, indexResult.elementMaxLevel);
        curElementCount_.fetch_add(1, std::memory_order_relaxed);

        // Register the label-to-ID mapping atomically with the existence check
        labelToIdLookup_[label] = internalId;
    } // Release labelLock after label is registered

    // Step 4: Acquire mock consistency lock for test/standalone use
    // In production, this is a real fork-safety lock acquired by the tiered layer.
    vecsim_disk::ConsistencySharedGuard consistency_guard;

    // Step 5: Store full-precision data to disk and connect to graph (same as tiered pattern)
    GraphNodeList nodesToRepair = storeVectorConnections(internalId, blob, quantizedQuery, std::move(indexResult));

    // Step 6: Process repair jobs synchronously
    for (const auto& [nodeId, level] : nodesToRepair) {
        repairNode(nodeId, level);
    }

    // Return 1 for new insert, 0 for overwrite
    return label_exists ? 0 : 1;
}

template <typename DataType, typename DistType>
int HNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    (void)label;
    // TODO(MOD-13164): Implement vector deletion
    // After successful deletion: curElementCount_--;
    // Also add the deleted ID to holes_ for recycling
    return 0;
}

template <typename DataType, typename DistType>
double HNSWDiskIndex<DataType, DistType>::getDistanceFrom_Unsafe(labelType label, const void* blob) const {
    (void)label;
    (void)blob;
    return 0.0; // Stub - MOD-13164
}

template <typename DataType, typename DistType>
VecSimQueryReply* HNSWDiskIndex<DataType, DistType>::topKQuery(const void* queryBlob, size_t k,
                                                               VecSimQueryParams* queryParams) const {
    auto rep = new VecSimQueryReply(this->allocator);
    this->lastMode = STANDARD_KNN;

    // Early exit for empty index or zero k
    if (curElementCount_.load(std::memory_order_relaxed) == 0 || k == 0) {
        return rep;
    }

    // Get runtime parameters
    void* timeoutCtx = queryParams ? queryParams->timeoutCtx : nullptr;
    size_t query_ef = efRuntime_;
    if (queryParams && queryParams->hnswDiskRuntimeParams.efRuntime != 0) {
        query_ef = queryParams->hnswDiskRuntimeParams.efRuntime;
    }
    bool should_rerank = rerankEnabled_;
    if (queryParams && queryParams->hnswDiskRuntimeParams.shouldRerank != VecSimBool_UNSET) {
        should_rerank = queryParams->hnswDiskRuntimeParams.shouldRerank == VecSimBool_TRUE;
    }
    // Preprocess query - get both quantized (for graph traversal) and full-precision (for reranking)
    auto processed = this->preprocess(queryBlob);
    const void* quantized_query = processed.getStorageBlob(); // SQ8 for graph traversal

    // Find entry point for bottom layer search by traversing upper layers
    // quantized_query=true (SQ8 query), running_query=true (check timeout)
    idType bottom_layer_ep = searchBottomLayerEP<true>(quantized_query, timeoutCtx, &rep->code);
    if (rep->code != VecSim_QueryReply_OK || bottom_layer_ep == INVALID_ID) {
        return rep;
    }

    // Search bottom layer with ef parameter
    // quantized_query=true (SQ8 query), running_query=true (check timeout)
    auto top_candidates =
        searchLayer<true, true>(bottom_layer_ep, quantized_query, 0, std::max(query_ef, k), timeoutCtx, &rep->code);
    if (rep->code != VecSim_QueryReply_OK) {
        return rep;
    }

    // Get top k results as sorted vector (ascending distance order)
    vecsim_stl::vector<std::pair<DistType, idType>> top_k(this->allocator);
    size_t result_count = std::min(k, top_candidates.size());

    if (should_rerank) {
        // Rerank: recompute distances using full-precision vectors from disk, then partial sort
        top_k = rerankCandidates(queryBlob, top_candidates, timeoutCtx, &rep->code);
        if (rep->code != VecSim_QueryReply_OK) {
            return rep;
        }
        std::partial_sort(top_k.begin(), top_k.begin() + result_count, top_k.end(),
                          [](const auto& a, const auto& b) { return a.first < b.first; });
    } else {
        // No reranking: extract top k from heap (already sorted by heap property)
        top_k.resize(result_count);
        while (top_candidates.size() > result_count) {
            top_candidates.pop();
        }
        // Fill in reverse (heap pops worst first, we want best first)
        for (size_t i = result_count; i > 0; --i) {
            top_k[i - 1] = top_candidates.top();
            top_candidates.pop();
        }
    }

    // Fill results from sorted vector
    rep->results.resize(result_count);
    {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        for (size_t i = 0; i < result_count; ++i) {
            rep->results[i].score = top_k[i].first;
            rep->results[i].id = idToMetaData[top_k[i].second].label;
        }
    }

    return rep;
}

template <typename DataType, typename DistType>
VecSimQueryReply* HNSWDiskIndex<DataType, DistType>::rangeQuery(const void* queryBlob, double radius,
                                                                VecSimQueryParams* queryParams) const {
    (void)queryBlob;
    (void)radius;
    (void)queryParams;
    return new VecSimQueryReply(this->allocator);
}

template <typename DataType, typename DistType>
vecsim_stl::vector<std::pair<DistType, idType>>
HNSWDiskIndex<DataType, DistType>::rerankCandidates(const void* queryBlob,
                                                    const candidatesMaxHeap<DistType>& candidates, void* timeoutCtx,
                                                    VecSimQueryReply_Code* rc) const {
    const size_t num_candidates = candidates.size();
    if (num_candidates == 0) {
        return vecsim_stl::vector<std::pair<DistType, idType>>(this->allocator);
    }

    // Collect all candidate IDs by iterating (not popping) the heap
    vecsim_stl::vector<idType> ids(this->allocator);
    ids.reserve(num_candidates);
    for (const auto& [dist, id] : candidates) {
        ids.push_back(id);
    }

    // Allocate a single contiguous buffer for all vectors
    const size_t vectorSize = this->getInputBlobSize();
    vecsim_stl::vector<char> buffer(num_candidates * vectorSize, this->allocator);

    // Build pointer array pointing into the contiguous buffer
    vecsim_stl::vector<void*> buffer_ptrs(this->allocator);
    buffer_ptrs.reserve(num_candidates);
    for (size_t i = 0; i < num_candidates; ++i) {
        buffer_ptrs.push_back(buffer.data() + i * vectorSize);
    }

    // Fetch all vectors from disk in one batch
    storage_->get_vectors_multi(ids, buffer_ptrs, vectorSize);

    // Compute distances into a vector of (distance, id) pairs
    vecsim_stl::vector<std::pair<DistType, idType>> reranked(this->allocator);
    reranked.reserve(num_candidates);
    for (size_t i = 0; i < num_candidates; ++i) {
        if (VECSIM_TIMEOUT(timeoutCtx)) {
            *rc = VecSim_QueryReply_TimedOut;
            return reranked;
        }
        DistType dist = calcFullDistance(buffer_ptrs[i], queryBlob);
        reranked.emplace_back(dist, ids[i]);
    }

    return reranked;
}

// =============================================================================
// Delete Flow Implementation
// =============================================================================

template <typename DataType, typename DistType>
vecsim_stl::vector<idType> HNSWDiskIndex<DataType, DistType>::markDelete(labelType label) {
    vecsim_stl::vector<idType> deleted_ids(this->allocator);

    // Atomically remove from label map and get the ID
    // This prevents TOCTOU race where two threads both try to delete the same label
    idType internalId;
    {
        std::unique_lock<std::shared_mutex> lock(labelLookupMutex_);
        auto it = labelToIdLookup_.find(label);
        if (it == labelToIdLookup_.end()) {
            return deleted_ids; // Label not found (or already deleted)
        }
        internalId = it->second;
        labelToIdLookup_.erase(it);
    }

    // Mark deleted - entry point replacement is done in executeDeleteInitJob() (worker thread)
    markAs<DISK_DELETE_MARK>(internalId);

    deleted_ids.push_back(internalId);
    return deleted_ids;
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::replaceEntryPointOnDelete() {
    // Locks: metadataMutex_ (shared) → entryPointMutex_ (exclusive)
    std::shared_lock<std::shared_mutex> metaLock(metadataMutex_);
    std::unique_lock<std::shared_mutex> epLock(entryPointMutex_);

    if (entryPoint_ == INVALID_ID) {
        return;
    }

    idType oldEp = entryPoint_;

    while (maxLevel_ != std::numeric_limits<levelType>::max()) {
        levelType currentLevel = maxLevel_;

        // Try neighbors at current level first
        vecsim_stl::vector<idType> neighbors(this->allocator);
        assert(storage_);
        storage_->get_outgoing_edges(oldEp, currentLevel, neighbors);

        for (idType neighbor : neighbors) {
            if (!isMarkedAsUnsafe<DISK_DELETE_MARK>(neighbor) && !isMarkedAsUnsafe<DISK_IN_PROCESS>(neighbor)) {
                entryPoint_ = neighbor;
                return;
            }
        }

        // No valid neighbor - scan all elements at this level
        for (idType id = 0; id < idToMetaData.size(); ++id) {
            if (id != oldEp && idToMetaData[id].maxLevel >= currentLevel && !isMarkedAsUnsafe<DISK_DELETE_MARK>(id) &&
                !isMarkedAsUnsafe<DISK_IN_PROCESS>(id)) {
                entryPoint_ = id;
                return;
            }
        }

        // Step 3: No element at this level - decrease maxLevel and try next level
        if (currentLevel > 0) {
            maxLevel_--;
        } else {
            break;
        }
    }

    // No valid element found at any level - index is empty
    entryPoint_ = INVALID_ID;
    maxLevel_ = std::numeric_limits<levelType>::max();
}

// =============================================================================
// repairNode helpers
// =============================================================================
template <class DataType, class DistType>
vecsim_stl::vector<idType> HNSWDiskIndex<DataType, DistType>::collectRepairCandidates(
    idType nodeId, const vecsim_stl::vector<idType>& outgoingEdges, levelType level,
    vecsim_stl::vector<idType>& deletedNeighbors) const {

    vecsim_stl::unordered_set<idType> visited(this->allocator);
    vecsim_stl::vector<idType> candidateIds(this->allocator);

    // Initialize output parameter
    deletedNeighbors.clear();

    // Add nodeId to visited set upfront to simplify the inner loop check
    visited.insert(nodeId);

    for (const idType neighborId : outgoingEdges) {
        if (!isMarkedDeleted(neighborId)) {
            // Non-deleted neighbor: add directly as candidate
            if (visited.find(neighborId) != visited.end()) {
                continue;
            }
            visited.insert(neighborId);
            candidateIds.push_back(neighborId);
        } else {
            // Deleted neighbor: collect it and explore its neighbors to maintain connectivity
            deletedNeighbors.push_back(neighborId);
            vecsim_stl::vector<idType> neighborsOfDeleted = getNeighbors(neighborId, level);

            for (const idType candidateId : neighborsOfDeleted) {
                if (visited.find(candidateId) != visited.end()) {
                    continue;
                }
                visited.insert(candidateId);

                // Only add non-deleted candidates
                if (!isMarkedDeleted(candidateId)) {
                    candidateIds.push_back(candidateId);
                }
            }
        }
    }

    return candidateIds;
}

template <class DataType, class DistType>
void HNSWDiskIndex<DataType, DistType>::updateIncomingEdgesAfterRepair(
    idType nodeId, const vecsim_stl::unordered_set<idType>& originalEdgesSet,
    const vecsim_stl::vector<idType>& newNeighbors, const vecsim_stl::vector<idType>& removedCandidates,
    const vecsim_stl::vector<idType>& deletedNeighbors, levelType level) const {
    // Per design:
    //   let removed_neighbors = neighbors - updated_neighbors;
    //   let new_neighbors = updated_neighbors - neighbors;

    vecsim_stl::vector<std::tuple<idType, levelType, idType, EdgeOperation>> operations_to_batch(this->allocator);

    // Per design: for (neighbor in removed_neighbors) { append_to_incoming(neighbor, level, -id); }
    // removedCandidates contains candidates rejected by heuristic. Filter to only those
    // that were in the original edges (not new candidates from exploring deleted neighbors).
    for (idType id : removedCandidates) {
        if (originalEdgesSet.find(id) != originalEdgesSet.end()) {
            operations_to_batch.emplace_back(id, level, nodeId, EdgeOperation::Delete);
        }
    }

    // Also remove nodeId from incoming edges of deleted neighbors.
    // This prevents redundant repairs: when the delete job for a deleted neighbor runs,
    // it won't find nodeId in its incoming edges and won't schedule another repair for nodeId.
    for (idType id : deletedNeighbors) {
        operations_to_batch.emplace_back(id, level, nodeId, EdgeOperation::Delete);
    }

    // Per design: for (neighbor in new_neighbors) { append_to_incoming(neighbor, level, id); }
    // For each neighbor that was in the new list but is NOT in the original list
    // we need to add the incoming edge from that neighbor to nodeId
    for (idType id : newNeighbors) {
        if (originalEdgesSet.find(id) == originalEdgesSet.end()) {
            operations_to_batch.emplace_back(id, level, nodeId, EdgeOperation::Append);
        }
    }

    if (!operations_to_batch.empty()) {
        storage_->batch_merge_incoming_edges(operations_to_batch);
    }
}

template <class DataType, class DistType>
void HNSWDiskIndex<DataType, DistType>::repairNode(idType id, levelType level) const {
    // Per design: if (invalid_job() || is_deleted(id)) { cleanup and return; }
    // Note: invalid_job() check would be in the async job wrapper, not here
    if (isMarkedDeleted(id)) {
        return;
    }

    // Per design: lock(id); // "write" lock - readers don't take it
    auto lock = lockNode(id);

    // Per design: let neighbors = get_outgoing_edges(id, level);
    vecsim_stl::vector<idType> outgoingEdges = getNeighbors(id, level);

    size_t maxConnections = (level == 0) ? M0_ : M_;

    // Collect candidates, filtering out deleted neighbors and exploring their connections
    vecsim_stl::vector<idType> deletedNeighbors(this->allocator);
    vecsim_stl::vector<idType> candidateIds = collectRepairCandidates(id, outgoingEdges, level, deletedNeighbors);

    // Early exit: if no overflow and no deleted neighbors were filtered out, no repair needed.
    // This is important, as some nodes may be marked for repair due to bad timing but may not
    // actually need repair when we get here.
    if (outgoingEdges.size() <= maxConnections && deletedNeighbors.empty()) {
        return;
    }

    // Build set from original edges for O(1) lookup in updateIncomingEdgesAfterRepair
    vecsim_stl::unordered_set<idType> originalEdgesSet(this->allocator);
    originalEdgesSet.reserve(outgoingEdges.size());
    for (idType edgeId : outgoingEdges) {
        originalEdgesSet.insert(edgeId);
    }

    vecsim_stl::vector<idType> newNeighbors(this->allocator);
    vecsim_stl::vector<idType> removedCandidates(this->allocator);

    // If we have <= maxConnections candidates, keep all of them (no heuristic needed)
    if (candidateIds.size() <= maxConnections) {
        newNeighbors = std::move(candidateIds);
    } else {
        // Need to prune candidates, so compute actual distances and run heuristic
        // Hold vectorsMutex_ for this section to prevent use-after-free.
        // getQuantizedDataByInternalId() returns raw pointers into the vectors container,
        // which can become invalid if another thread calls addElement() and triggers a resize.
        candidatesList<DistType> candidates(this->allocator);
        {
            std::shared_lock<std::shared_mutex> vectorsLock(vectorsMutex_);
            const char* nodeVector = getQuantizedDataByInternalId(id);
            candidates.reserve(candidateIds.size());
            for (const idType candidateId : candidateIds) {
                const char* candidateVector = getQuantizedDataByInternalId(candidateId);
                DistType dist = calcQuantizedDistance(nodeVector, candidateVector);
                candidates.emplace_back(dist, candidateId);
            }
        }

        // Use heuristic to select neighbors, tracking removed candidates
        getNeighborsByHeuristic2(candidates, maxConnections, removedCandidates);

        newNeighbors.reserve(candidates.size());
        for (const auto& pair : candidates) {
            newNeighbors.push_back(pair.second);
        }
    }

    setNeighbors(id, level, newNeighbors);

    updateIncomingEdgesAfterRepair(id, originalEdgesSet, newNeighbors, removedCandidates, deletedNeighbors, level);
}

template <typename DataType, typename DistType>
VecSimIndexBasicInfo HNSWDiskIndex<DataType, DistType>::basicInfo() const {
    VecSimIndexBasicInfo info = this->getBasicInfo();
    info.algo = VecSimAlgo_HNSWLIB;
    info.isTiered = false;
    return info;
}

template <typename DataType, typename DistType>
VecSimIndexDebugInfo HNSWDiskIndex<DataType, DistType>::debugInfo() const {
    VecSimIndexDebugInfo info = {};
    info.commonInfo = this->getCommonInfo();
    info.commonInfo.basicInfo.algo = VecSimAlgo_HNSWLIB;
    info.commonInfo.basicInfo.isTiered = false;
    info.hnswInfo.M = M_;
    info.hnswInfo.efConstruction = efConstruction_;
    info.hnswInfo.efRuntime = efRuntime_;
    return info;
}

template <typename DataType, typename DistType>
VecSimDebugInfoIterator* HNSWDiskIndex<DataType, DistType>::debugInfoIterator() const {
    VecSimIndexDebugInfo info = this->debugInfo();
    // Match standard HNSW field count: 1 (algo) + 9 (common) + 1 (block_size) + 7 (hnsw params) = 18
    size_t numberOfInfoFields = 18;
    auto* infoIterator = new VecSimDebugInfoIterator(numberOfInfoFields, this->allocator);

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::ALGORITHM_STRING,
        .fieldType = INFOFIELD_STRING,
        .fieldValue = {FieldValue{.stringValue = VecSimAlgo_ToString(info.commonInfo.basicInfo.algo)}}});

    this->addCommonInfoToIterator(infoIterator, info.commonInfo);

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::BLOCK_SIZE_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.commonInfo.basicInfo.blockSize}}});

    infoIterator->addInfoField(VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_M_STRING,
                                                .fieldType = INFOFIELD_UINT64,
                                                .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.M}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_EF_CONSTRUCTION_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.efConstruction}}});

    infoIterator->addInfoField(VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_EF_RUNTIME_STRING,
                                                .fieldType = INFOFIELD_UINT64,
                                                .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.efRuntime}}});

    infoIterator->addInfoField(VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_MAX_LEVEL,
                                                .fieldType = INFOFIELD_UINT64,
                                                .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.max_level}}});

    infoIterator->addInfoField(VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_ENTRYPOINT,
                                                .fieldType = INFOFIELD_UINT64,
                                                .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.entrypoint}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::EPSILON_STRING,
                         .fieldType = INFOFIELD_FLOAT64,
                         .fieldValue = {FieldValue{.floatingPointValue = info.hnswInfo.epsilon}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::NUM_MARKED_DELETED,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.numberOfMarkedDeletedNodes}}});

    return infoIterator;
}

template <typename DataType, typename DistType>
const char* HNSWDiskIndex<DataType, DistType>::getQuantizedDataByInternalId(idType internal_id) const {
    // Note: This returns a pointer that may become invalid if addElement() resizes the container.
    // Caller MUST hold vectorsMutex_ (shared or exclusive) before calling this function
    // and keep it held while using the returned pointer.
    return this->vectors->getElement(internal_id);
}
