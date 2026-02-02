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
#include "VecSim/utils/vec_utils.h"
#include "VecSim/info_iterator_struct.h"
#include "VecSim/spaces/L2_space.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/types/sq8.h"
#include "VecSim/tombstone_interface.h"
#include "storage/hnsw_storage.h"
#include "VecSim/utils/updatable_heap.h"
#include "VecSim/algorithms/hnsw/graph_data.h"
#include "VecSim/algorithms/hnsw/hnsw.h"
#include "factory/components/disk_components_factory.h"
#include "utils/vecsim_stl_ext.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <algorithm>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

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
using GraphNodeType = std::pair<idType, levelType>; // represented as: (element_id, level)
using GraphNodeList = vecsim_stl::vector<GraphNodeType>;

using elementFlags = uint8_t;

// Element flags for metadata (used with atomic operations)
// Redefined the hnsw.h Flags to allow more flags in the future.
enum ElementFlags : elementFlags {
    DISK_DELETE_MARK = 0x01, // Element is logically deleted but still in graph
    DISK_IN_PROCESS = 0x02,  // Element is being inserted into the graph
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
    size_t indexSize() const override { return curElementCount_; }
    size_t indexCapacity() const override { return maxElements_.load(std::memory_order_relaxed); }
    size_t indexLabelCount() const override { return curElementCount_; }

    /**
     * @brief Check if current capacity is full.
     *
     * @return true if nextId_ >= maxElements_ and holes is empty
     */
    bool isCapacityFull() const;

    VecSimQueryReply* topKQuery(const void* queryBlob, size_t k, VecSimQueryParams* queryParams) const override;
    VecSimQueryReply* rangeQuery(const void* queryBlob, double radius, VecSimQueryParams* queryParams) const override;

    VecSimIndexBasicInfo basicInfo() const override;
    VecSimIndexDebugInfo debugInfo() const override;
    VecSimDebugInfoIterator* debugInfoIterator() const override;

    VecSimBatchIterator* newBatchIterator(const void*, VecSimQueryParams*) const override { return nullptr; }
    bool preferAdHocSearch(size_t, size_t, bool) const override { return true; }

    vecsim_stl::set<labelType> getLabelsSet() const override { return vecsim_stl::set<labelType>(this->allocator); }

#ifdef BUILD_TESTS
    void fitMemory() override {}
    size_t indexMetaDataCapacity() const override { return maxElements_.load(std::memory_order_relaxed); }
    void getDataByLabel(labelType, std::vector<std::vector<DataType>>&) const override {}
    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType) const override { return {}; }
#endif

    size_t getEf() const { return efRuntime_; }
    void setEf(size_t ef) { efRuntime_ = ef; }

    HNSWStorage<DataType>* getStorage() const { return storage_.get(); }

    const char* getQuantizedDataByInternalId(idType internal_id) const;

    // =========================================================================
    // Test Accessors - Public accessors for testing
    // =========================================================================
    // Test methods are defined in hnsw_disk_test_helpers.h via macro
    // This keeps the main header clean while providing full test access
    HNSW_DISK_TEST_METHODS

    // TODO: Make thread safe (the the right locking mechanism)
    vecsim_stl::vector<idType> markDelete(labelType label) override {
        // Stub - returns dummy ID for testing purposes
        // TODO: Get internal ID by label, mark as deleted, return deleted IDs
        vecsim_stl::vector<idType> deleted_ids(this->allocator);
        deleted_ids.push_back(0); // Dummy ID - real implementation will look up by label
        return deleted_ids;
    }

protected:
    // =========================================================================
    // Protected Flagging API
    // =========================================================================
    // Uses shared lock to protect against concurrent resize
    template <ElementFlags FLAG>
    void markAs(idType internalId) {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        __atomic_fetch_or(&idToMetaData[internalId].flags, FLAG, 0);
    }
    template <ElementFlags FLAG>
    void unmarkAs(idType internalId) {
        std::shared_lock<std::shared_mutex> lock(metadataMutex_);
        __atomic_fetch_and(&idToMetaData[internalId].flags, ~FLAG, 0);
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
        return __atomic_load_n(&idToMetaData[internalId].flags, 0) & FLAG;
    }
    // =========================================================================
    // Member Variables
    // =========================================================================

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
    // Shared lock for reads (isMarkedAs, lockNode), exclusive lock for writes (growByBlock, initElementMetadata).
    mutable std::shared_mutex metadataMutex_;

    // --- Storage ---
    std::unique_ptr<HNSWStorage<DataType>> storage_;

    // --- Level Generation ---
    double mult_; // = 1 / ln(M), for level generation

    // --- ID Allocation ---
    // nextId_: Next fresh ID to allocate (monotonically increasing).
    //          Only used when holes_ is empty. After deletions, nextId_ > curElementCount_.
    std::atomic<idType> nextId_{0};
    mutable std::mutex holesMutex_; // Protects holes_
    // holes_: Recycled IDs from deleted elements. Populated in deleteVector().
    vecsim_stl::vector<idType> holes_;

    // --- Entry Point State ---
    mutable std::shared_mutex entryPointMutex_; // Protects entryPoint_ and maxLevel_
    idType entryPoint_{INVALID_ID};             // Current entry point ID
    size_t maxLevel_{HNSW_INVALID_LEVEL};       // Maximum level in the graph

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
    size_t getRandomLevel();

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
    idType allocateId();

    /**
     * @brief Return an ID to the pool for reuse.
     *
     * Called after an element is fully deleted and its storage is cleaned up.
     * Thread-safe.
     *
     * @param id The ID to recycle
     */
    void recycleId(idType id);

    // --- Capacity Management ---
    /**
     * @brief Grow both idToMetaData and nodeLocks_ by blockSize.
     *
     * Called when capacity is full before allocating a new ID.
     */
    void growByBlock();

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
     * @return Pair of (entryPoint, maxLevel)
     */
    std::pair<idType, size_t> safeGetEntryPointState() const;

    /**
     * @brief Attempt to update entry point if new element has higher level.
     *
     * Only updates if newLevel > current maxLevel_.
     * Thread-safe with exclusive lock.
     *
     * @param newId ID of the new element
     * @param newLevel Maximum level of the new element
     * @return true if entry point was updated, false otherwise
     */
    bool tryUpdateEntryPoint(idType newId, size_t newLevel);

    // --- Distance Computation ---
    /**
     * @brief Compute distance between a query vector and a stored element
     *
     * Template parameter `running_query` differentiates between:
     * - false (insertion): quantized↔quantized distance (both from memory)
     * - true (query traversal): quantized↔full distance (quantized from memory, full query)
     */
    template <bool running_query = false>
    inline DistType computeDistance(const void* query, idType elementId) const;
    // Quantized stored vs full precision query (used during query traversal)
    DistType computeDistanceQuantized_Full(const void* fullQuery, idType elementId) const;
    // Quantized stored vs quantized query (used during insertion graph traversal)
    DistType computeDistanceQuantized_Quantized(const void* quantizedQuery, idType elementId) const;
    // Full precision stored vs full precision query (used for reranking from disk)
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

    // --- Locking ---
    /**
     * @brief Acquire lock on a node for edge modifications.
     *
     * Used during mutuallyConnectNewElement and repairNode
     * to protect read-modify-write operations on a node's outgoing edges.
     *
     * @param id Internal ID of the node to lock
     * @return RAII lock guard that releases on destruction
     */
    ElementLockGuard lockNode(idType id) const;

    // --- Search Algorithms ---
    /**
     * @brief Greedy search at a single level to find the closest element.
     *
     * Template parameter `running_query` differentiates between:
     * - false (insertion): Must return non-deleted element, no timeout check
     * - true (query): Can return deleted element, checks for timeout
     *
     * Used for upper level traversal (L > 0) during insertion and queries.
     * Starts from entry point and greedily moves to closer neighbors
     * until no improvement is possible.
     *
     * NOTE: querySQ8 should be the SQ8-quantized version of the vector.
     * During insertion, this is the stored SQ8 data. During query, the
     * caller must quantize the FP32 query to SQ8 before calling.
     *
     * @tparam running_query true if called during query, false during insertion
     * @param querySQ8 Pointer to SQ8 quantized query vector data
     * @param level The level to search at
     * @param[in,out] currObj Current best element ID (updated to closest found)
     * @param[in,out] currDist Distance to current best (updated accordingly)
     * @param timeoutCtx Timeout context (only used when running_query=true)
     * @param rc Result code output (only used when running_query=true)
     */
    template <bool running_query>
    void greedySearchLevel(const void* querySQ8, size_t level, idType& currObj, DistType& currDist,
                           void* timeoutCtx = nullptr, VecSimQueryReply_Code* rc = nullptr) const;

    template <bool running_query>
    vecsim_stl::updatable_max_heap<DistType, idType> searchLayer(idType ep_id, const void* data_point, size_t layer,
                                                                 size_t ef) const;

    /**
     * @brief Process a candidate during beam search.
     *
     * Explores neighbors of a candidate node and updates the search heaps.
     * Used by searchLayer for level 0 search with ef parameter.
     *
     * @tparam running_query true if called during query, false during insertion
     * @param curNodeId Current node being processed
     * @param data_point Query vector (SQ8 for insertion, FP32 for query)
     * @param layer Current level being searched
     * @param ef Beam width
     * @param visited_set Set of already visited nodes
     * @param top_candidates Max-heap of best candidates found (farthest at top)
     * @param candidate_set Min-heap of nodes to explore (closest at top, via negation)
     * @param lowerBound Current distance threshold for adding candidates
     */
    template <bool running_query>
    void processCandidate(idType curNodeId, const void* data_point, size_t layer, size_t ef,
                          vecsim_stl::unordered_set<idType>& visited_set,
                          vecsim_stl::updatable_max_heap<DistType, idType>& top_candidates,
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
     * @brief Connect a new element to its neighbors bidirectionally.
     *
     * For each selected neighbor:
     * 1. Add new_node_id to neighbor's outgoing edges
     * 2. If neighbor is full, add repair job to queue
     * 3. Add to new node's incoming edges for bidirectional tracking
     *
     * @param new_node_id The new element being inserted
     * @param top_candidates Candidates from searchLayer (best neighbors found)
     * @param level The graph level being connected
     * @return List of graphNodeTypes that need to be repaired
     */
    GraphNodeList mutuallyConnectNewElement(idType new_node_id,
                                            vecsim_stl::updatable_max_heap<DistType, idType>& top_candidates,
                                            levelType level) const;

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
    void repairNode(idType id, levelType level);

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
                                                       levelType level, vecsim_stl::vector<idType>& deletedNeighbors);

    /**
     * @brief Update incoming edges after repair.
     *
     * Maintains bidirectional edge consistency by:
     * - For removed neighbors (in original but not in new): delete nodeId from their incoming edges
     * - For new neighbors (in new but not in original): add nodeId to their incoming edges
     * - For deleted neighbors: delete nodeId from their incoming edges (to prevent redundant repair jobs)
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
                                        const vecsim_stl::vector<idType>& deletedNeighbors, levelType level);

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
      storage_(std::move(storage)), holes_(abstractInitParams.allocator) {
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
size_t HNSWDiskIndex<DataType, DistType>::getRandomLevel() {
    // Use template helper for thread-local RNG (Seed=0 means random_device)
    auto& generator = getThreadLocalGenerator<Seed>();
    // Static distribution: constructed once, reused (thread-safe in C++11+)
    // Use (0.0, 1.0] range to avoid log(0) which is undefined.
    static const std::uniform_real_distribution<double> distribution(std::nextafter(0.0, 1.0), 1.0);
    double r = -std::log(distribution(generator)) * mult_;
    return static_cast<size_t>(r);
}

template <typename DataType, typename DistType>
bool HNSWDiskIndex<DataType, DistType>::isCapacityFull() const {
    // Capacity is full when we have no holes and nextId_ >= maxElements_
    std::lock_guard<std::mutex> lock(holesMutex_);
    return holes_.empty() && nextId_.load(std::memory_order_relaxed) >= maxElements_.load(std::memory_order_relaxed);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::growByBlock() {
    std::unique_lock<std::shared_mutex> metaLock(metadataMutex_);
    size_t currentMax = maxElements_.load(std::memory_order_relaxed);
    size_t newCapacity = currentMax + this->blockSize;

    // Pre-allocate idToMetaData and nodeLocks_ to avoid frequent reallocations
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
        growByBlock();
    }
    return newId;
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::recycleId(idType id) {
    // TODO(delete PR) [MOD-13172]: Smarter hole management (if the id is last, also trim any holes from the left)
    std::lock_guard<std::mutex> lock(holesMutex_);
    holes_.push_back(id);
}

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::initElementMetadata(idType id, labelType label, levelType level) {
    std::unique_lock<std::shared_mutex> lock(metadataMutex_);
    assert(id < maxElements_.load(std::memory_order_relaxed) && "initElementMetadata: id exceeds capacity");
    // Direct assignment - capacity was pre-allocated by growByBlock()
    idToMetaData[id] = DiskElementMetaData(label, level);
    // Reset the lock for recycled IDs (fresh IDs are already false from resize)
    nodeLocks_[id].store(false, std::memory_order_relaxed);
}

template <typename DataType, typename DistType>
std::pair<idType, size_t> HNSWDiskIndex<DataType, DistType>::safeGetEntryPointState() const {
    std::shared_lock<std::shared_mutex> lock(entryPointMutex_);
    return {entryPoint_, maxLevel_};
}

template <typename DataType, typename DistType>
bool HNSWDiskIndex<DataType, DistType>::tryUpdateEntryPoint(idType newId, size_t newLevel) {
    std::unique_lock<std::shared_mutex> lock(entryPointMutex_);
    if (entryPoint_ != INVALID_ID && newLevel <= maxLevel_) {
        return false;
    }
    entryPoint_ = newId;
    maxLevel_ = newLevel;
    return true;
}

template <typename DataType, typename DistType>
template <bool running_query>
inline DistType HNSWDiskIndex<DataType, DistType>::computeDistance(const void* query, idType elementId) const {
    if constexpr (running_query) {
        return computeDistanceQuantized_Full(query, elementId);
    } else {
        return computeDistanceQuantized_Quantized(query, elementId);
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
    return __atomic_load_n(&idToMetaData[id].flags, 0) & DISK_IN_PROCESS;
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
template <bool running_query>
void HNSWDiskIndex<DataType, DistType>::processCandidate(
    idType curNodeId, const void* data_point, size_t layer, size_t ef, vecsim_stl::unordered_set<idType>& visited_set,
    vecsim_stl::updatable_max_heap<DistType, idType>& top_candidates, candidatesMaxHeap<DistType>& candidate_set,
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
        DistType cur_dist = computeDistance<running_query>(data_point, candidate_id);

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
 * This is the core search algorithm for HNSW at level 0 and for
 * finding neighbor candidates during insertion.
 *
 * Algorithm:
 * 1. Initialize with entry point
 * 2. Maintain two heaps:
 *    - top_candidates (max-heap): Best ef results (farthest at top for easy trimming)
 *    - candidate_set (min-heap via negation): Nodes to explore (closest first)
 * 3. Process candidates until the closest unexplored is farther than farthest result
 *
 * Template parameter running_query:
 * - false (insertion): SQ8↔SQ8 distance
 * - true (query): FP32↔SQ8 distance
 */
template <typename DataType, typename DistType>
template <bool running_query>
vecsim_stl::updatable_max_heap<DistType, idType>
HNSWDiskIndex<DataType, DistType>::searchLayer(idType ep_id, const void* data_point, size_t layer, size_t ef) const {
    // TODO: Consider using tag-based visited set for better performance
    vecsim_stl::unordered_set<idType> visited_set(std::min(ef * 10, size_t(10000)), this->allocator);

    vecsim_stl::updatable_max_heap<DistType, idType> top_candidates(this->allocator);
    candidatesMaxHeap<DistType> candidate_set(this->allocator);

    DistType lowerBound;
    if (!isMarkedDeleted(ep_id)) {
        lowerBound = computeDistance<running_query>(data_point, ep_id);
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
        std::pair<DistType, idType> curr_el_pair = candidate_set.top();

        // Early termination: closest unexplored is farther than farthest result
        if ((-curr_el_pair.first) > lowerBound && top_candidates.size() >= ef) {
            break;
        }
        candidate_set.pop();

        // Process this candidate's neighbors
        processCandidate<running_query>(curr_el_pair.second, data_point, layer, ef, visited_set, top_candidates,
                                        candidate_set, lowerBound);
    }

    return top_candidates;
}

// =============================================================================
// Phase 2.2: Greedy Search Level Implementation
// =============================================================================

/**
 * Greedy search at a single level to find the closest element to the query vector.
 *
 * Template parameter `running_query` differentiates between:
 * - false (insertion): Must return non-deleted element, no timeout check
 * - true (query): Can return deleted element, checks for timeout
 *
 * Algorithm:
 * 1. Start from the current best element (currObj)
 * 2. Get all outgoing edges (neighbors) at this level from disk
 * 3. For each neighbor, compute distance and update best if closer
 * 4. Repeat until no improvement is found
 *
 * Performance notes:
 * - Called frequently during insertion (once per level above element's max level)
 * - Disk reads for edges and vectors are the bottleneck
 * - Skips DISK_IN_PROCESS elements to avoid reading incomplete data
 * - Template allows compile-time optimization of query vs insertion paths
 */
template <typename DataType, typename DistType>
template <bool running_query>
void HNSWDiskIndex<DataType, DistType>::greedySearchLevel(const void* query, size_t level, idType& currObj,
                                                          DistType& currDist, void* timeoutCtx,
                                                          VecSimQueryReply_Code* rc) const {
    bool changed;

    // Track best non-deleted candidate for insertion (not query)
    // During insertion, we need a non-deleted entry point for the next level.
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

            const DistType dist = computeDistance<running_query>(query, candidateId);

            // Track best non-deleted candidate only during insertion
            // During queries, deleted elements are OK as intermediate steps
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

    // For insertion, return non-deleted element as entry point for next level
    // For queries, return whatever element we found (can be deleted)
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
    // TODO: When implementing queries, add visited nodes counter for statistics:
    // if constexpr (running_query) {
    //     num_visited_nodes_higher_levels.fetch_add(visited.size(), std::memory_order_relaxed);
    // }
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
GraphNodeList HNSWDiskIndex<DataType, DistType>::mutuallyConnectNewElement(
    idType new_node_id, vecsim_stl::updatable_max_heap<DistType, idType>& top_candidates, levelType level) const {
    // Maximum neighbors allowed at this level
    const size_t max_M_cur = level ? M_ : M0_;

    // Copy candidates to list for heuristic processing.
    // Note: We pop elements from the heap and reverse to get ascending order by distance.
    candidatesList<DistType> top_candidates_list(this->allocator);
    top_candidates_list.reserve(top_candidates.size());
    while (!top_candidates.empty()) {
        top_candidates_list.push_back(top_candidates.top());
        top_candidates.pop();
    }
    // Reverse to get ascending order (max-heap gives descending order)
    std::reverse(top_candidates_list.begin(), top_candidates_list.end());

    // Use heuristic to filter candidates to at most max_M_cur neighbors
    getNeighborsByHeuristic2(top_candidates_list, max_M_cur);
    // Extract selected neighbor IDs for the new node
    vecsim_stl::vector<idType> new_node_neighbors(this->allocator);
    new_node_neighbors.reserve(top_candidates_list.size());
    for (const auto& [dist, id] : top_candidates_list) {
        new_node_neighbors.push_back(id);
    }

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

    GraphNodeList nodesToRepair(this->allocator);
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

        // If neighbor overflows, collect it for repair
        if (neighbor_neighbors.size() > max_M_cur) {
            nodesToRepair.push_back({selected_neighbor, level});
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

template <typename DataType, typename DistType>
int HNSWDiskIndex<DataType, DistType>::addVector(const void* blob, labelType label) {
    (void)blob;
    (void)label;
    // TODO(MOD-13164): Implement vector insertion
    // After successful insertion: curElementCount_++;
    return 0;
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
    (void)queryBlob;
    (void)k;
    (void)queryParams;
    return new VecSimQueryReply(this->allocator); // Stub - MOD-13164
}

template <typename DataType, typename DistType>
VecSimQueryReply* HNSWDiskIndex<DataType, DistType>::rangeQuery(const void* queryBlob, double radius,
                                                                VecSimQueryParams* queryParams) const {
    (void)queryBlob;
    (void)radius;
    (void)queryParams;
    return new VecSimQueryReply(this->allocator);
}

// =============================================================================
// repairNode helpers
// =============================================================================
template <class DataType, class DistType>
vecsim_stl::vector<idType> HNSWDiskIndex<DataType, DistType>::collectRepairCandidates(
    idType nodeId, const vecsim_stl::vector<idType>& outgoingEdges, levelType level,
    vecsim_stl::vector<idType>& deletedNeighbors) {

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
    const vecsim_stl::vector<idType>& deletedNeighbors, levelType level) {
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
    // This prevents redundant repair jobs: when the delete job for a deleted neighbor runs,
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
void HNSWDiskIndex<DataType, DistType>::repairNode(idType id, levelType level) {
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
    // This is important, as some repair jobs may be scheduled due to bad timing and may not be
    // needed eventually.
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
