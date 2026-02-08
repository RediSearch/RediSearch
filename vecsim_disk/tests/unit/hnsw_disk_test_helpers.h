/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

// Test helper methods for HNSWDiskIndex
// This file is only included when BUILD_TESTS is defined
// These methods provide access to private members for testing purposes

#ifdef BUILD_TESTS

// This macro defines the test accessor methods that should be added to HNSWDiskIndex
// Usage: Include this header and use HNSW_DISK_TEST_METHODS in the public section
// of HNSWDiskIndex class definition

#define HNSW_DISK_TEST_METHODS                                                                                         \
    size_t testGetRandomLevel() { return getRandomLevel(); }                                                           \
    idType testAllocateId() { return allocateId(); }                                                                   \
    void testRecycleId(idType id) { recycleId(id); }                                                                   \
    GraphNodeType testGetEntryPointState() const { return safeGetEntryPointState(); }                                  \
    void testTryUpdateEntryPoint(idType id, size_t level) { replaceEntryPoint(id, level); }                            \
    idType testGetNextId() const { return nextId_.load(); }                                                            \
    size_t testGetHolesCount() const {                                                                                 \
        std::lock_guard<std::mutex> lock(holesMutex_);                                                                 \
        return holes_.size();                                                                                          \
    }                                                                                                                  \
                                                                                                                       \
    /* Test reranking distance: full precision (disk) vs full precision (query) */                                     \
    DistType testComputeDistance(const void* query, idType elementId) const {                                          \
        return computeDistanceFull_Full(query, elementId);                                                             \
    }                                                                                                                  \
    /* Test query traversal distance: quantized (memory) vs full precision (query) */                                  \
    DistType testComputeDistanceQuantized_Full(const void* fullQuery, idType elementId) const {                        \
        auto processedQuery = this->preprocessQuery(fullQuery, false);                                                 \
        return computeDistanceQuantized_Full(processedQuery.get(), elementId);                                         \
    }                                                                                                                  \
    /* Test insertion distance: quantized (memory) vs quantized (memory) */                                            \
    DistType testComputeDistanceQuantized_Quantized(const void* quantizedQuery, idType elementId) const {              \
        return computeDistanceQuantized_Quantized(quantizedQuery, elementId);                                          \
    }                                                                                                                  \
    /* Test helper for greedySearchLevel - automatically preprocesses FP32 query based on mode */                      \
    template <bool running_query = true>                                                                               \
    void testGreedySearchLevel(const void* queryFP32, size_t level, idType& currObj, DistType& currDist,               \
                               void* timeoutCtx = nullptr, VecSimQueryReply_Code* rc = nullptr) const {                \
        if constexpr (running_query) {                                                                                 \
            auto processedQuery = this->preprocessQuery(queryFP32, false);                                             \
            greedySearchLevel<true>(processedQuery.get(), level, currObj, currDist, timeoutCtx, rc);                   \
        } else {                                                                                                       \
            auto processedQuery = this->preprocessForStorage(queryFP32);                                               \
            greedySearchLevel<false>(processedQuery.get(), level, currObj, currDist, timeoutCtx, rc);                  \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Test helpers for setting up graph state */                                                                      \
    void testAddVectorToContainer(idType id, const void* data) {                                                       \
        std::unique_lock<std::shared_mutex> vectorsLock(vectorsMutex_);                                                \
        this->vectors->addElement(data, id);                                                                           \
    }                                                                                                                  \
                                                                                                                       \
    /* Mark element as deleted for testing deletion handling */                                                        \
    void testMarkDeleted(idType id) {                                                                                  \
        if (id < idToMetaData.size()) {                                                                                \
            markAs<DISK_DELETE_MARK>(id);                                                                              \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Check if element is marked deleted (test accessor) */                                                           \
    bool testIsMarkedDeleted(idType id) const { return isMarkedDeleted(id); }                                          \
                                                                                                                       \
    /* Mark element as in-process for testing IN_PROCESS handling */                                                   \
    void testMarkInProcess(idType id) {                                                                                \
        if (id < idToMetaData.size()) {                                                                                \
            markAs<DISK_IN_PROCESS>(id);                                                                               \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Unmark element as in-process */                                                                                 \
    void testUnmarkInProcess(idType id) {                                                                              \
        if (id < idToMetaData.size()) {                                                                                \
            unmarkAs<DISK_IN_PROCESS>(id);                                                                             \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Check if element is marked in-process (test accessor) */                                                        \
    bool testIsInProcess(idType id) const { return isInProcess(id); }                                                  \
                                                                                                                       \
    /* Check if label exists (test accessor) */                                                                        \
    bool testIsLabelExists(labelType label) const { return isLabelExists(label); }                                     \
                                                                                                                       \
    /* Set element count for testing (needed for assertion in greedySearchLevel) */                                    \
    void testSetElementCount(size_t count) { curElementCount_ = count; }                                               \
                                                                                                                       \
    /* Test helper for searchLayer - automatically preprocesses FP32 query based on mode */                            \
    template <bool running_query = true>                                                                               \
    candidatesMaxHeap<DistType> testSearchLayer(idType ep_id, const void* queryFP32, size_t layer, size_t ef) const {  \
        if constexpr (running_query) {                                                                                 \
            auto processedQuery = this->preprocessQuery(queryFP32, false);                                             \
            return searchLayer<true>(ep_id, processedQuery.get(), layer, ef);                                          \
        } else {                                                                                                       \
            auto processedQuery = this->preprocessForStorage(queryFP32);                                               \
            return searchLayer<false>(ep_id, processedQuery.get(), layer, ef);                                         \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for getNeighborsByHeuristic2 */                                                                   \
    void testGetNeighborsByHeuristic2(candidatesList<DistType>& candidates, size_t M) const {                          \
        getNeighborsByHeuristic2(candidates, M);                                                                       \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for getNeighborsByHeuristic2 with removed tracking */                                             \
    void testGetNeighborsByHeuristic2WithRemoved(candidatesList<DistType>& candidates, size_t M,                       \
                                                 vecsim_stl::vector<idType>& removed) const {                          \
        getNeighborsByHeuristic2(candidates, M, removed);                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for mutuallyConnectNewElement */                                                                  \
    MutualConnectResult testMutuallyConnectNewElement(idType new_node_id, candidatesMaxHeap<DistType>& top_candidates, \
                                                      size_t level) const {                                            \
        return mutuallyConnectNewElement(new_node_id, top_candidates, level);                                          \
    }                                                                                                                  \
                                                                                                                       \
    /* Get M0 for testing (max neighbors at level 0) */                                                                \
    size_t testGetM0() const { return M0_; }                                                                           \
                                                                                                                       \
    /* Get M for testing (max neighbors at levels > 0) */                                                              \
    size_t testGetM() const { return M_; }                                                                             \
                                                                                                                       \
    /* Test accessor for repairNode */                                                                                 \
    void testRepairNode(idType id, levelType level) { repairNode(id, level); }                                         \
                                                                                                                       \
    /* Test accessor for updateIncomingEdgesAfterRepair */                                                             \
    void testUpdateIncomingEdgesAfterRepair(idType nodeId, const vecsim_stl::vector<idType>& originalEdges,            \
                                            const vecsim_stl::vector<idType>& newNeighbors, levelType level) {         \
        vecsim_stl::unordered_set<idType> originalEdgesSet(this->allocator);                                           \
        originalEdgesSet.reserve(originalEdges.size());                                                                \
        for (idType id : originalEdges) {                                                                              \
            originalEdgesSet.insert(id);                                                                               \
        }                                                                                                              \
        /* Compute removedCandidates as originalEdges - newNeighbors for test compatibility */                         \
        vecsim_stl::unordered_set<idType> newNeighborsSet(this->allocator);                                            \
        newNeighborsSet.reserve(newNeighbors.size());                                                                  \
        for (idType id : newNeighbors) {                                                                               \
            newNeighborsSet.insert(id);                                                                                \
        }                                                                                                              \
        vecsim_stl::vector<idType> removedCandidates(this->allocator);                                                 \
        for (idType id : originalEdges) {                                                                              \
            if (newNeighborsSet.find(id) == newNeighborsSet.end()) {                                                   \
                removedCandidates.push_back(id);                                                                       \
            }                                                                                                          \
        }                                                                                                              \
        vecsim_stl::vector<idType> deletedNeighbors(this->allocator); /* Empty for tests */                            \
        updateIncomingEdgesAfterRepair(nodeId, originalEdgesSet, newNeighbors, removedCandidates, deletedNeighbors,    \
                                       level);                                                                         \
    }                                                                                                                  \
                                                                                                                       \
    /* Block-based allocation test helpers */                                                                          \
    void testInitElementMetadata(idType id, labelType label, levelType level) {                                        \
        initElementMetadata(id, label, level);                                                                         \
    }                                                                                                                  \
    size_t testGetMaxElements() const { return maxElements_.load(std::memory_order_relaxed); }                         \
    void testGrowByBlock() { growByBlock(); }                                                                          \
    size_t testGetNodeLocksSize() const { return nodeLocks_.size(); }                                                  \
    size_t testGetIdToMetaDataSize() const { return idToMetaData.size(); }                                             \
                                                                                                                       \
    /* Helper to set up a complete element for testing (allocate + init metadata + add vector + store in SpeedB) */    \
    idType testSetupElement(labelType label, levelType level, const void* vectorData) {                                \
        idType id = allocateId();                                                                                      \
        initElementMetadata(id, label, level);                                                                         \
        /* Preprocess (quantize FP32 -> SQ8) and store in memory for graph traversal */                                \
        auto processedBlob = this->preprocessForStorage(vectorData);                                                   \
        {                                                                                                              \
            std::unique_lock<std::shared_mutex> vectorsLock(vectorsMutex_);                                            \
            this->vectors->addElement(processedBlob.get(), id);                                                        \
        }                                                                                                              \
        if (storage_) {                                                                                                \
            /* Store original FP32 vector to disk for reranking */                                                     \
            storage_->put_vector(id, vectorData, this->getInputBlobSize());                                            \
        }                                                                                                              \
        unmarkAs<DISK_IN_PROCESS>(id);                                                                                 \
        curElementCount_++;                                                                                            \
        return id;                                                                                                     \
    }                                                                                                                  \
                                                                                                                       \
    /* Helper to add edges between nodes in storage (outgoing + incoming) */                                           \
    void testAddEdge(idType from, idType to, levelType level) {                                                        \
        if (storage_) {                                                                                                \
            auto from_lock = lockNode(from);                                                                           \
            vecsim_stl::vector<idType> neighbors(this->allocator);                                                     \
            storage_->get_outgoing_edges(from, level, neighbors);                                                      \
            neighbors.push_back(to);                                                                                   \
            storage_->put_outgoing_edges(from, level, neighbors);                                                      \
            storage_->append_incoming_edge(to, level, from);                                                           \
        }                                                                                                              \
    }                                                                                                                  \
                                                                                                                       \
    /* Helper to get outgoing edges for verification */                                                                \
    vecsim_stl::vector<idType> testGetOutgoingEdges(idType nodeId, levelType level) const {                            \
        vecsim_stl::vector<idType> result(this->allocator);                                                            \
        if (storage_) {                                                                                                \
            storage_->get_outgoing_edges(nodeId, level, result);                                                       \
        }                                                                                                              \
        return result;                                                                                                 \
    }                                                                                                                  \
                                                                                                                       \
    /* Helper to get incoming edges for verification */                                                                \
    vecsim_stl::vector<idType> testGetIncomingEdges(idType nodeId, levelType level) const {                            \
        vecsim_stl::vector<idType> result(this->allocator);                                                            \
        if (storage_) {                                                                                                \
            storage_->get_incoming_edges(nodeId, level, result);                                                       \
        }                                                                                                              \
        return result;                                                                                                 \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for storeVector - Phase 1 of insertion */                                                         \
    HNSWDiskAddVectorState testStoreVector(const void* vector_data, labelType label) {                                 \
        return storeVector(vector_data, label);                                                                        \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for indexVector - Phase 2 of insertion */                                                         \
    GraphNodeList testIndexVector(const void* querySQ8, labelType label, const HNSWDiskAddVectorState& state) {        \
        return indexVector(querySQ8, label, state);                                                                    \
    }                                                                                                                  \
                                                                                                                       \
    /* Test accessor for insertElementToGraph - the core graph insertion algorithm */                                  \
    GraphNodeList testInsertElementToGraph(idType element_id, levelType element_max_level, idType entry_point,         \
                                           levelType global_max_level, const void* querySQ8) {                         \
        return insertElementToGraph(element_id, element_max_level, entry_point, global_max_level, querySQ8);           \
    }                                                                                                                  \
                                                                                                                       \
    /* Get quantized data by internal ID for testing */                                                                \
    const char* testGetQuantizedDataByInternalId(idType id) const { return getQuantizedDataByInternalId(id); }         \
                                                                                                                       \
    /* Get label by internal ID for testing */                                                                         \
    labelType testGetLabelById(idType id) const { return idToMetaData[id].label; }                                     \
                                                                                                                       \
    /* Get internal ID by label for testing */                                                                         \
    idType testGetIdByLabel(labelType label) const {                                                                   \
        std::shared_lock<std::shared_mutex> lock(labelLookupMutex_);                                                   \
        auto it = labelToIdLookup_.find(label);                                                                        \
        return (it != labelToIdLookup_.end()) ? it->second : INVALID_ID;                                               \
    }                                                                                                                  \
                                                                                                                       \
    /* Get max level for element by internal ID for testing */                                                         \
    levelType testGetElementLevel(idType id) const { return idToMetaData[id].maxLevel; }                               \
                                                                                                                       \
    /* Get vector from disk storage for testing */                                                                     \
    bool testGetVectorFromDisk(idType id, void* buffer, size_t bufferSize) const {                                     \
        if (storage_) {                                                                                                \
            return storage_->get_vector(id, buffer, bufferSize);                                                       \
        }                                                                                                              \
        return false;                                                                                                  \
    }

#else
// When BUILD_TESTS is not defined, this macro expands to nothing
#define HNSW_DISK_TEST_METHODS
#endif
