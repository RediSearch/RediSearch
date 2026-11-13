/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "graph_data.h"
#include "visited_nodes_handler.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/utils/vecsim_stl.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/containers/data_block.h"
#include "VecSim/containers/raw_data_container_interface.h"
#include "VecSim/containers/data_blocks_container.h"
#include "VecSim/containers/vecsim_results_container.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/vec_sim_common.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/spaces/computer/preprocessors.h"
#include "VecSim/quantization/sq8_mean.h"
#include <span>
#include "VecSim/tombstone_interface.h"

#ifdef BUILD_TESTS
#include "hnsw_serialization_utils.h"
#include "VecSim/utils/serializer.h"
#include "hnsw_serializer.h"
#endif

#include <deque>
#include <memory>
#include <cassert>
#include <climits>
#include <queue>
#include <random>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <sys/resource.h>
#include <fstream>
#include <shared_mutex>

using std::pair;

typedef uint8_t elementFlags;

template <typename DistType>
using candidatesMaxHeap = vecsim_stl::max_priority_queue<DistType, idType>;
template <typename DistType>
using candidatesList = vecsim_stl::vector<pair<DistType, idType>>;
template <typename DistType>
using candidatesLabelsMaxHeap = vecsim_stl::abstract_priority_queue<DistType, labelType>;
using graphNodeType = pair<idType, unsigned short>; // represented as: (element_id, level)

////////////////////////////////////// Auxiliary HNSW structs //////////////////////////////////////

// Vectors flags (for marking a specific vector)
typedef enum {
    DELETE_MARK = 0x1, // element is logically deleted, but still exists in the graph
    IN_PROCESS = 0x2,  // element is being inserted into the graph
} Flags;

// The state of the index and the newly stored vector to be passed to indexVector.
struct HNSWAddVectorState {
    idType newElementId;
    int elementMaxLevel;
    idType currEntryPoint;
    int currMaxLevel;
};

#pragma pack(1)
struct ElementMetaData {
    labelType label;
    elementFlags flags;

    explicit ElementMetaData(labelType label = SIZE_MAX) noexcept
        : label(label), flags(IN_PROCESS) {}
};
#pragma pack() // restore default packing

//////////////////////////////////// HNSW index implementation ////////////////////////////////////

template <typename DataType, typename DistType>
class HNSWIndex : public VecSimIndexAbstract<DataType, DistType>,
                  public VecSimIndexTombstone
#ifdef BUILD_TESTS
    ,
                  public HNSWSerializer
#endif
{
private:
    template <VecSimMetric Metric>
    void setSQ8Mean(std::span<const float> mean) noexcept {
        applySQ8Mean<DataType, Metric>(*this->getPreprocessorsContainer(),
                                       *this->getIndexCalculator(), mean);
    }

public:
    // Writer-only operation: install the mean once, before any stored vector, under the
    // exclusive tiered main lock and before submitting insertion jobs. The backend must have
    // its final SQ8 WithNorm components; the caller owns and enforces the one-time transition.
    // A quantizer that learns storedDataSize from data needs deferred backend creation.
    void setQuantizationMean(std::span<const float> mean) noexcept {
        assert(this->isQuantized && curElementCount == 0 && mean.size() == this->dim);
        if constexpr (QuantInput<DataType> && std::is_same_v<DistType, float>) {
            if (this->metric == VecSimMetric_L2) {
                setSQ8Mean<VecSimMetric_L2>(mean);
            } else {
                // Tiered cosine inputs are already normalized, and use IP SQ8 components.
                assert(this->metric == VecSimMetric_IP || this->metric == VecSimMetric_Cosine);
                setSQ8Mean<VecSimMetric_IP>(mean);
            }
#ifdef BUILD_TESTS
            assert(serializedMeanVector.size() == this->dim);
            std::copy(mean.begin(), mean.end(), serializedMeanVector.begin());
#endif
        } else {
            assert(false && "Unsupported SQ8 data type");
        }
    }

protected:
    // Index build parameters
    size_t maxElements;
    size_t M;
    size_t M0;
    size_t efConstruction;

    // Index search parameter
    size_t ef;
    double epsilon;

    // Index meta-data (based on the data dimensionality and index parameters)
    size_t elementGraphDataSize;
    size_t levelDataSize;
    double mult;

    // Index level generator of the top level for a new element
    std::default_random_engine levelGenerator;

    // Index global state - these should be guarded by the indexDataGuard lock in
    // multithreaded scenario.
    size_t curElementCount;
    idType entrypointNode;
    size_t maxLevel; // this is the top level of the entry point's element

    // Index data
    vecsim_stl::vector<DataBlock> graphDataBlocks;
    mutable vecsim_stl::vector<vecsim_stl::one_byte_mutex> elementLocks;
    vecsim_stl::vector<ElementMetaData> idToMetaData;

    // Used for marking the visited nodes in graph scans (the pool supports parallel graph scans).
    // This is mutable since the object changes upon search operations as well (which are const).
    mutable VisitedNodesHandlerPool visitedNodesHandlerPool;
    mutable std::shared_mutex indexDataGuard;

#ifdef BUILD_TESTS
#include "VecSim/algorithms/hnsw/hnsw_base_tests_friends.h"

#include "hnsw_serializer_declarations.h"

public:
    // Serialization-only fields for V5 format (SQ8 support)
    VecSimQuantType quantType = VecSimQuant_NONE;
    std::vector<float> serializedMeanVector; // Mean vector for SQ8 indices (empty if not SQ8)

#endif

protected:
    HNSWIndex() = delete;                  // default constructor is disabled.
    HNSWIndex(const HNSWIndex &) = delete; // default (shallow) copy constructor is disabled.
    size_t getRandomLevel(double reverse_size);
    template <typename Identifier> // Either idType or labelType
    void processCandidate(idType curNodeId, const void *data_point, size_t layer, size_t ef,
                          tag_t *elements_tags, tag_t visited_tag,
                          vecsim_stl::abstract_priority_queue<DistType, Identifier> &top_candidates,
                          candidatesMaxHeap<DistType> &candidates_set, DistType &lowerBound) const;
    void processCandidate_RangeSearch(
        idType curNodeId, const void *data_point, size_t layer, double epsilon,
        tag_t *elements_tags, tag_t visited_tag,
        std::unique_ptr<vecsim_stl::abstract_results_container> &top_candidates,
        candidatesMaxHeap<DistType> &candidate_set, DistType lowerBound, DistType radius) const;
    candidatesMaxHeap<DistType> searchLayer(idType ep_id, const void *data_point, size_t layer,
                                            size_t ef) const;
    candidatesLabelsMaxHeap<DistType> *
    searchBottomLayer_WithTimeout(idType ep_id, const void *data_point, size_t ef, size_t k,
                                  void *timeoutCtx, VecSimQueryReply_Code *rc) const;
    VecSimQueryResultContainer searchRangeBottomLayer_WithTimeout(idType ep_id,
                                                                  const void *data_point,
                                                                  double epsilon, DistType radius,
                                                                  void *timeoutCtx,
                                                                  VecSimQueryReply_Code *rc) const;
    idType getNeighborsByHeuristic2(candidatesList<DistType> &top_candidates, size_t M) const;
    void getNeighborsByHeuristic2(candidatesList<DistType> &top_candidates, size_t M,
                                  vecsim_stl::vector<idType> &not_chosen_candidates) const;
    template <bool record_removed>
    void getNeighborsByHeuristic2_internal(
        candidatesList<DistType> &top_candidates, size_t M,
        vecsim_stl::vector<idType> *removed_candidates = nullptr) const;
    // Helper function for re-selecting node's neighbors which was selected as a neighbor for
    // a newly inserted node. Also, responsible for mutually connect the new node and the neighbor
    // (unidirectional or bidirectional connection).
    // *Note that node_lock and neighbor_lock should be locked upon calling this function*
    void revisitNeighborConnections(size_t level, idType new_node_id,
                                    const std::pair<DistType, idType> &neighbor_data,
                                    ElementLevelData &new_node_level,
                                    ElementLevelData &neighbor_level);
    idType mutuallyConnectNewElement(idType new_node_id,
                                     candidatesMaxHeap<DistType> &top_candidates, size_t level);
    void mutuallyUpdateForRepairedNode(idType node_id, size_t level,
                                       vecsim_stl::vector<idType> &nodes_to_update,
                                       vecsim_stl::vector<idType> &chosen_neighbors,
                                       size_t max_M_cur);

    template <bool running_query>
    void greedySearchLevel(const void *vector_data, size_t level, idType &curObj, DistType &curDist,
                           void *timeoutCtx = nullptr, VecSimQueryReply_Code *rc = nullptr) const;
    void repairConnectionsForDeletion(idType element_internal_id, idType neighbour_id,
                                      ElementLevelData &node_level,
                                      ElementLevelData &neighbor_level, size_t level,
                                      vecsim_stl::vector<bool> &neighbours_bitmap);
    void replaceEntryPoint();

    void SwapLastIdWithDeletedId(idType element_internal_id, ElementGraphData *last_element,
                                 const void *last_element_data);

    /** Add vector functions */
    // Protected internal function that implements generic single vector insertion.

    void appendVector(const void *vector_data, labelType label);

    HNSWAddVectorState storeVector(const void *vector_data, const labelType label);

    // Protected internal functions for index resizing.
    void growByBlock();
    void shrinkByBlock();
    // DO NOT USE DIRECTLY. Use `[grow|shrink]ByBlock` instead.
    void resizeIndexCommon(size_t new_max_elements);

    void emplaceToHeap(vecsim_stl::abstract_priority_queue<DistType, idType> &heap, DistType dist,
                       idType id) const;
    void emplaceToHeap(vecsim_stl::abstract_priority_queue<DistType, labelType> &heap,
                       DistType dist, idType id) const;
    void swapWithLast(idType removedId);

    size_t getVectorRelativeIndex(idType id) const { return id % this->blockSize; }

    // Flagging API
    template <Flags FLAG>
    void markAs(idType internalId) {
        __atomic_fetch_or(&idToMetaData[internalId].flags, FLAG, 0);
    }
    template <Flags FLAG>
    void unmarkAs(idType internalId) {
        __atomic_fetch_and(&idToMetaData[internalId].flags, ~FLAG, 0);
    }
    template <Flags FLAG>
    bool isMarkedAs(idType internalId) const {
        return idToMetaData[internalId].flags & FLAG;
    }
    void mutuallyRemoveNeighborAtPos(ElementLevelData &node_level, size_t level, idType node_id,
                                     size_t pos);

public:
    HNSWIndex(const HNSWParams *params, const AbstractIndexInitParams &abstractInitParams,
              const IndexComponents<DataType, DistType> &components, size_t random_seed = 100);
    virtual ~HNSWIndex();

    void setEf(size_t ef);
    size_t getEf() const;
    void setEpsilon(double epsilon);
    double getEpsilon() const;
    size_t indexSize() const override;
    size_t indexCapacity() const override;
    /**
     * Checks if the index capacity is full to hint the caller a resize is needed.
     * @note Must be called with indexDataGuard locked.
     */
    size_t isCapacityFull() const;
    size_t getEfConstruction() const;
    size_t getM() const;
    size_t getMaxLevel() const;
    labelType getEntryPointLabel() const;
    labelType getExternalLabel(idType internal_id) const { return idToMetaData[internal_id].label; }
    auto safeGetEntryPointState() const;
    void lockIndexDataGuard() const;
    void unlockIndexDataGuard() const;
    void lockSharedIndexDataGuard() const;
    void unlockSharedIndexDataGuard() const;
    std::shared_lock<std::shared_mutex> acquireSharedIndexDataGuard() const;
    void lockNodeLinks(idType node_id) const;
    void unlockNodeLinks(idType node_id) const;
    VisitedNodesHandler *getVisitedList() const;
    void returnVisitedList(VisitedNodesHandler *visited_nodes_handler) const;
    VecSimIndexDebugInfo debugInfo() const override;
    VecSimIndexBasicInfo basicInfo() const override;
    VecSimDebugInfoIterator *debugInfoIterator() const override;
    bool preferAdHocSearch(size_t subsetSize, size_t k, bool initial_check) const override;
    const char *getDataByInternalId(idType internal_id) const;
    ElementGraphData *getGraphDataByInternalId(idType internal_id) const;
    ElementLevelData &getElementLevelData(idType internal_id, size_t level) const;
    ElementLevelData &getElementLevelData(ElementGraphData *element, size_t level) const;
    idType searchBottomLayerEP(const void *query_data, void *timeoutCtx,
                               VecSimQueryReply_Code *rc) const;

    void indexVector(const void *vector_data, const labelType label,
                     const HNSWAddVectorState &state);
    VecSimQueryReply *topKQuery(const void *query_data, size_t k,
                                VecSimQueryParams *queryParams) const override;
    VecSimQueryReply *rangeQuery(const void *query_data, double radius,
                                 VecSimQueryParams *queryParams) const override;

    void markDeletedInternal(idType internalId);
    bool isMarkedDeleted(idType internalId) const;
    bool isInProcess(idType internalId) const;
    void unmarkInProcess(idType internalId);
    // `elementId` is optional:
    // - INVALID_ID (the default): a new id is allocated by advancing `curElementCount`, and the
    //   vector and graph data are *appended* to their containers (growing the index by a block
    //   first if it is full).
    // - otherwise: the given id must already be a valid slot with no owned graph data left in it -
    //   the caller must have detached it (see `repairConnectionsAndEntryPoint` +
    //   `disposeElementData`) first, otherwise its link lists are leaked by the overwrite here.
    //   `curElementCount` is left untouched, since the slot isn't being handed back to the index,
    //   just overwritten in place.
    HNSWAddVectorState storeNewElement(labelType label, const void *vector_data,
                                       idType elementId = INVALID_ID);
    // Reusing the slot of `deletedId`, a marked-deleted element whose repairs are all done, for a
    // new element instead of appending one - the counterpart of `swapDeletedElement` that keeps
    // the slot - takes two steps:
    // 1. `clearDeletedSlot` frees the element's graph data and leaves an empty one (level 0, no
    //    links) in its place. No graph scan may be in flight, as one may still hold a stale link to
    //    it. The slot stays marked deleted, and is safe to be moved by a swap like any other.
    // 2. `storeNewElementInClearedSlot` then stores the new element there, like `storeNewElement`
    //    appends one - with the index data guard held exclusively, and graph scans allowed. Unlike
    //    an in-place overwrite, the label no longer maps to this id (it was dropped when marked
    //    deleted), so it's mapped again here.
    void clearDeletedSlot(idType deletedId);
    HNSWAddVectorState storeNewElementInClearedSlot(labelType label, const void *vector_data,
                                                    idType clearedId);
    void swapDeletedElement(idType internalId);
    // Repairs every neighbor affected by removing `element_internal_id` (both directions, every
    // level) exactly as a real removal would, then replaces the entry point if the element held
    // it. Leaves `curElementCount` and the element's own graph data untouched -
    // `disposeElementData` finishes the detach.
    void repairConnectionsAndEntryPoint(idType element_internal_id);
    // Take back what an element still owns - its incoming-edge bookkeeping on remaining neighbors,
    // and its own graph data - without touching `curElementCount`. Shared by `removeFromGraph`
    // (which also gives the slot back to the index) and the callers that detach an element without
    // removing it - `removeVectorInPlace` and `overwriteVectorInPlace` - which call this directly
    // after `repairConnectionsAndEntryPoint` instead of touching the slot itself.
    void disposeElementData(idType internalId);
    void removeFromGraph(idType internalId);
    // Whether `level_data` holds a link to `id`.
    static bool hasLink(const ElementLevelData &level_data, idType id) {
        for (size_t i = 0; i < level_data.getNumLinks(); i++) {
            if (level_data.getLinkAtPos(i) == id) {
                return true;
            }
        }
        return false;
    }
    // Take a marked-deleted element out of the graph entirely: drop every edge going out of it and
    // every edge coming into it, at every level, keeping the bookkeeping of the other side
    // consistent. To be called once all the repair jobs created for its deletion are done and
    // before its swap job disposes of it, so that from that point no element refers to it and it
    // refers to no element.
    // Takes the per-element links locks (one at a time), so holding the main index guard for shared
    // ownership is enough.
    void isolateDeletedElement(idType internalId);
    void repairNodeConnections(idType node_id, size_t level);
    // For prefetching only.
    const ElementMetaData *getMetaDataAddress(idType internal_id) const {
        return idToMetaData.data() + internal_id;
    }
    vecsim_stl::vector<graphNodeType> safeCollectAllNodeIncomingNeighbors(idType node_id) const;
    VecSimDebugCommandCode getHNSWElementNeighbors(size_t label, int ***neighborsData);
    void insertElementToGraph(idType element_id, size_t element_max_level, idType entry_point,
                              size_t global_max_level, const void *vector_data);
    void removeVectorInPlace(idType id);
    // Overwrite the vector at `old_id` in place, keeping its internal id: detaches it from the
    // graph (`repairConnectionsAndEntryPoint` + `disposeElementData` - the same detach
    // `removeVectorInPlace` does, without the trailing compaction) and immediately writes
    // `vector_data` into that same slot instead of appending a fresh one, so a label being
    // replaced never touches `curElementCount` or the swap-to-last compaction a real removal
    // would use. `old_id` must not
    // be the id of any other, still-live element - callers overwrite a label they hold the id of.
    void overwriteVectorInPlace(idType old_id, const void *vector_data, labelType label);

    /*************************** Labels lookup API ***************************/

    // Inline priority queue getter that need to be implemented by derived class.
    virtual inline candidatesLabelsMaxHeap<DistType> *getNewMaxPriorityQueue() const = 0;

    // Unsafe (assume index data guard is held in MT mode).
    virtual vecsim_stl::vector<idType> getElementIds(size_t label) = 0;

    // How many ids `label` currently maps to, without copying the id list out the way
    // `getElementIds(label).size()` would.
    virtual size_t getLabelSize(labelType label) const = 0;

    // Remove label from the index.
    virtual int removeLabel(labelType label) = 0;

    // Remove and return whichever id `getElementIds(label).back()` would have returned, without
    // copying the label's whole id list out first (as a `getElementIds` call would) or searching
    // it again to find that same id for removal. `label` must currently map to at least one id.
    virtual idType popLastIdFromLabel(labelType label) = 0;

    // Check whether a label currently maps to at least one element. Note that a label whose
    // element was marked deleted is *not* considered to exist, matching `getElementIds`.
    virtual bool isLabelExists(labelType label) = 0;

    VecSimRelabelCode relabelVector(labelType old_label, labelType new_label) override;

    VecSimUpdateCode updateVectors(labelType label, const void *new_blobs, size_t n) override;

#ifdef BUILD_TESTS
    void fitMemory() override {
        if (maxElements > 0) {
            idToMetaData.shrink_to_fit();
            resizeLabelLookup(idToMetaData.size());
        }
    }

    size_t indexMetaDataCapacity() const override { return idToMetaData.capacity(); }
#endif

protected:
    // inline label to id setters that need to be implemented by derived class
    virtual std::unique_ptr<vecsim_stl::abstract_results_container>
    getNewResultsContainer(size_t cap) const = 0;
    virtual void replaceIdOfLabel(labelType label, idType new_id, idType old_id) = 0;
    virtual void setVectorId(labelType label, idType id) = 0;
    virtual void resizeLabelLookup(size_t new_max_elements) = 0;
};

/**
 * getters and setters of index data
 */

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::setEf(size_t ef) {
    this->ef = ef;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::getEf() const {
    return this->ef;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::setEpsilon(double epsilon) {
    this->epsilon = epsilon;
}

template <typename DataType, typename DistType>
double HNSWIndex<DataType, DistType>::getEpsilon() const {
    return this->epsilon;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::indexSize() const {
    return this->curElementCount;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::indexCapacity() const {
    return this->maxElements;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::isCapacityFull() const {
    return indexSize() == this->maxElements;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::getEfConstruction() const {
    return this->efConstruction;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::getM() const {
    return this->M;
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::getMaxLevel() const {
    return this->maxLevel;
}

template <typename DataType, typename DistType>
labelType HNSWIndex<DataType, DistType>::getEntryPointLabel() const {
    if (entrypointNode != INVALID_ID)
        return getExternalLabel(entrypointNode);
    return SIZE_MAX;
}

template <typename DataType, typename DistType>
const char *HNSWIndex<DataType, DistType>::getDataByInternalId(idType internal_id) const {
    // `vectors` is always a DataBlocksContainer; skip the RawDataContainer vtable on the hot path.
    return static_cast<const DataBlocksContainer *>(this->vectors)
        ->DataBlocksContainer::getElement(internal_id);
}

template <typename DataType, typename DistType>
ElementGraphData *
HNSWIndex<DataType, DistType>::getGraphDataByInternalId(idType internal_id) const {
    return (ElementGraphData *)graphDataBlocks[internal_id / this->blockSize].getElement(
        internal_id % this->blockSize);
}

template <typename DataType, typename DistType>
size_t HNSWIndex<DataType, DistType>::getRandomLevel(double reverse_size) {
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    double r = -log(distribution(levelGenerator)) * reverse_size;
    return (size_t)r;
}

template <typename DataType, typename DistType>
ElementLevelData &HNSWIndex<DataType, DistType>::getElementLevelData(idType internal_id,
                                                                     size_t level) const {
    return getGraphDataByInternalId(internal_id)->getElementLevelData(level, this->levelDataSize);
}

template <typename DataType, typename DistType>
ElementLevelData &HNSWIndex<DataType, DistType>::getElementLevelData(ElementGraphData *graph_data,
                                                                     size_t level) const {
    return graph_data->getElementLevelData(level, this->levelDataSize);
}

template <typename DataType, typename DistType>
VisitedNodesHandler *HNSWIndex<DataType, DistType>::getVisitedList() const {
    return visitedNodesHandlerPool.getAvailableVisitedNodesHandler();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::returnVisitedList(
    VisitedNodesHandler *visited_nodes_handler) const {
    visitedNodesHandlerPool.returnVisitedNodesHandlerToPool(visited_nodes_handler);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::markDeletedInternal(idType internalId) {
    // Here we are holding the global index data guard (and the main index lock of the tiered index
    // for shared ownership).
    assert(internalId < this->curElementCount);
    if (!isMarkedDeleted(internalId)) {
        if (internalId == entrypointNode) {
            // Internally, we hold and release the entrypoint neighbors lock.
            replaceEntryPoint();
        }
        // Atomically set the deletion mark flag (note that other parallel threads may set the flags
        // at the same time (for changing the IN_PROCESS flag).
        markAs<DELETE_MARK>(internalId);
        this->numMarkedDeleted++;
    }
}

template <typename DataType, typename DistType>
bool HNSWIndex<DataType, DistType>::isMarkedDeleted(idType internalId) const {
    return isMarkedAs<DELETE_MARK>(internalId);
}

template <typename DataType, typename DistType>
bool HNSWIndex<DataType, DistType>::isInProcess(idType internalId) const {
    return isMarkedAs<IN_PROCESS>(internalId);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::unmarkInProcess(idType internalId) {
    // Atomically unset the IN_PROCESS mark flag (note that other parallel threads may set the flags
    // at the same time (for marking the element with IN_PROCCESS flag).
    unmarkAs<IN_PROCESS>(internalId);
}

/**
 * Move every element stored under `old_label` to `new_label`. The graph is keyed purely on internal
 * ids - links, entry point and levels never mention a label - so this only has to fix the two
 * places a label is kept: `idToMetaData[id].label` (id -> label, read by `getExternalLabel` on
 * every query result) and the derived class's label -> id lookup.
 *
 * Takes `indexDataGuard` exclusively, like `markDelete`, since both containers it mutates are
 * guarded by it. A tiered caller also excludes query label reads through its label guard;
 * query traversal does not hold `indexDataGuard` for the entire scan.
 */
template <typename DataType, typename DistType>
VecSimRelabelCode HNSWIndex<DataType, DistType>::relabelVector(labelType old_label,
                                                               labelType new_label) {
    if (old_label == new_label) {
        return VecSimRelabel_SameLabel;
    }
    std::unique_lock<std::shared_mutex> index_data_lock(indexDataGuard);

    // An absent source is reported before an occupied target, so that a caller which resolves
    // `NewLabelTaken` by freeing the target is never told to do so for a move that has nothing to
    // move. Elements that were marked deleted are already out of the label lookup, so they are
    // reported as absent here and are left alone - their `idToMetaData` label is still needed by
    // the pending swap/repair jobs that reference their id.
    auto ids = getElementIds(old_label);
    if (ids.empty()) {
        return VecSimRelabel_OldLabelMissing;
    }
    if (isLabelExists(new_label)) {
        return VecSimRelabel_NewLabelTaken;
    }

    removeLabel(old_label);
    for (idType id : ids) {
        // Assign the label field rather than the whole struct: `ElementMetaData`'s constructor
        // resets `flags` to IN_PROCESS, which would hide a live element from queries.
        idToMetaData[id].label = new_label;
        setVectorId(new_label, id);
    }
    return VecSimRelabel_OK;
}

/**
 * Replace everything stored under a label with the given vectors.
 *
 * Removal and insertion rather than a write over the stored data: an element's neighbours, and the
 * elements that chose it as one, were selected from its old value, so overwriting the bytes would
 * leave the graph describing vectors that are no longer there. Both halves go through this index's
 * own delete and add, so the elements are taken out and the new ones linked in exactly as they
 * would be on their own - the caller is spared having to know how many vectors were there, not the
 * work of moving them.
 *
 * Callers are the single writer, as they are for `addVector` and `deleteVector`; a query between
 * the two halves sees the label without its vectors.
 */
template <typename DataType, typename DistType>
VecSimUpdateCode HNSWIndex<DataType, DistType>::updateVectors(labelType label,
                                                              const void *new_blobs, size_t n) {
    if (!this->isMultiValue() && n > 1) {
        return VecSimUpdate_MultiNotSupported;
    }
    // One vector replacing a single-value label *is* an overwrite, which is what `addVector`
    // already does for that case - and it is the case a caller hits most, since a hash document
    // holds one vector per field. Delegating keeps the two operations from drifting apart, and
    // avoids paying for a removal whose slot the insertion below would only fill again.
    if (n == 1 && !this->isMultiValue()) {
        this->addVector(new_blobs, label);
        return VecSimUpdate_OK;
    }

    this->deleteVector(label);
    const char *blob = static_cast<const char *>(new_blobs);
    for (size_t i = 0; i < n; i++) {
        this->addVector(blob + i * this->getInputBlobSize(), label);
    }
    return VecSimUpdate_OK;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::lockIndexDataGuard() const {
    indexDataGuard.lock();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::unlockIndexDataGuard() const {
    indexDataGuard.unlock();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::lockSharedIndexDataGuard() const {
    indexDataGuard.lock_shared();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::unlockSharedIndexDataGuard() const {
    indexDataGuard.unlock_shared();
}

template <typename DataType, typename DistType>
std::shared_lock<std::shared_mutex>
HNSWIndex<DataType, DistType>::acquireSharedIndexDataGuard() const {
    return std::shared_lock<std::shared_mutex>(indexDataGuard);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::lockNodeLinks(idType node_id) const {
    elementLocks[node_id].lock();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::unlockNodeLinks(idType node_id) const {
    elementLocks[node_id].unlock();
}

/**
 * helper functions
 */

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::emplaceToHeap(
    vecsim_stl::abstract_priority_queue<DistType, idType> &heap, DistType dist, idType id) const {
    heap.emplace(dist, id);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::emplaceToHeap(
    vecsim_stl::abstract_priority_queue<DistType, labelType> &heap, DistType dist,
    idType id) const {
    heap.emplace(dist, getExternalLabel(id));
}

// This function handles both label heaps and internal ids heaps. It uses the `emplaceToHeap`
// overloading to emplace correctly for both cases.
template <typename DataType, typename DistType>
template <typename Identifier>
void HNSWIndex<DataType, DistType>::processCandidate(
    idType curNodeId, const void *query_data, size_t layer, size_t ef, tag_t *elements_tags,
    tag_t visited_tag, vecsim_stl::abstract_priority_queue<DistType, Identifier> &top_candidates,
    candidatesMaxHeap<DistType> &candidate_set, DistType &lowerBound) const {

    ElementGraphData *cur_element = getGraphDataByInternalId(curNodeId);
    lockNodeLinks(curNodeId);
    ElementLevelData &node_level = getElementLevelData(cur_element, layer);
    linkListSize num_links = node_level.getNumLinks();
    if (num_links > 0) {

        const char *cur_data, *next_data;
        // Pre-fetch first candidate tag address.
        __builtin_prefetch(elements_tags + node_level.getLinkAtPos(0));
        // Pre-fetch first candidate data block address.
        next_data = getDataByInternalId(node_level.getLinkAtPos(0));
        __builtin_prefetch(next_data);

        for (linkListSize j = 0; j < num_links - 1; j++) {
            idType candidate_id = node_level.getLinkAtPos(j);
            cur_data = next_data;

            // Pre-fetch next candidate tag address.
            __builtin_prefetch(elements_tags + node_level.getLinkAtPos(j + 1));
            // Pre-fetch next candidate data block address.
            next_data = getDataByInternalId(node_level.getLinkAtPos(j + 1));
            __builtin_prefetch(next_data);

            if (elements_tags[candidate_id] == visited_tag || isInProcess(candidate_id))
                continue;

            elements_tags[candidate_id] = visited_tag;

            DistType cur_dist = this->calcDistanceForQuery(cur_data, query_data);
            if (lowerBound > cur_dist || top_candidates.size() < ef) {

                candidate_set.emplace(-cur_dist, candidate_id);

                // Insert the candidate to the top candidates heap only if it is not marked as
                // deleted.
                if (!isMarkedDeleted(candidate_id))
                    emplaceToHeap(top_candidates, cur_dist, candidate_id);

                if (top_candidates.size() > ef)
                    top_candidates.pop();

                // If we have marked deleted elements, we need to verify that `top_candidates` is
                // not empty (since we might have not added any non-deleted element yet).
                if (!top_candidates.empty())
                    lowerBound = top_candidates.top().first;
            }
        }

        // Running the last neighbor outside the loop to avoid prefetching invalid neighbor
        idType candidate_id = node_level.getLinkAtPos(num_links - 1);
        cur_data = next_data;

        if (elements_tags[candidate_id] != visited_tag && !isInProcess(candidate_id)) {

            elements_tags[candidate_id] = visited_tag;

            DistType cur_dist = this->calcDistanceForQuery(cur_data, query_data);
            if (lowerBound > cur_dist || top_candidates.size() < ef) {
                candidate_set.emplace(-cur_dist, candidate_id);

                // Insert the candidate to the top candidates heap only if it is not marked as
                // deleted.
                if (!isMarkedDeleted(candidate_id))
                    emplaceToHeap(top_candidates, cur_dist, candidate_id);

                if (top_candidates.size() > ef)
                    top_candidates.pop();

                // If we have marked deleted elements, we need to verify that `top_candidates` is
                // not empty (since we might have not added any non-deleted element yet).
                if (!top_candidates.empty())
                    lowerBound = top_candidates.top().first;
            }
        }
    }
    unlockNodeLinks(curNodeId);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::processCandidate_RangeSearch(
    idType curNodeId, const void *query_data, size_t layer, double epsilon, tag_t *elements_tags,
    tag_t visited_tag, std::unique_ptr<vecsim_stl::abstract_results_container> &results,
    candidatesMaxHeap<DistType> &candidate_set, DistType dyn_range, DistType radius) const {

    auto *cur_element = getGraphDataByInternalId(curNodeId);
    lockNodeLinks(curNodeId);
    ElementLevelData &node_level = getElementLevelData(cur_element, layer);
    linkListSize num_links = node_level.getNumLinks();

    if (num_links > 0) {

        const char *cur_data, *next_data;
        // Pre-fetch first candidate tag address.
        __builtin_prefetch(elements_tags + node_level.getLinkAtPos(0));
        // Pre-fetch first candidate data block address.
        next_data = getDataByInternalId(node_level.getLinkAtPos(0));
        __builtin_prefetch(next_data);

        for (linkListSize j = 0; j < num_links - 1; j++) {
            idType candidate_id = node_level.getLinkAtPos(j);
            cur_data = next_data;

            // Pre-fetch next candidate tag address.
            __builtin_prefetch(elements_tags + node_level.getLinkAtPos(j + 1));
            // Pre-fetch next candidate data block address.
            next_data = getDataByInternalId(node_level.getLinkAtPos(j + 1));
            __builtin_prefetch(next_data);

            if (elements_tags[candidate_id] == visited_tag || isInProcess(candidate_id))
                continue;

            elements_tags[candidate_id] = visited_tag;

            DistType cur_dist = this->calcDistanceForQuery(cur_data, query_data);
            if (cur_dist < dyn_range) {
                candidate_set.emplace(-cur_dist, candidate_id);

                // If the new candidate is in the requested radius, add it to the results set.
                if (cur_dist <= radius && !isMarkedDeleted(candidate_id)) {
                    results->emplace(getExternalLabel(candidate_id), cur_dist);
                }
            }
        }
        // Running the last candidate outside the loop to avoid prefetching invalid candidate
        idType candidate_id = node_level.getLinkAtPos(num_links - 1);
        cur_data = next_data;

        if (elements_tags[candidate_id] != visited_tag && !isInProcess(candidate_id)) {

            elements_tags[candidate_id] = visited_tag;

            DistType cur_dist = this->calcDistanceForQuery(cur_data, query_data);
            if (cur_dist < dyn_range) {
                candidate_set.emplace(-cur_dist, candidate_id);

                // If the new candidate is in the requested radius, add it to the results set.
                if (cur_dist <= radius && !isMarkedDeleted(candidate_id)) {
                    results->emplace(getExternalLabel(candidate_id), cur_dist);
                }
            }
        }
    }
    unlockNodeLinks(curNodeId);
}

template <typename DataType, typename DistType>
candidatesMaxHeap<DistType>
HNSWIndex<DataType, DistType>::searchLayer(idType ep_id, const void *data_point, size_t layer,
                                           size_t ef) const {

    auto *visited_nodes_handler = getVisitedList();
    tag_t visited_tag = visited_nodes_handler->getFreshTag();

    candidatesMaxHeap<DistType> top_candidates(this->allocator);
    candidatesMaxHeap<DistType> candidate_set(this->allocator);

    DistType lowerBound;
    if (!isMarkedDeleted(ep_id)) {
        DistType dist = this->calcDistanceForQuery(getDataByInternalId(ep_id), data_point);
        lowerBound = dist;
        top_candidates.emplace(dist, ep_id);
        candidate_set.emplace(-dist, ep_id);
    } else {
        lowerBound = std::numeric_limits<DistType>::max();
        candidate_set.emplace(-lowerBound, ep_id);
    }

    visited_nodes_handler->tagNode(ep_id, visited_tag);

    while (!candidate_set.empty()) {
        pair<DistType, idType> curr_el_pair = candidate_set.top();

        if ((-curr_el_pair.first) > lowerBound && top_candidates.size() >= ef) {
            break;
        }
        candidate_set.pop();

        processCandidate(curr_el_pair.second, data_point, layer, ef,
                         visited_nodes_handler->getElementsTags(), visited_tag, top_candidates,
                         candidate_set, lowerBound);
    }

    returnVisitedList(visited_nodes_handler);
    return top_candidates;
}

template <typename DataType, typename DistType>
idType
HNSWIndex<DataType, DistType>::getNeighborsByHeuristic2(candidatesList<DistType> &top_candidates,
                                                        const size_t M) const {
    if (top_candidates.size() < M) {
        return std::min_element(top_candidates.begin(), top_candidates.end(),
                                [](const auto &a, const auto &b) { return a.first < b.first; })
            ->second;
    }
    getNeighborsByHeuristic2_internal<false>(top_candidates, M, nullptr);
    return top_candidates.front().second;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::getNeighborsByHeuristic2(
    candidatesList<DistType> &top_candidates, const size_t M,
    vecsim_stl::vector<idType> &removed_candidates) const {
    getNeighborsByHeuristic2_internal<true>(top_candidates, M, &removed_candidates);
}

template <typename DataType, typename DistType>
template <bool record_removed>
void HNSWIndex<DataType, DistType>::getNeighborsByHeuristic2_internal(
    candidatesList<DistType> &top_candidates, const size_t M,
    vecsim_stl::vector<idType> *removed_candidates) const {
    if (top_candidates.size() < M) {
        return;
    }

    candidatesList<DistType> return_list(this->allocator);
    vecsim_stl::vector<const void *> cached_vectors(this->allocator);
    return_list.reserve(M);
    cached_vectors.reserve(M);
    if constexpr (record_removed) {
        removed_candidates->reserve(top_candidates.size());
    }

    // Sort the candidates by their distance (we don't mind the secondary order (the internal id))
    std::sort(top_candidates.begin(), top_candidates.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    auto current_pair = top_candidates.begin();
    for (; current_pair != top_candidates.end() && return_list.size() < M; ++current_pair) {
        DistType candidate_to_query_dist = current_pair->first;
        bool good = true;
        const void *curr_vector = getDataByInternalId(current_pair->second);

        // a candidate is "good" to become a neighbour, unless we find
        // another item that was already selected to the neighbours set which is closer
        // to both q and the candidate than the distance between the candidate and q.
        for (size_t i = 0; i < return_list.size(); i++) {
            DistType candidate_to_selected_dist =
                this->calcDistance(cached_vectors[i], curr_vector);
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
void HNSWIndex<DataType, DistType>::revisitNeighborConnections(
    size_t level, idType new_node_id, const std::pair<DistType, idType> &neighbor_data,
    ElementLevelData &new_node_level, ElementLevelData &neighbor_level) {
    // Note - expect that node_lock and neighbor_lock are locked at that point.

    // Collect the existing neighbors and the new node as the neighbor's neighbors candidates.
    candidatesList<DistType> candidates(this->allocator);
    candidates.reserve(neighbor_level.getNumLinks() + 1);
    // Add the new node along with the pre-calculated distance to the current neighbor,
    candidates.emplace_back(neighbor_data.first, new_node_id);

    idType selected_neighbor = neighbor_data.second;
    const void *selected_neighbor_data = getDataByInternalId(selected_neighbor);
    for (size_t j = 0; j < neighbor_level.getNumLinks(); j++) {
        candidates.emplace_back(
            this->calcDistance(getDataByInternalId(neighbor_level.getLinkAtPos(j)),
                               selected_neighbor_data),
            neighbor_level.getLinkAtPos(j));
    }

    // Candidates will store the newly selected neighbours (for the neighbor).
    size_t max_M_cur = level ? M : M0;
    vecsim_stl::vector<idType> nodes_to_update(this->allocator);
    getNeighborsByHeuristic2(candidates, max_M_cur, nodes_to_update);

    // Acquire all relevant locks for making the updates for the selected neighbor - all its removed
    // neighbors, along with the neighbors itself and the cur node.
    // but first, we release the node and neighbors lock to avoid deadlocks.
    unlockNodeLinks(new_node_id);
    unlockNodeLinks(selected_neighbor);

    // Check if the new node was selected as a neighbor for the current neighbor.
    // Make sure to add the cur node to the list of nodes to update if it was selected.
    bool cur_node_chosen;
    auto new_node_iter = std::find(nodes_to_update.begin(), nodes_to_update.end(), new_node_id);
    if (new_node_iter != nodes_to_update.end()) {
        cur_node_chosen = false;
    } else {
        cur_node_chosen = true;
        nodes_to_update.push_back(new_node_id);
    }
    nodes_to_update.push_back(selected_neighbor);

    std::sort(nodes_to_update.begin(), nodes_to_update.end());
    size_t nodes_to_update_count = nodes_to_update.size();
    for (size_t i = 0; i < nodes_to_update_count; i++) {
        lockNodeLinks(nodes_to_update[i]);
    }
    size_t neighbour_neighbours_idx = 0;
    bool update_cur_node_required = true;
    for (size_t i = 0; i < neighbor_level.getNumLinks(); i++) {
        if (!std::binary_search(nodes_to_update.begin(), nodes_to_update.end(),
                                neighbor_level.getLinkAtPos(i))) {
            // The neighbor is not in the "to_update" nodes list - leave it as is.
            neighbor_level.setLinkAtPos(neighbour_neighbours_idx++, neighbor_level.getLinkAtPos(i));
            continue;
        }
        if (neighbor_level.getLinkAtPos(i) == new_node_id) {
            // The new node got into the neighbor's neighbours - this means there was an update in
            // another thread during between we released and reacquire the locks - leave it
            // as is.
            neighbor_level.setLinkAtPos(neighbour_neighbours_idx++, neighbor_level.getLinkAtPos(i));
            update_cur_node_required = false;
            continue;
        }
        // Now we know that we are looking at a node to be removed from the neighbor's neighbors.
        mutuallyRemoveNeighborAtPos(neighbor_level, level, selected_neighbor, i);
    }

    if (update_cur_node_required && new_node_level.getNumLinks() < max_M_cur &&
        !isMarkedDeleted(new_node_id) && !isMarkedDeleted(selected_neighbor)) {
        // update the connection between the new node and the neighbor.
        new_node_level.appendLink(selected_neighbor);
        if (cur_node_chosen && neighbour_neighbours_idx < max_M_cur) {
            // connection is mutual - both new node and the selected neighbor in each other's list.
            neighbor_level.setLinkAtPos(neighbour_neighbours_idx++, new_node_id);
        } else {
            // unidirectional connection - put the new node in the neighbour's incoming edges.
            neighbor_level.newIncomingUnidirectionalEdge(new_node_id);
        }
    }
    // Done updating the neighbor's neighbors.
    neighbor_level.setNumLinks(neighbour_neighbours_idx);
    for (size_t i = 0; i < nodes_to_update_count; i++) {
        unlockNodeLinks(nodes_to_update[i]);
    }
}

template <typename DataType, typename DistType>
idType HNSWIndex<DataType, DistType>::mutuallyConnectNewElement(
    idType new_node_id, candidatesMaxHeap<DistType> &top_candidates, size_t level) {

    // The maximum number of neighbors allowed for an existing neighbor (not new).
    size_t max_M_cur = level ? M : M0;

    // Filter the top candidates to the selected neighbors by the algorithm heuristics.
    // First, we need to copy the top candidates to a vector.
    candidatesList<DistType> top_candidates_list(this->allocator);
    top_candidates_list.insert(top_candidates_list.end(), top_candidates.begin(),
                               top_candidates.end());
    // Use the heuristic to filter the top candidates, and get the next closest entry point.
    idType next_closest_entry_point = getNeighborsByHeuristic2(top_candidates_list, M);
    assert(top_candidates_list.size() <= M &&
           "Should be not be more than M candidates returned by the heuristic");

    auto *new_node_level = getGraphDataByInternalId(new_node_id);
    ElementLevelData &new_node_level_data = getElementLevelData(new_node_level, level);
    assert(new_node_level_data.getNumLinks() == 0 &&
           "The newly inserted element should have blank link list");

    for (auto &neighbor_data : top_candidates_list) {
        idType selected_neighbor = neighbor_data.second; // neighbor's id
        auto *neighbor_graph_data = getGraphDataByInternalId(selected_neighbor);
        if (new_node_id < selected_neighbor) {
            lockNodeLinks(new_node_id);
            lockNodeLinks(selected_neighbor);
        } else {
            lockNodeLinks(selected_neighbor);
            lockNodeLinks(new_node_id);
        }

        // validations...
        assert(new_node_level_data.getNumLinks() <= max_M_cur && "Neighbors number exceeds limit");
        assert(selected_neighbor != new_node_id && "Trying to connect an element to itself");

        // Revalidate the updated count - this may change between iterations due to releasing the
        // lock.
        if (new_node_level_data.getNumLinks() == max_M_cur) {
            // The new node cannot add more neighbors
            this->log(VecSimCommonStrings::LOG_DEBUG_STRING,
                      "Couldn't add all chosen neighbors upon inserting a new node");
            unlockNodeLinks(new_node_id);
            unlockNodeLinks(selected_neighbor);
            break;
        }

        // If one of the two nodes has already deleted - skip the operation.
        if (isMarkedDeleted(new_node_id) || isMarkedDeleted(selected_neighbor)) {
            unlockNodeLinks(new_node_id);
            unlockNodeLinks(selected_neighbor);
            continue;
        }

        ElementLevelData &neighbor_level_data = getElementLevelData(neighbor_graph_data, level);

        // if the neighbor's neighbors list has the capacity to add the new node, make the update
        // and finish.
        if (neighbor_level_data.getNumLinks() < max_M_cur) {
            new_node_level_data.appendLink(selected_neighbor);
            neighbor_level_data.appendLink(new_node_id);
            unlockNodeLinks(new_node_id);
            unlockNodeLinks(selected_neighbor);
            continue;
        }

        // Otherwise - we need to re-evaluate the neighbor's neighbors.
        // We collect all the existing neighbors and the new node as candidates, and mutually update
        // the neighbor's neighbors. We also release the acquired locks inside this call.
        revisitNeighborConnections(level, new_node_id, neighbor_data, new_node_level_data,
                                   neighbor_level_data);
    }
    return next_closest_entry_point;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::repairConnectionsForDeletion(
    idType element_internal_id, idType neighbour_id, ElementLevelData &node_level,
    ElementLevelData &neighbor_level, size_t level, vecsim_stl::vector<bool> &neighbours_bitmap) {

    if (isMarkedDeleted(neighbour_id)) {
        // Just remove the deleted element from the neighbor's neighbors list. No need to repair as
        // this change is temporary, this neighbor is about to be removed from the graph as well.
        neighbor_level.removeLink(element_internal_id);
        return;
    }

    // Add the deleted element's neighbour's original neighbors in the candidates.
    vecsim_stl::vector<idType> candidate_ids(this->allocator);
    candidate_ids.reserve(node_level.getNumLinks() + neighbor_level.getNumLinks());
    vecsim_stl::vector<bool> neighbour_orig_neighbours_set(curElementCount, false, this->allocator);
    for (size_t j = 0; j < neighbor_level.getNumLinks(); j++) {
        idType cand = neighbor_level.getLinkAtPos(j);
        neighbour_orig_neighbours_set[cand] = true;
        // Don't add the removed element to the candidates, nor nodes that are neighbors of the
        // original deleted element and will also be added to the candidates set.
        if (cand != element_internal_id && !neighbours_bitmap[cand]) {
            candidate_ids.push_back(cand);
        }
    }
    // Put the deleted element's neighbours in the candidates.
    for (size_t j = 0; j < node_level.getNumLinks(); j++) {
        // Don't put the neighbor itself in his own candidates and nor marked deleted elements that
        // were not neighbors before.
        idType cand = node_level.getLinkAtPos(j);
        if (cand != neighbour_id &&
            (!isMarkedDeleted(cand) || neighbour_orig_neighbours_set[cand])) {
            candidate_ids.push_back(cand);
        }
    }

    size_t Mcurmax = level ? M : M0;
    if (candidate_ids.size() > Mcurmax) {
        // We need to filter the candidates by the heuristic.
        candidatesList<DistType> candidates(this->allocator);
        candidates.reserve(candidate_ids.size());
        auto neighbours_data = getDataByInternalId(neighbour_id);
        for (auto candidate_id : candidate_ids) {
            candidates.emplace_back(
                this->calcDistance(getDataByInternalId(candidate_id), neighbours_data),
                candidate_id);
        }

        candidate_ids.clear();
        auto &not_chosen_candidates = candidate_ids; // rename and reuse the vector
        getNeighborsByHeuristic2(candidates, Mcurmax, not_chosen_candidates);

        neighbor_level.setLinks(candidates);

        // Update unidirectional incoming edges w.r.t. the edges that were removed.
        for (auto node_id : not_chosen_candidates) {
            if (neighbour_orig_neighbours_set[node_id]) {
                // if the node id (the neighbour's neighbour to be removed)
                // wasn't pointing to the neighbour (edge was one directional),
                // we should remove it from the node's incoming edges.
                // otherwise, edge turned from bidirectional to one directional,
                // and it should be saved in the neighbor's incoming edges.
                auto &node_level_data = getElementLevelData(node_id, level);
                if (!node_level_data.removeIncomingUnidirectionalEdgeIfExists(neighbour_id)) {
                    neighbor_level.newIncomingUnidirectionalEdge(node_id);
                }
            }
        }
    } else {
        // We don't need to filter the candidates - just update the edges.
        neighbor_level.setLinks(candidate_ids);
    }

    // Updates for the new edges created
    for (size_t i = 0; i < neighbor_level.getNumLinks(); i++) {
        idType node_id = neighbor_level.getLinkAtPos(i);
        if (!neighbour_orig_neighbours_set[node_id]) {
            ElementLevelData &node_level = getElementLevelData(node_id, level);
            // If the node has an edge to the neighbour as well, remove it from the incoming nodes
            // of the neighbour. Otherwise, we need to update the edge as unidirectional incoming.
            bool bidirectional_edge = false;
            for (size_t j = 0; j < node_level.getNumLinks(); j++) {
                if (node_level.getLinkAtPos(j) == neighbour_id) {
                    // Swap the last element with the current one (equivalent to removing the
                    // neighbor from the list) - this should always succeed and return true.
                    bool res = neighbor_level.removeIncomingUnidirectionalEdgeIfExists(node_id);
                    (void)res;
                    assert(res && "The edge should be in the incoming unidirectional edges");
                    bidirectional_edge = true;
                    break;
                }
            }
            if (!bidirectional_edge) {
                node_level.newIncomingUnidirectionalEdge(neighbour_id);
            }
        }
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::replaceEntryPoint() {
    idType old_entry_point_id = entrypointNode;
    auto *old_entry_point = getGraphDataByInternalId(old_entry_point_id);

    // Sets an (arbitrary) new entry point, after deleting the current entry point.
    while (old_entry_point_id == entrypointNode) {
        // Use volatile for this variable, so that in case we would have to busy wait for this
        // element to finish its indexing, the compiler will not use optimizations. Otherwise,
        // the compiler might evaluate 'isInProcess(candidate_in_process)' once instead of calling
        // it multiple times in a busy wait manner, and we'll run into an infinite loop if the
        // candidate is in process when we reach the loop.
        volatile idType candidate_in_process = INVALID_ID;

        // Go over the entry point's neighbors at the top level.
        lockNodeLinks(old_entry_point_id);
        ElementLevelData &old_ep_level = getElementLevelData(old_entry_point, maxLevel);
        // Tries to set the (arbitrary) first neighbor as the entry point which is not deleted,
        // if exists.
        for (size_t i = 0; i < old_ep_level.getNumLinks(); i++) {
            if (!isMarkedDeleted(old_ep_level.getLinkAtPos(i))) {
                if (!isInProcess(old_ep_level.getLinkAtPos(i))) {
                    entrypointNode = old_ep_level.getLinkAtPos(i);
                    unlockNodeLinks(old_entry_point_id);
                    return;
                } else {
                    // Store this candidate which is currently being inserted into the graph in
                    // case we won't find other candidate at the top level.
                    candidate_in_process = old_ep_level.getLinkAtPos(i);
                }
            }
        }
        unlockNodeLinks(old_entry_point_id);

        // If there is no neighbors in the current level, check for any vector at
        // this level to be the new entry point.
        idType cur_id = 0;
        for (DataBlock &graph_data_block : graphDataBlocks) {
            size_t size = graph_data_block.getLength();
            for (size_t i = 0; i < size; i++) {
                auto cur_element = (ElementGraphData *)graph_data_block.getElement(i);
                if (cur_element->toplevel == maxLevel && cur_id != old_entry_point_id &&
                    !isMarkedDeleted(cur_id)) {
                    // Found a non element in the current max level.
                    if (!isInProcess(cur_id)) {
                        entrypointNode = cur_id;
                        return;
                    } else if (candidate_in_process == INVALID_ID) {
                        // This element is still in process, and there hasn't been another candidate
                        // in process that has found in this level.
                        candidate_in_process = cur_id;
                    }
                }
                cur_id++;
            }
        }
        // If we only found candidates which are in process at this level, do busy wait until they
        // are done being processed (this should happen in very rare cases...). Since
        // candidate_in_process was declared volatile, we can be sure that isInProcess is called in
        // every iteration.
        if (candidate_in_process != INVALID_ID) {
            while (isInProcess(candidate_in_process))
                ;
            entrypointNode = candidate_in_process;
            return;
        }
        // If we didn't find any vector at the top level, decrease the maxLevel and try again,
        // until we find a new entry point, or the index is empty.
        assert(old_entry_point_id == entrypointNode);
        maxLevel--;
        if ((int)maxLevel < 0) {
            maxLevel = HNSW_INVALID_LEVEL;
            entrypointNode = INVALID_ID;
        }
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::SwapLastIdWithDeletedId(idType element_internal_id,
                                                            ElementGraphData *last_element,
                                                            const void *last_element_data) {
    // Swap label - this is relevant when the last element's label exists (it is not marked as
    // deleted).
    if (!isMarkedDeleted(curElementCount)) {
        replaceIdOfLabel(getExternalLabel(curElementCount), element_internal_id, curElementCount);
    }

    // Swap neighbours
    for (size_t level = 0; level <= last_element->toplevel; level++) {
        auto &cur_level = getElementLevelData(last_element, level);

        // Go over the neighbours that also points back to the last element whose is going to
        // change, and update the id.
        for (size_t i = 0; i < cur_level.getNumLinks(); i++) {
            idType neighbour_id = cur_level.getLinkAtPos(i);
            ElementLevelData &neighbor_level = getElementLevelData(neighbour_id, level);

            bool bidirectional_edge = false;
            for (size_t j = 0; j < neighbor_level.getNumLinks(); j++) {
                // if the edge is bidirectional, update for this neighbor
                if (neighbor_level.getLinkAtPos(j) == curElementCount) {
                    bidirectional_edge = true;
                    neighbor_level.setLinkAtPos(j, element_internal_id);
                    break;
                }
            }

            // If this edge is uni-directional, we should update the id in the neighbor's
            // incoming edges.
            if (!bidirectional_edge) {
                neighbor_level.swapNodeIdInIncomingEdges(curElementCount, element_internal_id);
            }
        }

        // Next, go over the rest of incoming edges (the ones that are not bidirectional) and make
        // updates.
        for (auto incoming_edge : cur_level.getIncomingEdges()) {
            ElementLevelData &incoming_neighbor_level = getElementLevelData(incoming_edge, level);
            for (size_t j = 0; j < incoming_neighbor_level.getNumLinks(); j++) {
                if (incoming_neighbor_level.getLinkAtPos(j) == curElementCount) {
                    incoming_neighbor_level.setLinkAtPos(j, element_internal_id);
                    break;
                }
            }
        }
    }

    // Move the last element's data to the deleted element's place
    auto element = getGraphDataByInternalId(element_internal_id);
    memcpy((void *)element, last_element, this->elementGraphDataSize);

    auto data = getDataByInternalId(element_internal_id);
    memcpy((void *)data, last_element_data, this->getStoredDataSize());

    this->idToMetaData[element_internal_id] = this->idToMetaData[curElementCount];

    if (curElementCount == this->entrypointNode) {
        this->entrypointNode = element_internal_id;
    }
}

// This function is greedily searching for the closest candidate to the given data point at the
// given level, starting at the given node. It sets `curObj` to the closest node found, and
// `curDist` to the distance to this node. If `running_query` is true, the search will check for
// timeout and return if it has occurred. `timeoutCtx` and `rc` must be valid if `running_query` is
// true. *Note that we assume that level is higher than 0*. Also, if we're not running a query (we
// are searching neighbors for a new vector), then bestCand should be a non-deleted element!
template <typename DataType, typename DistType>
template <bool running_query>
void HNSWIndex<DataType, DistType>::greedySearchLevel(const void *vector_data, size_t level,
                                                      idType &bestCand, DistType &curDist,
                                                      void *timeoutCtx,
                                                      VecSimQueryReply_Code *rc) const {
    bool changed;
    // Don't allow choosing a deleted node as an entry point upon searching for neighbors
    // candidates (that is, we're NOT running a query, but inserting a new vector).
    idType bestNonDeletedCand = bestCand;

    do {
        if (running_query && VECSIM_TIMEOUT(timeoutCtx)) {
            *rc = VecSim_QueryReply_TimedOut;
            bestCand = INVALID_ID;
            return;
        }

        changed = false;
        idType locked_id = bestCand;
        auto *element = getGraphDataByInternalId(bestCand);
        lockNodeLinks(locked_id);
        ElementLevelData &node_level_data = getElementLevelData(element, level);

        for (int i = 0; i < node_level_data.getNumLinks(); i++) {
            idType candidate = node_level_data.getLinkAtPos(i);
            assert(candidate < this->curElementCount && "candidate error: out of index range");
            if (isInProcess(candidate)) {
                continue;
            }
            DistType d = this->calcDistanceForQuery(getDataByInternalId(candidate), vector_data);
            if (d < curDist) {
                curDist = d;
                bestCand = candidate;
                changed = true;
                // Run this code only for non-query code - update the best non deleted cand as well.
                // Upon running a query, we don't mind having a deleted element as an entry point
                // for the next level, as eventually we return non-deleted elements in level 0.
                if (!running_query && !isMarkedDeleted(candidate)) {
                    bestNonDeletedCand = bestCand;
                }
            }
        }
        unlockNodeLinks(locked_id);
    } while (changed);
    if (!running_query) {
        bestCand = bestNonDeletedCand;
    }
}

template <typename DataType, typename DistType>
vecsim_stl::vector<graphNodeType>
HNSWIndex<DataType, DistType>::safeCollectAllNodeIncomingNeighbors(idType node_id) const {
    vecsim_stl::vector<graphNodeType> incoming_neighbors(this->allocator);

    auto element = getGraphDataByInternalId(node_id);
    for (size_t level = 0; level <= element->toplevel; level++) {
        // Save the node neighbor's in the current level while holding its neighbors lock.
        lockNodeLinks(node_id);
        auto &node_level_data = getElementLevelData(element, level);
        // Store the deleted element's neighbours.
        auto neighbors_copy = node_level_data.copyLinks();
        unlockNodeLinks(node_id);

        // Go over the neighbours and collect tho ones that also points back to the removed node.
        for (auto neighbour_id : neighbors_copy) {
            // Hold the neighbor's lock while we are going over its neighbors.
            auto *neighbor = getGraphDataByInternalId(neighbour_id);
            lockNodeLinks(neighbour_id);
            ElementLevelData &neighbour_level_data = getElementLevelData(neighbor, level);

            for (size_t j = 0; j < neighbour_level_data.getNumLinks(); j++) {
                // A bidirectional edge was found - this connection should be repaired.
                if (neighbour_level_data.getLinkAtPos(j) == node_id) {
                    incoming_neighbors.emplace_back(neighbour_id, (unsigned short)level);
                    break;
                }
            }
            unlockNodeLinks(neighbour_id);
        }

        // Next, collect the rest of incoming edges (the ones that are not bidirectional) in the
        // current level to repair them.
        lockNodeLinks(node_id);
        for (auto incoming_edge : node_level_data.getIncomingEdges()) {
            incoming_neighbors.emplace_back(incoming_edge, (unsigned short)level);
        }
        unlockNodeLinks(node_id);
    }
    return incoming_neighbors;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::resizeIndexCommon(size_t new_max_elements) {
    assert(new_max_elements % this->blockSize == 0 &&
           "new_max_elements must be a multiple of blockSize");
    this->log(VecSimCommonStrings::LOG_VERBOSE_STRING, "Resizing HNSW index from %zu to %zu",
              idToMetaData.capacity(), new_max_elements);
    resizeLabelLookup(new_max_elements);
    visitedNodesHandlerPool.resize(new_max_elements);
    elementLocks.resize(new_max_elements);
    elementLocks.shrink_to_fit();
    assert(idToMetaData.capacity() == idToMetaData.size());
    idToMetaData.resize(new_max_elements);
    idToMetaData.shrink_to_fit();
    assert(idToMetaData.capacity() == idToMetaData.size());
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::growByBlock() {
    assert(this->maxElements % this->blockSize == 0);
    assert(this->maxElements == indexSize());
    assert(graphDataBlocks.size() == this->maxElements / this->blockSize);
    assert(idToMetaData.capacity() == maxElements ||
           idToMetaData.capacity() == maxElements + this->blockSize);

    this->log(VecSimCommonStrings::LOG_VERBOSE_STRING,
              "Updating HNSW index capacity from %zu to %zu", maxElements,
              maxElements + this->blockSize);
    maxElements += this->blockSize;

    graphDataBlocks.emplace_back(this->blockSize, this->elementGraphDataSize, this->allocator);

    if (idToMetaData.capacity() == indexSize()) {
        resizeIndexCommon(maxElements);
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::shrinkByBlock() {
    assert(this->maxElements >= this->blockSize);
    assert(this->maxElements % this->blockSize == 0);

    if (indexSize() % this->blockSize == 0) {
        this->log(VecSimCommonStrings::LOG_VERBOSE_STRING,
                  "Updating HNSW index capacity from %zu to %zu", maxElements,
                  maxElements - this->blockSize);
        graphDataBlocks.pop_back();
        assert(graphDataBlocks.size() == indexSize() / this->blockSize);

        // assuming idToMetaData reflects the capacity of the heavy reallocation containers.
        if (indexSize() == 0) {
            resizeIndexCommon(0);
        } else if (idToMetaData.capacity() >= (indexSize() + 2 * this->blockSize)) {
            assert(this->maxElements + this->blockSize == idToMetaData.capacity());
            resizeIndexCommon(idToMetaData.capacity() - this->blockSize);
        }

        // Take the lower bound into account.
        maxElements -= this->blockSize;
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::mutuallyUpdateForRepairedNode(
    idType node_id, size_t level, vecsim_stl::vector<idType> &nodes_to_update,
    vecsim_stl::vector<idType> &chosen_neighbors, size_t max_M_cur) {

    // Acquire the required locks for the updates, after sorting the nodes to update
    // (to avoid deadlocks)
    nodes_to_update.push_back(node_id);
    std::sort(nodes_to_update.begin(), nodes_to_update.end());
    size_t nodes_to_update_count = nodes_to_update.size();
    for (size_t i = 0; i < nodes_to_update_count; i++) {
        lockNodeLinks(nodes_to_update[i]);
    }

    ElementLevelData &node_level = getElementLevelData(node_id, level);

    // Perform mutual updates: go over the node's neighbors and overwrite the neighbors to remove
    // that are still exist.
    size_t node_neighbors_idx = 0;
    for (size_t i = 0; i < node_level.getNumLinks(); i++) {
        if (!std::binary_search(nodes_to_update.begin(), nodes_to_update.end(),
                                node_level.getLinkAtPos(i))) {
            // The repaired node added a new neighbor that we didn't account for before in the
            // meantime - leave it as is.
            node_level.setLinkAtPos(node_neighbors_idx++, node_level.getLinkAtPos(i));
            continue;
        }
        // Check if the current neighbor is in the chosen neighbors list, and remove it from there
        // if so.
        if (chosen_neighbors.remove(node_level.getLinkAtPos(i))) {
            // A chosen neighbor is already connected to the node - leave it as is.
            node_level.setLinkAtPos(node_neighbors_idx++, node_level.getLinkAtPos(i));
            continue;
        }
        // Now we know that we are looking at a neighbor that needs to be removed.
        mutuallyRemoveNeighborAtPos(node_level, level, node_id, i);
    }

    // Go over the chosen new neighbors that are not connected yet and perform updates.
    for (auto chosen_id : chosen_neighbors) {
        if (node_neighbors_idx == max_M_cur) {
            // Cannot add more new neighbors, we reached the capacity.
            this->log(VecSimCommonStrings::LOG_DEBUG_STRING,
                      "Couldn't add all the chosen new nodes upon updating %u, as we reached the"
                      " maximum number of neighbors per node",
                      node_id);
            break;
        }
        // We don't add new neighbors for deleted nodes - if node_id is deleted we can finish.
        // Also, don't add new neighbors to a node who is currently being indexed in parallel, as it
        // may choose the same element as its neighbor right after the repair is done and connect it
        // to it, and have a duplicate neighbor as a result.
        if (isMarkedDeleted(node_id) || isInProcess(node_id)) {
            break;
        }
        // If this specific new neighbor is deleted, we don't add this connection and continue.
        // Also, don't add a new node whose being indexed in parallel, as it may choose this node
        // as its neighbor and create a double connection (then this node will have a duplicate
        // neighbor).
        if (isMarkedDeleted(chosen_id) || isInProcess(chosen_id)) {
            continue;
        }
        node_level.setLinkAtPos(node_neighbors_idx++, chosen_id);
        // If the node is in the chosen new node incoming edges, there is a unidirectional
        // connection from the chosen node to the repaired node that turns into bidirectional. Then,
        // remove it from the incoming edges set. Otherwise, the edge is created unidirectional, so
        // we add it to the unidirectional edges set. Note: we assume that all updates occur
        // mutually and atomically, then can rely on this assumption.
        auto &chosen_node_level_data = getElementLevelData(chosen_id, level);
        if (!node_level.removeIncomingUnidirectionalEdgeIfExists(chosen_id)) {
            chosen_node_level_data.newIncomingUnidirectionalEdge(node_id);
        }
    }
    // Done updating the node's neighbors.
    node_level.setNumLinks(node_neighbors_idx);
    for (size_t i = 0; i < nodes_to_update_count; i++) {
        unlockNodeLinks(nodes_to_update[i]);
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::repairNodeConnections(idType node_id, size_t level) {

    vecsim_stl::vector<idType> neighbors_candidate_ids(this->allocator);
    // Use bitmaps for fast accesses:
    // node_orig_neighbours_set is used to differentiate between the neighbors that will *not* be
    // selected by the heuristics - only the ones that were originally neighbors should be removed.
    vecsim_stl::vector<bool> node_orig_neighbours_set(maxElements, false, this->allocator);
    // neighbors_candidates_set is used to store the nodes that were already collected as
    // candidates, so we will not collect them again as candidates if we run into them from another
    // path.
    vecsim_stl::vector<bool> neighbors_candidates_set(maxElements, false, this->allocator);
    vecsim_stl::vector<idType> deleted_neighbors(this->allocator);

    // Go over the repaired node neighbors, collect the non-deleted ones to be neighbors candidates
    // after the repair as well.
    auto *element = getGraphDataByInternalId(node_id);
    lockNodeLinks(node_id);
    ElementLevelData &node_level_data = getElementLevelData(element, level);
    for (size_t j = 0; j < node_level_data.getNumLinks(); j++) {
        node_orig_neighbours_set[node_level_data.getLinkAtPos(j)] = true;
        // Don't add the removed element to the candidates.
        if (isMarkedDeleted(node_level_data.getLinkAtPos(j))) {
            deleted_neighbors.push_back(node_level_data.getLinkAtPos(j));
            continue;
        }
        neighbors_candidates_set[node_level_data.getLinkAtPos(j)] = true;
        neighbors_candidate_ids.push_back(node_level_data.getLinkAtPos(j));
    }
    unlockNodeLinks(node_id);

    // If there are not deleted neighbors at that point the repair job has already been made by
    // another parallel job, and there is no need to repair the node anymore.
    if (deleted_neighbors.empty()) {
        return;
    }

    // Hold 3 sets of nodes - all the original neighbors at that point to later (potentially)
    // update, subset of these which are the chosen neighbors nodes, and a subset of the original
    // neighbors that are going to be removed.
    vecsim_stl::vector<idType> nodes_to_update(this->allocator);
    vecsim_stl::vector<idType> chosen_neighbors(this->allocator);

    // Go over the deleted nodes and collect their neighbors to the candidates set.
    for (idType deleted_neighbor_id : deleted_neighbors) {
        nodes_to_update.push_back(deleted_neighbor_id);

        auto *neighbor = getGraphDataByInternalId(deleted_neighbor_id);
        lockNodeLinks(deleted_neighbor_id);
        ElementLevelData &neighbor_level_data = getElementLevelData(neighbor, level);

        for (size_t j = 0; j < neighbor_level_data.getNumLinks(); j++) {
            // Don't add removed elements to the candidates, nor nodes that are already in the
            // candidates set, nor the original node to repair itself.
            if (isMarkedDeleted(neighbor_level_data.getLinkAtPos(j)) ||
                neighbors_candidates_set[neighbor_level_data.getLinkAtPos(j)] ||
                neighbor_level_data.getLinkAtPos(j) == node_id) {
                continue;
            }
            neighbors_candidates_set[neighbor_level_data.getLinkAtPos(j)] = true;
            neighbors_candidate_ids.push_back(neighbor_level_data.getLinkAtPos(j));
        }
        unlockNodeLinks(deleted_neighbor_id);
    }

    size_t max_M_cur = level ? M : M0;
    if (neighbors_candidate_ids.size() > max_M_cur) {
        // We have more candidates than the maximum number of neighbors, so we need to select which
        // ones to keep. We use the heuristic to select the neighbors, and then remove the ones that
        // were not originally neighbors.
        candidatesList<DistType> neighbors_candidates(this->allocator);
        neighbors_candidates.reserve(neighbors_candidate_ids.size());
        const void *node_data = getDataByInternalId(node_id);
        for (idType candidate : neighbors_candidate_ids) {
            neighbors_candidates.emplace_back(
                this->calcDistance(getDataByInternalId(candidate), node_data), candidate);
        }
        vecsim_stl::vector<idType> not_chosen_neighbors(this->allocator);
        getNeighborsByHeuristic2(neighbors_candidates, max_M_cur, not_chosen_neighbors);

        for (idType not_chosen_neighbor : not_chosen_neighbors) {
            if (node_orig_neighbours_set[not_chosen_neighbor]) {
                nodes_to_update.push_back(not_chosen_neighbor);
            }
        }

        for (auto &neighbor : neighbors_candidates) {
            chosen_neighbors.push_back(neighbor.second);
            nodes_to_update.push_back(neighbor.second);
        }
    } else {
        // We have less candidates than the maximum number of neighbors, so we choose them all, and
        // extend the nodes to update with them.
        chosen_neighbors.swap(neighbors_candidate_ids);
        nodes_to_update.insert(nodes_to_update.end(), chosen_neighbors.begin(),
                               chosen_neighbors.end());
    }

    // Perform the actual updates for the node and the impacted neighbors while holding the nodes'
    // locks.
    mutuallyUpdateForRepairedNode(node_id, level, nodes_to_update, chosen_neighbors, max_M_cur);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::mutuallyRemoveNeighborAtPos(ElementLevelData &node_level,
                                                                size_t level, idType node_id,
                                                                size_t pos) {
    // Now we know that we are looking at a neighbor that needs to be removed.
    auto removed_node = node_level.getLinkAtPos(pos);
    ElementLevelData &removed_node_level = getElementLevelData(removed_node, level);
    // Perform the mutual update:
    // if the removed node id (the node's neighbour to be removed)
    // wasn't pointing to the node (i.e., the edge was uni-directional),
    // we should remove the current neighbor from the node's incoming edges.
    // otherwise, the edge turned from bidirectional to uni-directional, so we insert it to the
    // neighbour's incoming edges set. Note: we assume that every update is performed atomically
    // mutually, so it should be sufficient to look at the removed node's incoming edges set
    // alone.
    if (!removed_node_level.removeIncomingUnidirectionalEdgeIfExists(node_id)) {
        node_level.newIncomingUnidirectionalEdge(removed_node);
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::insertElementToGraph(idType element_id,
                                                         size_t element_max_level,
                                                         idType entry_point,
                                                         size_t global_max_level,
                                                         const void *vector_data) {

    idType curr_element = entry_point;
    DistType cur_dist = std::numeric_limits<DistType>::max();
    size_t max_common_level;
    if (element_max_level < global_max_level) {
        max_common_level = element_max_level;
        cur_dist = this->calcDistanceForQuery(getDataByInternalId(curr_element), vector_data);
        for (auto level = static_cast<int>(global_max_level);
             level > static_cast<int>(element_max_level); level--) {
            // this is done for the levels which are above the max level
            // to which we are going to insert the new element. We do
            // a greedy search in the graph starting from the entry point
            // at each level, and move on with the closest element we can find.
            // When there is no improvement to do, we take a step down.
            greedySearchLevel<false>(vector_data, level, curr_element, cur_dist);
        }
    } else {
        max_common_level = global_max_level;
    }

    for (auto level = static_cast<int>(max_common_level); level >= 0; level--) {
        candidatesMaxHeap<DistType> top_candidates =
            searchLayer(curr_element, vector_data, level, efConstruction);
        // If the entry point was marked deleted between iterations, we may receive an empty
        // candidates set.
        if (!top_candidates.empty()) {
            curr_element = mutuallyConnectNewElement(element_id, top_candidates, level);
        }
    }
}

/**
 * Ctor / Dtor
 */
/* typedef struct {
    VecSimType type;     // Datatype to index.
    size_t dim;          // Vector's dimension.
    VecSimMetric metric; // Distance metric to use in the index.
    size_t initialCapacity;  // Deprecated and not respected.
    size_t blockSize;
    size_t M;
    size_t efConstruction;
    size_t efRuntime;
    double epsilon;
} HNSWParams; */
template <typename DataType, typename DistType>
HNSWIndex<DataType, DistType>::HNSWIndex(const HNSWParams *params,
                                         const AbstractIndexInitParams &abstractInitParams,
                                         const IndexComponents<DataType, DistType> &components,
                                         size_t random_seed)
    : VecSimIndexAbstract<DataType, DistType>(abstractInitParams, components),
      VecSimIndexTombstone(), maxElements(0), graphDataBlocks(this->allocator),
      elementLocks(this->allocator), idToMetaData(this->allocator),
      visitedNodesHandlerPool(0, this->allocator) {

    M = params->M ? params->M : HNSW_DEFAULT_M;
    M0 = M * 2;
    if (M0 > UINT16_MAX)
        throw std::runtime_error("HNSW index parameter M is too large: argument overflow");

    efConstruction = params->efConstruction ? params->efConstruction : HNSW_DEFAULT_EF_C;
    efConstruction = std::max(efConstruction, M);
    ef = params->efRuntime ? params->efRuntime : HNSW_DEFAULT_EF_RT;
    epsilon = params->epsilon > 0.0 ? params->epsilon : HNSW_DEFAULT_EPSILON;

    curElementCount = 0;
    numMarkedDeleted = 0;

    // initializations for special treatment of the first node
    entrypointNode = INVALID_ID;
    maxLevel = HNSW_INVALID_LEVEL;

    if (M <= 1)
        throw std::runtime_error("HNSW index parameter M cannot be 1");
    mult = 1 / log(1.0 * M);
    levelGenerator.seed(random_seed);

    elementGraphDataSize = sizeof(ElementGraphData) + sizeof(idType) * M0;
    levelDataSize = sizeof(ElementLevelData) + sizeof(idType) * M;
}

template <typename DataType, typename DistType>
HNSWIndex<DataType, DistType>::~HNSWIndex() {
    for (idType id = 0; id < curElementCount; id++) {
        getGraphDataByInternalId(id)->destroy(this->levelDataSize, this->allocator);
    }
}

/**
 * Index API functions
 */

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::isolateDeletedElement(idType internalId) {
    assert(isMarkedDeleted(internalId) && "Only a marked-deleted element may be isolated");
    auto element = getGraphDataByInternalId(internalId);
    for (size_t level = 0; level <= element->toplevel; level++) {
        // Collect the elements this one shares an edge with at this level, in either direction. No
        // edge can be added to a deleted element, so this set only shrinks from here on (a repair
        // job of another element may still remove an edge concurrently).
        lockNodeLinks(internalId);
        ElementLevelData &level_data = getElementLevelData(element, level);
        auto others = level_data.copyLinks();
        others.insert(others.end(), level_data.getIncomingEdges().begin(),
                      level_data.getIncomingEdges().end());
        unlockNodeLinks(internalId);

        for (idType other_id : others) {
            // Remove both sides of the edge as one atomic step, holding the two elements' locks in
            // ascending id order (as every other multi-lock site here does, to avoid deadlocks).
            // Doing it mutually keeps the "an edge is recorded on exactly one side" invariant true
            // at every observable point, which is what lets `mutuallyRemoveNeighborAtPos` tell a
            // bidirectional edge from a unidirectional one by the record alone.
            idType first = std::min(internalId, other_id);
            idType second = std::max(internalId, other_id);
            lockNodeLinks(first);
            lockNodeLinks(second);

            ElementLevelData &other = getElementLevelData(other_id, level);
            bool points_to_other = hasLink(level_data, other_id);
            bool other_points_here = hasLink(other, internalId);

            if (points_to_other && other_points_here) {
                // Bidirectional, so neither side recorded it as an incoming edge - just drop both
                // links. Only two deleted elements can still point at each other at this stage:
                // neither of them gets a repair job for the other's deletion.
                assert(isMarkedDeleted(other_id) &&
                       "a live element still points to a fully repaired deleted element");
                level_data.removeLink(other_id);
                other.removeLink(internalId);
            } else if (points_to_other) {
                // Unidirectional out - the other side recorded it as an incoming edge.
                level_data.removeLink(other_id);
                bool res = other.removeIncomingUnidirectionalEdgeIfExists(internalId);
                (void)res;
                assert(res && "The edge should be in the incoming unidirectional edges");
            } else if (other_points_here) {
                // Unidirectional in - recorded as an incoming edge here. As above, at this stage it
                // can only come from another deleted element.
                assert(isMarkedDeleted(other_id) &&
                       "a live element still points to a fully repaired deleted element");
                other.removeLink(internalId);
                bool res = level_data.removeIncomingUnidirectionalEdgeIfExists(other_id);
                (void)res;
                assert(res && "The edge should be in the incoming unidirectional edges");
            }
            // Else the edge is already gone - a repair job of another element got to it first.

            unlockNodeLinks(second);
            unlockNodeLinks(first);
        }

#ifdef BUILD_TESTS
        // Sanity check - every edge of this element at this level was taken out above.
        lockNodeLinks(internalId);
        assert(level_data.getNumLinks() == 0 && level_data.getIncomingEdges().empty() &&
               "the element should have no edge left at this level");
        unlockNodeLinks(internalId);
#endif
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::disposeElementData(idType internalId) {
    auto element = getGraphDataByInternalId(internalId);

    // Remove the deleted id form the relevant incoming edges sets in which it appears. For an
    // asynchronously deleted element there is nothing to walk here: `isolateDeletedElement` already
    // took all of its edges out when its last repair job completed.
    for (size_t level = 0; level <= element->toplevel; level++) {
        ElementLevelData &cur_level = getElementLevelData(element, level);
        for (size_t i = 0; i < cur_level.getNumLinks(); i++) {
            ElementLevelData &neighbour = getElementLevelData(cur_level.getLinkAtPos(i), level);
            // Note that in case of in-place delete, we might have not accounted for this edge
            // in the unidirectional edges, since there is no point in keeping it there temporarily
            // . (We know we will get here and remove this deleted id permanently.)
            // However, upon asynchronous delete, this should always succeed since we do update
            // the incoming edges in the mutual update even for deleted elements.
            bool res = neighbour.removeIncomingUnidirectionalEdgeIfExists(internalId);
            // Assert the logical condition of: is_marked_deleted(id) => res==True.
            (void)res;
            assert((!isMarkedDeleted(internalId) || res) && "The edge should be in the incoming "
                                                            "unidirectional edges");
        }
    }

    // Free the element's resources
    element->destroy(this->levelDataSize, this->allocator);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::removeFromGraph(idType internalId) {
    // Sanity check - the id to remove cannot be the entry point, as it should have been replaced
    // upon marking it as deleted.
    assert(entrypointNode != internalId);
    disposeElementData(internalId);
    // We can say now that the element has removed completely from index.
    --curElementCount;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::swapWithLast(idType removedId) {
    // Get the last element's metadata and data.
    // If we are deleting the last element, we already destroyed its metadata.
    auto *last_element_data = getDataByInternalId(curElementCount);
    DataBlock &last_gd_block = graphDataBlocks.back();
    auto last_element = (ElementGraphData *)last_gd_block.removeAndFetchLastElement();

    // Swap the last id with the deleted one, and invalidate the last id data.
    if (curElementCount != removedId) {
        SwapLastIdWithDeletedId(removedId, last_element, last_element_data);
    }

    // If we need to free a complete block and there is at least one block between the
    // capacity and the size.
    this->vectors->removeElement(curElementCount);
    shrinkByBlock();
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::swapDeletedElement(idType internalId) {
    swapWithLast(internalId);
    // element is permanently removed from the index, it is no longer counted as marked deleted.
    --numMarkedDeleted;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::repairConnectionsAndEntryPoint(
    const idType element_internal_id) {
    vecsim_stl::vector<bool> neighbours_bitmap(this->allocator);
    ElementGraphData *element_data = getGraphDataByInternalId(element_internal_id);
    for (size_t level = 0; level <= element_data->toplevel; level++) {
        ElementLevelData &cur_level = getElementLevelData(element_data, level);
        // Reset the neighbours' bitmap for the current level - `curElementCount` must be
        // serialized by the caller taking `indexDataGuard` before calling this.
        neighbours_bitmap.assign(curElementCount, false);
        // Store the deleted element's neighbours set in a bitmap for fast access.
        for (size_t j = 0; j < cur_level.getNumLinks(); j++) {
            neighbours_bitmap[cur_level.getLinkAtPos(j)] = true;
        }
        // Go over the neighbours that also points back to the removed point and make a local
        // repair.
        for (size_t i = 0; i < cur_level.getNumLinks(); i++) {
            idType neighbour_id = cur_level.getLinkAtPos(i);
            ElementLevelData &neighbor_level = getElementLevelData(neighbour_id, level);

            bool bidirectional_edge = false;
            for (size_t j = 0; j < neighbor_level.getNumLinks(); j++) {
                // If the edge is bidirectional, do repair for this neighbor.
                if (neighbor_level.getLinkAtPos(j) == element_internal_id) {
                    bidirectional_edge = true;
                    repairConnectionsForDeletion(element_internal_id, neighbour_id, cur_level,
                                                 neighbor_level, level, neighbours_bitmap);
                    break;
                }
            }

            // If this edge is uni-directional, we should remove the element from the neighbor's
            // incoming edges.
            if (!bidirectional_edge) {
                // This should always return true (remove should succeed).
                bool res =
                    neighbor_level.removeIncomingUnidirectionalEdgeIfExists(element_internal_id);
                (void)res;
                assert(res && "The edge should be in the incoming unidirectional edges");
            }
        }

        // Next, go over the rest of incoming edges (the ones that are not bidirectional) and make
        // repairs.
        for (auto incoming_edge : cur_level.getIncomingEdges()) {
            repairConnectionsForDeletion(element_internal_id, incoming_edge, cur_level,
                                         getElementLevelData(incoming_edge, level), level,
                                         neighbours_bitmap);
        }
    }
    // Replacing the entry point relies on the graph around it already being repaired above.
    if (entrypointNode == element_internal_id) {
        assert(element_data->toplevel == maxLevel);
        replaceEntryPoint();
    }
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::clearDeletedSlot(idType deletedId) {
    assert(isMarkedDeleted(deletedId) && "Only a marked-deleted element's slot may be reclaimed");
    // Normally a no-op, since the last repair of a deleted element isolates it. Not when its
    // repairs were invalidated instead of run (their node was disposed), which leaves its edges
    // for whoever disposes of it - us, here.
    isolateDeletedElement(deletedId);
    disposeElementData(deletedId);
    // Leave a valid, empty element rather than freed memory: the slot may still be read (or moved
    // by a swap) before the new element is stored in it.
    auto tmpData = this->allocator->allocate_unique(this->elementGraphDataSize);
    memset(tmpData.get(), 0, this->elementGraphDataSize);
    new (tmpData.get()) ElementGraphData(0, levelDataSize, this->allocator);
    this->graphDataBlocks[deletedId / this->blockSize].updateElement(deletedId % this->blockSize,
                                                                     tmpData.get());
}

template <typename DataType, typename DistType>
HNSWAddVectorState HNSWIndex<DataType, DistType>::storeNewElementInClearedSlot(
    labelType label, const void *vector_data, idType clearedId) {
    assert(isMarkedDeleted(clearedId) && "Only a cleared deleted element's slot may be reused");
    // Free the empty element `clearDeletedSlot` left, since `storeNewElement` overwrites the slot.
    getGraphDataByInternalId(clearedId)->destroy(this->levelDataSize, this->allocator);
    // The slot is no longer a deleted element, so it's no longer counted as one.
    --numMarkedDeleted;
    HNSWAddVectorState state = storeNewElement(label, vector_data, clearedId);
    setVectorId(label, clearedId);
    return state;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::removeVectorInPlace(const idType element_internal_id) {
    repairConnectionsAndEntryPoint(element_internal_id);
    // The element is fully detached now - every neighbor has been repaired, and nothing refers to
    // it. Free what it owns, but leave the slot itself (and `curElementCount`).
    disposeElementData(element_internal_id);
    // Finally, remove the element from the index and make a swap with the last internal id to
    // avoid fragmentation and reclaim memory when needed.
    --curElementCount;
    swapWithLast(element_internal_id);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::overwriteVectorInPlace(idType old_id, const void *vector_data,
                                                           labelType label) {
    ProcessedBlobs processedBlobs = this->preprocess(vector_data);
    this->lockIndexDataGuard();
    //    -- Delete Old --- //
    repairConnectionsAndEntryPoint(old_id);
    // The element is fully detached now - every neighbor has been repaired, and nothing refers to
    // it. Free what it owns, but leave the slot itself (and `curElementCount`)
    disposeElementData(old_id);
    //      -- Add New --- //
    HNSWAddVectorState state = storeNewElement(label, processedBlobs.getStorageBlob(), old_id);
    if (state.currMaxLevel >= state.elementMaxLevel) {
        this->unlockIndexDataGuard();
    }

    this->indexVector(processedBlobs.getQueryBlob(), label, state);

    if (state.currMaxLevel < state.elementMaxLevel) {
        this->unlockIndexDataGuard();
    }
}

// Store the new element in the global data structures and keep the new state. In multithreaded
// scenario, the index data guard should be held by the caller (exclusive lock).
template <typename DataType, typename DistType>
HNSWAddVectorState HNSWIndex<DataType, DistType>::storeNewElement(labelType label,
                                                                  const void *vector_data,
                                                                  idType elementId) {
    // When an id is given we are reusing an existing (already detached) slot, so the index
    // doesn't grow and no capacity check is needed - the slot is within the current capacity by
    // definition.
    const bool reuseSlot = (elementId != INVALID_ID);
    assert((!reuseSlot || elementId < curElementCount) &&
           "The given id must be an existing slot in the index");
    if (!reuseSlot && isCapacityFull()) {
        growByBlock();
    }
    HNSWAddVectorState state{};

    // Choose randomly the maximum level in which the new element will be in the index.
    state.elementMaxLevel = getRandomLevel(mult);

    // Access and update the index global data structures with the new element meta-data.
    // Allocate a fresh id only if the caller didn't provide one.
    state.newElementId = reuseSlot ? elementId : curElementCount++;

    // Create the new element's graph metadata.
    // We must assign manually enough memory on the stack and not just declare an `ElementGraphData`
    // variable, since it has a flexible array member.
    auto tmpData = this->allocator->allocate_unique(this->elementGraphDataSize);
    memset(tmpData.get(), 0, this->elementGraphDataSize);
    ElementGraphData *element_data = (ElementGraphData *)tmpData.get();
    // Allocate memory (inside `ElementGraphData` constructor) for the links in higher levels and
    // initialize this memory to zeros. The reason for doing it here is that we might mark this
    // vector as deleted BEFORE we finish its indexing. In that case, we will collect the incoming
    // edges to this element in every level, and try to access its link lists in higher levels.
    // Therefore, we allocate it here and initialize it with zeros, (otherwise we might crash...)
    try {
        new (element_data) ElementGraphData(state.elementMaxLevel, levelDataSize, this->allocator);
    } catch (std::runtime_error &e) {
        this->log(VecSimCommonStrings::LOG_WARNING_STRING,
                  "Error - allocating memory for new element failed due to low memory");
        throw e;
    }

    // Insert the new element to the data block. The containers only support *appending* at
    // `curElementCount`, so when reusing an existing slot we have to overwrite it in place
    // instead. The previous occupant's graph data must have already been destroyed by the caller
    // (see `repairConnectionsAndEntryPoint` + `disposeElementData`), otherwise its link lists are
    // leaked by the overwrite.
    if (reuseSlot) {
        this->vectors->updateElement(state.newElementId, vector_data);
        this->graphDataBlocks[state.newElementId / this->blockSize].updateElement(
            state.newElementId % this->blockSize, element_data);
    } else {
        this->vectors->addElement(vector_data, state.newElementId);
        this->graphDataBlocks.back().addElement(element_data);
    }
    // We mark id as in process *before* we set it in the label lookup, so that IN_PROCESS flag is
    // set when checking if label .
    this->idToMetaData[state.newElementId] = ElementMetaData(label);
    // When reusing a slot the label lookup already maps `label` to this exact id - that's how the
    // caller found it to reuse in the first place - so there is nothing to add here. Skipping this
    // matters beyond being redundant: a multi-value label lookup appends rather than assigns, so
    // calling it again on a reused id would record it twice under the same label.
    if (!reuseSlot) {
        setVectorId(label, state.newElementId);
    }

    state.currMaxLevel = (int)maxLevel;
    state.currEntryPoint = entrypointNode;
    if (state.elementMaxLevel > state.currMaxLevel) {
        if (entrypointNode == INVALID_ID && maxLevel != HNSW_INVALID_LEVEL) {
            throw std::runtime_error("Internal error - inserting the first element to the graph,"
                                     " but the current max level is not INVALID");
        }
        // If the new elements max level is higher than the maximum level the currently exists in
        // the graph, update the max level and set the new element as entry point.
        entrypointNode = state.newElementId;
        maxLevel = state.elementMaxLevel;
    }
    return state;
}

template <typename DataType, typename DistType>
HNSWAddVectorState HNSWIndex<DataType, DistType>::storeVector(const void *vector_data,
                                                              const labelType label) {
    HNSWAddVectorState state{};

    this->lockIndexDataGuard();
    state = storeNewElement(label, vector_data);
    if (state.currMaxLevel >= state.elementMaxLevel) {
        this->unlockIndexDataGuard();
    }

    return state;
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::indexVector(const void *vector_data, const labelType label,
                                                const HNSWAddVectorState &state) {
    // Deconstruct the state variables from the auxiliaryCtx. prev_entry_point and prev_max_level
    // are the entry point and index max level at the point of time when the element was stored, and
    // they may (or may not) have changed due to the insertion.
    auto [new_element_id, element_max_level, prev_entry_point, prev_max_level] = state;

    // This condition only means that we are not inserting the first (non-deleted) element (for the
    // first element we do nothing - we don't need to connect to it).
    if (prev_entry_point != INVALID_ID) {
        // Start scanning the graph from the current entry point.
        insertElementToGraph(new_element_id, element_max_level, prev_entry_point, prev_max_level,
                             vector_data);
    }
    unmarkInProcess(new_element_id);
}

template <typename DataType, typename DistType>
void HNSWIndex<DataType, DistType>::appendVector(const void *vector_data, const labelType label) {

    ProcessedBlobs processedBlobs = this->preprocess(vector_data);
    HNSWAddVectorState state = this->storeVector(processedBlobs.getStorageBlob(), label);

    this->indexVector(processedBlobs.getQueryBlob(), label, state);

    if (state.currMaxLevel < state.elementMaxLevel) {
        // No external auxiliaryCtx, so it's this function responsibility to release the lock.
        this->unlockIndexDataGuard();
    }
}

template <typename DataType, typename DistType>
auto HNSWIndex<DataType, DistType>::safeGetEntryPointState() const {
    std::shared_lock<std::shared_mutex> lock(indexDataGuard);
    return std::make_pair(entrypointNode, maxLevel);
}

template <typename DataType, typename DistType>
idType HNSWIndex<DataType, DistType>::searchBottomLayerEP(const void *query_data, void *timeoutCtx,
                                                          VecSimQueryReply_Code *rc) const {
    *rc = VecSim_QueryReply_OK;

    auto [curr_element, max_level] = safeGetEntryPointState();
    if (curr_element == INVALID_ID)
        return curr_element; // index is empty.

    DistType cur_dist = this->calcDistanceForQuery(getDataByInternalId(curr_element), query_data);
    for (size_t level = max_level; level > 0 && curr_element != INVALID_ID; --level) {
        greedySearchLevel<true>(query_data, level, curr_element, cur_dist, timeoutCtx, rc);
    }
    return curr_element;
}

template <typename DataType, typename DistType>
candidatesLabelsMaxHeap<DistType> *
HNSWIndex<DataType, DistType>::searchBottomLayer_WithTimeout(idType ep_id, const void *data_point,
                                                             size_t ef, size_t k, void *timeoutCtx,
                                                             VecSimQueryReply_Code *rc) const {

    auto *visited_nodes_handler = getVisitedList();
    tag_t visited_tag = visited_nodes_handler->getFreshTag();

    candidatesLabelsMaxHeap<DistType> *top_candidates = getNewMaxPriorityQueue();
    candidatesMaxHeap<DistType> candidate_set(this->allocator);

    DistType lowerBound;
    if (!isMarkedDeleted(ep_id)) {
        // If ep is not marked as deleted, get its distance and set lower bound and heaps
        // accordingly
        DistType dist = this->calcDistanceForQuery(getDataByInternalId(ep_id), data_point);
        lowerBound = dist;
        top_candidates->emplace(dist, getExternalLabel(ep_id));
        candidate_set.emplace(-dist, ep_id);
    } else {
        // If ep is marked as deleted, set initial lower bound to max, and don't insert to top
        // candidates heap
        lowerBound = std::numeric_limits<DistType>::max();
        candidate_set.emplace(-lowerBound, ep_id);
    }

    visited_nodes_handler->tagNode(ep_id, visited_tag);

    while (!candidate_set.empty()) {
        pair<DistType, idType> curr_el_pair = candidate_set.top();

        if ((-curr_el_pair.first) > lowerBound && top_candidates->size() >= ef) {
            break;
        }
        if (VECSIM_TIMEOUT(timeoutCtx)) {
            returnVisitedList(visited_nodes_handler);
            *rc = VecSim_QueryReply_TimedOut;
            return top_candidates;
        }
        candidate_set.pop();

        processCandidate(curr_el_pair.second, data_point, 0, ef,
                         visited_nodes_handler->getElementsTags(), visited_tag, *top_candidates,
                         candidate_set, lowerBound);
    }
    returnVisitedList(visited_nodes_handler);
    while (top_candidates->size() > k) {
        top_candidates->pop();
    }
    *rc = VecSim_QueryReply_OK;
    return top_candidates;
}

template <typename DataType, typename DistType>
VecSimQueryReply *HNSWIndex<DataType, DistType>::topKQuery(const void *query_data, size_t k,
                                                           VecSimQueryParams *queryParams) const {

    auto rep = new VecSimQueryReply(this->allocator);
    this->lastMode = STANDARD_KNN;

    if (curElementCount == 0 || k == 0) {
        return rep;
    }

    auto processed_query_ptr = this->preprocessQuery(query_data);
    const void *processed_query = processed_query_ptr.get();
    void *timeoutCtx = nullptr;

    // Get original efRuntime and store it.
    size_t query_ef = this->ef;

    if (queryParams) {
        timeoutCtx = queryParams->timeoutCtx;
        if (queryParams->hnswRuntimeParams.efRuntime != 0) {
            query_ef = queryParams->hnswRuntimeParams.efRuntime;
        }
    }

    idType bottom_layer_ep = searchBottomLayerEP(processed_query, timeoutCtx, &rep->code);
    if (VecSim_OK != rep->code || bottom_layer_ep == INVALID_ID) {
        // Although we checked that the index is not empty (curElementCount == 0), it might be
        // that another thread deleted all the elements or didn't finish inserting the first element
        // yet. Anyway, we observed that the index is empty, so we return an empty result list.
        return rep;
    }

    // We now oun the results heap, we need to free (delete) it when we done
    candidatesLabelsMaxHeap<DistType> *results;
    results = searchBottomLayer_WithTimeout(bottom_layer_ep, processed_query, std::max(query_ef, k),
                                            k, timeoutCtx, &rep->code);

    if (VecSim_OK == rep->code) {
        rep->results.resize(results->size());
        for (auto result = rep->results.rbegin(); result != rep->results.rend(); result++) {
            std::tie(result->score, result->id) = results->top();
            results->pop();
        }
    }
    delete results;
    return rep;
}

template <typename DataType, typename DistType>
VecSimQueryResultContainer HNSWIndex<DataType, DistType>::searchRangeBottomLayer_WithTimeout(
    idType ep_id, const void *data_point, double epsilon, DistType radius, void *timeoutCtx,
    VecSimQueryReply_Code *rc) const {

    *rc = VecSim_QueryReply_OK;
    auto res_container = getNewResultsContainer(10); // arbitrary initial cap.

    auto *visited_nodes_handler = getVisitedList();
    tag_t visited_tag = visited_nodes_handler->getFreshTag();

    candidatesMaxHeap<DistType> candidate_set(this->allocator);

    // Set the initial effective-range to be at least the distance from the entry-point.
    DistType ep_dist, dynamic_range, dynamic_range_search_boundaries;
    if (isMarkedDeleted(ep_id)) {
        // If ep is marked as deleted, set initial ranges to max
        ep_dist = std::numeric_limits<DistType>::max();
        dynamic_range_search_boundaries = dynamic_range = ep_dist;
    } else {
        // If ep is not marked as deleted, get its distance and set ranges accordingly
        ep_dist = this->calcDistanceForQuery(getDataByInternalId(ep_id), data_point);
        dynamic_range = ep_dist;
        if (ep_dist <= radius) {
            // Entry-point is within the radius - add it to the results.
            res_container->emplace(getExternalLabel(ep_id), ep_dist);
            dynamic_range = radius; // to ensure that dyn_range >= radius.
        }
        dynamic_range_search_boundaries = dynamic_range * (1.0 + epsilon);
    }

    candidate_set.emplace(-ep_dist, ep_id);
    visited_nodes_handler->tagNode(ep_id, visited_tag);

    while (!candidate_set.empty()) {
        pair<DistType, idType> curr_el_pair = candidate_set.top();
        // If the best candidate is outside the dynamic range in more than epsilon (relatively) - we
        // finish the search.

        if ((-curr_el_pair.first) > dynamic_range_search_boundaries) {
            break;
        }
        if (VECSIM_TIMEOUT(timeoutCtx)) {
            *rc = VecSim_QueryReply_TimedOut;
            break;
        }
        candidate_set.pop();

        // Decrease the effective range, but keep dyn_range >= radius.
        if (-curr_el_pair.first < dynamic_range && -curr_el_pair.first >= radius) {
            dynamic_range = -curr_el_pair.first;
            dynamic_range_search_boundaries = dynamic_range * (1.0 + epsilon);
        }

        // Go over the candidate neighbours, add them to the candidates list if they are within the
        // epsilon environment of the dynamic range, and add them to the results if they are in the
        // requested radius.
        // Here we send the radius as double to match the function arguments type.
        processCandidate_RangeSearch(
            curr_el_pair.second, data_point, 0, epsilon, visited_nodes_handler->getElementsTags(),
            visited_tag, res_container, candidate_set, dynamic_range_search_boundaries, radius);
    }
    returnVisitedList(visited_nodes_handler);
    return res_container->get_results();
}

template <typename DataType, typename DistType>
VecSimQueryReply *HNSWIndex<DataType, DistType>::rangeQuery(const void *query_data, double radius,
                                                            VecSimQueryParams *queryParams) const {

    auto rep = new VecSimQueryReply(this->allocator);
    this->lastMode = RANGE_QUERY;

    if (curElementCount == 0) {
        return rep;
    }
    auto processed_query_ptr = this->preprocessQuery(query_data);
    const void *processed_query = processed_query_ptr.get();
    void *timeoutCtx = nullptr;

    double query_epsilon = epsilon;
    if (queryParams) {
        timeoutCtx = queryParams->timeoutCtx;
        if (queryParams->hnswRuntimeParams.epsilon != 0.0) {
            query_epsilon = queryParams->hnswRuntimeParams.epsilon;
        }
    }

    idType bottom_layer_ep = searchBottomLayerEP(processed_query, timeoutCtx, &rep->code);
    // Although we checked that the index is not empty (curElementCount == 0), it might be
    // that another thread deleted all the elements or didn't finish inserting the first element
    // yet. Anyway, we observed that the index is empty, so we return an empty result list.
    if (VecSim_OK != rep->code || bottom_layer_ep == INVALID_ID) {
        return rep;
    }

    // search bottom layer
    // Here we send the radius as double to match the function arguments type.
    rep->results = searchRangeBottomLayer_WithTimeout(
        bottom_layer_ep, processed_query, query_epsilon, radius, timeoutCtx, &rep->code);
    return rep;
}

template <typename DataType, typename DistType>
VecSimIndexDebugInfo HNSWIndex<DataType, DistType>::debugInfo() const {

    VecSimIndexDebugInfo info;
    info.commonInfo = this->getCommonInfo();
    auto [ep_id, max_level] = this->safeGetEntryPointState();

    info.commonInfo.basicInfo.algo = VecSimAlgo_HNSWLIB;
    info.hnswInfo.M = this->getM();
    info.hnswInfo.efConstruction = this->getEfConstruction();
    info.hnswInfo.efRuntime = this->getEf();
    info.hnswInfo.epsilon = this->epsilon;
    info.hnswInfo.max_level = max_level;
    info.hnswInfo.entrypoint = ep_id != INVALID_ID ? getExternalLabel(ep_id) : INVALID_LABEL;
    info.hnswInfo.visitedNodesPoolSize = this->visitedNodesHandlerPool.getPoolSize();
    info.hnswInfo.numberOfMarkedDeletedNodes = this->getNumMarkedDeleted();
    return info;
}

template <typename DataType, typename DistType>
VecSimIndexBasicInfo HNSWIndex<DataType, DistType>::basicInfo() const {
    VecSimIndexBasicInfo info = this->getBasicInfo();
    info.algo = VecSimAlgo_HNSWLIB;
    info.isTiered = false;
    return info;
}

template <typename DataType, typename DistType>
VecSimDebugInfoIterator *HNSWIndex<DataType, DistType>::debugInfoIterator() const {
    VecSimIndexDebugInfo info = this->debugInfo();
    // For readability. Update this number when needed.
    size_t numberOfInfoFields = 17;
    auto *infoIterator = new VecSimDebugInfoIterator(numberOfInfoFields, this->allocator);

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::ALGORITHM_STRING,
                         .fieldType = INFOFIELD_STRING,
                         .fieldValue = {FieldValue{
                             .stringValue = VecSimAlgo_ToString(info.commonInfo.basicInfo.algo)}}});

    this->addCommonInfoToIterator(infoIterator, info.commonInfo);

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::BLOCK_SIZE_STRING,
        .fieldType = INFOFIELD_UINT64,
        .fieldValue = {FieldValue{.uintegerValue = info.commonInfo.basicInfo.blockSize}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_M_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.M}}});

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::HNSW_EF_CONSTRUCTION_STRING,
        .fieldType = INFOFIELD_UINT64,
        .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.efConstruction}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_EF_RUNTIME_STRING,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.efRuntime}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_MAX_LEVEL,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.max_level}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::HNSW_ENTRYPOINT,
                         .fieldType = INFOFIELD_UINT64,
                         .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.entrypoint}}});

    infoIterator->addInfoField(
        VecSim_InfoField{.fieldName = VecSimCommonStrings::EPSILON_STRING,
                         .fieldType = INFOFIELD_FLOAT64,
                         .fieldValue = {FieldValue{.floatingPointValue = info.hnswInfo.epsilon}}});

    infoIterator->addInfoField(VecSim_InfoField{
        .fieldName = VecSimCommonStrings::NUM_MARKED_DELETED,
        .fieldType = INFOFIELD_UINT64,
        .fieldValue = {FieldValue{.uintegerValue = info.hnswInfo.numberOfMarkedDeletedNodes}}});

    return infoIterator;
}

template <typename DataType, typename DistType>
bool HNSWIndex<DataType, DistType>::preferAdHocSearch(size_t subsetSize, size_t k,
                                                      bool initial_check) const {
    // This heuristic is based on sklearn decision tree classifier (with 20 leaves nodes) -
    // see scripts/HNSW_batches_clf.py
    size_t index_size = this->indexSize();
    // Referring to too large subset size as if it was the maximum possible size.
    subsetSize = std::min(subsetSize, index_size);

    size_t d = this->dim;
    size_t M = this->getM();
    float r = (index_size == 0) ? 0.0f : (float)(subsetSize) / (float)this->indexLabelCount();
    bool res;

    // node 0
    if (index_size <= 30000) {
        // node 1
        if (index_size <= 5500) {
            // node 5
            res = true;
        } else {
            // node 6
            if (r <= 0.17) {
                // node 11
                res = true;
            } else {
                // node 12
                if (k <= 12) {
                    // node 13
                    if (d <= 55) {
                        // node 17
                        res = false;
                    } else {
                        // node 18
                        if (M <= 10) {
                            // node 19
                            res = false;
                        } else {
                            // node 20
                            res = true;
                        }
                    }
                } else {
                    // node 14
                    res = true;
                }
            }
        }
    } else {
        // node 2
        if (r < 0.07) {
            // node 3
            if (index_size <= 750000) {
                // node 15
                res = true;
            } else {
                // node 16
                if (k <= 7) {
                    // node 21
                    res = false;
                } else {
                    // node 22
                    if (r <= 0.03) {
                        // node 23
                        res = true;
                    } else {
                        // node 24
                        res = false;
                    }
                }
            }
        } else {
            // node 4
            if (d <= 75) {
                // node 7
                res = false;
            } else {
                // node 8
                if (k <= 12) {
                    // node 9
                    if (r <= 0.21) {
                        // node 27
                        if (M <= 57) {
                            // node 29
                            if (index_size <= 75000) {
                                // node 31
                                res = true;
                            } else {
                                // node 32
                                res = false;
                            }
                        } else {
                            // node 30
                            res = true;
                        }
                    } else {
                        // node 28
                        res = false;
                    }
                } else {
                    // node 10
                    if (M <= 10) {
                        // node 25
                        if (r <= 0.17) {
                            // node 33
                            res = true;
                        } else {
                            // node 34
                            res = false;
                        }
                    } else {
                        // node 26
                        if (index_size <= 300000) {
                            // node 35
                            res = true;
                        } else {
                            // node 36
                            if (r <= 0.17) {
                                // node 37
                                res = true;
                            } else {
                                // node 38
                                res = false;
                            }
                        }
                    }
                }
            }
        }
    }
    // Set the mode - if this isn't the initial check, we switched mode form batches to ad-hoc.
    this->lastMode =
        res ? (initial_check ? HYBRID_ADHOC_BF : HYBRID_BATCHES_TO_ADHOC_BF) : HYBRID_BATCHES;
    return res;
}

/********************************************** Debug commands ******************************/

template <typename DataType, typename DistType>
VecSimDebugCommandCode
HNSWIndex<DataType, DistType>::getHNSWElementNeighbors(size_t label, int ***neighborsData) {
    std::shared_lock<std::shared_mutex> lock(indexDataGuard);
    // Assume single value index. TODO: support for multi as well.
    if (this->isMultiValue()) {
        return VecSimDebugCommandCode_MultiNotSupported;
    }
    auto ids = this->getElementIds(label);
    if (ids.empty()) {
        return VecSimDebugCommandCode_LabelNotExists;
    }
    idType id = ids[0];
    auto graph_data = this->getGraphDataByInternalId(id);
    lockNodeLinks(id);
    *neighborsData = new int *[graph_data->toplevel + 2];
    for (size_t level = 0; level <= graph_data->toplevel; level++) {
        auto &level_data = this->getElementLevelData(graph_data, level);
        assert(level_data.getNumLinks() <= (level > 0 ? this->getM() : 2 * this->getM()));
        (*neighborsData)[level] = new int[level_data.getNumLinks() + 1];
        (*neighborsData)[level][0] = level_data.getNumLinks();
        for (size_t i = 0; i < level_data.getNumLinks(); i++) {
            (*neighborsData)[level][i + 1] = (int)idToMetaData.at(level_data.getLinkAtPos(i)).label;
        }
    }
    (*neighborsData)[graph_data->toplevel + 1] = nullptr;
    unlockNodeLinks(id);
    return VecSimDebugCommandCode_OK;
}

#ifdef BUILD_TESTS
#include "hnsw_serializer_impl.h"
#endif
