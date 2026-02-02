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
#include "VecSim/tombstone_interface.h"
#include "storage/hnsw_storage.h"
#include "factory/components/disk_components_factory.h"

#include <memory>
#include <string>
#include <atomic>

template <typename DistType>
using candidatesList = vecsim_stl::vector<std::pair<DistType, idType>>;

// Vectors flags (for marking a specific vector)
using elementFlags = uint8_t;
typedef enum : elementFlags {
    DELETE_MARK = 0x1, // element is logically deleted, but still exists in the graph
    IN_PROCESS = 0x2,  // element is being inserted into the graph
} Flags;

#pragma pack(1)
struct ElementMetaData {
    labelType label;
    elementFlags flags;

private:
    std::atomic_bool lock_flag;

public:
    explicit ElementMetaData(labelType label = SIZE_MAX) noexcept : label(label), flags(IN_PROCESS), lock_flag(false) {}

    void lock() noexcept {
        bool expected = false;
        while (!lock_flag.compare_exchange_weak(expected, true, std::memory_order_acquire)) {
            expected = false;
        }
    }
    void unlock() noexcept { lock_flag.store(false, std::memory_order_release); }
};
#pragma pack() // restore default packing

// RAII lock guard for ElementMetaData
class ElementLockGuard {
    ElementMetaData& element;

public:
    explicit ElementLockGuard(ElementMetaData& elem) noexcept : element(elem) { element.lock(); }
    ~ElementLockGuard() noexcept { element.unlock(); }
    ElementLockGuard(const ElementLockGuard&) = delete;
    ElementLockGuard& operator=(const ElementLockGuard&) = delete;
};

template <typename DataType, typename DistType>
class HNSWDiskIndex : public VecSimIndexAbstract<DataType, DistType>, public VecSimIndexTombstone {
public:
    // Constructor for factory use - takes ownership of storage and components
    HNSWDiskIndex(const VecSimParamsDisk* params, const AbstractIndexInitParams& abstractInitParams,
                  const DiskIndexComponents<DataType, DistType>& components,
                  std::unique_ptr<HNSWStorage<DataType>> storage);
    ~HNSWDiskIndex() override = default;

    // VecSimIndexInterface - stubs for MOD-13164
    int addVector(const void* blob, labelType label) override;
    int deleteVector(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void* blob) const override;
    size_t indexSize() const override { return curElementCount_; }
    size_t indexCapacity() const override { return curElementCount_; }
    size_t indexLabelCount() const override { return curElementCount_; }

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
    size_t indexMetaDataCapacity() const override { return curElementCount_; }
    void getDataByLabel(labelType, std::vector<std::vector<DataType>>&) const override {}
    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType) const override { return {}; }
#endif

    size_t getEf() const { return efRuntime_; }
    void setEf(size_t ef) { efRuntime_ = ef; }

    HNSWStorage<DataType>* getStorage() const { return storage_.get(); }

    void repairNode(idType id, levelType level);

    // TODO: Make thread safe (the the right locking mechanism)
    vecsim_stl::vector<idType> markDelete(labelType label) override {
        // Stub - returns dummy ID for testing purposes
        // TODO: Get internal ID by label, mark as deleted, return deleted IDs
        vecsim_stl::vector<idType> deleted_ids(this->allocator);
        deleted_ids.push_back(0); // Dummy ID - real implementation will look up by label
        return deleted_ids;
    }

protected:
    void getNeighborsByHeuristic2(candidatesList<DistType>& top_candidates, size_t M,
                                  vecsim_stl::vector<idType>& not_chosen_candidates) const;

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
        return __atomic_load_n(&idToMetaData[internalId].flags, 0) & FLAG;
    }

private:
    size_t M_;
    size_t M0_;
    size_t efConstruction_;
    size_t efRuntime_;
    std::string indexName_;
    size_t curElementCount_ = 0;

    // Index data
    vecsim_stl::vector<ElementMetaData> idToMetaData;

    // Storage backend (owned by this index)
    std::unique_ptr<HNSWStorage<DataType>> storage_;

    // Helper to access the disk calculator with proper type for templated calcDistance<Mode>()
    const DiskDistanceCalculator<DistType>* getDiskCalculator() const {
        return static_cast<const DiskDistanceCalculator<DistType>*>(this->getIndexCalculator());
    }

    // Distance calculation methods - zero overhead via compile-time mode selection
    // Names match DistanceMode enum: Full (FP32↔FP32), QuantizedVsFull (SQ8↔FP32), Quantized (SQ8↔SQ8)
    DistType calcFullDistance(const void* v1, const void* v2) const noexcept;
    DistType calcQuantizedVsFullDistance(const void* v1, const void* v2) const noexcept;
    DistType calcQuantizedDistance(const void* v1, const void* v2) const noexcept;

    HNSWDiskIndex(const HNSWDiskIndex&) = delete;
    HNSWDiskIndex& operator=(const HNSWDiskIndex&) = delete;
};

// Template Implementation

template <typename DataType, typename DistType>
void HNSWDiskIndex<DataType, DistType>::getNeighborsByHeuristic2(
    candidatesList<DistType>& top_candidates, size_t M, vecsim_stl::vector<idType>& not_chosen_candidates) const {

    // Stub - To be implemented correctly
    not_chosen_candidates.clear();
    if (top_candidates.size() <= M) {
        return;
    }
    // Extract the IDs from the pairs that are beyond M
    for (size_t i = M; i < top_candidates.size(); ++i) {
        not_chosen_candidates.push_back(top_candidates[i].second);
    }
    top_candidates.resize(M);
}

template <typename DataType, typename DistType>
HNSWDiskIndex<DataType, DistType>::HNSWDiskIndex(const VecSimParamsDisk* params,
                                                 const AbstractIndexInitParams& abstractInitParams,
                                                 const DiskIndexComponents<DataType, DistType>& components,
                                                 std::unique_ptr<HNSWStorage<DataType>> storage)
    : VecSimIndexAbstract<DataType, DistType>(abstractInitParams, components),
      indexName_(params->diskContext->indexName, params->diskContext->indexNameLen), idToMetaData(this->allocator),
      storage_(std::move(storage)) {
    const HNSWParams& hnswParams = params->indexParams->algoParams.hnswParams;
    // Apply defaults for zero values (uses the public VectorSimilarity definitions)
    M_ = hnswParams.M ? hnswParams.M : HNSW_DEFAULT_M;
    M0_ = M_ * 2;
    efConstruction_ = hnswParams.efConstruction ? hnswParams.efConstruction : HNSW_DEFAULT_EF_C;
    efRuntime_ = hnswParams.efRuntime ? hnswParams.efRuntime : HNSW_DEFAULT_EF_RT;
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcFullDistance(const void* v1, const void* v2) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::Full>(v1, v2, this->dim);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcQuantizedVsFullDistance(const void* v1, const void* v2) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::QuantizedVsFull>(v1, v2, this->dim);
}

template <typename DataType, typename DistType>
DistType HNSWDiskIndex<DataType, DistType>::calcQuantizedDistance(const void* v1, const void* v2) const noexcept {
    return getDiskCalculator()->template calcDistance<DistanceMode::Quantized>(v1, v2, this->dim);
}

template <typename DataType, typename DistType>
int HNSWDiskIndex<DataType, DistType>::addVector(const void* blob, labelType label) {
    (void)blob;
    (void)label;
    return 0; // Stub - MOD-13164
}

template <typename DataType, typename DistType>
int HNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    (void)label;
    return 0; // Stub - MOD-13164
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

template <class DataType, class DistType>
void HNSWDiskIndex<DataType, DistType>::repairNode(idType id, levelType level) {
    if (isMarkedAs<DELETE_MARK>(id)) {
        return; // Node is deleted, no need to repair
    }
    auto& metaData = idToMetaData[id];
    ElementLockGuard lock(metaData);

    // Get outgoing edges
    vecsim_stl::vector<idType> outgoingEdges(this->allocator);
    if (!storage_->get_outgoing_edges(id, level, outgoingEdges)) {
        throw std::runtime_error("Failed to get outgoing edges from storage");
    }
    // Collect candidates for neighbors. We take:
    // 1. The current not deleted neighbors
    // 2. The not deleted neighbors of the current deleted neighbors
    vecsim_stl::vector<idType> candidates(this->allocator);
    vecsim_stl::unordered_set<idType> uniqueCandidates(this->allocator);
    for (idType neighborId : outgoingEdges) {
        if (!isMarkedAs<DELETE_MARK>(neighborId)) {
            if (uniqueCandidates.find(neighborId) != uniqueCandidates.end()) {
                continue;
            }
            uniqueCandidates.insert(neighborId);
            candidates.push_back(neighborId);
        } else {
            // Get neighbors of the deleted neighbor
            vecsim_stl::vector<idType> neighborNeighbors(this->allocator);
            if (!storage_->get_outgoing_edges(neighborId, level, neighborNeighbors)) {
                throw std::runtime_error("Failed to get outgoing edges from storage");
            }
            for (idType neighborNeighborId : neighborNeighbors) {
                if (neighborNeighborId == id || uniqueCandidates.find(neighborNeighborId) != uniqueCandidates.end()) {
                    continue;
                }
                uniqueCandidates.insert(neighborNeighborId);
                if (!isMarkedAs<DELETE_MARK>(neighborNeighborId)) {
                    candidates.push_back(neighborNeighborId);
                }
            }
        }
    }
    size_t M = (level == 0) ? M0_ : M_;
    if (candidates.size() > M) {
        // Compute distances to candidates
        candidatesList<DistType> candidateDistances(this->allocator);
        candidateDistances.reserve(candidates.size());
        for (idType candidateId : candidates) {
            // Compute distance - TODO: Use SQ distance
            DistType distance = 0.042; // Placeholder
            candidateDistances.push_back(std::make_pair(distance, candidateId));
        }
        // Select neighbors using heuristic
        vecsim_stl::vector<idType> notChosenCandidates(this->allocator);
        this->getNeighborsByHeuristic2(candidateDistances, M, notChosenCandidates);
        // Update candidates to chosen ones
        candidates.clear();
        for (const auto& pair : candidateDistances) {
            candidates.push_back(pair.second);
        }
        // TODO: For ids we were not connected to, but are now connected to, add `id` to their incoming edges
        // TODO: For ids we were connected to, but are not connected to anymore, remove `id` from their incoming edges
    }
    auto success = storage_->put_outgoing_edges(id, level, candidates);
    if (!success) {
        throw std::runtime_error("Failed to put outgoing edges to storage");
    }
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
