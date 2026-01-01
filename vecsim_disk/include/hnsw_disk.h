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
#include "VecSim/index_factories/components/components_factory.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/info_iterator_struct.h"
#include "vector_storage.h"

#include <memory>
#include <string>

template <typename DataType, typename DistType>
class HNSWDiskIndex : public VecSimIndexAbstract<DataType, DistType> {
public:
    // Constructor for factory use - takes ownership of storage
    HNSWDiskIndex(const VecSimHNSWDiskParams* params, std::shared_ptr<VecSimAllocator> allocator,
                  std::unique_ptr<VectorStore> storage);
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

    VectorStore* getStorage() const { return storage_.get(); }

private:
    size_t M_;
    size_t efConstruction_;
    size_t efRuntime_;
    std::string indexName_;
    size_t curElementCount_ = 0;

    // Storage backend (owned by this index)
    std::unique_ptr<VectorStore> storage_;

    static AbstractIndexInitParams createAbstractParams(const VecSimHNSWDiskParams* params,
                                                        std::shared_ptr<VecSimAllocator> allocator);

    HNSWDiskIndex(const HNSWDiskIndex&) = delete;
    HNSWDiskIndex& operator=(const HNSWDiskIndex&) = delete;
};

// Template Implementation

template <typename DataType, typename DistType>
AbstractIndexInitParams
HNSWDiskIndex<DataType, DistType>::createAbstractParams(const VecSimHNSWDiskParams* params,
                                                        std::shared_ptr<VecSimAllocator> allocator) {
    return AbstractIndexInitParams{
        .allocator = allocator,
        .dim = params->dim,
        .vecType = params->type,
        .storedDataSize = VecSimParams_GetStoredDataSize(params->type, params->dim, params->metric),
        .metric = params->metric,
        .blockSize = params->blockSize,
        .multi = params->multi,
        .isDisk = true,
        .logCtx = params->logCtx,
        .inputBlobSize = params->dim * sizeof(DataType),
    };
}

template <typename DataType, typename DistType>
HNSWDiskIndex<DataType, DistType>::HNSWDiskIndex(const VecSimHNSWDiskParams* params,
                                                 std::shared_ptr<VecSimAllocator> allocator,
                                                 std::unique_ptr<VectorStore> storage)
    : VecSimIndexAbstract<DataType, DistType>(
          createAbstractParams(params, allocator),
          CreateIndexComponents<DataType, DistType>(allocator, params->metric, params->dim, false)),
      M_(params->M), efConstruction_(params->efConstruction), efRuntime_(params->efRuntime),
      indexName_(params->indexName, params->indexNameLen), storage_(std::move(storage)) {}

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
