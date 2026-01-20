/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "VecSim/vec_sim_tiered_index.h"
#include "algorithms/hnsw/hnsw_disk.h"

template <typename DataType, typename DistType>
class TieredHNSWDiskIndex : public VecSimTieredIndex<DataType, DistType> {
public:
    TieredHNSWDiskIndex(HNSWDiskIndex<DataType, DistType>* hnsw_index,
                        BruteForceIndex<DataType, DistType>* brute_force_index, const TieredIndexParams& tieredParams,
                        std::shared_ptr<VecSimAllocator> allocator);

    ~TieredHNSWDiskIndex() { /* Nothing to release */ }

    // VecSimIndexInterface
    int addVector(const void* blob, labelType label) override;
    int deleteVector(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void* blob) const override;
    size_t indexSize() const override { /* TBD */ return 0; }
    size_t indexCapacity() const override { /* TBD */ return 0; }
    VecSimIndexBasicInfo basicInfo() const override;
    VecSimBatchIterator* newBatchIterator(const void*, VecSimQueryParams*) const override;
    void setLastSearchMode(VecSearchMode mode) override { return this->backendIndex->setLastSearchMode(mode); }
    void runGC() override;
    void acquireSharedLocks() override { /* TBD */ }
    void releaseSharedLocks() override { /* TBD */ }

    // VecSimTieredIndex interface
    size_t getNumMarkedDeleted() const override { /* TBD */ return 0; }
};

/******************** Index API ****************************************/
template <typename DataType, typename DistType>
TieredHNSWDiskIndex<DataType, DistType>::TieredHNSWDiskIndex(HNSWDiskIndex<DataType, DistType>* hnsw_index,
                                                             BruteForceIndex<DataType, DistType>* brute_force_index,
                                                             const TieredIndexParams& tieredParams,
                                                             std::shared_ptr<VecSimAllocator> allocator)
    : VecSimTieredIndex<DataType, DistType>(hnsw_index, brute_force_index, tieredParams, allocator) {}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::addVector(const void* data, labelType label) {
    // TBD
    return 0;
}

template <typename DataType, typename DistType>
int TieredHNSWDiskIndex<DataType, DistType>::deleteVector(labelType label) {
    // TBD
    return 0;
}

template <typename DataType, typename DistType>
double TieredHNSWDiskIndex<DataType, DistType>::getDistanceFrom_Unsafe(labelType label, const void* blob) const {
    // TBD
    return 0.0;
}

template <typename DataType, typename DistType>
VecSimIndexBasicInfo TieredHNSWDiskIndex<DataType, DistType>::basicInfo() const {
    VecSimIndexBasicInfo info = this->backendIndex->getBasicInfo();
    info.isTiered = true;
    info.algo = VecSimAlgo_HNSWLIB;
    return info;
}

template <typename DataType, typename DistType>
VecSimBatchIterator* TieredHNSWDiskIndex<DataType, DistType>::newBatchIterator(const void* queryBlob,
                                                                               VecSimQueryParams* queryParams) const {
    // TBD
    return nullptr;
}

template <typename DataType, typename DistType>
void TieredHNSWDiskIndex<DataType, DistType>::runGC() {
    // TBD
}
