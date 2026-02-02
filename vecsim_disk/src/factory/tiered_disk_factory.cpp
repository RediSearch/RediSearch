/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "tiered_disk_factory.h"
#include "disk_index_factory.h"
#include "hnsw_disk_factory.h"
#include "VecSim/index_factories/factory_utils.h"
#include "algorithms/hnsw/hnsw_disk.h"
#include "VecSim/algorithms/brute_force/brute_force.h"
#include "VecSim/index_factories/brute_force_factory.h"
#include "algorithms/hnsw/hnsw_disk_tiered.h"

namespace TieredHNSWDiskFactory {
/* ================ API ================ */
VecSimIndex* NewIndex(const VecSimParamsDisk* params);
} // namespace TieredHNSWDiskFactory

namespace TieredDiskFactory {
VecSimIndex* NewIndex(const VecSimParamsDisk* params) {
    const TieredIndexParams* tiered_params = &params->indexParams->algoParams.tieredParams;
    switch (tiered_params->primaryIndexParams->algo) {
    case VecSimAlgo_HNSWLIB:
        return TieredHNSWDiskFactory::NewIndex(params);

    default:
        return nullptr;
    }
}
} // namespace TieredDiskFactory

namespace TieredHNSWDiskFactory {
/* ================ HELPERS ================ */
static inline BFParams NewBFParams(const HNSWParams& hnsw_params) {
    BFParams bf_params = {.type = hnsw_params.type,
                          .dim = hnsw_params.dim,
                          .metric = hnsw_params.metric,
                          .multi = hnsw_params.multi,
                          .blockSize = hnsw_params.blockSize};

    return bf_params;
}

/* ================ IMPLEMENTATION ================ */

VecSimIndex* NewIndex(const VecSimParamsDisk* params) {
    const TieredIndexParams& tiered_params = params->indexParams->algoParams.tieredParams;
    VecSimParams* hnsw_backend_params = tiered_params.primaryIndexParams;
    // initialize HNSWDisk index
    // Create VecSimParamsDisk for hnsw index
    VecSimParamsDisk hnsw_params_disk = {
        .indexParams = hnsw_backend_params,
        .diskContext = params->diskContext,
    };
    // static_cast is safe here because HNSWDiskFactory::NewIndex() with float type parameters
    // always returns HNSWDiskIndex<float, float>* (see hnsw_disk_factory.cpp). The assert below
    // catches null returns but not type mismatches, so we rely on the factory's type guarantee.
    auto* hnswDiskIndex = static_cast<HNSWDiskIndex<float, float>*>(HNSWDiskFactory::NewIndex(&hnsw_params_disk, true));
    assert(hnswDiskIndex);

    // Initialize Brute Force frontend index.
    // Frontend stores vectors in FP32 (full precision), so both inputBlobSize and storedDataSize = FP32 size.
    // The assertion verifies that frontend.storedDataSize == backend.inputBlobSize (both FP32).
    const HNSWParams& hnsw_params = hnsw_backend_params->algoParams.hnswParams;
    auto bf_params = NewBFParams(hnsw_params);

    AbstractIndexInitParams frontendInitParams =
        VecSimFactory::NewAbstractInitParams(&bf_params, hnsw_backend_params->logCtx, false);
    assert(hnswDiskIndex->getInputBlobSize() == frontendInitParams.storedDataSize);
    auto frontendIndex =
        static_cast<BruteForceIndex<float, float>*>(BruteForceFactory::NewIndex(&bf_params, frontendInitParams, false));
    assert(frontendIndex);

    // Create tiered index
    auto mgmt_allocator = VecSimAllocator::newVecsimAllocator();
    return new (mgmt_allocator)
        TieredHNSWDiskIndex<float, float>(hnswDiskIndex, frontendIndex, tiered_params, mgmt_allocator);
}
} // namespace TieredHNSWDiskFactory
