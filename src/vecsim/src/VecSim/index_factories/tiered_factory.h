/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_common.h"
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"
#if HAVE_SVS
#include "VecSim/algorithms/svs/svs_tiered.h"
#endif
#include "VecSim/algorithms/brute_force/brute_force.h"
#include "VecSim/index_factories/factory_utils.h"

namespace TieredFactory {

VecSimIndex *NewIndex(const TieredIndexParams *params);

// The size estimation is the sum of the buffer (brute force) and main index initial sizes
// estimations, plus the tiered index class size. Note it does not include the size of internal
// containers such as the job queue, as those depend on the user implementation.
size_t EstimateInitialSize(const TieredIndexParams *params);
size_t EstimateElementSize(const TieredIndexParams *params);

#ifdef BUILD_TESTS
namespace TieredHNSWFactory {
// Build tiered index from existing HNSW index - for internal benchmarks purposes
template <typename DataType, typename DistType>
VecSimIndex *NewIndex(const TieredIndexParams *params, HNSWIndex<DataType, DistType> *hnsw_index) {
    // Initialize brute force index.
    BFParams bf_params = {.type = hnsw_index->getType(),
                          .dim = hnsw_index->getDim(),
                          .metric = hnsw_index->getMetric(),
                          .multi = hnsw_index->isMultiValue(),
                          .blockSize = hnsw_index->getBlockSize()};

    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(&bf_params, nullptr, false);
    assert(hnsw_index->getInputBlobSize() == abstractInitParams.storedDataSize);
    assert(hnsw_index->usesQuantizedStorage() ||
           hnsw_index->getStoredDataSize() == abstractInitParams.storedDataSize);
    auto frontendIndex = static_cast<BruteForceIndex<DataType, DistType> *>(
        BruteForceFactory::NewIndex(&bf_params, abstractInitParams, false));

    // Create new tiered hnsw index
    std::shared_ptr<VecSimAllocator> management_layer_allocator =
        VecSimAllocator::newVecsimAllocator();
    return new (management_layer_allocator) TieredHNSWIndex<DataType, DistType>(
        hnsw_index, frontendIndex, *params, management_layer_allocator);
}
} // namespace TieredHNSWFactory

// The function below is exported to calculate a brute force index size in tests to align
// with the logic of the TieredFactory::EstimateInitialSize(), which currently doesn’t have a
// verification of the backend index algorithm. To be removed once a proper verification is
// introduced.
namespace TieredSVSFactory {

#if HAVE_SVS
template <typename DataType>
inline VecSimIndex *NewIndex(const TieredIndexParams *params,
                             VecSimIndexAbstract<DataType, float> *svs_index) {
    // Initialize brute force index.
    BFParams bf_params = {.type = svs_index->getType(),
                          .dim = svs_index->getDim(),
                          .metric = svs_index->getMetric(),
                          .multi = svs_index->isMultiValue(),
                          .blockSize = svs_index->getBlockSize()};

    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(&bf_params, params->primaryIndexParams->logCtx, false);
    assert(svs_index->getInputBlobSize() == abstractInitParams.storedDataSize);
    assert(svs_index->getStoredDataSize() == abstractInitParams.storedDataSize);
    auto frontendIndex = static_cast<BruteForceIndex<DataType, float> *>(
        BruteForceFactory::NewIndex(&bf_params, abstractInitParams, false));

    // Create new tiered svs index
    std::shared_ptr<VecSimAllocator> management_layer_allocator =
        VecSimAllocator::newVecsimAllocator();

    return new (management_layer_allocator)
        TieredSVSIndex<DataType>(svs_index, frontendIndex, *params, management_layer_allocator);
}
#endif
BFParams NewBFParams(const TieredIndexParams *params);
} // namespace TieredSVSFactory
#endif

}; // namespace TieredFactory
