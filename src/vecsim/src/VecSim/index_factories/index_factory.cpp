/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/vec_sim_index.h"
#include "index_factory.h"
#include "hnsw_factory.h"
#include "brute_force_factory.h"
#include "tiered_factory.h"
#include "svs_factory.h"

namespace VecSimFactory {
VecSimIndex *NewIndex(const VecSimParams *params) {
    VecSimIndex *index = NULL;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    try {
        switch (params->algo) {
        case VecSimAlgo_HNSWLIB: {
            index = HNSWFactory::NewIndex(params);
            break;
        }

        case VecSimAlgo_BF: {
            index = BruteForceFactory::NewIndex(params);
            break;
        }
        case VecSimAlgo_TIERED: {
            index = TieredFactory::NewIndex(&params->algoParams.tieredParams);
            break;
        }
        case VecSimAlgo_SVS: {
            index = SVSFactory::NewIndex(params);
            break;
        }
        }
    } catch (...) {
        // Index will delete itself. For now, do nothing.
    }
    return index;
}

size_t EstimateInitialSize(const VecSimParams *params) {
    switch (params->algo) {
    case VecSimAlgo_HNSWLIB:
        return HNSWFactory::EstimateInitialSize(&params->algoParams.hnswParams);
    case VecSimAlgo_BF:
        return BruteForceFactory::EstimateInitialSize(&params->algoParams.bfParams);
    case VecSimAlgo_TIERED:
        return TieredFactory::EstimateInitialSize(&params->algoParams.tieredParams);
    case VecSimAlgo_SVS:; // empty statement if svs not available
        return SVSFactory::EstimateInitialSize(&params->algoParams.svsParams);
    }
    return -1;
}

size_t EstimateElementSize(const VecSimParams *params) {
    switch (params->algo) {
    case VecSimAlgo_HNSWLIB:
        return HNSWFactory::EstimateElementSize(&params->algoParams.hnswParams);
    case VecSimAlgo_BF:
        return BruteForceFactory::EstimateElementSize(&params->algoParams.bfParams);
    case VecSimAlgo_TIERED:
        return TieredFactory::EstimateElementSize(&params->algoParams.tieredParams);
    case VecSimAlgo_SVS:; // empty statement if svs not available
        return SVSFactory::EstimateElementSize(&params->algoParams.svsParams);
    }
    return -1;
}

} // namespace VecSimFactory
