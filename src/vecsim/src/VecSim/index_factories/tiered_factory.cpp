/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/index_factories/tiered_factory.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "VecSim/index_factories/brute_force_factory.h"

#include "VecSim/algorithms/hnsw/hnsw_tiered.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/index_factories/svs_factory.h"

#include <algorithm>

#if HAVE_SVS
#include "VecSim/algorithms/svs/svs_tiered.h"
#endif

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

namespace TieredFactory {

namespace TieredHNSWFactory {

static inline BFParams NewBFParams(const TieredIndexParams *params) {
    auto hnsw_params = params->primaryIndexParams->algoParams.hnswParams;
    BFParams bf_params = {.type = hnsw_params.type,
                          .dim = hnsw_params.dim,
                          .metric = hnsw_params.metric,
                          .multi = hnsw_params.multi,
                          .blockSize = hnsw_params.blockSize};

    return bf_params;
}

static inline bool RequiresQuantizationTraining(const TieredIndexParams *params) {
    return params->primaryIndexParams->algoParams.hnswParams.quantType == VecSimQuant_SQ8 &&
           params->specificParams.tieredHnswParams.QuantNormalizationSetSize > 0;
}

template <typename DataType, typename DistType>
static inline bool IsQuantizationSupported(const TieredIndexParams *params) {
    const auto &hnsw_params = params->primaryIndexParams->algoParams.hnswParams;
    if (hnsw_params.quantType == VecSimQuant_NONE) {
        return true;
    }

    if constexpr (!QuantInput<DataType> || !std::is_same_v<DistType, float>) {
        return false;
    } else {
        return hnsw_params.quantType == VecSimQuant_SQ8;
    }
}

template <typename DataType, typename DistType = DataType>
inline VecSimIndex *NewIndex(const TieredIndexParams *params) {
    if (!IsQuantizationSupported<DataType, DistType>(params)) {
        return nullptr;
    }

    const auto &hnsw_params = params->primaryIndexParams->algoParams.hnswParams;
    auto management_layer_allocator = VecSimAllocator::newVecsimAllocator();
    VecSimParams backend_params = *params->primaryIndexParams;
    vecsim_stl::vector<float> initial_mean(management_layer_allocator);
    if (RequiresQuantizationTraining(params)) {
        // Allocate the final SQ8 WithMean layout now. The factory copies this temporary mean;
        // tiered keeps the graph empty until it installs the mean accumulated from FLAT.
        initial_mean.resize(hnsw_params.dim, 0.0f);
        backend_params.algoParams.hnswParams.quantParams = initial_mean.data();
    }
    auto *hnsw_index =
        static_cast<HNSWIndex<DataType, DistType> *>(HNSWFactory::NewIndex(&backend_params, true));
    if (!hnsw_index) {
        return nullptr;
    }

    BFParams bf_params = NewBFParams(params);

    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(&bf_params, params->primaryIndexParams->logCtx, false);
    assert(hnsw_index->getInputBlobSize() == abstractInitParams.storedDataSize);
    assert(hnsw_params.quantType != VecSimQuant_NONE ||
           hnsw_index->getStoredDataSize() == abstractInitParams.storedDataSize);
    auto frontendIndex = static_cast<BruteForceIndex<DataType, DistType> *>(
        BruteForceFactory::NewIndex(&bf_params, abstractInitParams, false));

    if (hnsw_params.quantType == VecSimQuant_SQ8 && hnsw_params.dim < 64) {
        frontendIndex->log(
            VecSimCommonStrings::LOG_WARNING_STRING,
            "HNSW SQ8 compression is not recommended for vectors with fewer than "
            "64 dimensions because per-vector metadata overhead reduces memory savings");
    }

    return new (management_layer_allocator) TieredHNSWIndex<DataType, DistType>(
        hnsw_index, frontendIndex, *params, management_layer_allocator);
}

inline size_t EstimateInitialSize(const TieredIndexParams *params) {
    const auto &hnsw_params = params->primaryIndexParams->algoParams.hnswParams;

    const bool requires_training = RequiresQuantizationTraining(params);
    const bool with_mean = requires_training || hnsw_params.quantParams != nullptr;
    size_t est = HNSWFactory::EstimateInitialSize(&hnsw_params, true, with_mean);
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();

    if (requires_training) {
        est += allocations_overhead + hnsw_params.dim * sizeof(double);
    }

    // Management layer allocator overhead.
    est += sizeof(VecSimAllocator) + allocations_overhead;

    // Size of the TieredHNSWIndex struct.
    if (hnsw_params.type == VecSimType_FLOAT32) {
        est += sizeof(TieredHNSWIndex<float, float>);
    } else if (hnsw_params.type == VecSimType_FLOAT64) {
        est += sizeof(TieredHNSWIndex<double, double>);
    } else if (hnsw_params.type == VecSimType_BFLOAT16) {
        est += sizeof(TieredHNSWIndex<bfloat16, float>);
    } else if (hnsw_params.type == VecSimType_FLOAT16) {
        est += sizeof(TieredHNSWIndex<float16, float>);
    } else if (hnsw_params.type == VecSimType_INT8) {
        est += sizeof(TieredHNSWIndex<int8_t, float>);
    } else if (hnsw_params.type == VecSimType_UINT8) {
        est += sizeof(TieredHNSWIndex<uint8_t, float>);
    } else {
        throw std::invalid_argument("Invalid hnsw_params.type");
    }

    return est;
}

VecSimIndex *NewIndex(const TieredIndexParams *params) {
    // Tiered index that contains HNSW index as primary index
    VecSimType type = params->primaryIndexParams->algoParams.hnswParams.type;
    if (type == VecSimType_FLOAT32) {
        return TieredHNSWFactory::NewIndex<float>(params);
    } else if (type == VecSimType_FLOAT64) {
        return TieredHNSWFactory::NewIndex<double>(params);
    } else if (type == VecSimType_BFLOAT16) {
        return TieredHNSWFactory::NewIndex<bfloat16, float>(params);
    } else if (type == VecSimType_FLOAT16) {
        return TieredHNSWFactory::NewIndex<float16, float>(params);
    } else if (type == VecSimType_INT8) {
        return TieredHNSWFactory::NewIndex<int8_t, float>(params);
    } else if (type == VecSimType_UINT8) {
        return TieredHNSWFactory::NewIndex<uint8_t, float>(params);
    }
    return nullptr; // Invalid type.
}
} // namespace TieredHNSWFactory

namespace TieredSVSFactory {
BFParams NewBFParams(const TieredIndexParams *params) {
    auto &svs_params = params->primaryIndexParams->algoParams.svsParams;
    return BFParams{.type = svs_params.type,
                    .dim = svs_params.dim,
                    .metric = svs_params.metric,
                    .multi = svs_params.multi,
                    .blockSize = svs_params.blockSize};
}

#if HAVE_SVS
template <typename DataType>
inline VecSimIndex *NewIndex(const TieredIndexParams *params) {

    // initialize svs index
    // Normalization is done by the frontend index.
    auto *svs_index = static_cast<VecSimIndexAbstract<DataType, float> *>(
        SVSFactory::NewIndex(params->primaryIndexParams, true));
    assert(svs_index != nullptr);
    // initialize brute force index

    auto bf_params = NewBFParams(params);

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

inline size_t EstimateInitialSize(const TieredIndexParams *params) {
    auto &svs_params = params->primaryIndexParams->algoParams.svsParams;

    // Add size estimation of VecSimTieredIndex sub indexes.
    size_t est = SVSFactory::EstimateInitialSize(&svs_params, true);

    // Management layer allocator overhead.
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();
    est += sizeof(VecSimAllocator) + allocations_overhead;

    // Size of the TieredHNSWIndex struct.
    switch (svs_params.type) {
    case VecSimType_FLOAT32:
        est += sizeof(TieredSVSIndex<float>);
        break;
    case VecSimType_FLOAT16:
        est += sizeof(TieredSVSIndex<float16>);
        break;
    default:
        assert(false && "Unsupported data type");
        break;
    }

    return est;
}

VecSimIndex *NewIndex(const TieredIndexParams *params) {
    // Tiered index that contains SVS index as primary index
    VecSimType type = params->primaryIndexParams->algoParams.svsParams.type;
    switch (type) {
    case VecSimType_FLOAT32:
        return TieredSVSFactory::NewIndex<float>(params);
    case VecSimType_FLOAT16:
        return TieredSVSFactory::NewIndex<float16>(params);
    default:
        assert(false && "Unsupported data type");
        return nullptr; // Invalid type.
    }
    return nullptr; // Invalid type.
}

// This is a temporary solution to avoid breaking the build when SVS is not available
// and to allow the code to compile without SVS support.
// TODO: remove HAVE_SVS when SVS will support all Redis platforms and compilers
#else // HAVE_SVS
inline VecSimIndex *NewIndex(const TieredIndexParams *params) { return nullptr; }
inline size_t EstimateInitialSize(const TieredIndexParams *params) { return 0; }
inline size_t EstimateElementSize(const TieredIndexParams *params) { return 0; }
#endif
} // namespace TieredSVSFactory

VecSimIndex *NewIndex(const TieredIndexParams *params) {
    // Tiered index that contains HNSW index as primary index
    if (params->primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
        return TieredHNSWFactory::NewIndex(params);
    }
    // Tiered index that contains SVS index as primary index
    if (params->primaryIndexParams->algo == VecSimAlgo_SVS) {
        return TieredSVSFactory::NewIndex(params);
    }
    return nullptr; // Invalid algorithm or type.
}
size_t EstimateInitialSize(const TieredIndexParams *params) {

    size_t est = 0;

    BFParams bf_params{};
    if (params->primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
        est += TieredHNSWFactory::EstimateInitialSize(params);
        bf_params = TieredHNSWFactory::NewBFParams(params);
    }
    if (params->primaryIndexParams->algo == VecSimAlgo_SVS) {
        est += TieredSVSFactory::EstimateInitialSize(params);
        bf_params = TieredSVSFactory::NewBFParams(params);
    }

    est += BruteForceFactory::EstimateInitialSize(&bf_params, false);
    return est;
}

size_t EstimateElementSize(const TieredIndexParams *params) {
    // Match HNSW's element estimator, which leaves validation to NewIndex.
    size_t est = 0;
    if (params->primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
        const auto &hnsw_params = params->primaryIndexParams->algoParams.hnswParams;
        const bool with_mean = TieredHNSWFactory::RequiresQuantizationTraining(params) ||
                               hnsw_params.quantParams != nullptr;
        est = HNSWFactory::EstimateElementSize(&hnsw_params, /* with_mean = */ with_mean);
        const auto bf_params = TieredHNSWFactory::NewBFParams(params);
        est = std::max(est, BruteForceFactory::EstimateElementSize(&bf_params));
    }
    if (params->primaryIndexParams->algo == VecSimAlgo_SVS) {
        est = SVSFactory::EstimateElementSize(&params->primaryIndexParams->algoParams.svsParams);
    }
    return est;
}

}; // namespace TieredFactory
