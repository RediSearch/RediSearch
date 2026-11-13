/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/index_factories/brute_force_factory.h"
#include "VecSim/algorithms/brute_force/brute_force.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/algorithms/brute_force/brute_force_multi.h"
#include "VecSim/index_factories/components/components_factory.h"
#include "VecSim/index_factories/factory_utils.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

namespace BruteForceFactory {
template <typename DataType, typename DistType = DataType>
inline VecSimIndex *NewIndex_ChooseMultiOrSingle(const BFParams *params,
                                                 const AbstractIndexInitParams &abstractInitParams,
                                                 IndexComponents<DataType, DistType> &components) {

    // check if single and return new bf_index
    if (params->multi)
        return new (abstractInitParams.allocator)
            BruteForceIndex_Multi<DataType, DistType>(params, abstractInitParams, components);
    else
        return new (abstractInitParams.allocator)
            BruteForceIndex_Single<DataType, DistType>(params, abstractInitParams, components);
}

VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized) {
    const BFParams *bfParams = &params->algoParams.bfParams;
    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(bfParams, params->logCtx, is_normalized);
    return NewIndex(bfParams, abstractInitParams, is_normalized);
}

VecSimIndex *NewIndex(const BFParams *bfparams, const AbstractIndexInitParams &abstractInitParams,
                      bool is_normalized) {
    assert(is_normalized ||
           abstractInitParams.inputBlobSize == bfparams->dim * VecSimType_sizeof(bfparams->type));
    assert(!is_normalized ||
           abstractInitParams.inputBlobSize != bfparams->dim * VecSimType_sizeof(bfparams->type));
    if (bfparams->type == VecSimType_FLOAT32) {
        IndexComponents<float, float> indexComponents = CreateIndexComponents<float, float>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float>(bfparams, abstractInitParams, indexComponents);
    } else if (bfparams->type == VecSimType_FLOAT64) {
        IndexComponents<double, double> indexComponents = CreateIndexComponents<double, double>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<double>(bfparams, abstractInitParams, indexComponents);
    } else if (bfparams->type == VecSimType_BFLOAT16) {
        IndexComponents<bfloat16, float> indexComponents = CreateIndexComponents<bfloat16, float>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<bfloat16, float>(bfparams, abstractInitParams,
                                                             indexComponents);
    } else if (bfparams->type == VecSimType_FLOAT16) {
        IndexComponents<float16, float> indexComponents = CreateIndexComponents<float16, float>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float16, float>(bfparams, abstractInitParams,
                                                            indexComponents);
    } else if (bfparams->type == VecSimType_INT8) {
        IndexComponents<int8_t, float> indexComponents = CreateIndexComponents<int8_t, float>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<int8_t, float>(bfparams, abstractInitParams,
                                                           indexComponents);
    } else if (bfparams->type == VecSimType_UINT8) {
        IndexComponents<uint8_t, float> indexComponents = CreateIndexComponents<uint8_t, float>(
            abstractInitParams.allocator, bfparams->metric, bfparams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<uint8_t, float>(bfparams, abstractInitParams,
                                                            indexComponents);
    }

    // If we got here something is wrong.
    return NULL;
}

VecSimIndex *NewIndex(const BFParams *bfparams, bool is_normalized) {
    VecSimParams params = {.algoParams{.bfParams = BFParams{*bfparams}}};
    return NewIndex(&params, is_normalized);
}

template <typename DataType, typename DistType = DataType>
inline size_t EstimateInitialSize_ChooseMultiOrSingle(bool is_multi) {
    // check if single and return new bf_index
    if (is_multi)
        return sizeof(BruteForceIndex_Multi<DataType, DistType>);
    else
        return sizeof(BruteForceIndex_Single<DataType, DistType>);
}

size_t EstimateInitialSize(const BFParams *params, bool is_normalized) {

    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();

    // Constant part (not effected by parameters).
    size_t est = sizeof(VecSimAllocator) + allocations_overhead;

    if (params->type == VecSimType_FLOAT32) {
        est += EstimateComponentsMemory<float, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<float>(params->multi);
    } else if (params->type == VecSimType_FLOAT64) {
        est += EstimateComponentsMemory<double, double>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<double>(params->multi);
    } else if (params->type == VecSimType_BFLOAT16) {
        est += EstimateComponentsMemory<bfloat16, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<bfloat16, float>(params->multi);
    } else if (params->type == VecSimType_FLOAT16) {
        est += EstimateComponentsMemory<float16, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<float16, float>(params->multi);
    } else if (params->type == VecSimType_INT8) {
        est += EstimateComponentsMemory<int8_t, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<int8_t, float>(params->multi);
    } else if (params->type == VecSimType_UINT8) {
        est += EstimateComponentsMemory<uint8_t, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<uint8_t, float>(params->multi);
    } else {
        throw std::invalid_argument("Invalid params->type");
    }

    est += sizeof(DataBlocksContainer) + allocations_overhead;
    return est;
}

size_t EstimateElementSize(const BFParams *params) {
    // counting the vector size + idToLabel entry + LabelToIds entry (map reservation)
    return VecSimParams_GetStoredDataSize(params->type, params->dim, params->metric) +
           sizeof(labelType) + sizeof(void *);
}
}; // namespace BruteForceFactory
