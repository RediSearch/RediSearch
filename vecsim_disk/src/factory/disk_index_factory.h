/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <stdexcept>

#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/types/sq8.h"

namespace VecSimDiskFactory {
VecSimIndex* NewIndex(const VecSimParamsDisk* params);

// Calculate the stored data size for SQ8 quantization given dimension and metric
// storage_metadata_count

template <VecSimMetric Metric>
constexpr size_t GetSQ8StoredDataSizeT(size_t dim) {
    constexpr size_t metadata_floats = vecsim_types::sq8::storage_metadata_count<Metric>();
    return dim * sizeof(uint8_t) + metadata_floats * sizeof(float);
}

inline size_t GetSQ8StoredDataSize(size_t dim, VecSimMetric metric) {
    switch (metric) {
    case VecSimMetric_L2:
        return GetSQ8StoredDataSizeT<VecSimMetric_L2>(dim);
    case VecSimMetric_IP:
        return GetSQ8StoredDataSizeT<VecSimMetric_IP>(dim);
    case VecSimMetric_Cosine:
        return GetSQ8StoredDataSizeT<VecSimMetric_Cosine>(dim);
    default:
        throw std::invalid_argument("Invalid metric");
    }
}

// Create init params for the disk HNSW index using SQ8 quantization.
// storedDataSize = SQ8 quantized size (for in-memory RawDataContainer)
// inputBlobSize = FP32 size (vectors come from frontend in FP32)
template <typename IndexParams>
AbstractIndexInitParams NewDiskInitParams(const IndexParams* algo_params, void* logCtx,
                                          std::shared_ptr<VecSimAllocator> allocator) {
    size_t storedDataSize = GetSQ8StoredDataSize(algo_params->dim, algo_params->metric);
    size_t inputBlobSize = algo_params->dim * VecSimType_sizeof(algo_params->type);

    AbstractIndexInitParams abstractInitParams = {.allocator = allocator,
                                                  .dim = algo_params->dim,
                                                  .vecType = algo_params->type,
                                                  .storedDataSize = storedDataSize,
                                                  .metric = algo_params->metric,
                                                  .blockSize = algo_params->blockSize,
                                                  .multi = algo_params->multi,
                                                  .isDisk = true,
                                                  .logCtx = logCtx,
                                                  .inputBlobSize = inputBlobSize};
    return abstractInitParams;
}

} // namespace VecSimDiskFactory
