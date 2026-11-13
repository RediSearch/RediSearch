/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/vec_sim_index.h"

namespace VecSimFactory {
template <typename IndexParams>
static AbstractIndexInitParams NewAbstractInitParams(const IndexParams *algo_params, void *logCtx,
                                                     bool is_input_preprocessed) {

    size_t storedDataSize =
        VecSimParams_GetStoredDataSize(algo_params->type, algo_params->dim, algo_params->metric);

    // If the input vectors are already processed (for example, normalized), the input blob size is
    // the same as the stored data size. inputBlobSize = storedDataSize Otherwise, the input blob
    // size is the original size of the vector. inputBlobSize = algo_params->dim *
    // VecSimType_sizeof(algo_params->type)
    size_t inputBlobSize = is_input_preprocessed
                               ? storedDataSize
                               : algo_params->dim * VecSimType_sizeof(algo_params->type);
    AbstractIndexInitParams abstractInitParams = {.allocator =
                                                      VecSimAllocator::newVecsimAllocator(),
                                                  .dim = algo_params->dim,
                                                  .vecType = algo_params->type,
                                                  .storedDataSize = storedDataSize,
                                                  .metric = algo_params->metric,
                                                  .blockSize = algo_params->blockSize,
                                                  .multi = algo_params->multi,
                                                  .isDisk = false,
                                                  .logCtx = logCtx,
                                                  .inputBlobSize = inputBlobSize};
    return abstractInitParams;
}
} // namespace VecSimFactory
