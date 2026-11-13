/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstdlib> // size_t
#include <memory>  // std::shared_ptr

#include "VecSim/vec_sim.h"              //typedef VecSimIndex
#include "VecSim/vec_sim_common.h"       // BFParams
#include "VecSim/memory/vecsim_malloc.h" // VecSimAllocator
#include "VecSim/vec_sim_index.h"

namespace BruteForceFactory {
/** Overloading the NewIndex function to support different parameters
 * @param is_normalized is used to determine the index's computer type. If the index metric is
 * Cosine, and is_normalized == true, we will create the computer as if the metric is IP, assuming
 * the blobs sent to the index are already normalized. For example, in case it's a tiered index,
 * where the blobs are normalized by the frontend index.
 */
VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized = false);
VecSimIndex *NewIndex(const BFParams *bfparams, bool is_normalized = false);
VecSimIndex *NewIndex(const BFParams *bfparams, const AbstractIndexInitParams &abstractInitParams,
                      bool is_normalized);
size_t EstimateInitialSize(const BFParams *params, bool is_normalized = false);
size_t EstimateElementSize(const BFParams *params);

}; // namespace BruteForceFactory
