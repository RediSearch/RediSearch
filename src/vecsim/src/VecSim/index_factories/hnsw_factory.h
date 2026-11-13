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
#include "VecSim/vec_sim_common.h"       // HNSWParams
#include "VecSim/memory/vecsim_malloc.h" // VecSimAllocator
#include "VecSim/vec_sim_index.h"

namespace HNSWFactory {
/** @param is_normalized is used to determine the index's computer type. If the index metric is
 * Cosine, and is_normalized == true, we will create the computer as if the metric is IP, assuming
 * the blobs sent to the index are already normalized. For example, in case it's a tiered index,
 * where the blobs are normalized by the frontend index.
 */
VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized = false);
VecSimIndex *NewIndex(const HNSWParams *params, bool is_normalized = false);
size_t EstimateInitialSize(const HNSWParams *params, bool is_normalized = false);
size_t EstimateInitialSize(const HNSWParams *params, bool is_normalized, bool with_mean);
size_t EstimateElementSize(const HNSWParams *params);
size_t EstimateElementSize(const HNSWParams *params, bool with_mean);

#ifdef BUILD_TESTS
/** Load a serialized HNSW index.
 *
 * For SQ8 indexes with Cosine metric, pass is_normalized=true when the stored vectors were
 * pre-normalized before insertion. This loads the SQ8 backend using inner product, which is
 * equivalent to cosine for those vectors. Queries supplied to the loaded index must also be
 * normalized. The default false value intentionally rejects standalone SQ8 Cosine indexes.
 */
VecSimIndex *NewIndex(const std::string &location, bool is_normalized = false);

#endif

}; // namespace HNSWFactory
