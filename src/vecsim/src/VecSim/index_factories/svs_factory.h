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
#include <string>

#include "VecSim/vec_sim.h"        //typedef VecSimIndex
#include "VecSim/vec_sim_common.h" // VecSimParams, SVSParams

namespace SVSFactory {
VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized = false);
#ifdef BUILD_TESTS
VecSimIndex *NewIndex(const std::string &location, const VecSimParams *params,
                      bool is_normalized = false);
#endif
size_t EstimateInitialSize(const SVSParams *params, bool is_normalized = false);
size_t EstimateElementSize(const SVSParams *params);
}; // namespace SVSFactory
