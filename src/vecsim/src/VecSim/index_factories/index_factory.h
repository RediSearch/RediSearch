/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_common.h"
#include "VecSim/memory/vecsim_malloc.h"

namespace VecSimFactory {
VecSimIndex *NewIndex(const VecSimParams *params);
size_t EstimateInitialSize(const VecSimParams *params);
size_t EstimateElementSize(const VecSimParams *params);
}; // namespace VecSimFactory
