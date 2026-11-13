/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/spaces/spaces.h"

namespace spaces {

dist_func_t<float> Choose_FP32_IP_implementation_SSE(size_t dim);
dist_func_t<double> Choose_FP64_IP_implementation_SSE(size_t dim);

dist_func_t<float> Choose_FP32_L2_implementation_SSE(size_t dim);
dist_func_t<double> Choose_FP64_L2_implementation_SSE(size_t dim);

} // namespace spaces
