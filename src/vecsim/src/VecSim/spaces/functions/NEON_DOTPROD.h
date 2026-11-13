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

dist_func_t<float> Choose_INT8_IP_implementation_NEON_DOTPROD(size_t dim);
dist_func_t<float> Choose_INT8_Cosine_implementation_NEON_DOTPROD(size_t dim);

dist_func_t<float> Choose_UINT8_IP_implementation_NEON_DOTPROD(size_t dim);
dist_func_t<float> Choose_UINT8_Cosine_implementation_NEON_DOTPROD(size_t dim);

dist_func_t<float> Choose_INT8_L2_implementation_NEON_DOTPROD(size_t dim);
dist_func_t<float> Choose_UINT8_L2_implementation_NEON_DOTPROD(size_t dim);

// SQ8-to-SQ8 DOTPROD-optimized distance functions (with precomputed sum)
dist_func_t<float> Choose_SQ8_SQ8_IP_implementation_NEON_DOTPROD(size_t dim);
dist_func_t<float> Choose_SQ8_SQ8_Cosine_implementation_NEON_DOTPROD(size_t dim);
dist_func_t<float> Choose_SQ8_SQ8_L2_implementation_NEON_DOTPROD(size_t dim);

} // namespace spaces
