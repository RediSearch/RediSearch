/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/spaces/spaces.h"

namespace spaces {

dist_func_t<float> Choose_SQ8_FP32_IP_implementation_AVX2_FMA(size_t dim);
dist_func_t<float> Choose_SQ8_FP32_Cosine_implementation_AVX2_FMA(size_t dim);
dist_func_t<float> Choose_SQ8_FP32_L2_implementation_AVX2_FMA(size_t dim);

// SQ4 storage with an FP32 query
dist_func_t<float> Choose_SQ4_FP32_IP_implementation_AVX2_FMA(size_t dim);
dist_func_t<float> Choose_SQ4_FP32_L2_implementation_AVX2_FMA(size_t dim);

// TQ2/TQ3: packed codes against an FP32 rotated query, and packed codes against packed codes.
tq_packed_dot_t Choose_TQ_FP32_IP_implementation_AVX2_FMA(unsigned bits);
tq_packed_pair_dot_t Choose_TQ_TQ_IP_implementation_AVX2_FMA(unsigned bits);

} // namespace spaces
