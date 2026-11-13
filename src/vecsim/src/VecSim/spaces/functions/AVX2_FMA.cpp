/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "AVX2_FMA.h"
#include "VecSim/spaces/L2/L2_AVX2_FMA_SQ8_FP32.h"
#include "VecSim/spaces/IP/IP_AVX2_FMA_SQ8_FP32.h"

namespace spaces {

#include "implementation_chooser.h"
// FMA optimized implementations
dist_func_t<float> Choose_SQ8_FP32_IP_implementation_AVX2_FMA(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_InnerProductSIMD16_AVX2_FMA);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_Cosine_implementation_AVX2_FMA(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_CosineSIMD16_AVX2_FMA);
    return ret_dist_func;
}
dist_func_t<float> Choose_SQ8_FP32_L2_implementation_AVX2_FMA(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_L2SqrSIMD16_AVX2_FMA);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
