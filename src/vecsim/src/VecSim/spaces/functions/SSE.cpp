/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "SSE.h"

#include "VecSim/spaces/L2/L2_SSE_FP32.h"
#include "VecSim/spaces/L2/L2_SSE_FP64.h"
#include "VecSim/spaces/L2/L2_SSE4_SQ8_FP32.h"

#include "VecSim/spaces/IP/IP_SSE_FP32.h"
#include "VecSim/spaces/IP/IP_SSE_FP64.h"
#include "VecSim/spaces/IP/IP_SSE4_SQ8_FP32.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_FP32_IP_implementation_SSE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, FP32_InnerProductSIMD16_SSE);
    return ret_dist_func;
}

dist_func_t<double> Choose_FP64_IP_implementation_SSE(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 4, FP64_InnerProductSIMD8_SSE);
    return ret_dist_func;
}

dist_func_t<float> Choose_FP32_L2_implementation_SSE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, FP32_L2SqrSIMD16_SSE);
    return ret_dist_func;
}

dist_func_t<double> Choose_FP64_L2_implementation_SSE(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 4, FP64_L2SqrSIMD8_SSE);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
