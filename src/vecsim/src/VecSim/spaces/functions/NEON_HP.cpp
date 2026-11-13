/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "NEON_HP.h"

#include "VecSim/spaces/L2/L2_NEON_FP16.h"
#include "VecSim/spaces/IP/IP_NEON_FP16.h"
#include "VecSim/spaces/IP/IP_NEON_SQ8_FP16.h"
#include "VecSim/spaces/L2/L2_NEON_SQ8_FP16.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_FP16_L2_implementation_NEON_HP(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, FP16_L2Sqr_NEON_HP);
    return ret_dist_func;
}

dist_func_t<float> Choose_FP16_IP_implementation_NEON_HP(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, FP16_InnerProduct_NEON_HP);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_IP_implementation_NEON_HP(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP16_InnerProductSIMD16_NEON_HP);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_L2_implementation_NEON_HP(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP16_L2SqrSIMD16_NEON_HP);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_Cosine_implementation_NEON_HP(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP16_CosineSIMD16_NEON_HP);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
