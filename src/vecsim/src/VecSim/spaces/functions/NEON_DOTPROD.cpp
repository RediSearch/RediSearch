/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "NEON.h"
#include "VecSim/spaces/IP/IP_NEON_DOTPROD_INT8.h"
#include "VecSim/spaces/IP/IP_NEON_DOTPROD_UINT8.h"
#include "VecSim/spaces/IP/IP_NEON_DOTPROD_SQ8_SQ8.h"
#include "VecSim/spaces/L2/L2_NEON_DOTPROD_INT8.h"
#include "VecSim/spaces/L2/L2_NEON_DOTPROD_UINT8.h"
#include "VecSim/spaces/L2/L2_NEON_DOTPROD_SQ8_SQ8.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_INT8_IP_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_InnerProductSIMD16_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_IP_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_InnerProductSIMD16_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_Cosine_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_CosineSIMD_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_Cosine_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_CosineSIMD_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_L2_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_L2SqrSIMD16_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_L2_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_L2SqrSIMD16_NEON_DOTPROD);
    return ret_dist_func;
}

// SQ8-to-SQ8 distance functions (both vectors are uint8 quantized with precomputed sum)
dist_func_t<float> Choose_SQ8_SQ8_IP_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_InnerProductSIMD64_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_Cosine_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_CosineSIMD64_NEON_DOTPROD);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_L2_implementation_NEON_DOTPROD(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_L2SqrSIMD64_NEON_DOTPROD);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
