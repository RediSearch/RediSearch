/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "NEON.h"
#include "VecSim/spaces/L2/L2_NEON_FP32.h"
#include "VecSim/spaces/IP/IP_NEON_FP32.h"
#include "VecSim/spaces/L2/L2_NEON_INT8.h"
#include "VecSim/spaces/L2/L2_NEON_UINT8.h"
#include "VecSim/spaces/IP/IP_NEON_INT8.h"
#include "VecSim/spaces/IP/IP_NEON_UINT8.h"
#include "VecSim/spaces/L2/L2_NEON_FP64.h"
#include "VecSim/spaces/IP/IP_NEON_FP64.h"
#include "VecSim/spaces/L2/L2_NEON_SQ8_FP32.h"
#include "VecSim/spaces/IP/IP_NEON_SQ8_FP32.h"
#include "VecSim/spaces/IP/IP_NEON_SQ8_SQ8.h"
#include "VecSim/spaces/L2/L2_NEON_SQ8_SQ8.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_INT8_IP_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_InnerProductSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_IP_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_InnerProductSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_FP32_IP_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, FP32_InnerProductSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<double> Choose_FP64_IP_implementation_NEON(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 8, FP64_InnerProductSIMD8_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_Cosine_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_CosineSIMD_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_Cosine_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_CosineSIMD_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_FP32_L2_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, FP32_L2SqrSIMD16_NEON);
    return ret_dist_func;
}
dist_func_t<float> Choose_INT8_L2_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_L2SqrSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_L2_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_L2SqrSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<double> Choose_FP64_L2_implementation_NEON(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 8, FP64_L2SqrSIMD8_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_L2_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP32_L2SqrSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_IP_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP32_InnerProductSIMD16_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_Cosine_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 16, SQ8_FP32_CosineSIMD16_NEON);
    return ret_dist_func;
}

// SQ8-to-SQ8 distance functions (both vectors are uint8 quantized with precomputed sum)
// Uses 64-element chunking to leverage efficient UINT8_InnerProductImp
dist_func_t<float> Choose_SQ8_SQ8_IP_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_InnerProductSIMD64_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_Cosine_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_CosineSIMD64_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_L2_implementation_NEON(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_L2SqrSIMD64_NEON);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
