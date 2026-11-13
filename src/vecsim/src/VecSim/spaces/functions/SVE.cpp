/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "SVE.h"

#include "VecSim/spaces/L2/L2_SVE_FP32.h"
#include "VecSim/spaces/IP/IP_SVE_FP32.h"

#include "VecSim/spaces/IP/IP_SVE_FP16.h"
#include "VecSim/spaces/L2/L2_SVE_FP16.h"

#include "VecSim/spaces/IP/IP_SVE_FP64.h"
#include "VecSim/spaces/L2/L2_SVE_FP64.h"

#include "VecSim/spaces/L2/L2_SVE_INT8.h"
#include "VecSim/spaces/IP/IP_SVE_INT8.h"

#include "VecSim/spaces/L2/L2_SVE_UINT8.h"
#include "VecSim/spaces/IP/IP_SVE_UINT8.h"
#include "VecSim/spaces/IP/IP_SVE_SQ8_FP32.h"
#include "VecSim/spaces/L2/L2_SVE_SQ8_FP32.h"

#include "VecSim/spaces/IP/IP_SVE_SQ8_FP16.h"
#include "VecSim/spaces/L2/L2_SVE_SQ8_FP16.h"

#include "VecSim/spaces/IP/IP_SVE_SQ8_SQ8.h"
#include "VecSim/spaces/L2/L2_SVE_SQ8_SQ8.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_FP32_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP32_InnerProductSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}
dist_func_t<float> Choose_FP32_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP32_L2SqrSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_FP16_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP16_InnerProduct_SVE, dim, svcnth);
    return ret_dist_func;
}
dist_func_t<float> Choose_FP16_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP16_L2Sqr_SVE, dim, svcnth);
    return ret_dist_func;
}

dist_func_t<double> Choose_FP64_IP_implementation_SVE(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP64_InnerProductSIMD_SVE, dim, svcntd);
    return ret_dist_func;
}
dist_func_t<double> Choose_FP64_L2_implementation_SVE(size_t dim) {
    dist_func_t<double> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, FP64_L2SqrSIMD_SVE, dim, svcntd);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, INT8_L2SqrSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, INT8_InnerProductSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_Cosine_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, INT8_CosineSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, UINT8_L2SqrSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, UINT8_InnerProductSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_Cosine_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, UINT8_CosineSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP32_InnerProductSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_Cosine_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP32_CosineSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP32_L2SqrSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP16_InnerProductSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_Cosine_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP16_CosineSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP16_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_FP16_L2SqrSIMD_SVE, dim, svcntw);
    return ret_dist_func;
}

// SQ8-to-SQ8 distance functions (both vectors are uint8 quantized with precomputed sum)
// Note: Use svcntb for uint8 elements (not svcntw which is for 32-bit elements)
dist_func_t<float> Choose_SQ8_SQ8_IP_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_SQ8_InnerProductSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_Cosine_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_SQ8_CosineSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_L2_implementation_SVE(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_SVE_IMPLEMENTATION(ret_dist_func, SQ8_SQ8_L2SqrSIMD_SVE, dim, svcntb);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
