/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "AVX512F_BW_VL_VNNI.h"

#include "VecSim/spaces/L2/L2_AVX512F_BW_VL_VNNI_INT8.h"
#include "VecSim/spaces/IP/IP_AVX512F_BW_VL_VNNI_INT8.h"

#include "VecSim/spaces/L2/L2_AVX512F_BW_VL_VNNI_UINT8.h"
#include "VecSim/spaces/IP/IP_AVX512F_BW_VL_VNNI_UINT8.h"

#include "VecSim/spaces/IP/IP_AVX512F_BW_VL_VNNI_SQ8_FP32.h"
#include "VecSim/spaces/L2/L2_AVX512F_BW_VL_VNNI_SQ8_FP32.h"

#include "VecSim/spaces/IP/IP_AVX512F_BW_VL_VNNI_SQ8_SQ8.h"
#include "VecSim/spaces/L2/L2_AVX512F_BW_VL_VNNI_SQ8_SQ8.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_INT8_L2_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_L2SqrSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_IP_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_InnerProductSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_INT8_Cosine_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, INT8_CosineSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_L2_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_L2SqrSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_IP_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_InnerProductSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_UINT8_Cosine_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, UINT8_CosineSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_FP32_IP_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_InnerProductSIMD16_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}
dist_func_t<float> Choose_SQ8_FP32_Cosine_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_CosineSIMD16_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}
dist_func_t<float> Choose_SQ8_FP32_L2_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, SQ8_FP32_L2SqrSIMD16_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}
// SQ8-to-SQ8 distance functions (both vectors are uint8 quantized with precomputed sum)
dist_func_t<float> Choose_SQ8_SQ8_IP_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_InnerProductSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_Cosine_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_CosineSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

dist_func_t<float> Choose_SQ8_SQ8_L2_implementation_AVX512F_BW_VL_VNNI(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 64, SQ8_SQ8_L2SqrSIMD64_AVX512F_BW_VL_VNNI);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
