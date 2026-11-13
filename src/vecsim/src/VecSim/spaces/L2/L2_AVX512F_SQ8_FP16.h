/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/IP/IP_AVX512F_SQ8_FP16.h"
#include "VecSim/types/sq8.h"

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

// L2² = x_sum_squares + y_sum_squares - 2 * IP(x, y), computed via the AVX-512 IP impl above.
template <unsigned char residual> // 0..15
float SQ8_FP16_L2SqrSIMD16_AVX512F(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const float ip = SQ8_FP16_InnerProductImp_AVX512<residual>(pVect1v, pVect2v, dimension);

    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const uint8_t *params_bytes = pVect1 + dimension;
    const float x_sum_sq = load_unaligned<float>(params_bytes + sq8::SUM_SQUARES * sizeof(float));

    const float16 *pVect2 = static_cast<const float16 *>(pVect2v);
    const auto *query_meta_bytes = reinterpret_cast<const uint8_t *>(pVect2 + dimension);
    const float y_sum_sq =
        load_unaligned<float>(query_meta_bytes + sq8::SUM_SQUARES_QUERY * sizeof(float));

    return x_sum_sq + y_sum_sq - 2.0f * ip;
}
