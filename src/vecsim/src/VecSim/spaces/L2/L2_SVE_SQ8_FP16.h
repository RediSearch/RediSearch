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
#include "VecSim/spaces/IP/IP_SVE_SQ8_FP16.h"

/*
 * SVE SQ8<->FP16 L2 squared distance:
 *   ||x - y||^2 = x_sum_squares - 2 * IP(x, y) + y_sum_squares
 * IP is computed by SQ8_FP16_InnerProductSIMD_SVE_IMP; metadata is FP32.
 */

template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP16_L2SqrSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const float ip = SQ8_FP16_InnerProductSIMD_SVE_IMP<partial_chunk, additional_steps>(
        pVect1v, pVect2v, dimension);

    const uint8_t *params_bytes = static_cast<const uint8_t *>(pVect1v) + dimension;
    const float x_sum_sq = load_unaligned<float>(params_bytes + sq8::SUM_SQUARES * sizeof(float));
    const uint8_t *query_meta_bytes =
        reinterpret_cast<const uint8_t *>(static_cast<const float16 *>(pVect2v) + dimension);
    const float y_sum_sq =
        load_unaligned<float>(query_meta_bytes + sq8::SUM_SQUARES_QUERY * sizeof(float));

    return x_sum_sq + y_sum_sq - 2.0f * ip;
}
