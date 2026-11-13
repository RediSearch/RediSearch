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
#include "VecSim/spaces/IP/IP_NEON_DOTPROD_SQ8_SQ8.h"
#include "VecSim/types/sq8.h"

using sq8 = vecsim_types::sq8;

/**
 * SQ8-to-SQ8 L2 squared distance functions for NEON with DOTPROD extension.
 * Computes L2 squared distance between two SQ8 (scalar quantized 8-bit) vectors,
 * where BOTH vectors are uint8 quantized.
 *
 * Uses the identity: ||x - y||² = ||x||² + ||y||² - 2*IP(x, y)
 * where ||x||² and ||y||² are precomputed sum of squares stored in the vector data.
 *
 * Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
 * [sum_of_squares (float)]
 */

// L2 squared distance using the common inner product implementation
template <unsigned char residual> // 0..63
float SQ8_SQ8_L2SqrSIMD64_NEON_DOTPROD(const void *pVec1v, const void *pVec2v, size_t dimension) {
    // Use the common inner product implementation (returns raw IP, not distance)
    const float ip =
        SQ8_SQ8_InnerProductSIMD64_NEON_DOTPROD_IMP<residual>(pVec1v, pVec2v, dimension);

    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v);
    const uint8_t *pVec2 = static_cast<const uint8_t *>(pVec2v);

    // Get precomputed sum of squares from both vectors
    // Layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_of_squares]
    const float sum_sq_1 =
        load_unaligned<float>(pVec1 + dimension + sq8::SUM_SQUARES * sizeof(float));
    const float sum_sq_2 =
        load_unaligned<float>(pVec2 + dimension + sq8::SUM_SQUARES * sizeof(float));

    // L2² = ||x||² + ||y||² - 2*IP(x, y)
    return sum_sq_1 + sum_sq_2 - 2.0f * ip;
}
