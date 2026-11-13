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
#include "VecSim/spaces/IP/IP_NEON_UINT8.h"
#include "VecSim/types/sq8.h"
#include <arm_neon.h>

using sq8 = vecsim_types::sq8;

/**
 * SQ8-to-SQ8 distance functions using ARM NEON with precomputed sum.
 * These functions compute distance between two SQ8 (scalar quantized 8-bit) vectors,
 * where BOTH vectors are uint8 quantized.
 *
 * Uses precomputed sum stored in the vector data,
 * eliminating the need to compute them during distance calculation.
 *
 * Uses algebraic optimization:
 *
 * With sum = Σv[i] (sum of original float values), the formula is:
 * IP = min1*sum2 + min2*sum1 + δ1*δ2 * Σ(q1[i]*q2[i]) - dim*min1*min2
 *
 * Since sum is precomputed, we only need to compute the dot product Σ(q1[i]*q2[i]).
 * The dot product is computed using the efficient UINT8_InnerProductImp which uses
 * native NEON uint8 multiply-accumulate instructions (vmull_u8, vpadalq_u16).
 *
 * Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
 */

// Common implementation for inner product between two SQ8 vectors with precomputed sum
// Uses UINT8_InnerProductImp for efficient dot product computation
template <unsigned char residual> // 0..63
float SQ8_SQ8_InnerProductSIMD64_NEON_IMP(const void *pVec1v, const void *pVec2v,
                                          size_t dimension) {
    // Compute raw dot product using efficient UINT8 implementation
    // UINT8_InnerProductImp processes 16 elements at a time using native uint8 instructions
    float dot_product = UINT8_InnerProductImp<residual>(pVec1v, pVec2v, dimension);

    // Get dequantization parameters and precomputed values from the end of pVec1
    // Layout: [data (dim)] [min (float)] [delta (float)] [sum (float)]
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v);
    const uint8_t *pVec2 = static_cast<const uint8_t *>(pVec2v);

    const auto *params1 = pVec1 + dimension;
    const float min1 = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta1 = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const float sum1 = load_unaligned<float>(params1 + sq8::SUM * sizeof(float));

    // Get dequantization parameters and precomputed values from the end of pVec2
    const auto *params2 = pVec2 + dimension;
    const float min2 = load_unaligned<float>(params2 + sq8::MIN_VAL * sizeof(float));
    const float delta2 = load_unaligned<float>(params2 + sq8::DELTA * sizeof(float));
    const float sum2 = load_unaligned<float>(params2 + sq8::SUM * sizeof(float));

    // Apply algebraic formula using precomputed sums:
    // IP = min1*sum2 + min2*sum1 + δ1*δ2 * Σ(q1*q2) - dim*min1*min2
    return min1 * sum2 + min2 * sum1 + delta1 * delta2 * dot_product -
           static_cast<float>(dimension) * min1 * min2;
}

// SQ8-to-SQ8 Inner Product distance function
// Returns 1 - inner_product (distance form)
template <unsigned char residual> // 0..63
float SQ8_SQ8_InnerProductSIMD64_NEON(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return 1.0f - SQ8_SQ8_InnerProductSIMD64_NEON_IMP<residual>(pVec1v, pVec2v, dimension);
}

// SQ8-to-SQ8 Cosine distance function
// Returns 1 - inner_product (assumes vectors are pre-normalized)
template <unsigned char residual> // 0..63
float SQ8_SQ8_CosineSIMD64_NEON(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return SQ8_SQ8_InnerProductSIMD64_NEON<residual>(pVec1v, pVec2v, dimension);
}
