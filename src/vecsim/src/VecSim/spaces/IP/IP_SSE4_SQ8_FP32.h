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
#include "VecSim/types/sq8.h"
using sq8 = vecsim_types::sq8;

/*
 * Optimized asymmetric SQ8 inner product using algebraic identity:
 *
 *   IP(x, y) = Σ(x_i * y_i)
 *            ≈ Σ((min + delta * q_i) * y_i)
 *            = min * Σy_i + delta * Σ(q_i * y_i)
 *            = min * y_sum + delta * quantized_dot_product
 *
 * where y_sum = Σy_i is precomputed and stored in the query blob.
 * This avoids dequantization in the hot loop - we only compute Σ(q_i * y_i).
 */

// Helper: compute Σ(q_i * y_i) for 4 elements (no dequantization)
// pVect1 = SQ8 storage (quantized values), pVect2 = FP32 query
static inline void InnerProductStepSQ8_FP32(const uint8_t *&pVect1, const float *&pVect2,
                                            __m128 &sum) {
    // Load 4 uint8 elements and convert to float
    __m128i v1_i = _mm_cvtepu8_epi32(_mm_cvtsi32_si128(load_unaligned<int32_t>(pVect1)));
    pVect1 += 4;

    __m128 v1_f = _mm_cvtepi32_ps(v1_i);

    // Load 4 float elements from query
    __m128 v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;

    // Accumulate q_i * y_i (no dequantization!)
    // SSE doesn't have FMA, so use mul + add
    sum = _mm_add_ps(sum, _mm_mul_ps(v1_f, v2));
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <unsigned char residual> // 0..15
float SQ8_FP32_InnerProductSIMD16_SSE4_IMP(const void *pVect1v, const void *pVect2v,
                                           size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    const uint8_t *pEnd1 = pVect1 + dimension;

    // Initialize sum accumulators for Σ(q_i * y_i). Four accumulators break the mul->add
    // dependency chain, letting more loads/adds be in flight at once.
    __m128 sum0 = _mm_setzero_ps();
    __m128 sum1 = _mm_setzero_ps();
    __m128 sum2 = _mm_setzero_ps();
    __m128 sum3 = _mm_setzero_ps();

    // Process residual elements first (1-3 elements). Loads touch only the residual elements,
    // so they are safe for any dimension.
    if constexpr (residual % 4) {
        __m128 v1_f;
        __m128 v2;

        if constexpr (residual % 4 == 3) {
            v1_f = _mm_set_ps(0.0f, static_cast<float>(pVect1[2]), static_cast<float>(pVect1[1]),
                              static_cast<float>(pVect1[0]));
            v2 = _mm_set_ps(0.0f, pVect2[2], pVect2[1], pVect2[0]);
        } else if constexpr (residual % 4 == 2) {
            v1_f = _mm_set_ps(0.0f, 0.0f, static_cast<float>(pVect1[1]),
                              static_cast<float>(pVect1[0]));
            v2 = _mm_set_ps(0.0f, 0.0f, pVect2[1], pVect2[0]);
        } else if constexpr (residual % 4 == 1) {
            v1_f = _mm_set_ps(0.0f, 0.0f, 0.0f, static_cast<float>(pVect1[0]));
            v2 = _mm_set_ps(0.0f, 0.0f, 0.0f, pVect2[0]);
        }

        pVect1 += residual % 4;
        pVect2 += residual % 4;

        // Compute q_i * y_i (no dequantization)
        sum0 = _mm_mul_ps(v1_f, v2);
    }

    // Handle remaining residual in chunks of 4 (for residual 4-15)
    if constexpr (residual >= 4) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum1);
    }
    if constexpr (residual >= 8) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum2);
    }
    if constexpr (residual >= 12) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum3);
    }

    // Process remaining full chunks of 16 elements (4x4). The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum0);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum1);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum2);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum3);
    }

    // Horizontal sum to get Σ(q_i * y_i)
    __m128 sum = _mm_add_ps(_mm_add_ps(sum0, sum1), _mm_add_ps(sum2, sum3));
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum);
    float quantized_dot = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVect1Base = static_cast<const uint8_t *>(pVect1v);
    const auto *params1 = pVect1Base + dimension;
    const float min_val = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (stored after the dim floats)
    const float *pVect2Base = static_cast<const float *>(pVect2v);
    const float y_sum = pVect2Base[dimension + sq8::SUM_QUERY];

    // Apply the algebraic formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

template <unsigned char residual> // 0..15
float SQ8_FP32_InnerProductSIMD16_SSE4(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP32_InnerProductSIMD16_SSE4_IMP<residual>(pVect1v, pVect2v, dimension);
}

template <unsigned char residual> // 0..15
float SQ8_FP32_CosineSIMD16_SSE4(const void *pVect1v, const void *pVect2v, size_t dimension) {
    // Cosine distance = 1 - IP (vectors are pre-normalized)
    return SQ8_FP32_InnerProductSIMD16_SSE4<residual>(pVect1v, pVect2v, dimension);
}
