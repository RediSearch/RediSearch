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
#include "VecSim/spaces/AVX_utils.h"
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

// Helper: compute Σ(q_i * y_i) for 8 elements (no dequantization)
// pVect1 = SQ8 storage (quantized values), pVect2 = FP32 query
static inline void InnerProductStepSQ8_FP32(const uint8_t *&pVect1, const float *&pVect2,
                                            __m256 &sum256) {
    // Load 8 uint8 elements and convert to float
    __m128i v1_128 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVect1));
    pVect1 += 8;

    __m256i v1_256 = _mm256_cvtepu8_epi32(v1_128);
    __m256 v1_f = _mm256_cvtepi32_ps(v1_256);

    // Load 8 float elements from query
    __m256 v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    // Accumulate q_i * y_i (no dequantization!)
    // Using mul + add since this is the non-FMA version
    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1_f, v2));
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <unsigned char residual> // 0..31
float SQ8_FP32_InnerProductImp_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    const uint8_t *pEnd1 = pVect1 + dimension;

    // Initialize sum accumulators for Σ(q_i * y_i). Four accumulators break the mul->add
    // dependency chain, letting more loads/adds be in flight at once.
    __m256 sum0 = _mm256_setzero_ps();
    __m256 sum1 = _mm256_setzero_ps();
    __m256 sum2 = _mm256_setzero_ps();
    __m256 sum3 = _mm256_setzero_ps();

    // Handle residual elements first (0-7 elements). The full-width query load is safe because
    // `dim` is at least 8, so the query spans at least 8 floats.
    if constexpr (residual % 8) {
        __mmask8 constexpr mask = (1 << (residual % 8)) - 1;

        // Load uint8 elements and convert to float
        __m128i v1_128 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVect1));
        pVect1 += residual % 8;

        __m256i v1_256 = _mm256_cvtepu8_epi32(v1_128);
        __m256 v1_f = _mm256_cvtepi32_ps(v1_256);

        // Load masked float elements from query
        __m256 v2 = my_mm256_maskz_loadu_ps<mask>(pVect2);
        pVect2 += residual % 8;

        // Compute q_i * y_i (no dequantization)
        sum0 = _mm256_mul_ps(v1_f, v2);
    }

    // Handle the remaining full 8-element blocks of the residual (compile-time resolved).
    if constexpr (residual >= 8) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum1);
    }
    if constexpr (residual >= 16) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum2);
    }
    if constexpr (residual >= 24) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum3);
    }

    // We dealt with the residual part. We are left with some multiple of 32 elements.
    // In each iteration we calculate 32 elements = 4 chunks of 8. The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum0);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum1);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum2);
        InnerProductStepSQ8_FP32(pVect1, pVect2, sum3);
    }

    // Reduce to get Σ(q_i * y_i)
    float quantized_dot =
        my_mm256_reduce_add_ps(_mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3)));

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVect1Base = static_cast<const uint8_t *>(pVect1v);
    const auto *params1 = pVect1Base + dimension;
    const float min_val = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (stored after the dim floats)
    const float y_sum = static_cast<const float *>(pVect2v)[dimension + sq8::SUM_QUERY];

    // Apply the algebraic formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

template <unsigned char residual> // 0..31
float SQ8_FP32_InnerProductSIMD16_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP32_InnerProductImp_AVX2<residual>(pVect1v, pVect2v, dimension);
}

template <unsigned char residual> // 0..31
float SQ8_FP32_CosineSIMD16_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    // Calculate inner product using common implementation with normalization
    return SQ8_FP32_InnerProductSIMD16_AVX2<residual>(pVect1v, pVect2v, dimension);
}
