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
#include <immintrin.h>

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

// Helper: compute Σ(q_i * y_i) for 16 elements
// pVec1 = SQ8 storage (quantized values), pVec2 = FP32 query
static inline void SQ8_FP32_InnerProductStep(const uint8_t *&pVec1, const float *&pVec2,
                                             __m512 &sum) {
    // Load 16 uint8 elements from quantized vector and convert to float
    __m128i v1_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec1));
    __m512i v1_512 = _mm512_cvtepu8_epi32(v1_128);
    __m512 v1_f = _mm512_cvtepi32_ps(v1_512);

    // Load 16 float elements from query (pVec2)
    __m512 v2 = _mm512_loadu_ps(pVec2);

    // Accumulate q_i * y_i (no dequantization!)
    sum = _mm512_fmadd_ps(v1_f, v2, sum);

    pVec1 += 16;
    pVec2 += 16;
}

// Common implementation for both inner product and cosine similarity
// pVec1v = SQ8 storage, pVec2v = FP32 query
template <unsigned char residual> // 0..31
float SQ8_FP32_InnerProductImp_AVX512(const void *pVec1v, const void *pVec2v, size_t dimension) {
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v); // SQ8 storage
    const float *pVec2 = static_cast<const float *>(pVec2v);     // FP32 query
    const uint8_t *pEnd1 = pVec1 + dimension;

    // Initialize sum accumulators for Σ(q_i * y_i). Two accumulators break the FMA dependency
    // chain, letting more FMAs be in flight at once.
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();

    // Handle the sub-16 residual elements first
    if constexpr (residual % 16) {
        __mmask16 constexpr mask = (1U << (residual % 16)) - 1;

        // Load uint8 elements (safe to load 16 bytes due to the metadata padding after the
        // quantized values). The query load is masked, which suppresses faults on masked-out
        // lanes, so both loads are safe for any dimension.
        __m128i v1_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec1));
        __m512i v1_512 = _mm512_cvtepu8_epi32(v1_128);
        __m512 v1_f = _mm512_cvtepi32_ps(v1_512);

        // Load masked float elements from query
        __m512 v2 = _mm512_maskz_loadu_ps(mask, pVec2);

        // Compute q_i * y_i with mask (no dequantization)
        sum0 = _mm512_maskz_mul_ps(mask, v1_f, v2);

        pVec1 += residual % 16;
        pVec2 += residual % 16;
    }

    // Handle the remaining full 16-element block of the residual (compile-time resolved).
    if constexpr (residual >= 16) {
        SQ8_FP32_InnerProductStep(pVec1, pVec2, sum1);
    }

    // We dealt with the residual part. We are left with some multiple of 32 elements.
    // In each iteration we calculate 32 elements = 2 chunks of 16. The loop may run zero times
    // (dim can be as small as 8).
    while (pVec1 < pEnd1) {
        SQ8_FP32_InnerProductStep(pVec1, pVec2, sum0);
        SQ8_FP32_InnerProductStep(pVec1, pVec2, sum1);
    }

    // Reduce to get Σ(q_i * y_i)
    __m512 sum = _mm512_add_ps(sum0, sum1);
    float quantized_dot = _mm512_reduce_add_ps(sum);

    // Get quantization parameters from stored vector (after quantized data)
    // Use the original base pointer since pVec1 has been advanced
    const uint8_t *pVec1Base = static_cast<const uint8_t *>(pVec1v);
    const auto *params1 = pVec1Base + dimension;
    const float min_val = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (stored after the dim floats)
    // Use the original base pointer since pVec2 has been advanced
    const float y_sum = static_cast<const float *>(pVec2v)[dimension + sq8::SUM_QUERY];

    // Apply the algebraic formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

template <unsigned char residual> // 0..31
float SQ8_FP32_InnerProductSIMD16_AVX512F_BW_VL_VNNI(const void *pVec1v, const void *pVec2v,
                                                     size_t dimension) {
    // The inner product similarity is 1 - ip
    return 1.0f - SQ8_FP32_InnerProductImp_AVX512<residual>(pVec1v, pVec2v, dimension);
}

template <unsigned char residual> // 0..31
float SQ8_FP32_CosineSIMD16_AVX512F_BW_VL_VNNI(const void *pVec1v, const void *pVec2v,
                                               size_t dimension) {
    // Cosine distance = 1 - IP (vectors are pre-normalized)
    return SQ8_FP32_InnerProductSIMD16_AVX512F_BW_VL_VNNI<residual>(pVec1v, pVec2v, dimension);
}
