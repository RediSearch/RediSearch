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
 * Asymmetric SQ8 L2 squared distance computed via direct residual accumulation:
 *
 *   ||x - y||² = Σ(dequant(x_i) - y_i)²
 *   where dequant(x_i) = min_val + delta * q_i
 *
 * The subtract is fused into the FMA: diff = fma(delta, q, min - y).
 */

// Helper: compute Σ(diff_i²) for 16 elements, where diff_i = dequant(x_i) - y_i.
// pVec1 = SQ8 storage (quantized values), pVec2 = FP32 query.
// min_val_vec/delta_vec are broadcast scalars from the stored vector's metadata.
static inline void L2StepSQ8_FP32_AVX512(const uint8_t *&pVec1, const float *&pVec2, __m512 &sum,
                                         __m512 min_val_vec, __m512 delta_vec) {
    // Load 16 uint8 elements from quantized vector and convert to float
    __m128i v1_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec1));
    __m512i v1_512 = _mm512_cvtepu8_epi32(v1_128);
    __m512 v1_f = _mm512_cvtepi32_ps(v1_512);

    // Load 16 float elements from query (pVec2)
    __m512 v2 = _mm512_loadu_ps(pVec2);

    // min - y computed once per lane, then fuse the dequantize-and-subtract into a single FMA:
    // diff = delta*q + (min - y).
    __m512 min_minus_y = _mm512_sub_ps(min_val_vec, v2);
    __m512 diff = _mm512_fmadd_ps(delta_vec, v1_f, min_minus_y);

    sum = _mm512_fmadd_ps(diff, diff, sum);

    pVec1 += 16;
    pVec2 += 16;
}

// pVec1v = SQ8 storage, pVec2v = FP32 query
template <unsigned char residual> // 0..31
float SQ8_FP32_L2SqrSIMD16_AVX512F_BW_VL_VNNI(const void *pVec1v, const void *pVec2v,
                                              size_t dimension) {
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v); // SQ8 storage
    const float *pVec2 = static_cast<const float *>(pVec2v);     // FP32 query
    const uint8_t *pEnd1 = pVec1 + dimension;

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVec1Base = static_cast<const uint8_t *>(pVec1v);
    const auto *params1 = pVec1Base + dimension;
    const float min_val_scalar = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta_scalar = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const __m512 min_val_vec = _mm512_set1_ps(min_val_scalar);
    const __m512 delta_vec = _mm512_set1_ps(delta_scalar);

    // Initialize sum accumulators for Σ(diff_i²). Two accumulators break the FMA dependency
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

        // min - y, then dequantize-and-subtract
        __m512 min_minus_y = _mm512_sub_ps(min_val_vec, v2);
        __m512 diff = _mm512_fmadd_ps(delta_vec, v1_f, min_minus_y);

        // Masked-out lanes carry garbage (v2 was zeroed, not set to min_val), so mask the
        // squared diff to zero for those lanes before accumulating.
        sum0 = _mm512_maskz_mul_ps(mask, diff, diff);

        pVec1 += residual % 16;
        pVec2 += residual % 16;
    }

    // Handle the remaining full 16-element block of the residual (compile-time resolved).
    if constexpr (residual >= 16) {
        L2StepSQ8_FP32_AVX512(pVec1, pVec2, sum1, min_val_vec, delta_vec);
    }

    // We dealt with the residual part. We are left with some multiple of 32 elements.
    // In each iteration we calculate 32 elements = 2 chunks of 16. The loop may run zero times
    // (dim can be as small as 8).
    while (pVec1 < pEnd1) {
        L2StepSQ8_FP32_AVX512(pVec1, pVec2, sum0, min_val_vec, delta_vec);
        L2StepSQ8_FP32_AVX512(pVec1, pVec2, sum1, min_val_vec, delta_vec);
    }

    // Reduce to get Σ(diff_i²)
    __m512 sum = _mm512_add_ps(sum0, sum1);
    return _mm512_reduce_add_ps(sum);
}
