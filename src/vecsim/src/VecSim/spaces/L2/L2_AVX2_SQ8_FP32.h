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
 * Asymmetric SQ8 L2 squared distance computed via direct residual accumulation:
 *
 *   ||x - y||² = Σ(dequant(x_i) - y_i)²
 *   where dequant(x_i) = min_val + delta * q_i
 */

// Helper: compute Σ(diff_i²) for 8 elements, where diff_i = dequant(x_i) - y_i.
// pVect1 = SQ8 storage (quantized values), pVect2 = FP32 query.
// min_val_vec/delta_vec are broadcast scalars from the stored vector's metadata.
static inline void L2StepSQ8_FP32_AVX2(const uint8_t *&pVect1, const float *&pVect2, __m256 &sum,
                                       __m256 min_val_vec, __m256 delta_vec) {
    // Load 8 uint8 elements and convert to float
    __m128i v1_128 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVect1));
    pVect1 += 8;
    __m256i v1_256 = _mm256_cvtepu8_epi32(v1_128);
    __m256 v1_f = _mm256_cvtepi32_ps(v1_256);

    // Load 8 float elements from query
    __m256 v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;

    // min - y computed once per lane, then dequantize-and-subtract: diff = delta*q + (min - y).
    // No FMA in this variant, so mul + add.
    __m256 min_minus_y = _mm256_sub_ps(min_val_vec, v2);
    __m256 diff = _mm256_add_ps(_mm256_mul_ps(delta_vec, v1_f), min_minus_y);

    sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <unsigned char residual> // 0..31
float SQ8_FP32_L2SqrSIMD16_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    const uint8_t *pEnd1 = pVect1 + dimension;

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVect1Base = static_cast<const uint8_t *>(pVect1v);
    const auto *params1 = pVect1Base + dimension;
    const float min_val_scalar = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta_scalar = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const __m256 min_val_vec = _mm256_set1_ps(min_val_scalar);
    const __m256 delta_vec = _mm256_set1_ps(delta_scalar);

    // Initialize sum accumulators. Four accumulators break the dependency chain, letting more
    // ops be in flight at once.
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

        // min - y, then dequantize-and-subtract
        __m256 min_minus_y = _mm256_sub_ps(min_val_vec, v2);
        __m256 diff = _mm256_add_ps(_mm256_mul_ps(delta_vec, v1_f), min_minus_y);

        // Masked-out lanes carry garbage (v2 was zeroed, not set to min_val), so blend the
        // squared diff with zero for those lanes before accumulating.
        __m256 diff_sq = _mm256_mul_ps(diff, diff);
        sum0 = _mm256_blend_ps(_mm256_setzero_ps(), diff_sq, mask);
    }

    // Handle the remaining full 8-element blocks of the residual (compile-time resolved).
    if constexpr (residual >= 8) {
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum1, min_val_vec, delta_vec);
    }
    if constexpr (residual >= 16) {
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum2, min_val_vec, delta_vec);
    }
    if constexpr (residual >= 24) {
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum3, min_val_vec, delta_vec);
    }

    // We dealt with the residual part. We are left with some multiple of 32 elements.
    // In each iteration we calculate 32 elements = 4 chunks of 8. The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum0, min_val_vec, delta_vec);
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum1, min_val_vec, delta_vec);
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum2, min_val_vec, delta_vec);
        L2StepSQ8_FP32_AVX2(pVect1, pVect2, sum3, min_val_vec, delta_vec);
    }

    // Reduce to get Σ(diff_i²)
    // Tree reduction: the fixed cost of the stack-based fold is a large share of
    // total time at small dimensions. See my_mm256_reduce_add_ps_tree in AVX_utils.h.
    return my_mm256_reduce_add_ps_tree(
        _mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3)));
}
