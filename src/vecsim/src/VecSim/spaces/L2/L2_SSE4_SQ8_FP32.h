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
 * Asymmetric SQ8 L2 squared distance computed via direct residual accumulation:
 *
 *   ||x - y||² = Σ(dequant(x_i) - y_i)²
 *   where dequant(x_i) = min_val + delta * q_i
 */

// Helper: compute Σ(diff_i²) for 4 elements, where diff_i = dequant(x_i) - y_i.
// pVect1 = SQ8 storage (quantized values), pVect2 = FP32 query.
// min_val/delta are broadcast scalars from the stored vector's metadata.
static inline void L2StepSQ8_FP32_SSE4(const uint8_t *&pVect1, const float *&pVect2, __m128 &sum,
                                       __m128 min_val, __m128 delta) {
    // Load 4 uint8 elements and convert to float
    __m128i v1_i = _mm_cvtepu8_epi32(_mm_cvtsi32_si128(load_unaligned<int32_t>(pVect1)));
    pVect1 += 4;
    __m128 v1_f = _mm_cvtepi32_ps(v1_i);

    // Load 4 float elements from query
    __m128 v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;

    // min - y computed once per lane, then fuse the dequantize-and-subtract: diff = delta*q +
    // (min - y). SSE has no FMA, so this is mul + add.
    __m128 min_minus_y = _mm_sub_ps(min_val, v2);
    __m128 diff = _mm_add_ps(_mm_mul_ps(delta, v1_f), min_minus_y);

    sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <unsigned char residual> // 0..15
float SQ8_FP32_L2SqrSIMD16_SSE4(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    const uint8_t *pEnd1 = pVect1 + dimension;

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVect1Base = static_cast<const uint8_t *>(pVect1v);
    const auto *params1 = pVect1Base + dimension;
    const float min_val_scalar = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta_scalar = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const __m128 min_val = _mm_set1_ps(min_val_scalar);
    const __m128 delta = _mm_set1_ps(delta_scalar);

    // Initialize sum accumulators. Four accumulators break the dependency chain, letting more
    // ops be in flight at once.
    __m128 sum0 = _mm_setzero_ps();
    __m128 sum1 = _mm_setzero_ps();
    __m128 sum2 = _mm_setzero_ps();
    __m128 sum3 = _mm_setzero_ps();

    // Residual elements (1-3), loaded at full width with the lanes past the residual masked
    // off. Staging them through stack arrays instead costs a store-to-load stall. In bounds
    // because the x86 chooser never reaches this kernel below dim 8.
    if constexpr (residual % 4) {
        constexpr unsigned char r = residual % 4;

        __m128i v1_i = _mm_cvtepu8_epi32(_mm_cvtsi32_si128(load_unaligned<int32_t>(pVect1)));
        __m128 v1_f = _mm_cvtepi32_ps(v1_i);
        __m128 v2 = _mm_loadu_ps(pVect2);

        __m128 min_minus_y = _mm_sub_ps(min_val, v2);
        __m128 diff = _mm_add_ps(_mm_mul_ps(delta, v1_f), min_minus_y);

        // Lanes >= r hold elements the main loop will process; zero their contribution.
        const __m128 lane_mask = _mm_castsi128_ps(
            _mm_set_epi32(r > 3 ? -1 : 0, r > 2 ? -1 : 0, r > 1 ? -1 : 0, r > 0 ? -1 : 0));
        diff = _mm_and_ps(diff, lane_mask);

        pVect1 += r;
        pVect2 += r;

        sum0 = _mm_mul_ps(diff, diff);
    }

    // Handle remaining residual in chunks of 4 (for residual 4-15)
    if constexpr (residual >= 4) {
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum1, min_val, delta);
    }
    if constexpr (residual >= 8) {
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum2, min_val, delta);
    }
    if constexpr (residual >= 12) {
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum3, min_val, delta);
    }

    // Process remaining full chunks of 16 elements (4x4). The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum0, min_val, delta);
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum1, min_val, delta);
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum2, min_val, delta);
        L2StepSQ8_FP32_SSE4(pVect1, pVect2, sum3, min_val, delta);
    }

    // Horizontal sum to get Σ(diff_i²)
    __m128 sum = _mm_add_ps(_mm_add_ps(sum0, sum1), _mm_add_ps(sum2, sum3));
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum);
    return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}
