/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"
#include "VecSim/types/bfloat16.h"

using bfloat16 = vecsim_types::bfloat16;

static inline void L2SqrLowHalfStep(__m128i v1, __m128i v2, __m128i zeros, __m128 &sum) {
    // Convert next 0..3 bf16 to 4 floats
    __m128i bf16_low1 = _mm_unpacklo_epi16(zeros, v1); // SSE2
    __m128i bf16_low2 = _mm_unpacklo_epi16(zeros, v2);

    __m128 diff = _mm_sub_ps(_mm_castsi128_ps(bf16_low1), _mm_castsi128_ps(bf16_low2));
    sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
}

static inline void L2SqrHighHalfStep(__m128i v1, __m128i v2, __m128i zeros, __m128 &sum) {
    // Convert next 4..7 bf16 to 4 floats
    __m128i bf16_high1 = _mm_unpackhi_epi16(zeros, v1);
    __m128i bf16_high2 = _mm_unpackhi_epi16(zeros, v2);

    __m128 diff = _mm_sub_ps(_mm_castsi128_ps(bf16_high1), _mm_castsi128_ps(bf16_high2));
    sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
}

static inline void L2SqrStep(bfloat16 *&pVect1, bfloat16 *&pVect2, __m128 &sum) {
    // Load 8 bf16 elements
    __m128i v1 = _mm_lddqu_si128((__m128i *)pVect1); // SSE3
    pVect1 += 8;
    __m128i v2 = _mm_lddqu_si128((__m128i *)pVect2);
    pVect2 += 8;

    __m128i zeros = _mm_setzero_si128(); // SSE2

    // Compute dist for 0..3 bf16
    L2SqrLowHalfStep(v1, v2, zeros, sum);

    // Compute dist for 4..7 bf16
    L2SqrHighHalfStep(v1, v2, zeros, sum);
}

template <unsigned char residual> // 0..31
float BF16_L2SqrSIMD32_SSE3(const void *pVect1v, const void *pVect2v, size_t dimension) {
    bfloat16 *pVect1 = (bfloat16 *)pVect1v;
    bfloat16 *pVect2 = (bfloat16 *)pVect2v;

    const bfloat16 *pEnd1 = pVect1 + dimension;

    __m128 sum = _mm_setzero_ps();

    // Handle first residual % 8 elements (smaller than step chunk size)

    // Handle residual % 4
    if constexpr (residual % 4) {
        __m128i v1, v2;
        constexpr bfloat16 zero = bfloat16(0);
        if constexpr (residual % 4 == 3) {
            v1 = _mm_setr_epi16(zero, pVect1[0], zero, pVect1[1], zero, pVect1[2], zero,
                                zero); // SSE2
            v2 = _mm_setr_epi16(zero, pVect2[0], zero, pVect2[1], zero, pVect2[2], zero, zero);
        } else if constexpr (residual % 4 == 2) {
            // Load 2 bf16 element set the rest to 0
            v1 = _mm_setr_epi16(zero, pVect1[0], zero, pVect1[1], zero, zero, zero, zero); // SSE2
            v2 = _mm_setr_epi16(zero, pVect2[0], zero, pVect2[1], zero, zero, zero, zero);
        } else if constexpr (residual % 4 == 1) {
            // Load only first element
            v1 = _mm_setr_epi16(zero, pVect1[0], zero, zero, zero, zero, zero, zero); // SSE2
            v2 = _mm_setr_epi16(zero, pVect2[0], zero, zero, zero, zero, zero, zero);
        }
        __m128 diff = _mm_sub_ps(_mm_castsi128_ps(v1), _mm_castsi128_ps(v2));
        sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
        pVect1 += residual % 4;
        pVect2 += residual % 4;
    }

    // If (residual % 8 >= 4) we need to handle 4 more elements
    if constexpr (residual % 8 >= 4) {
        __m128i v1 = _mm_lddqu_si128((__m128i *)pVect1);
        __m128i v2 = _mm_lddqu_si128((__m128i *)pVect2);
        L2SqrLowHalfStep(v1, v2, _mm_setzero_si128(), sum);
        pVect1 += 4;
        pVect2 += 4;
    }

    // Handle (residual - (residual % 8)) in chunks of 8 bfloat16
    if constexpr (residual >= 24)
        L2SqrStep(pVect1, pVect2, sum);
    if constexpr (residual >= 16)
        L2SqrStep(pVect1, pVect2, sum);
    if constexpr (residual >= 8)
        L2SqrStep(pVect1, pVect2, sum);

    // Handle 512 bits (32 bfloat16) in chunks of max SIMD = 128 bits = 8 bfloat16
    do {
        L2SqrStep(pVect1, pVect2, sum);
        L2SqrStep(pVect1, pVect2, sum);
        L2SqrStep(pVect1, pVect2, sum);
        L2SqrStep(pVect1, pVect2, sum);
    } while (pVect1 < pEnd1);

    // TmpRes must be 16 bytes aligned
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum);
    return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}
