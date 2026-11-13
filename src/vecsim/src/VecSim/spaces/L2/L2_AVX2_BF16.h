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

static inline void L2SqrLowHalfStep(__m256i v1, __m256i v2, __m256i zeros, __m256 &sum) {
    // Convert next  0:3, 8:11 bf16 to 8 floats
    __m256i bf16_low1 = _mm256_unpacklo_epi16(zeros, v1); // AVX2
    __m256i bf16_low2 = _mm256_unpacklo_epi16(zeros, v2);

    __m256 diff = _mm256_sub_ps(_mm256_castsi256_ps(bf16_low1), _mm256_castsi256_ps(bf16_low2));
    sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
}

static inline void L2SqrHighHalfStep(__m256i v1, __m256i v2, __m256i zeros, __m256 &sum) {
    // Convert next 4:7, 12:15 bf16 to 8 floats
    __m256i bf16_high1 = _mm256_unpackhi_epi16(zeros, v1);
    __m256i bf16_high2 = _mm256_unpackhi_epi16(zeros, v2);

    __m256 diff = _mm256_sub_ps(_mm256_castsi256_ps(bf16_high1), _mm256_castsi256_ps(bf16_high2));
    sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
}

static inline void L2SqrStep(bfloat16 *&pVect1, bfloat16 *&pVect2, __m256 &sum) {
    // Load 16 bf16 elements
    __m256i v1 = _mm256_lddqu_si256((__m256i *)pVect1); // avx
    pVect1 += 16;
    __m256i v2 = _mm256_lddqu_si256((__m256i *)pVect2);
    pVect2 += 16;

    __m256i zeros = _mm256_setzero_si256(); // avx

    // Compute dist for 0:3, 8:11 bf16
    L2SqrLowHalfStep(v1, v2, zeros, sum);

    // Compute dist for 4:7, 12:15 bf16
    L2SqrHighHalfStep(v1, v2, zeros, sum);
}

template <unsigned char residual> // 0..31
float BF16_L2SqrSIMD32_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    bfloat16 *pVect1 = (bfloat16 *)pVect1v;
    bfloat16 *pVect2 = (bfloat16 *)pVect2v;

    const bfloat16 *pEnd1 = pVect1 + dimension;

    __m256 sum = _mm256_setzero_ps();

    // Handle first (residual % 16) elements
    if constexpr (residual % 16) {
        // Load all 16 elements to a 256 bit register
        __m256i v1 = _mm256_lddqu_si256((__m256i *)pVect1); // avx
        pVect1 += residual % 16;
        __m256i v2 = _mm256_lddqu_si256((__m256i *)pVect2);
        pVect2 += residual % 16;

        // Unpack  0:3, 8:11 bf16 to 8 floats
        __m256i zeros = _mm256_setzero_si256();
        __m256i v1_low = _mm256_unpacklo_epi16(zeros, v1);
        __m256i v2_low = _mm256_unpacklo_epi16(zeros, v2);

        __m256 low_diff = _mm256_sub_ps(_mm256_castsi256_ps(v1_low), _mm256_castsi256_ps(v2_low));
        if constexpr (residual % 16 <= 4) {
            constexpr unsigned char elem_to_calc = residual % 16;
            constexpr __mmask8 mask = (1 << elem_to_calc) - 1;
            low_diff = _mm256_blend_ps(_mm256_setzero_ps(), low_diff, mask);
        } else {
            __m256i v1_high = _mm256_unpackhi_epi16(zeros, v1);
            __m256i v2_high = _mm256_unpackhi_epi16(zeros, v2);
            __m256 high_diff =
                _mm256_sub_ps(_mm256_castsi256_ps(v1_high), _mm256_castsi256_ps(v2_high));

            if constexpr (4 < residual % 16 && residual % 16 <= 8) {
                // Keep only 4 first elements of low pack
                constexpr __mmask8 mask2 = (1 << 4) - 1;
                low_diff = _mm256_blend_ps(_mm256_setzero_ps(), low_diff, mask2);

                // Keep (residual % 16 - 4) first elements of high_diff
                constexpr unsigned char elem_to_calc = residual % 16 - 4;
                constexpr __mmask8 mask3 = (1 << elem_to_calc) - 1;
                high_diff = _mm256_blend_ps(_mm256_setzero_ps(), high_diff, mask3);
            } else if constexpr (8 < residual % 16 && residual % 16 < 12) {
                // Keep (residual % 16 - 4) first elements of low_diff
                constexpr unsigned char elem_to_calc = residual % 16 - 4;
                constexpr __mmask8 mask2 = (1 << elem_to_calc) - 1;
                low_diff = _mm256_blend_ps(_mm256_setzero_ps(), low_diff, mask2);

                // Keep ony 4 first elements of high_diff
                constexpr __mmask8 mask3 = (1 << 4) - 1;
                high_diff = _mm256_blend_ps(_mm256_setzero_ps(), high_diff, mask3);
            } else if constexpr (residual % 16 >= 12) {
                // Keep (residual % 16 - 8) first elements of high
                constexpr unsigned char elem_to_calc = residual % 16 - 8;
                constexpr __mmask8 mask2 = (1 << elem_to_calc) - 1;
                high_diff = _mm256_blend_ps(_mm256_setzero_ps(), high_diff, mask2);
            }
            sum = _mm256_add_ps(sum, _mm256_mul_ps(high_diff, high_diff));
        }
        sum = _mm256_add_ps(sum, _mm256_mul_ps(low_diff, low_diff));
    }

    // Do a single step if residual >=16
    if constexpr (residual >= 16) {
        L2SqrStep(pVect1, pVect2, sum);
    }

    // Handle 512 bits (32 bfloat16) in chunks of max SIMD = 256 bits = 16 bfloat16
    do {
        L2SqrStep(pVect1, pVect2, sum);
        L2SqrStep(pVect1, pVect2, sum);
    } while (pVect1 < pEnd1);

    return my_mm256_reduce_add_ps(sum);
}
