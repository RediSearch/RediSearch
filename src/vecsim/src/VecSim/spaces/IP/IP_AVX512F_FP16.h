/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <cstdint>
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/AVX_utils.h"
#include "VecSim/types/float16.h"

using float16 = vecsim_types::float16;

static void InnerProductStep(float16 *&pVect1, float16 *&pVect2, __m512 &sum) {
    // Convert 16 half-floats into floats and store them in 512 bits register.
    auto v1 = _mm512_cvtph_ps(_mm256_lddqu_si256((__m256i *)pVect1));
    auto v2 = _mm512_cvtph_ps(_mm256_lddqu_si256((__m256i *)pVect2));

    // sum = v1 * v2 + sum
    sum = _mm512_fmadd_ps(v1, v2, sum);
    pVect1 += 16;
    pVect2 += 16;
}

template <unsigned short residual> // 0..31
float FP16_InnerProductSIMD32_AVX512(const void *pVect1v, const void *pVect2v, size_t dimension) {
    auto *pVect1 = (float16 *)pVect1v;
    auto *pVect2 = (float16 *)pVect2v;

    const float16 *pEnd1 = pVect1 + dimension;

    // Two accumulators break the FMA dependency chain, letting more FMAs be in flight at once.
    auto sum0 = _mm512_setzero_ps();
    auto sum1 = _mm512_setzero_ps();

    if constexpr (residual % 16) {
        // Deal with remainder first. The full-width load of 16 16-bit floats is safe because
        // `dim` is at least 16, so the vector spans at least 16 elements.
        __mmask16 constexpr residuals_mask = (1 << (residual % 16)) - 1;
        // Convert the first half-floats in the residual positions into floats and store them
        // 512 bits register, where the floats in the positions corresponding to the non-residuals
        // positions are zeros.
        auto v1 = _mm512_maskz_mov_ps(residuals_mask,
                                      _mm512_cvtph_ps(_mm256_lddqu_si256((__m256i *)pVect1)));
        auto v2 = _mm512_maskz_mov_ps(residuals_mask,
                                      _mm512_cvtph_ps(_mm256_lddqu_si256((__m256i *)pVect2)));
        sum0 = _mm512_mul_ps(v1, v2);
        pVect1 += residual % 16;
        pVect2 += residual % 16;
    }
    // Handle the remaining full 16-element block of the residual (compile-time resolved).
    if constexpr (residual >= 16) {
        InnerProductStep(pVect1, pVect2, sum1);
    }

    // We dealt with the residual part. We are left with some multiple of 32 16-bit floats.
    // In each iteration we calculate 32 elements = 2 chunks of 256 bits (converted to 512).
    // The loop may run zero times (dim can be as small as 16).
    while (pVect1 < pEnd1) {
        InnerProductStep(pVect1, pVect2, sum0);
        InnerProductStep(pVect1, pVect2, sum1);
    }

    auto sum = _mm512_add_ps(sum0, sum1);
    return 1.0f - _mm512_reduce_add_ps(sum);
}
