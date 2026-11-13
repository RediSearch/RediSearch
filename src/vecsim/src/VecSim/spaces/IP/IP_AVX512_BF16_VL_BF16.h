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

static inline void InnerProductStep(bfloat16 *&pVect1, bfloat16 *&pVect2, __m512 &sum) {
    __m512i vec1 = _mm512_loadu_si512((__m512i *)pVect1);
    __m512i vec2 = _mm512_loadu_si512((__m512i *)pVect2);

    sum = _mm512_dpbf16_ps(sum, (__m512bh)vec1, (__m512bh)vec2);
    pVect1 += 32;
    pVect2 += 32;
}

template <unsigned char residual> // 0..31
float BF16_InnerProductSIMD32_AVX512BF16_VL(const void *pVect1v, const void *pVect2v,
                                            size_t dimension) {
    bfloat16 *pVect1 = (bfloat16 *)pVect1v;
    bfloat16 *pVect2 = (bfloat16 *)pVect2v;

    const bfloat16 *pEnd1 = pVect1 + dimension;

    __m512 sum = _mm512_setzero_ps();

    if constexpr (residual) {
        constexpr __mmask32 mask = (1LU << residual) - 1;
        __m512i v1 = _mm512_maskz_loadu_epi16(mask, pVect1);
        pVect1 += residual;
        __m512i v2 = _mm512_maskz_loadu_epi16(mask, pVect2);
        pVect2 += residual;
        sum = _mm512_dpbf16_ps(sum, (__m512bh)v1, (__m512bh)v2);
    }

    do {
        InnerProductStep(pVect1, pVect2, sum);
    } while (pVect1 < pEnd1);

    return 1.0f - _mm512_reduce_add_ps(sum);
}
