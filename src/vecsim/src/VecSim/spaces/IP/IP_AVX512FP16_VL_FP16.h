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
#include "VecSim/types/float16.h"
#include <cstring>

using float16 = vecsim_types::float16;

static void InnerProductStep(float16 *&pVect1, float16 *&pVect2, __m512h &sum) {
    __m512h v1 = _mm512_loadu_ph(pVect1);
    __m512h v2 = _mm512_loadu_ph(pVect2);

    sum = _mm512_fmadd_ph(v1, v2, sum);
    pVect1 += 32;
    pVect2 += 32;
}

template <unsigned short residual> // 0..31
float FP16_InnerProductSIMD32_AVX512FP16_VL(const void *pVect1v, const void *pVect2v,
                                            size_t dimension) {
    auto *pVect1 = (float16 *)pVect1v;
    auto *pVect2 = (float16 *)pVect2v;

    const float16 *pEnd1 = pVect1 + dimension;

    __m512h sum = _mm512_setzero_ph();

    if constexpr (residual) {
        constexpr __mmask32 mask = (1LU << residual) - 1;
        __m512h v1 = _mm512_loadu_ph(pVect1);
        pVect1 += residual;
        __m512h v2 = _mm512_loadu_ph(pVect2);
        pVect2 += residual;
        sum = _mm512_maskz_mul_ph(mask, v1, v2);
    }

    // We dealt with the residual part. We are left with some multiple of 32 16-bit floats.
    do {
        InnerProductStep(pVect1, pVect2, sum);
    } while (pVect1 < pEnd1);

    const _Float16 reduced = _mm512_reduce_add_ph(sum);
    // Subtract in fp32 so distances close to 1.0 are not rounded back to fp16.
    return 1.0f - static_cast<float>(reduced);
}
