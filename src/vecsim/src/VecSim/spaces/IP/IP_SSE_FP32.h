/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"

static inline void InnerProductStep(float *&pVect1, float *&pVect2, __m128 &sum_prod) {
    __m128 v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    __m128 v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
}

template <unsigned char residual> // 0..15
float FP32_InnerProductSIMD16_SSE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;

    const float *pEnd1 = pVect1 + dimension;

    // Four accumulators break the mul->add dependency chain, letting more loads/adds be in
    // flight at once.
    __m128 sum0 = _mm_setzero_ps();
    __m128 sum1 = _mm_setzero_ps();
    __m128 sum2 = _mm_setzero_ps();
    __m128 sum3 = _mm_setzero_ps();

    // Deal with %4 remainder first. All the loads below touch at most `residual % 4` floats,
    // so they are safe for any dimension.
    if constexpr (residual % 4) {
        __m128 v1, v2;
        if constexpr (residual % 4 == 3) {
            // Load 3 floats and set the last one to 0
            v1 = _mm_load_ss(pVect1); // load 1 float, set the rest to 0
            v2 = _mm_load_ss(pVect2);
            v1 = _mm_loadh_pi(v1, (__m64 *)(pVect1 + 1));
            v2 = _mm_loadh_pi(v2, (__m64 *)(pVect2 + 1));
        } else if constexpr (residual % 4 == 2) {
            // Load 2 floats and set the last two to 0
            v1 = _mm_loadh_pi(_mm_setzero_ps(), (__m64 *)pVect1);
            v2 = _mm_loadh_pi(_mm_setzero_ps(), (__m64 *)pVect2);
        } else if constexpr (residual % 4 == 1) {
            // Load 1 float and set the last three to 0
            v1 = _mm_load_ss(pVect1);
            v2 = _mm_load_ss(pVect2);
        }
        pVect1 += residual % 4;
        pVect2 += residual % 4;
        sum0 = _mm_mul_ps(v1, v2);
    }

    // have another 1, 2 or 3 4-float steps according to residual
    if constexpr (residual >= 12)
        InnerProductStep(pVect1, pVect2, sum1);
    if constexpr (residual >= 8)
        InnerProductStep(pVect1, pVect2, sum2);
    if constexpr (residual >= 4)
        InnerProductStep(pVect1, pVect2, sum3);

    // We dealt with the residual part. We are left with some multiple of 16 floats.
    // In each iteration we calculate 16 floats = 512 bits. The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        InnerProductStep(pVect1, pVect2, sum0);
        InnerProductStep(pVect1, pVect2, sum1);
        InnerProductStep(pVect1, pVect2, sum2);
        InnerProductStep(pVect1, pVect2, sum3);
    }

    __m128 sum_prod = _mm_add_ps(_mm_add_ps(sum0, sum1), _mm_add_ps(sum2, sum3));
    // TmpRes must be 16 bytes aligned.
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum_prod);
    float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

    return 1.0f - sum;
}
