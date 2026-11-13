/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"

static inline void L2SqrStep(float *&pVect1, float *&pVect2, __m128 &sum) {
    __m128 v1 = _mm_loadu_ps(pVect1);
    pVect1 += 4;
    __m128 v2 = _mm_loadu_ps(pVect2);
    pVect2 += 4;
    __m128 diff = _mm_sub_ps(v1, v2);
    sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
}

template <unsigned char residual> // 0..15
float FP32_L2SqrSIMD16_SSE(const void *pVect1v, const void *pVect2v, size_t dimension) {
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
    // except for the residual == 3 case, which reads a full (unaligned) 16-byte block and then
    // masks the out-of-residual lane. This is safe because `dim` is at least 8 whenever
    // residual % 4 == 3, so the 4 floats read from the start are always in-bounds.
    if constexpr (residual % 4) {
        __m128 v1, v2, diff;
        if constexpr (residual % 4 == 3) {
            // Load 4 floats (unaligned). Vectors are not guaranteed to be 16-byte aligned here
            // (block stride is dim * 4 and no alignment hint is set when dim % 4 != 0), so an
            // aligned load (_mm_loadr_ps / movaps) would fault.
            v1 = _mm_loadu_ps(pVect1);
            v2 = _mm_loadu_ps(pVect2);
            diff = _mm_sub_ps(v1, v2);
            // Rotate the out-of-residual 4th element into the low lane and zero it - that
            // element is processed by the next step.
            diff = _mm_shuffle_ps(diff, diff, _MM_SHUFFLE(2, 1, 0, 3));
            diff = _mm_move_ss(diff, _mm_setzero_ps());
        } else if constexpr (residual % 4 == 2) {
            // Load 2 floats and set the last two to 0
            v1 = _mm_loadh_pi(_mm_setzero_ps(), (__m64 *)pVect1);
            v2 = _mm_loadh_pi(_mm_setzero_ps(), (__m64 *)pVect2);
            diff = _mm_sub_ps(v1, v2);
        } else if constexpr (residual % 4 == 1) {
            // Load 1 float and set the last three to 0
            v1 = _mm_load_ss(pVect1);
            v2 = _mm_load_ss(pVect2);
            diff = _mm_sub_ps(v1, v2);
        }
        pVect1 += residual % 4;
        pVect2 += residual % 4;
        sum0 = _mm_mul_ps(diff, diff);
    }

    // have another 1, 2 or 3 4-floats steps according to residual
    if constexpr (residual >= 12)
        L2SqrStep(pVect1, pVect2, sum1);
    if constexpr (residual >= 8)
        L2SqrStep(pVect1, pVect2, sum2);
    if constexpr (residual >= 4)
        L2SqrStep(pVect1, pVect2, sum3);

    // We dealt with the residual part. We are left with some multiple of 16 floats.
    // In each iteration we calculate 16 floats = 512 bits. The loop may run zero times
    // (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        L2SqrStep(pVect1, pVect2, sum0);
        L2SqrStep(pVect1, pVect2, sum1);
        L2SqrStep(pVect1, pVect2, sum2);
        L2SqrStep(pVect1, pVect2, sum3);
    }

    __m128 sum = _mm_add_ps(_mm_add_ps(sum0, sum1), _mm_add_ps(sum2, sum3));
    // TmpRes must be 16 bytes aligned
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum);
    return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}
