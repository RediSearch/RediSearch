/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"

static inline void L2SqrStep(double *&pVect1, double *&pVect2, __m128d &sum) {
    __m128d v1 = _mm_loadu_pd(pVect1);
    pVect1 += 2;
    __m128d v2 = _mm_loadu_pd(pVect2);
    pVect2 += 2;
    __m128d diff = _mm_sub_pd(v1, v2);
    sum = _mm_add_pd(sum, _mm_mul_pd(diff, diff));
}

template <unsigned char residual> // 0..3
double FP64_L2SqrSIMD8_SSE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    double *pVect1 = (double *)pVect1v;
    double *pVect2 = (double *)pVect2v;

    const double *pEnd1 = pVect1 + dimension;

    // Two accumulators break the mul->add dependency chain, letting more loads/adds be in
    // flight at once. FP64 has too few lanes to benefit from more than two (measured).
    __m128d sum0 = _mm_setzero_pd();
    __m128d sum1 = _mm_setzero_pd();

    // If residual is odd, we load 1 double and set the last one to 0
    if constexpr (residual % 2 == 1) {
        __m128d v1 = _mm_load_sd(pVect1);
        pVect1++;
        __m128d v2 = _mm_load_sd(pVect2);
        pVect2++;
        __m128d diff = _mm_sub_pd(v1, v2);
        sum0 = _mm_mul_pd(diff, diff);
    }

    // have another 2-double step according to residual
    if constexpr (residual >= 2)
        L2SqrStep(pVect1, pVect2, sum1);

    // We dealt with the residual part. We are left with some multiple of 4 doubles.
    // In each iteration we calculate 4 doubles = 2 chunks of 128 bits. The loop may run zero times
    // (dim can be as small as 4).
    while (pVect1 < pEnd1) {
        L2SqrStep(pVect1, pVect2, sum0);
        L2SqrStep(pVect1, pVect2, sum1);
    }

    __m128d sum = _mm_add_pd(sum0, sum1);
    // TmpRes must be 16 bytes aligned
    double PORTABLE_ALIGN16 TmpRes[2];
    _mm_store_pd(TmpRes, sum);
    return TmpRes[0] + TmpRes[1];
}
