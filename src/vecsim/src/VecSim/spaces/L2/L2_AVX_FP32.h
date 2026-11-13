/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/AVX_utils.h"

static inline void L2SqrStep(float *&pVect1, float *&pVect2, __m256 &sum) {
    __m256 v1 = _mm256_loadu_ps(pVect1);
    pVect1 += 8;
    __m256 v2 = _mm256_loadu_ps(pVect2);
    pVect2 += 8;
    __m256 diff = _mm256_sub_ps(v1, v2);
    // sum = _mm256_fmadd_ps(diff, diff, sum);
    sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
}

template <unsigned char residual> // 0..31
float FP32_L2SqrSIMD16_AVX(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;

    const float *pEnd1 = pVect1 + dimension;

    // Four accumulators break the mul->add dependency chain, letting more loads/adds be in
    // flight at once.
    __m256 sum0 = _mm256_setzero_ps();
    __m256 sum1 = _mm256_setzero_ps();
    __m256 sum2 = _mm256_setzero_ps();
    __m256 sum3 = _mm256_setzero_ps();

    // Deal with 1-7 floats with mask loading, if needed. The full-width load is safe because
    // `dim` is at least 8, so the vector spans at least 8 floats.
    if constexpr (residual % 8) {
        __mmask8 constexpr mask8 = (1 << (residual % 8)) - 1;
        __m256 v1 = my_mm256_maskz_loadu_ps<mask8>(pVect1);
        pVect1 += residual % 8;
        __m256 v2 = my_mm256_maskz_loadu_ps<mask8>(pVect2);
        pVect2 += residual % 8;
        __m256 diff = _mm256_sub_ps(v1, v2);
        sum0 = _mm256_mul_ps(diff, diff);
    }

    // Handle the remaining full 8-float blocks of the residual (compile-time resolved).
    if constexpr (residual >= 8) {
        L2SqrStep(pVect1, pVect2, sum1);
    }
    if constexpr (residual >= 16) {
        L2SqrStep(pVect1, pVect2, sum2);
    }
    if constexpr (residual >= 24) {
        L2SqrStep(pVect1, pVect2, sum3);
    }

    // We dealt with the residual part. We are left with some multiple of 32 floats.
    // In each iteration we calculate 32 floats = 4 chunks of 256 bits. The loop may run zero
    // times (dim can be as small as 8).
    while (pVect1 < pEnd1) {
        L2SqrStep(pVect1, pVect2, sum0);
        L2SqrStep(pVect1, pVect2, sum1);
        L2SqrStep(pVect1, pVect2, sum2);
        L2SqrStep(pVect1, pVect2, sum3);
    }

    __m256 sum = _mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3));
    return my_mm256_reduce_add_ps(sum);
}
