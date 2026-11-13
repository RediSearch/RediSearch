/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"

static inline void L2SqrStep(uint8_t *&pVect1, uint8_t *&pVect2, __m512i &sum) {
    __m512i va = _mm512_loadu_epi8(pVect1); // AVX512BW
    pVect1 += 64;

    __m512i vb = _mm512_loadu_epi8(pVect2); // AVX512BW
    pVect2 += 64;

    __m512i va_lo = _mm512_unpacklo_epi8(va, _mm512_setzero_si512()); // AVX512BW
    __m512i vb_lo = _mm512_unpacklo_epi8(vb, _mm512_setzero_si512());
    __m512i diff_lo = _mm512_sub_epi16(va_lo, vb_lo);
    sum = _mm512_dpwssd_epi32(sum, diff_lo, diff_lo);

    __m512i va_hi = _mm512_unpackhi_epi8(va, _mm512_setzero_si512()); // AVX512BW
    __m512i vb_hi = _mm512_unpackhi_epi8(vb, _mm512_setzero_si512());
    __m512i diff_hi = _mm512_sub_epi16(va_hi, vb_hi);
    sum = _mm512_dpwssd_epi32(sum, diff_hi, diff_hi);

    // _mm512_dpwssd_epi32(src, a, b)
    // Multiply groups of 2 adjacent pairs of signed 16-bit integers in `a` with corresponding
    // 16-bit integers in `b`, producing 2 intermediate signed 32-bit results. Sum these 2 results
    // with the corresponding 32-bit integer in src, and store the packed 32-bit results in dst.
}

template <unsigned char residual> // 0..63
float UINT8_L2SqrSIMD64_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                           size_t dimension) {
    uint8_t *pVect1 = (uint8_t *)pVect1v;
    uint8_t *pVect2 = (uint8_t *)pVect2v;

    const uint8_t *pEnd1 = pVect1 + dimension;

    __m512i sum = _mm512_setzero_epi32();

    // Deal with remainder first.
    if constexpr (residual) {
        if constexpr (residual < 32) {
            constexpr __mmask32 mask = (1LU << residual) - 1;
            __m256i temp_a = _mm256_maskz_loadu_epi8(mask, pVect1);
            __m512i va = _mm512_cvtepu8_epi16(temp_a);

            __m256i temp_b = _mm256_maskz_loadu_epi8(mask, pVect2);
            __m512i vb = _mm512_cvtepu8_epi16(temp_b);

            __m512i diff = _mm512_sub_epi16(va, vb);
            sum = _mm512_dpwssd_epi32(sum, diff, diff);
        } else if constexpr (residual == 32) {
            __m256i temp_a = _mm256_loadu_epi8(pVect1);
            __m512i va = _mm512_cvtepu8_epi16(temp_a);

            __m256i temp_b = _mm256_loadu_epi8(pVect2);
            __m512i vb = _mm512_cvtepu8_epi16(temp_b);

            __m512i diff = _mm512_sub_epi16(va, vb);
            sum = _mm512_dpwssd_epi32(sum, diff, diff);
        } else {
            constexpr __mmask64 mask = (1LU << residual) - 1;
            __m512i va = _mm512_maskz_loadu_epi8(mask, pVect1); // AVX512BW
            __m512i vb = _mm512_maskz_loadu_epi8(mask, pVect2); // AVX512BW

            __m512i va_lo = _mm512_unpacklo_epi8(va, _mm512_setzero_si512()); // AVX512BW
            __m512i vb_lo = _mm512_unpacklo_epi8(vb, _mm512_setzero_si512());
            __m512i diff_lo = _mm512_sub_epi16(va_lo, vb_lo);
            sum = _mm512_dpwssd_epi32(sum, diff_lo, diff_lo);

            __m512i va_hi = _mm512_unpackhi_epi8(va, _mm512_setzero_si512()); // AVX512BW
            __m512i vb_hi = _mm512_unpackhi_epi8(vb, _mm512_setzero_si512());
            __m512i diff_hi = _mm512_sub_epi16(va_hi, vb_hi);
            sum = _mm512_dpwssd_epi32(sum, diff_hi, diff_hi);
        }
        pVect1 += residual;
        pVect2 += residual;

        // We dealt with the residual part.
        // We are left with some multiple of 64-uint_8 (might be 0).
        while (pVect1 < pEnd1) {
            L2SqrStep(pVect1, pVect2, sum);
        }
    } else {
        // We have no residual, we have some non-zero multiple of 64-uint_8.
        do {
            L2SqrStep(pVect1, pVect2, sum);
        } while (pVect1 < pEnd1);
    }

    return _mm512_reduce_add_epi32(sum);
}
