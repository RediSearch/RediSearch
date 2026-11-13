/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "VecSim/spaces/space_includes.h"

static inline void InnerProductStep(uint8_t *&pVect1, uint8_t *&pVect2, __m512i &sum) {
    __m512i va = _mm512_loadu_epi8(pVect1); // AVX512BW
    pVect1 += 64;

    __m512i vb = _mm512_loadu_epi8(pVect2); // AVX512BW
    pVect2 += 64;

    __m512i va_lo = _mm512_unpacklo_epi8(va, _mm512_setzero_si512()); // AVX512BW
    __m512i vb_lo = _mm512_unpacklo_epi8(vb, _mm512_setzero_si512());
    sum = _mm512_dpwssd_epi32(sum, va_lo, vb_lo);

    __m512i va_hi = _mm512_unpackhi_epi8(va, _mm512_setzero_si512()); // AVX512BW
    __m512i vb_hi = _mm512_unpackhi_epi8(vb, _mm512_setzero_si512());
    sum = _mm512_dpwssd_epi32(sum, va_hi, vb_hi);

    // _mm512_dpwssd_epi32(src, a, b)
    // Multiply groups of 2 adjacent pairs of signed 16-bit integers in `a` with corresponding
    // 16-bit integers in `b`, producing 2 intermediate signed 32-bit results. Sum these 2 results
    // with the corresponding 32-bit integer in src, and store the packed 32-bit results in dst.
}

template <unsigned char residual> // 0..63
static inline int UINT8_InnerProductImp(const void *pVect1v, const void *pVect2v,
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

            sum = _mm512_dpwssd_epi32(sum, va, vb);
        } else if constexpr (residual == 32) {
            __m256i temp_a = _mm256_loadu_epi8(pVect1);
            __m512i va = _mm512_cvtepu8_epi16(temp_a);

            __m256i temp_b = _mm256_loadu_epi8(pVect2);
            __m512i vb = _mm512_cvtepu8_epi16(temp_b);

            sum = _mm512_dpwssd_epi32(sum, va, vb);
        } else {
            constexpr __mmask64 mask = (1LU << residual) - 1;
            __m512i va = _mm512_maskz_loadu_epi8(mask, pVect1);
            __m512i vb = _mm512_maskz_loadu_epi8(mask, pVect2);

            __m512i va_lo = _mm512_unpacklo_epi8(va, _mm512_setzero_si512());
            __m512i vb_lo = _mm512_unpacklo_epi8(vb, _mm512_setzero_si512());
            sum = _mm512_dpwssd_epi32(sum, va_lo, vb_lo);

            __m512i va_hi = _mm512_unpackhi_epi8(va, _mm512_setzero_si512());
            __m512i vb_hi = _mm512_unpackhi_epi8(vb, _mm512_setzero_si512());
            sum = _mm512_dpwssd_epi32(sum, va_hi, vb_hi);
        }
        pVect1 += residual;
        pVect2 += residual;

        // We dealt with the residual part.
        // We are left with some multiple of 64-uint_8 (might be 0).
        while (pVect1 < pEnd1) {
            InnerProductStep(pVect1, pVect2, sum);
        }
    } else {
        // We have no residual, we have some non-zero multiple of 64-uint_8.
        do {
            InnerProductStep(pVect1, pVect2, sum);
        } while (pVect1 < pEnd1);
    }

    return _mm512_reduce_add_epi32(sum);
}

template <unsigned char residual> // 0..63
float UINT8_InnerProductSIMD64_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                                  size_t dimension) {

    return 1 - UINT8_InnerProductImp<residual>(pVect1v, pVect2v, dimension);
}
template <unsigned char residual> // 0..63
float UINT8_CosineSIMD64_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                            size_t dimension) {
    float ip = UINT8_InnerProductImp<residual>(pVect1v, pVect2v, dimension);
    const float norm_v1 = load_unaligned<float>(static_cast<const uint8_t *>(pVect1v) + dimension);
    const float norm_v2 = load_unaligned<float>(static_cast<const uint8_t *>(pVect2v) + dimension);
    return 1.0f - ip / (norm_v1 * norm_v2);
}
