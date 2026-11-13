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
#include "VecSim/types/sq8.h"
#include "VecSim/types/float16.h"
#include "VecSim/utils/alignment.h"

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

/*
 * Asymmetric SQ8 (storage) <-> FP16 (query) inner product using algebraic identity:
 *   IP(x, y) = min * y_sum + delta * Σ(q_i * y_i)
 *
 * FP16 query lanes are widened to FP32 per 16-lane chunk via _mm512_cvtph_ps (AVX512F);
 * inner-loop arithmetic runs in FP32 with _mm512_fmadd_ps.
 */

// 16-wide AVX512F step: 16 SQ8 lanes + 16 FP16 lanes -> 16 FP32 fused-multiply-add.
static inline void SQ8_FP16_InnerProductStep_AVX512(const uint8_t *&pVec1, const float16 *&pVec2,
                                                    __m512 &sum) {
    __m128i v1_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec1));
    __m512i v1_512 = _mm512_cvtepu8_epi32(v1_128);
    __m512 v1_f = _mm512_cvtepi32_ps(v1_512);

    __m256i v2_16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(pVec2));
    __m512 v2_f = _mm512_cvtph_ps(v2_16);

    sum = _mm512_fmadd_ps(v1_f, v2_f, sum);

    pVec1 += 16;
    pVec2 += 16;
}

// pVec1v = SQ8 storage, pVec2v = FP16 query. Precondition: dim >= 16 (enforced by dispatcher).
template <unsigned char residual> // 0..15
float SQ8_FP16_InnerProductImp_AVX512(const void *pVec1v, const void *pVec2v, size_t dimension) {
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v);
    const float16 *pVec2 = static_cast<const float16 *>(pVec2v);
    const uint8_t *pEnd1 = pVec1 + dimension;

    // Four accumulators break the FMA dependency chain to saturate both FMA ports.
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();
    __m512 sum2 = _mm512_setzero_ps();
    __m512 sum3 = _mm512_setzero_ps();

    if constexpr (residual > 0) {
        __mmask16 mask = (1U << residual) - 1;

        __m128i v1_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec1));
        __m512i v1_512 = _mm512_cvtepu8_epi32(v1_128);
        __m512 v1_f = _mm512_cvtepi32_ps(v1_512);

        __m256i v2_16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(pVec2));
        __m512 v2_f = _mm512_cvtph_ps(v2_16);

        sum0 = _mm512_maskz_mul_ps(mask, v1_f, v2_f);

        pVec1 += residual;
        pVec2 += residual;
    }

    // Main loop: 4 chunks of 16 lanes per iteration, one chunk per accumulator.
    while (static_cast<size_t>(pEnd1 - pVec1) >= 64) {
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum0);
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum1);
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum2);
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum3);
    }

    // Tail: at most three remaining 16-lane chunks (post-residual remainder is a multiple of 16).
    // Keep chunks on distinct accumulators to preserve ILP when the main loop did not run.
    const size_t remaining = pEnd1 - pVec1;
    if (remaining >= 16)
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum0);
    if (remaining >= 32)
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum1);
    if (remaining >= 48)
        SQ8_FP16_InnerProductStep_AVX512(pVec1, pVec2, sum2);

    __m512 sum = _mm512_add_ps(_mm512_add_ps(sum0, sum1), _mm512_add_ps(sum2, sum3));
    float quantized_dot = _mm512_reduce_add_ps(sum);

    const uint8_t *pVec1Base = static_cast<const uint8_t *>(pVec1v);
    const uint8_t *params_bytes = pVec1Base + dimension;
    const float min_val = load_unaligned<float>(params_bytes + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params_bytes + sq8::DELTA * sizeof(float));

    const float16 *pVec2Base = static_cast<const float16 *>(pVec2v);
    const auto *query_meta_bytes = reinterpret_cast<const uint8_t *>(pVec2Base + dimension);
    const float y_sum = load_unaligned<float>(query_meta_bytes + sq8::SUM_QUERY * sizeof(float));

    return min_val * y_sum + delta * quantized_dot;
}

template <unsigned char residual> // 0..15
float SQ8_FP16_InnerProductSIMD16_AVX512F(const void *pVec1v, const void *pVec2v,
                                          size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProductImp_AVX512<residual>(pVec1v, pVec2v, dimension);
}

template <unsigned char residual> // 0..15
float SQ8_FP16_CosineSIMD16_AVX512F(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD16_AVX512F<residual>(pVec1v, pVec2v, dimension);
}
