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
#include "VecSim/spaces/AVX_utils.h"
#include "VecSim/types/sq8.h"
#include "VecSim/types/float16.h"
#include "VecSim/utils/alignment.h"

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

/*
 * Asymmetric SQ8 (storage) <-> FP16 (query) inner product using algebraic identity:
 *   IP(x, y) = min * y_sum + delta * Σ(q_i * y_i)
 *
 * FP16 query lanes are widened to FP32 per 8-lane chunk via _mm256_cvtph_ps (F16C);
 * inner-loop arithmetic runs in FP32 with separate _mm256_mul_ps + _mm256_add_ps (no FMA).
 */

// 8-wide AVX2 step (no FMA): 8 SQ8 lanes + 8 FP16 lanes -> mul + add into sum.
static inline void SQ8_FP16_InnerProductStep_AVX2(const uint8_t *&pVect1, const float16 *&pVect2,
                                                  __m256 &sum256) {
    __m128i v1_128 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVect1));
    pVect1 += 8;
    __m256i v1_256 = _mm256_cvtepu8_epi32(v1_128);
    __m256 v1_f = _mm256_cvtepi32_ps(v1_256);

    __m128i v2_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVect2));
    __m256 v2_f = _mm256_cvtph_ps(v2_128);
    pVect2 += 8;

    sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1_f, v2_f));
}

// pVec1v = SQ8 storage, pVec2v = FP16 query. Precondition: dim >= 16 (enforced by dispatcher).
template <unsigned char residual> // 0..15
float SQ8_FP16_InnerProductImp_AVX2(const void *pVec1v, const void *pVec2v, size_t dimension) {
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v);
    const float16 *pVec2 = static_cast<const float16 *>(pVec2v);
    const uint8_t *pEnd1 = pVec1 + dimension;

    // Two accumulators break the mul->add dependency chain (no FMA on this tier).
    __m256 sum_a = _mm256_setzero_ps();
    __m256 sum_b = _mm256_setzero_ps();

    if constexpr (residual % 8) {
        constexpr int mask = (1 << (residual % 8)) - 1;

        __m128i v1_128 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVec1));
        pVec1 += residual % 8;
        __m256i v1_256 = _mm256_cvtepu8_epi32(v1_128);
        __m256 v1_f = _mm256_cvtepi32_ps(v1_256);

        __m128i v2_128 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pVec2));
        __m256 v2_f = _mm256_cvtph_ps(v2_128);
        v2_f = _mm256_blend_ps(_mm256_setzero_ps(), v2_f, mask);
        pVec2 += residual % 8;

        sum_a = _mm256_mul_ps(v1_f, v2_f);
    }

    if constexpr (residual >= 8) {
        SQ8_FP16_InnerProductStep_AVX2(pVec1, pVec2, sum_b);
    }

    do {
        SQ8_FP16_InnerProductStep_AVX2(pVec1, pVec2, sum_a);
        SQ8_FP16_InnerProductStep_AVX2(pVec1, pVec2, sum_b);
    } while (pVec1 < pEnd1);

    __m256 sum256 = _mm256_add_ps(sum_a, sum_b);
    float quantized_dot = my_mm256_reduce_add_ps(sum256);

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
float SQ8_FP16_InnerProductSIMD16_AVX2(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProductImp_AVX2<residual>(pVec1v, pVec2v, dimension);
}

template <unsigned char residual> // 0..15
float SQ8_FP16_CosineSIMD16_AVX2(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD16_AVX2<residual>(pVec1v, pVec2v, dimension);
}
