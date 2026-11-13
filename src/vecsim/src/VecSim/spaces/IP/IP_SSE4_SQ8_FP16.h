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
 * FP16 query lanes are widened to FP32 per 4-lane chunk via _mm_cvtph_ps (F16C);
 * inner-loop arithmetic runs in FP32 with separate _mm_mul_ps + _mm_add_ps (no FMA).
 */

// 4-wide SSE4+F16C step: 4 SQ8 lanes + 4 FP16 lanes -> mul + add into sum.
static inline void SQ8_FP16_InnerProductStep_SSE4(const uint8_t *&pVect1, const float16 *&pVect2,
                                                  __m128 &sum) {
    __m128i v1_i = _mm_cvtepu8_epi32(_mm_cvtsi32_si128(load_unaligned<int32_t>(pVect1)));
    pVect1 += 4;
    __m128 v1_f = _mm_cvtepi32_ps(v1_i);

    __m128i v2_8 = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(pVect2));
    __m128 v2_f = _mm_cvtph_ps(v2_8);
    pVect2 += 4;

    sum = _mm_add_ps(sum, _mm_mul_ps(v1_f, v2_f));
}

// pVec1v = SQ8 storage, pVec2v = FP16 query. Precondition: dim >= 16 (enforced by dispatcher).
template <unsigned char residual> // 0..15
float SQ8_FP16_InnerProductSIMD16_SSE4_IMP(const void *pVec1v, const void *pVec2v,
                                           size_t dimension) {
    const uint8_t *pVec1 = static_cast<const uint8_t *>(pVec1v);
    const float16 *pVec2 = static_cast<const float16 *>(pVec2v);
    const uint8_t *pEnd1 = pVec1 + dimension;

    // Two accumulators break the mul->add dependency chain (no FMA on this tier).
    __m128 sum_a = _mm_setzero_ps();
    __m128 sum_b = _mm_setzero_ps();

    if constexpr (residual % 4) {
        __m128 v1_f;
        __m128 v2_f;

        if constexpr (residual % 4 == 3) {
            v1_f = _mm_set_ps(0.0f, static_cast<float>(pVec1[2]), static_cast<float>(pVec1[1]),
                              static_cast<float>(pVec1[0]));
            v2_f = _mm_set_ps(0.0f, vecsim_types::FP16_to_FP32(pVec2[2]),
                              vecsim_types::FP16_to_FP32(pVec2[1]),
                              vecsim_types::FP16_to_FP32(pVec2[0]));
        } else if constexpr (residual % 4 == 2) {
            v1_f =
                _mm_set_ps(0.0f, 0.0f, static_cast<float>(pVec1[1]), static_cast<float>(pVec1[0]));
            v2_f = _mm_set_ps(0.0f, 0.0f, vecsim_types::FP16_to_FP32(pVec2[1]),
                              vecsim_types::FP16_to_FP32(pVec2[0]));
        } else if constexpr (residual % 4 == 1) {
            v1_f = _mm_set_ps(0.0f, 0.0f, 0.0f, static_cast<float>(pVec1[0]));
            v2_f = _mm_set_ps(0.0f, 0.0f, 0.0f, vecsim_types::FP16_to_FP32(pVec2[0]));
        }

        pVec1 += residual % 4;
        pVec2 += residual % 4;

        sum_a = _mm_mul_ps(v1_f, v2_f);
    }

    if constexpr (residual >= 4) {
        SQ8_FP16_InnerProductStep_SSE4(pVec1, pVec2, sum_b);
    }
    if constexpr (residual >= 8) {
        SQ8_FP16_InnerProductStep_SSE4(pVec1, pVec2, sum_a);
    }
    if constexpr (residual >= 12) {
        SQ8_FP16_InnerProductStep_SSE4(pVec1, pVec2, sum_b);
    }

    do {
        SQ8_FP16_InnerProductStep_SSE4(pVec1, pVec2, sum_a);
        SQ8_FP16_InnerProductStep_SSE4(pVec1, pVec2, sum_b);
    } while (pVec1 < pEnd1);

    __m128 sum = _mm_add_ps(sum_a, sum_b);
    float PORTABLE_ALIGN16 TmpRes[4];
    _mm_store_ps(TmpRes, sum);
    float quantized_dot = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

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
float SQ8_FP16_InnerProductSIMD16_SSE4(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProductSIMD16_SSE4_IMP<residual>(pVec1v, pVec2v, dimension);
}

template <unsigned char residual> // 0..15
float SQ8_FP16_CosineSIMD16_SSE4(const void *pVec1v, const void *pVec2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD16_SSE4<residual>(pVec1v, pVec2v, dimension);
}
