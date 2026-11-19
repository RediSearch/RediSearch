/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cmath>
#include "vecsim/spaces/space_includes.h"
#include "vecsim/spaces/functions/AVX512F.h"
#include "vecsim/types/sq8.h"
#include "vecsim/types/float16.h"
#include "vecsim/utils/alignment.h"

static inline __m512h SQ8_FP16_LoadQuantizedValues_AVX512FP16(const uint8_t *quantized_values) {
    const __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(quantized_values));
    return _mm512_cvtepu16_ph(_mm512_cvtepu8_epi16(bytes));
}

static inline void SQ8_FP16_InnerProductStep_AVX512FP16(const uint8_t *&quantized_values,
                                                        const vecsim_types::float16 *&query,
                                                        __m512h &sum) {
    const __m512h values = SQ8_FP16_LoadQuantizedValues_AVX512FP16(quantized_values);
    const __m512h query_values = _mm512_loadu_ph(query);
    sum = _mm512_fmadd_ph(values, query_values, sum);
    quantized_values += 32;
    query += 32;
}

// dim >= 32 keeps the residual's full loads within the vector payloads.
// Four independent half accumulators shorten each rounding chain.
template <unsigned char residual>
static inline __m512h SQ8_FP16_AccumulateInnerProduct_AVX512FP16(const void *storage,
                                                                 const void *query_blob,
                                                                 size_t dimension) {
    using float16 = vecsim_types::float16;
    const auto *quantized_values = static_cast<const uint8_t *>(storage);
    const auto *query = static_cast<const float16 *>(query_blob);
    const auto *end = quantized_values + dimension;
    __m512h sum = _mm512_setzero_ph();
    if constexpr (residual) {
        constexpr __mmask32 mask = (1U << residual) - 1;
        const __m512h values = SQ8_FP16_LoadQuantizedValues_AVX512FP16(quantized_values);
        const __m512h query_values = _mm512_loadu_ph(query);
        sum = _mm512_maskz_mul_ph(mask, values, query_values);
        quantized_values += residual;
        query += residual;
    }
    __m512h sum1 = _mm512_setzero_ph();
    __m512h sum2 = _mm512_setzero_ph();
    __m512h sum3 = _mm512_setzero_ph();
    while (end - quantized_values >= 128) {
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum);
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum1);
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum2);
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum3);
    }
    const size_t remaining = end - quantized_values;
    if (remaining >= 32)
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum);
    if (remaining >= 64)
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum1);
    if (remaining >= 96)
        SQ8_FP16_InnerProductStep_AVX512FP16(quantized_values, query, sum2);
    return _mm512_add_ph(_mm512_add_ph(sum, sum1), _mm512_add_ph(sum2, sum3));
}

// Widening the merged lanes keeps the horizontal reduction out of half precision.
static inline float SQ8_FP16_ReduceInnerProduct_AVX512FP16(__m512h sum) {
    const __m512i bits = _mm512_castph_si512(sum);
    const __m512 low = _mm512_cvtph_ps(_mm512_castsi512_si256(bits));
    const __m512 high = _mm512_cvtph_ps(_mm512_extracti64x4_epi64(bits, 1));
    return _mm512_reduce_add_ps(_mm512_add_ps(low, high));
}

static inline float SQ8_FP16_ApplyInnerProductCorrection_AVX512FP16(const void *storage,
                                                                    const void *query_blob,
                                                                    size_t dimension, float dot) {
    using sq8 = vecsim_types::sq8;
    using float16 = vecsim_types::float16;
    const auto *storage_meta = static_cast<const uint8_t *>(storage) + dimension;
    const float min_val = load_unaligned<float>(storage_meta + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(storage_meta + sq8::DELTA * sizeof(float));
    const auto *query_meta =
        reinterpret_cast<const uint8_t *>(static_cast<const float16 *>(query_blob) + dimension);
    const float query_sum = load_unaligned<float>(query_meta + sq8::SUM_QUERY * sizeof(float));
    return 1.0f - (min_val * query_sum + delta * dot);
}

template <unsigned char residual>
float SQ8_FP16_InnerProductSIMD32_AVX512FP16_BW_VL(const void *storage, const void *query_blob,
                                                   size_t dimension) {
    // Directed rounding can saturate half overflow, hiding it from the finite check below.
    if ((_mm_getcsr() & _MM_ROUND_MASK) != _MM_ROUND_NEAREST)
        return spaces::Choose_SQ8_FP16_IP_implementation_AVX512F(dimension)(storage, query_blob,
                                                                            dimension);
    const __m512h sum =
        SQ8_FP16_AccumulateInnerProduct_AVX512FP16<residual>(storage, query_blob, dimension);
    const float dot = SQ8_FP16_ReduceInnerProduct_AVX512FP16(sum);
    // Nearest rounding leaves intermediate and reduction overflow nonfinite.
    if (!std::isfinite(dot))
        return spaces::Choose_SQ8_FP16_IP_implementation_AVX512F(dimension)(storage, query_blob,
                                                                            dimension);
    return SQ8_FP16_ApplyInnerProductCorrection_AVX512FP16(storage, query_blob, dimension, dot);
}
