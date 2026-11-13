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
#include "VecSim/types/sq4.h"
#include "VecSim/spaces/quantized_distance_helpers.h"
#include <cstring>
#include <immintrin.h>

namespace {

/*
 * The packed code dot is the sum of low-nibble products and high-nibble products.
 * Both operands' codes are 0..15, so maddubs' signed operand is nonnegative and its
 * saturating pair sum is at most 450. madd with ones widens groups of four products
 * to int32 before accumulation. Each lane of each accumulator adds at most 900 per
 * 64 coordinates. Even after combining low and high, 1800 * ceil(dim / 64) is below
 * 537 million at SQ4_MAX_EXACT_SIMD_DIM, safely below INT32_MAX. The horizontal sum
 * uses wrapping SIMD additions and unsigned extraction: the dispatch bound keeps
 * the full sum, at most 225 * dim, within UINT32_MAX.
 */

static inline void SQ4_SQ4_DotStep_AVX2(__m256i &sum_low, __m256i &sum_high, __m256i x, __m256i y) {
    const __m256i nibble = _mm256_set1_epi8(15);
    const __m256i ones = _mm256_set1_epi16(1);
    const __m256i low =
        _mm256_maddubs_epi16(_mm256_and_si256(x, nibble), _mm256_and_si256(y, nibble));
    const __m256i high = _mm256_maddubs_epi16(_mm256_and_si256(_mm256_srli_epi16(x, 4), nibble),
                                              _mm256_and_si256(_mm256_srli_epi16(y, 4), nibble));
    sum_low = _mm256_add_epi32(sum_low, _mm256_madd_epi16(low, ones));
    sum_high = _mm256_add_epi32(sum_high, _mm256_madd_epi16(high, ones));
}

// Raw inner product, shared with the stored-sum L2 identity.
template <unsigned char residual> // 0..63
float SQ4_SQ4_InnerProductSIMD64_AVX2_IMP(const void *pVect1v, const void *pVect2v,
                                          size_t dimension) {
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    __m256i sum_low = _mm256_setzero_si256();
    __m256i sum_high = _mm256_setzero_si256();
    const size_t complete = dimension - residual;
    for (size_t i = 0; i < complete; i += 64) {
        SQ4_SQ4_DotStep_AVX2(sum_low, sum_high,
                             _mm256_loadu_si256(reinterpret_cast<const __m256i *>(x + i / 2)),
                             _mm256_loadu_si256(reinterpret_cast<const __m256i *>(y + i / 2)));
    }
    if constexpr (residual > 0) {
        // Copy only code bytes, without reading into the stored metadata. Clear the unused
        // high nibble for an odd dimension, even if a foreign writer left it nonzero.
        alignas(32) uint8_t xt[32] = {};
        alignas(32) uint8_t yt[32] = {};
        std::memcpy(xt, x + complete / 2, (residual + 1) / 2);
        std::memcpy(yt, y + complete / 2, (residual + 1) / 2);
        if constexpr (residual % 2 != 0) {
            xt[residual / 2] &= 15;
        }
        SQ4_SQ4_DotStep_AVX2(sum_low, sum_high,
                             _mm256_load_si256(reinterpret_cast<const __m256i *>(xt)),
                             _mm256_load_si256(reinterpret_cast<const __m256i *>(yt)));
    }
    const __m256i sum = _mm256_add_epi32(sum_low, sum_high);
    const __m128i s4 = _mm_add_epi32(_mm256_castsi256_si128(sum), _mm256_extracti128_si256(sum, 1));
    const __m128i s2 = _mm_add_epi32(s4, _mm_srli_si128(s4, 8));
    const __m128i s1 = _mm_add_epi32(s2, _mm_srli_si128(s2, 4));
    const auto code_dot = static_cast<uint32_t>(_mm_cvtsi128_si32(s1));
    return spaces::quantized::Stored_InnerProduct_FromCodeDot<vecsim_types::sq4>(
        x, y, dimension, static_cast<float>(code_dot));
}

template <unsigned char residual> // 0..63
float SQ4_SQ4_InnerProductSIMD64_AVX2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ4_SQ4_InnerProductSIMD64_AVX2_IMP<residual>(pVect1v, pVect2v, dimension);
}

} // namespace
