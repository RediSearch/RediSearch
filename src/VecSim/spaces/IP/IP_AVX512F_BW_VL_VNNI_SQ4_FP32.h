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
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/types/sq4.h"
#include "VecSim/utils/alignment.h"
#include <immintrin.h>
#include <cmath>

namespace {

/*
 * Asymmetric SQ4 inner product with the same identity as SQ8:
 *
 *   IP(x, y) ≈ Σ((min + delta * q_i) * y_i) = min * y_sum + delta * Σ(q_i * y_i)
 *
 * Storage packs coordinate i into byte i / 2, the low nibble for even i and the high nibble for
 * odd i, followed by unaligned FP32 metadata. The query blob carries y_sum after its dim floats.
 */

// Unpack 16 packed bytes (32 coordinates) into two float vectors in coordinate order.
static inline void SQ4_Unpack32_AVX512F_BW_VL_VNNI(__m128i packed, __m512 &first, __m512 &second) {
    const __m128i nibble = _mm_set1_epi8(15);
    const __m128i low = _mm_and_si128(packed, nibble);
    const __m128i high = _mm_and_si128(_mm_srli_epi16(packed, 4), nibble);
    first = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(_mm_unpacklo_epi8(low, high)));
    second = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(_mm_unpackhi_epi8(low, high)));
}

// Masks for a tail of `residual` (0..31) coordinates: the packed bytes that hold it, and the query
// lanes of each 16-coordinate half.
template <unsigned char residual>
struct SQ4TailMasks_AVX512F_BW_VL_VNNI {
    static constexpr __mmask16 bytes = (1u << ((residual + 1) / 2)) - 1;
    static constexpr __mmask16 first = residual >= 16 ? 0xffff : (1u << residual) - 1;
    static constexpr __mmask16 second = residual > 16 ? (1u << (residual - 16)) - 1 : 0;
};

template <unsigned char residual> // 0..31
float SQ4_FP32_InnerProductSIMD32_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                                     size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();
    const size_t complete = dimension - residual;
    for (size_t i = 0; i < complete; i += 32) {
        __m512 codes0, codes1;
        SQ4_Unpack32_AVX512F_BW_VL_VNNI(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(x + i / 2)), codes0, codes1);
        sum0 = _mm512_fmadd_ps(codes0, _mm512_loadu_ps(y + i), sum0);
        sum1 = _mm512_fmadd_ps(codes1, _mm512_loadu_ps(y + i + 16), sum1);
    }
    if constexpr (residual > 0) {
        // Masked loads of exactly the tail's code bytes and query floats, so nothing past the codes
        // or the query's dim floats is read. Masked-out query lanes are zero, so the padding nibble
        // of an odd dim adds nothing.
        using masks = SQ4TailMasks_AVX512F_BW_VL_VNNI<residual>;
        __m512 codes0, codes1;
        SQ4_Unpack32_AVX512F_BW_VL_VNNI(_mm_maskz_loadu_epi8(masks::bytes, x + complete / 2),
                                        codes0, codes1);
        sum0 = _mm512_fmadd_ps(codes0, _mm512_maskz_loadu_ps(masks::first, y + complete), sum0);
        if constexpr (masks::second != 0) {
            sum1 = _mm512_fmadd_ps(codes1, _mm512_maskz_loadu_ps(masks::second, y + complete + 16),
                                   sum1);
        }
    }
    const float dot = _mm512_reduce_add_ps(_mm512_add_ps(sum0, sum1));
    const auto *params = x + sq4::data_bytes_count(dimension);
    const float min_val = load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + sq4::DELTA * sizeof(float));
    return 1.0f - (min_val * y[dimension + sq4::SUM_QUERY] + delta * dot);
}

} // namespace
