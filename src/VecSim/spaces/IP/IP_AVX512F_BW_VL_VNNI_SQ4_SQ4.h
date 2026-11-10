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
#include <immintrin.h>

namespace {

/*
 * Symmetric SQ4-SQ4 inner product with the same identity as SQ8-SQ8:
 *
 *   IP(x, y) = min_x * sum_y + min_y * sum_x - dim * min_x * min_y
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *
 * Only the integer code dot Σ(qx_i * qy_i) is vectorized. Both operands pack coordinate i into
 * byte i / 2, the low nibble for even i and the high nibble for odd i, so the code dot is the sum
 * of the low-nibble products plus the sum of the high-nibble products and no lane reordering is
 * needed. Each half is unpacked to bytes and multiplied with vpdpbusd into signed int32 lanes.
 * The packed codes are followed by unaligned FP32 metadata.
 *
 * Exactness: every vpdpbusd adds four products of at most 225 to each lane, and a lane takes part
 * in at most dim / 128 + 2 steps per nibble half, so a lane of the summed accumulators holds at
 * most 1800 * (dim / 128 + 2), about 268 million at spaces::SQ4_MAX_EXACT_SIMD_DIM, far below
 * INT32_MAX. Explicit wrapping SIMD additions reduce the lanes to uint32;
 * the whole total, at most 225 * dim, is what the dispatcher's uint32-based bound keeps exact.
 */

// Byte masks for a tail of `residual` (0..63) coordinates: the packed bytes that hold it, and the
// bytes whose high nibble is in it. For an odd residual the last byte's high nibble is the padding.
template <unsigned char residual>
struct SQ4SQ4TailMasks_AVX512F_BW_VL_VNNI {
    static constexpr __mmask64 bytes = (uint64_t(1) << ((residual + 1) / 2)) - 1;
    static constexpr __mmask64 high = (uint64_t(1) << (residual / 2)) - 1;
};

// Code products of up to 64 packed bytes (128 coordinates): low nibbles into sum_low, high nibbles
// into sum_high. Codes are 0..15, so vpdpbusd's unsigned-by-signed bytes multiply them exactly.
static inline void SQ4_SQ4_DotStep_AVX512F_BW_VL_VNNI(__m512i &sum_low, __m512i &sum_high,
                                                      __m512i x, __m512i y) {
    const __m512i nibble = _mm512_set1_epi8(15);
    sum_low =
        _mm512_dpbusd_epi32(sum_low, _mm512_and_si512(x, nibble), _mm512_and_si512(y, nibble));
    sum_high = _mm512_dpbusd_epi32(sum_high, _mm512_and_si512(_mm512_srli_epi16(x, 4), nibble),
                                   _mm512_and_si512(_mm512_srli_epi16(y, 4), nibble));
}

// Returns the raw inner product (not the distance); the L2 identity kernels share it.
template <unsigned char residual> // 0..63
float SQ4_SQ4_InnerProductSIMD64_AVX512F_BW_VL_VNNI_IMP(const void *pVect1v, const void *pVect2v,
                                                        size_t dimension) {
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    __m512i sum_low = _mm512_setzero_si512();
    __m512i sum_high = _mm512_setzero_si512();
    const size_t complete = dimension - residual;
    size_t i = 0;
    for (; i + 128 <= complete; i += 128) {
        SQ4_SQ4_DotStep_AVX512F_BW_VL_VNNI(sum_low, sum_high, _mm512_loadu_si512(x + i / 2),
                                           _mm512_loadu_si512(y + i / 2));
    }
    if (i < complete) {
        // One 64-coordinate chunk left: exactly its 32 bytes per operand.
        constexpr __mmask64 half = 0xffffffffull;
        SQ4_SQ4_DotStep_AVX512F_BW_VL_VNNI(sum_low, sum_high,
                                           _mm512_maskz_loadu_epi8(half, x + i / 2),
                                           _mm512_maskz_loadu_epi8(half, y + i / 2));
    }
    if constexpr (residual > 0) {
        // Masked loads of exactly the tail's code bytes, so nothing past the codes is read. An odd
        // dim's padding nibble is cleared in x, so its product is zero.
        using masks = SQ4SQ4TailMasks_AVX512F_BW_VL_VNNI<residual>;
        __m512i xt = _mm512_maskz_loadu_epi8(masks::bytes, x + complete / 2);
        if constexpr (residual % 2 != 0) {
            xt = _mm512_mask_mov_epi8(xt, masks::bytes & ~masks::high,
                                      _mm512_and_si512(xt, _mm512_set1_epi8(15)));
        }
        SQ4_SQ4_DotStep_AVX512F_BW_VL_VNNI(sum_low, sum_high, xt,
                                           _mm512_maskz_loadu_epi8(masks::bytes, y + complete / 2));
    }
    // Existing dispatch bounds the full sum by UINT32_MAX. Keep all additions in SIMD,
    // then extract as unsigned before FP32 conversion.
    const __m512i sum = _mm512_add_epi32(sum_low, sum_high);
    const __m256i s8 =
        _mm256_add_epi32(_mm512_castsi512_si256(sum), _mm512_extracti64x4_epi64(sum, 1));
    const __m128i s4 = _mm_add_epi32(_mm256_castsi256_si128(s8), _mm256_extracti128_si256(s8, 1));
    const __m128i s2 = _mm_add_epi32(s4, _mm_srli_si128(s4, 8));
    const __m128i s1 = _mm_add_epi32(s2, _mm_srli_si128(s2, 4));
    const auto code_dot = static_cast<uint32_t>(_mm_cvtsi128_si32(s1));
    return spaces::quantized::Stored_InnerProduct_FromCodeDot<vecsim_types::sq4>(
        x, y, dimension, static_cast<float>(code_dot));
}

template <unsigned char residual> // 0..63
float SQ4_SQ4_InnerProductSIMD64_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                                    size_t dimension) {
    return 1.0f -
           SQ4_SQ4_InnerProductSIMD64_AVX512F_BW_VL_VNNI_IMP<residual>(pVect1v, pVect2v, dimension);
}

} // namespace
