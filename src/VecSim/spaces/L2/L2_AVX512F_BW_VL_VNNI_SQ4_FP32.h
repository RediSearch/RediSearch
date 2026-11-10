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
// SQ4_Unpack32_AVX512F_BW_VL_VNNI, SQ4TailMasks_AVX512F_BW_VL_VNNI
#include "VecSim/spaces/IP/IP_AVX512F_BW_VL_VNNI_SQ4_FP32.h"
#include "VecSim/types/sq4.h"
#include "VecSim/utils/alignment.h"
#include <immintrin.h>

namespace {

/*
 * Asymmetric SQ4-FP32 L2 squared distance via direct residual accumulation, as SQ8:
 *
 *   ||x - y||² = Σ(delta * q_i + (min - y_i))²
 *
 * Keeping min - y first (one fused multiply-add per coordinate) rounds at the scale of the
 * residual, not of min, when the data has a large common offset. Storage packs coordinate i into
 * byte i / 2, the low nibble for even i and the high nibble for odd i, followed by unaligned FP32
 * metadata.
 */

static inline __m512 SQ4_FP32_L2Diff_AVX512F_BW_VL_VNNI(__m512 codes, __m512 query,
                                                        __m512 min_val_vec, __m512 delta_vec) {
    return _mm512_fmadd_ps(delta_vec, codes, _mm512_sub_ps(min_val_vec, query));
}

template <unsigned char residual> // 0..31
float SQ4_FP32_L2SqrSIMD32_AVX512F_BW_VL_VNNI(const void *pVect1v, const void *pVect2v,
                                              size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    const auto *params = x + sq4::data_bytes_count(dimension);
    const __m512 min_val_vec =
        _mm512_set1_ps(load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float)));
    const __m512 delta_vec =
        _mm512_set1_ps(load_unaligned<float>(params + sq4::DELTA * sizeof(float)));
    __m512 sum0 = _mm512_setzero_ps();
    __m512 sum1 = _mm512_setzero_ps();
    const size_t complete = dimension - residual;
    for (size_t i = 0; i < complete; i += 32) {
        __m512 codes0, codes1;
        SQ4_Unpack32_AVX512F_BW_VL_VNNI(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(x + i / 2)), codes0, codes1);
        const __m512 diff0 = SQ4_FP32_L2Diff_AVX512F_BW_VL_VNNI(codes0, _mm512_loadu_ps(y + i),
                                                                min_val_vec, delta_vec);
        const __m512 diff1 = SQ4_FP32_L2Diff_AVX512F_BW_VL_VNNI(codes1, _mm512_loadu_ps(y + i + 16),
                                                                min_val_vec, delta_vec);
        sum0 = _mm512_fmadd_ps(diff0, diff0, sum0);
        sum1 = _mm512_fmadd_ps(diff1, diff1, sum1);
    }
    if constexpr (residual > 0) {
        // Masked loads of exactly the tail's code bytes and query floats, so nothing past the codes
        // or the query's dim floats is read. Masked-out lanes keep the running sum, which also
        // drops the padding nibble of an odd dim.
        using masks = SQ4TailMasks_AVX512F_BW_VL_VNNI<residual>;
        __m512 codes0, codes1;
        SQ4_Unpack32_AVX512F_BW_VL_VNNI(_mm_maskz_loadu_epi8(masks::bytes, x + complete / 2),
                                        codes0, codes1);
        const __m512 diff0 = SQ4_FP32_L2Diff_AVX512F_BW_VL_VNNI(
            codes0, _mm512_maskz_loadu_ps(masks::first, y + complete), min_val_vec, delta_vec);
        sum0 = _mm512_mask3_fmadd_ps(diff0, diff0, sum0, masks::first);
        if constexpr (masks::second != 0) {
            const __m512 diff1 = SQ4_FP32_L2Diff_AVX512F_BW_VL_VNNI(
                codes1, _mm512_maskz_loadu_ps(masks::second, y + complete + 16), min_val_vec,
                delta_vec);
            sum1 = _mm512_mask3_fmadd_ps(diff1, diff1, sum1, masks::second);
        }
    }
    return _mm512_reduce_add_ps(_mm512_add_ps(sum0, sum1));
}

} // namespace
