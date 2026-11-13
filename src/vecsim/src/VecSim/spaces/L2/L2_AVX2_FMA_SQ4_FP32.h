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
// SQ4_Unpack_AVX2_FMA, SQ4_ToFloats_AVX2_FMA, SQ4_LoadBytes_AVX2_FMA, SQ4_TailLanes_AVX2_FMA,
// SQ4_TailQuery_AVX2_FMA, SQ4_TailCodes_AVX2_FMA
#include "VecSim/spaces/IP/IP_AVX2_FMA_SQ4_FP32.h"
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

// Accumulate Σ(diff_i²) for eight coordinates.
static inline __m256 SQ4_FP32_L2Step_AVX2_FMA(__m256 sum, __m256 codes, __m256 query,
                                              __m256 min_val_vec, __m256 delta_vec) {
    const __m256 diff = _mm256_fmadd_ps(delta_vec, codes, _mm256_sub_ps(min_val_vec, query));
    return _mm256_fmadd_ps(diff, diff, sum);
}

// As SQ4_FP32_L2Step_AVX2_FMA for a tail of `lanes` (1..8) coordinates held in the top lanes (see
// SQ4_TailQuery_AVX2_FMA); the other lanes' diff is zeroed.
template <unsigned lanes>
static inline __m256 SQ4_FP32_L2TailStep_AVX2_FMA(__m256 sum, __m256 codes, __m256 query,
                                                  __m256 min_val_vec, __m256 delta_vec) {
    const __m256 diff = _mm256_blend_ps(
        _mm256_setzero_ps(), _mm256_fmadd_ps(delta_vec, codes, _mm256_sub_ps(min_val_vec, query)),
        SQ4_TailLanes_AVX2_FMA<lanes>);
    return _mm256_fmadd_ps(diff, diff, sum);
}

template <unsigned char residual> // 0..31
float SQ4_FP32_L2SqrSIMD32_AVX2_FMA(const void *pVect1v, const void *pVect2v, size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    const auto *params = x + sq4::data_bytes_count(dimension);
    const __m256 min_val_vec =
        _mm256_set1_ps(load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float)));
    const __m256 delta_vec =
        _mm256_set1_ps(load_unaligned<float>(params + sq4::DELTA * sizeof(float)));
    // Four accumulators break the FMA dependency chain.
    __m256 sum0 = _mm256_setzero_ps();
    __m256 sum1 = _mm256_setzero_ps();
    __m256 sum2 = _mm256_setzero_ps();
    __m256 sum3 = _mm256_setzero_ps();
    const size_t complete = dimension - residual;
    for (size_t i = 0; i < complete; i += 32) {
        // One 16-byte load covers exactly 32 coordinates.
        __m128i ordered0, ordered1;
        SQ4_Unpack_AVX2_FMA(_mm_loadu_si128(reinterpret_cast<const __m128i *>(x + i / 2)), ordered0,
                            ordered1);
        __m256 codes0, codes1, codes2, codes3;
        SQ4_ToFloats_AVX2_FMA(ordered0, codes0, codes1);
        SQ4_ToFloats_AVX2_FMA(ordered1, codes2, codes3);
        sum0 =
            SQ4_FP32_L2Step_AVX2_FMA(sum0, codes0, _mm256_loadu_ps(y + i), min_val_vec, delta_vec);
        sum1 = SQ4_FP32_L2Step_AVX2_FMA(sum1, codes1, _mm256_loadu_ps(y + i + 8), min_val_vec,
                                        delta_vec);
        sum2 = SQ4_FP32_L2Step_AVX2_FMA(sum2, codes2, _mm256_loadu_ps(y + i + 16), min_val_vec,
                                        delta_vec);
        sum3 = SQ4_FP32_L2Step_AVX2_FMA(sum3, codes3, _mm256_loadu_ps(y + i + 24), min_val_vec,
                                        delta_vec);
    }
    size_t i = complete;
    if constexpr (residual >= 16) {
        // One 8-byte load covers exactly 16 coordinates.
        __m128i ordered, unused;
        SQ4_Unpack_AVX2_FMA(_mm_loadl_epi64(reinterpret_cast<const __m128i *>(x + i / 2)), ordered,
                            unused);
        __m256 codes0, codes1;
        SQ4_ToFloats_AVX2_FMA(ordered, codes0, codes1);
        sum0 =
            SQ4_FP32_L2Step_AVX2_FMA(sum0, codes0, _mm256_loadu_ps(y + i), min_val_vec, delta_vec);
        sum1 = SQ4_FP32_L2Step_AVX2_FMA(sum1, codes1, _mm256_loadu_ps(y + i + 8), min_val_vec,
                                        delta_vec);
        i += 16;
    }
    constexpr unsigned rest = residual % 16;
    if constexpr (rest > 0) {
        // Exactly the tail's code bytes. The last query window ends at the last coordinate (see
        // SQ4_TailQuery_AVX2_FMA).
        __m128i ordered, unused;
        SQ4_Unpack_AVX2_FMA(SQ4_LoadBytes_AVX2_FMA<(rest + 1) / 2>(x + i / 2), ordered, unused);
        const float *end = y + dimension;
        if constexpr (rest > 8) {
            const __m256 codes0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(ordered));
            sum2 = SQ4_FP32_L2Step_AVX2_FMA(sum2, codes0, _mm256_loadu_ps(y + i), min_val_vec,
                                            delta_vec);
            sum3 = SQ4_FP32_L2TailStep_AVX2_FMA<rest - 8>(
                sum3, SQ4_TailCodes_AVX2_FMA<rest - 8>(_mm_unpackhi_epi64(ordered, ordered)),
                SQ4_TailQuery_AVX2_FMA<rest - 8>(end, dimension), min_val_vec, delta_vec);
        } else {
            sum2 = SQ4_FP32_L2TailStep_AVX2_FMA<rest>(sum2, SQ4_TailCodes_AVX2_FMA<rest>(ordered),
                                                      SQ4_TailQuery_AVX2_FMA<rest>(end, dimension),
                                                      min_val_vec, delta_vec);
        }
    }
    return my_mm256_reduce_add_ps_tree(
        _mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3)));
}

} // namespace
