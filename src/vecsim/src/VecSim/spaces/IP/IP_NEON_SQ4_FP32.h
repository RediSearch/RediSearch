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
#include <arm_neon.h>
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

// Byte-to-u32 widening indices for TBL: lane j of the result is byte `first + j` of the table,
// zero-extended (out-of-range indices yield zero).
static inline uint8x16_t SQ4_NEON_WidenIndex(uint8_t first) {
    const uint8_t z = 0xff;
    const uint8_t f = first;
    const uint8_t idx[16] = {
        f, z, z, z, uint8_t(f + 1), z, z, z, uint8_t(f + 2), z, z, z, uint8_t(f + 3), z, z, z};
    return vld1q_u8(idx);
}

// Load exactly two packed bytes, yielding four coordinates in order without touching metadata.
static inline float32x4_t SQ4_NEON_Unpack4(const uint8_t *data) {
    const uint8x8_t packed = vcreate_u8(load_unaligned<uint16_t>(data));
    const uint8x8_t ordered = vzip_u8(vand_u8(packed, vdup_n_u8(15)), vshr_n_u8(packed, 4)).val[0];
    return vcvtq_f32_u32(vmovl_u16(vget_low_u16(vmovl_u8(ordered))));
}

template <unsigned char residual> // 0..15
float SQ4_FP32_InnerProductSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);
    const size_t complete = dimension - residual;
    size_t i = 0;
    const uint8x16_t widen0 = SQ4_NEON_WidenIndex(0), widen1 = SQ4_NEON_WidenIndex(4);
    const uint32x4_t low_mask = vdupq_n_u32(15);
    for (; i < complete; i += 16) {
        // One eight-byte load covers sixteen coordinates. TBL puts one packed byte in each u32
        // lane; its low nibble is an even coordinate and its high nibble the next odd one, and a
        // structure load (LD2) splits the query into the same even and odd lanes.
        const uint8x16_t packed = vcombine_u8(vld1_u8(x + i / 2), vdup_n_u8(0));
        const uint32x4_t bytes0 = vreinterpretq_u32_u8(vqtbl1q_u8(packed, widen0));
        const uint32x4_t bytes1 = vreinterpretq_u32_u8(vqtbl1q_u8(packed, widen1));
        const float32x4x2_t query0 = vld2q_f32(y + i), query1 = vld2q_f32(y + i + 8);
        sum0 = vfmaq_f32(sum0, vcvtq_f32_u32(vandq_u32(bytes0, low_mask)), query0.val[0]);
        sum1 = vfmaq_f32(sum1, vcvtq_f32_u32(vshrq_n_u32(bytes0, 4)), query0.val[1]);
        sum2 = vfmaq_f32(sum2, vcvtq_f32_u32(vandq_u32(bytes1, low_mask)), query1.val[0]);
        sum3 = vfmaq_f32(sum3, vcvtq_f32_u32(vshrq_n_u32(bytes1, 4)), query1.val[1]);
    }
    // Packed tail: groups of four read two whole bytes, the last 1..3 coordinates read nibbles,
    // so no load passes the code bytes (an odd dim's unused high nibble is never read).
    if constexpr (residual >= 4) {
        sum0 = vfmaq_f32(sum0, SQ4_NEON_Unpack4(x + i / 2), vld1q_f32(y + i));
        i += 4;
    }
    if constexpr (residual >= 8) {
        sum1 = vfmaq_f32(sum1, SQ4_NEON_Unpack4(x + i / 2), vld1q_f32(y + i));
        i += 4;
    }
    if constexpr (residual >= 12) {
        sum2 = vfmaq_f32(sum2, SQ4_NEON_Unpack4(x + i / 2), vld1q_f32(y + i));
        i += 4;
    }
    if constexpr (residual % 4 > 0) {
        float32x4_t codes = vdupq_n_f32(0.0f);
        float32x4_t query = vdupq_n_f32(0.0f);
        const float q0 = sq4::get_code(x, i);
        codes = vld1q_lane_f32(&q0, codes, 0);
        query = vld1q_lane_f32(y + i, query, 0);
        if constexpr (residual % 4 >= 2) {
            const float q1 = sq4::get_code(x, i + 1);
            codes = vld1q_lane_f32(&q1, codes, 1);
            query = vld1q_lane_f32(y + i + 1, query, 1);
        }
        if constexpr (residual % 4 >= 3) {
            const float q2 = sq4::get_code(x, i + 2);
            codes = vld1q_lane_f32(&q2, codes, 2);
            query = vld1q_lane_f32(y + i + 2, query, 2);
        }
        sum3 = vfmaq_f32(sum3, codes, query);
    }
    const float dot = vaddvq_f32(vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3)));
    const auto *params = x + sq4::data_bytes_count(dimension);
    const float min_val = load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + sq4::DELTA * sizeof(float));
    return 1.0f - (min_val * y[dimension + sq4::SUM_QUERY] + delta * dot);
}

} // namespace
