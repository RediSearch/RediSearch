/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <arm_neon.h>

// Assumes little-endianess
inline void L2Sqr_Op(float32x4_t &acc, bfloat16x8_t &v1, bfloat16x8_t &v2) {
    float32x4_t v1_lo = vcvtq_low_f32_bf16(v1);
    float32x4_t v2_lo = vcvtq_low_f32_bf16(v2);
    float32x4_t diff_lo = vsubq_f32(v1_lo, v2_lo);

    acc = vfmaq_f32(acc, diff_lo, diff_lo);

    float32x4_t v1_hi = vcvtq_high_f32_bf16(v1);
    float32x4_t v2_hi = vcvtq_high_f32_bf16(v2);
    float32x4_t diff_hi = vsubq_f32(v1_hi, v2_hi);

    acc = vfmaq_f32(acc, diff_hi, diff_hi);
}

inline void L2Sqr_Step(const bfloat16_t *&vec1, const bfloat16_t *&vec2, float32x4_t &acc) {
    // Load brain-half-precision vectors
    bfloat16x8_t v1 = vld1q_bf16(vec1);
    bfloat16x8_t v2 = vld1q_bf16(vec2);
    vec1 += 8;
    vec2 += 8;
    L2Sqr_Op(acc, v1, v2);
}

template <unsigned char residual> // 0..31
float BF16_L2Sqr_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *vec1 = static_cast<const bfloat16_t *>(pVect1v);
    const auto *vec2 = static_cast<const bfloat16_t *>(pVect2v);
    const auto *const v1End = vec1 + dimension;
    float32x4_t acc1 = vdupq_n_f32(0.0f);
    float32x4_t acc2 = vdupq_n_f32(0.0f);
    float32x4_t acc3 = vdupq_n_f32(0.0f);
    float32x4_t acc4 = vdupq_n_f32(0.0f);

    // First, handle the partial chunk residual
    if constexpr (residual % 8) {
        auto constexpr chunk_residual = residual % 8;
        // TODO: special cases for some residuals and benchmark if its better
        constexpr uint16x8_t mask = {
            0xFFFF,
            (chunk_residual >= 2) ? 0xFFFF : 0,
            (chunk_residual >= 3) ? 0xFFFF : 0,
            (chunk_residual >= 4) ? 0xFFFF : 0,
            (chunk_residual >= 5) ? 0xFFFF : 0,
            (chunk_residual >= 6) ? 0xFFFF : 0,
            (chunk_residual >= 7) ? 0xFFFF : 0,
            0,
        };

        // Load partial vectors
        bfloat16x8_t v1 = vld1q_bf16(vec1);
        bfloat16x8_t v2 = vld1q_bf16(vec2);

        // Apply mask to both vectors
        bfloat16x8_t masked_v1 =
            vreinterpretq_bf16_u16(vandq_u16(vreinterpretq_u16_bf16(v1), mask));
        bfloat16x8_t masked_v2 =
            vreinterpretq_bf16_u16(vandq_u16(vreinterpretq_u16_bf16(v2), mask));

        L2Sqr_Op(acc1, masked_v1, masked_v2);

        // Advance pointers
        vec1 += chunk_residual;
        vec2 += chunk_residual;
    }

    // Handle (residual - (residual % 8)) in chunks of 8 bfloat16
    if constexpr (residual >= 8)
        L2Sqr_Step(vec1, vec2, acc2);
    if constexpr (residual >= 16)
        L2Sqr_Step(vec1, vec2, acc3);
    if constexpr (residual >= 24)
        L2Sqr_Step(vec1, vec2, acc4);

    // Process the rest of the vectors (the full chunks part)
    while (vec1 < v1End) {
        // TODO: use `vld1q_f16_x4` for quad-loading?
        L2Sqr_Step(vec1, vec2, acc1);
        L2Sqr_Step(vec1, vec2, acc2);
        L2Sqr_Step(vec1, vec2, acc3);
        L2Sqr_Step(vec1, vec2, acc4);
    }

    // Accumulate accumulators
    acc1 = vpaddq_f32(acc1, acc3);
    acc2 = vpaddq_f32(acc2, acc4);
    acc1 = vpaddq_f32(acc1, acc2);

    // Pairwise add to get horizontal sum
    float32x2_t folded = vadd_f32(vget_low_f32(acc1), vget_high_f32(acc1));
    folded = vpadd_f32(folded, folded);

    // Extract result
    return vget_lane_f32(folded, 0);
}
