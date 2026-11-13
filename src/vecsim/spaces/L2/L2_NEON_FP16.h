/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <arm_neon.h>

inline void L2Sqr_Step(const float16_t *&vec1, const float16_t *&vec2, float16x8_t &acc) {
    // Load half-precision vectors
    float16x8_t v1 = vld1q_f16(vec1);
    float16x8_t v2 = vld1q_f16(vec2);
    vec1 += 8;
    vec2 += 8;

    // Calculate differences
    float16x8_t diff = vsubq_f16(v1, v2);
    // Square and accumulate
    acc = vfmaq_f16(acc, diff, diff);
}

template <unsigned char residual> // 0..31
float FP16_L2Sqr_NEON_HP(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *vec1 = static_cast<const float16_t *>(pVect1v);
    const auto *vec2 = static_cast<const float16_t *>(pVect2v);
    const auto *const v1End = vec1 + dimension;
    float16x8_t acc1 = vdupq_n_f16(0.0f);
    float16x8_t acc2 = vdupq_n_f16(0.0f);
    float16x8_t acc3 = vdupq_n_f16(0.0f);
    float16x8_t acc4 = vdupq_n_f16(0.0f);

    // First, handle the partial chunk residual
    if constexpr (residual % 8) {
        auto constexpr chunk_residual = residual % 8;
        // TODO: spacial cases for some residuals and benchmark if its better
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
        float16x8_t v1 = vld1q_f16(vec1);
        float16x8_t v2 = vld1q_f16(vec2);

        // Apply mask to both vectors
        float16x8_t masked_v1 = vbslq_f16(mask, v1, acc1); // `acc1` should be all zeros here
        float16x8_t masked_v2 = vbslq_f16(mask, v2, acc2); // `acc2` should be all zeros here

        // Calculate differences
        float16x8_t diff = vsubq_f16(masked_v1, masked_v2);
        // Square and accumulate
        acc1 = vfmaq_f16(acc1, diff, diff);

        // Advance pointers
        vec1 += chunk_residual;
        vec2 += chunk_residual;
    }

    // Handle (residual - (residual % 8)) in chunks of 8 float16
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
    acc1 = vpaddq_f16(acc1, acc3);
    acc2 = vpaddq_f16(acc2, acc4);
    acc1 = vpaddq_f16(acc1, acc2);

    // Horizontal sum of the accumulated values
    float32x4_t sum_f32 = vcvt_f32_f16(vget_low_f16(acc1));
    sum_f32 = vaddq_f32(sum_f32, vcvt_f32_f16(vget_high_f16(acc1)));

    // Pairwise add to get horizontal sum
    float32x2_t sum_2 = vadd_f32(vget_low_f32(sum_f32), vget_high_f32(sum_f32));
    sum_2 = vpadd_f32(sum_2, sum_2);

    // Extract result
    return vget_lane_f32(sum_2, 0);
}
