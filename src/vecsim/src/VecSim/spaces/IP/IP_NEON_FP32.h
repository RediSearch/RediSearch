/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"
#include <arm_neon.h>

static inline void InnerProductStep(float *&pVect1, float *&pVect2, float32x4_t &sum) {
    float32x4_t v1 = vld1q_f32(pVect1);
    float32x4_t v2 = vld1q_f32(pVect2);
    sum = vmlaq_f32(sum, v1, v2);
    pVect1 += 4;
    pVect2 += 4;
}

template <unsigned char residual> // 0..15
float FP32_InnerProductSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;

    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);

    const size_t num_of_chunks = dimension / 16;

    for (size_t i = 0; i < num_of_chunks; i++) {
        InnerProductStep(pVect1, pVect2, sum0);
        InnerProductStep(pVect1, pVect2, sum1);
        InnerProductStep(pVect1, pVect2, sum2);
        InnerProductStep(pVect1, pVect2, sum3);
    }

    // Handle remaining complete 4-float blocks within residual
    constexpr size_t remaining_chunks = residual / 4;

    // Unrolled loop for the 4-float blocks
    if constexpr (remaining_chunks >= 1) {
        InnerProductStep(pVect1, pVect2, sum0);
    }
    if constexpr (remaining_chunks >= 2) {
        InnerProductStep(pVect1, pVect2, sum1);
    }
    if constexpr (remaining_chunks >= 3) {
        InnerProductStep(pVect1, pVect2, sum2);
    }

    // Handle final residual elements (0-3 elements)
    constexpr size_t final_residual = residual % 4;
    if constexpr (final_residual > 0) {
        float32x4_t v1 = vdupq_n_f32(0.0f);
        float32x4_t v2 = vdupq_n_f32(0.0f);

        if constexpr (final_residual >= 1) {
            v1 = vld1q_lane_f32(pVect1, v1, 0);
            v2 = vld1q_lane_f32(pVect2, v2, 0);
        }
        if constexpr (final_residual >= 2) {
            v1 = vld1q_lane_f32(pVect1 + 1, v1, 1);
            v2 = vld1q_lane_f32(pVect2 + 1, v2, 1);
        }
        if constexpr (final_residual >= 3) {
            v1 = vld1q_lane_f32(pVect1 + 2, v1, 2);
            v2 = vld1q_lane_f32(pVect2 + 2, v2, 2);
        }

        sum3 = vmlaq_f32(sum3, v1, v2);
    }

    // Combine all four sum accumulators
    float32x4_t sum_combined = vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3));

    // Horizontal sum of the 4 elements in the combined NEON register
    float32x2_t sum_halves = vadd_f32(vget_low_f32(sum_combined), vget_high_f32(sum_combined));
    float32x2_t summed = vpadd_f32(sum_halves, sum_halves);
    float sum = vget_lane_f32(summed, 0);

    return 1.0f - sum;
}
