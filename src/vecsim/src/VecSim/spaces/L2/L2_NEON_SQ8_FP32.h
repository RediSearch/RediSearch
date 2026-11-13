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
#include "VecSim/types/sq8.h"
#include <arm_neon.h>

using sq8 = vecsim_types::sq8;

/*
 * Asymmetric SQ8-FP32 L2 squared distance via direct residual accumulation:
 *
 *   ||x - y||² = Σ(dequant(x_i) - y_i)², where dequant(x_i) = min_val + delta * q_i
 */

// 4 elements of Σ(diff_i²). Used for the tail; the main loop uses the 16-element form below.
static inline void L2StepSQ8_FP32_NEON(const uint8_t *&pVect1, const float *&pVect2,
                                       float32x4_t &sum, float32x4_t min_val_vec,
                                       float32x4_t delta_vec) {
    uint8x8_t v1_u8 = vld1_u8(pVect1);
    pVect1 += 4;

    uint32x4_t v1_u32 = vmovl_u16(vget_low_u16(vmovl_u8(v1_u8)));
    float32x4_t v1_f = vcvtq_f32_u32(v1_u32);

    float32x4_t v2 = vld1q_f32(pVect2);
    pVect2 += 4;

    // Explicit vfmaq_f32, not vmlaq_f32: keeping min - y first is what preserves the residual.
    float32x4_t min_minus_y = vsubq_f32(min_val_vec, v2);
    float32x4_t diff = vfmaq_f32(min_minus_y, delta_vec, v1_f);

    sum = vfmaq_f32(sum, diff, diff);
}

// 16 elements off a single 16-byte load. The 4-element form reads 8 bytes and uses only 4, so
// back-to-back calls re-read half of every load. Per-lane arithmetic and accumulator mapping are
// unchanged, so results match the 4-element path exactly.
static inline void L2Step16SQ8_FP32_NEON(const uint8_t *&pVect1, const float *&pVect2,
                                         float32x4_t &sum0, float32x4_t &sum1, float32x4_t &sum2,
                                         float32x4_t &sum3, float32x4_t min_val_vec,
                                         float32x4_t delta_vec) {
    uint8x16_t v1_u8 = vld1q_u8(pVect1);
    pVect1 += 16;

    const uint16x8_t wide_lo = vmovl_u8(vget_low_u8(v1_u8));
    const uint16x8_t wide_hi = vmovl_u8(vget_high_u8(v1_u8));

    const float32x4_t q0 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(wide_lo)));
    const float32x4_t q1 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(wide_lo)));
    const float32x4_t q2 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(wide_hi)));
    const float32x4_t q3 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(wide_hi)));

    const float32x4_t y0 = vld1q_f32(pVect2);
    const float32x4_t y1 = vld1q_f32(pVect2 + 4);
    const float32x4_t y2 = vld1q_f32(pVect2 + 8);
    const float32x4_t y3 = vld1q_f32(pVect2 + 12);
    pVect2 += 16;

    const float32x4_t d0 = vfmaq_f32(vsubq_f32(min_val_vec, y0), delta_vec, q0);
    const float32x4_t d1 = vfmaq_f32(vsubq_f32(min_val_vec, y1), delta_vec, q1);
    const float32x4_t d2 = vfmaq_f32(vsubq_f32(min_val_vec, y2), delta_vec, q2);
    const float32x4_t d3 = vfmaq_f32(vsubq_f32(min_val_vec, y3), delta_vec, q3);

    sum0 = vfmaq_f32(sum0, d0, d0);
    sum1 = vfmaq_f32(sum1, d1, d1);
    sum2 = vfmaq_f32(sum2, d2, d2);
    sum3 = vfmaq_f32(sum3, d3, d3);
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <unsigned char residual> // 0..15
float SQ8_FP32_L2SqrSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query

    // Get quantization parameters from stored vector (after quantized data)
    const uint8_t *pVect1Base = static_cast<const uint8_t *>(pVect1v);
    const auto *params1 = pVect1Base + dimension;
    const float min_val_scalar = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta_scalar = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const float32x4_t min_val_vec = vdupq_n_f32(min_val_scalar);
    const float32x4_t delta_vec = vdupq_n_f32(delta_scalar);

    // Multiple accumulators for ILP
    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);

    const size_t num_of_chunks = dimension / 16;

    // Process 16 elements at a time in the main loop, one 16-byte load per iteration.
    for (size_t i = 0; i < num_of_chunks; i++) {
        L2Step16SQ8_FP32_NEON(pVect1, pVect2, sum0, sum1, sum2, sum3, min_val_vec, delta_vec);
    }

    // Handle remaining complete 4-element blocks within residual
    if constexpr (residual >= 4) {
        L2StepSQ8_FP32_NEON(pVect1, pVect2, sum0, min_val_vec, delta_vec);
    }
    if constexpr (residual >= 8) {
        L2StepSQ8_FP32_NEON(pVect1, pVect2, sum1, min_val_vec, delta_vec);
    }
    if constexpr (residual >= 12) {
        L2StepSQ8_FP32_NEON(pVect1, pVect2, sum2, min_val_vec, delta_vec);
    }

    // Handle final residual elements (0-3 elements)
    constexpr size_t final_residual = residual % 4;
    if constexpr (final_residual > 0) {
        // Padding lanes get q=0, y=min_val_scalar, so diff = delta*0 + (min - min) = 0.
        float32x4_t v1_f = vdupq_n_f32(0.0f);
        float32x4_t v2 = vdupq_n_f32(min_val_scalar);

        if constexpr (final_residual >= 1) {
            float q0 = static_cast<float>(pVect1[0]);
            v1_f = vld1q_lane_f32(&q0, v1_f, 0);
            v2 = vld1q_lane_f32(pVect2, v2, 0);
        }
        if constexpr (final_residual >= 2) {
            float q1 = static_cast<float>(pVect1[1]);
            v1_f = vld1q_lane_f32(&q1, v1_f, 1);
            v2 = vld1q_lane_f32(pVect2 + 1, v2, 1);
        }
        if constexpr (final_residual >= 3) {
            float q2 = static_cast<float>(pVect1[2]);
            v1_f = vld1q_lane_f32(&q2, v1_f, 2);
            v2 = vld1q_lane_f32(pVect2 + 2, v2, 2);
        }

        float32x4_t min_minus_y = vsubq_f32(min_val_vec, v2);
        float32x4_t diff = vfmaq_f32(min_minus_y, delta_vec, v1_f);
        sum3 = vfmaq_f32(sum3, diff, diff);
    }

    // Combine all four sum accumulators
    float32x4_t sum_combined = vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3));

    // Horizontal sum to get Σ(diff_i²)
    float32x2_t sum_halves = vadd_f32(vget_low_f32(sum_combined), vget_high_f32(sum_combined));
    float32x2_t summed = vpadd_f32(sum_halves, sum_halves);
    return vget_lane_f32(summed, 0);
}
