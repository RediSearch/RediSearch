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

__attribute__((always_inline)) static inline void
L2SquareOp(const uint8x16_t &v1, const uint8x16_t &v2, uint32x4_t &sum) {
    // Compute absolute differences and widen to 16-bit in one step
    uint16x8_t diff_low = vabdl_u8(vget_low_u8(v1), vget_low_u8(v2));
    uint16x8_t diff_high = vabdl_u8(vget_high_u8(v1), vget_high_u8(v2));

    // Square and accumulate the differences using vmlal_u16
    sum = vmlal_u16(sum, vget_low_u16(diff_low), vget_low_u16(diff_low));
    sum = vmlal_u16(sum, vget_high_u16(diff_low), vget_high_u16(diff_low));
    sum = vmlal_u16(sum, vget_low_u16(diff_high), vget_low_u16(diff_high));
    sum = vmlal_u16(sum, vget_high_u16(diff_high), vget_high_u16(diff_high));
}

__attribute__((always_inline)) static inline void L2SquareStep16(uint8_t *&pVect1, uint8_t *&pVect2,
                                                                 uint32x4_t &sum) {
    // Load 16 uint8 elements into NEON registers
    uint8x16_t v1 = vld1q_u8(pVect1);
    uint8x16_t v2 = vld1q_u8(pVect2);

    L2SquareOp(v1, v2, sum);

    pVect1 += 16;
    pVect2 += 16;
}

__attribute__((always_inline)) static inline void
L2SquareStep32(uint8_t *&pVect1, uint8_t *&pVect2, uint32x4_t &sum1, uint32x4_t &sum2) {
    uint8x16x2_t v1_pair = vld1q_u8_x2(pVect1);
    uint8x16x2_t v2_pair = vld1q_u8_x2(pVect2);

    // Reference the individual vectors
    uint8x16_t v1_first = v1_pair.val[0];
    uint8x16_t v1_second = v1_pair.val[1];
    uint8x16_t v2_first = v2_pair.val[0];
    uint8x16_t v2_second = v2_pair.val[1];

    L2SquareOp(v1_first, v2_first, sum1);
    L2SquareOp(v1_second, v2_second, sum2);

    pVect1 += 32;
    pVect2 += 32;
}

template <unsigned char residual> // 0..63
float UINT8_L2SqrSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    uint8_t *pVect1 = (uint8_t *)pVect1v;
    uint8_t *pVect2 = (uint8_t *)pVect2v;

    uint32x4_t sum0 = vdupq_n_u32(0);
    uint32x4_t sum1 = vdupq_n_u32(0);
    uint32x4_t sum2 = vdupq_n_u32(0);
    uint32x4_t sum3 = vdupq_n_u32(0);

    constexpr size_t final_residual = residual % 16;
    if constexpr (final_residual > 0) {
        constexpr uint8x16_t mask = {
            0xFF,
            (final_residual >= 2) ? 0xFF : 0,
            (final_residual >= 3) ? 0xFF : 0,
            (final_residual >= 4) ? 0xFF : 0,
            (final_residual >= 5) ? 0xFF : 0,
            (final_residual >= 6) ? 0xFF : 0,
            (final_residual >= 7) ? 0xFF : 0,
            (final_residual >= 8) ? 0xFF : 0,
            (final_residual >= 9) ? 0xFF : 0,
            (final_residual >= 10) ? 0xFF : 0,
            (final_residual >= 11) ? 0xFF : 0,
            (final_residual >= 12) ? 0xFF : 0,
            (final_residual >= 13) ? 0xFF : 0,
            (final_residual >= 14) ? 0xFF : 0,
            (final_residual >= 15) ? 0xFF : 0,
            0,
        };

        // Load data directly from input vectors
        uint8x16_t v1 = vld1q_u8(pVect1);
        uint8x16_t v2 = vld1q_u8(pVect2);

        // Zero vector for replacement
        uint8x16_t zeros = vdupq_n_u8(0);

        // Apply bit select to zero out irrelevant elements
        v1 = vbslq_u8(mask, v1, zeros);
        v2 = vbslq_u8(mask, v2, zeros);
        L2SquareOp(v1, v2, sum1);
        pVect1 += final_residual;
        pVect2 += final_residual;
    }

    // Process 64 elements at a time in the main loop
    size_t num_of_chunks = dimension / 64;

    for (size_t i = 0; i < num_of_chunks; i++) {
        L2SquareStep32(pVect1, pVect2, sum0, sum2);
        L2SquareStep32(pVect1, pVect2, sum1, sum3);
    }

    constexpr size_t num_of_32_chunks = residual / 32;

    if constexpr (num_of_32_chunks) {
        L2SquareStep32(pVect1, pVect2, sum0, sum1);
    }

    // Handle residual elements (0-63)
    // First, process full chunks of 16 elements
    constexpr size_t residual_chunks = (residual % 32) / 16;
    if constexpr (residual_chunks > 0) {
        L2SquareStep16(pVect1, pVect2, sum0);
    }

    // Horizontal sum of the 4 elements in the sum register to get final result
    uint32x4_t total_sum = vaddq_u32(sum0, sum1);

    total_sum = vaddq_u32(total_sum, sum2);
    total_sum = vaddq_u32(total_sum, sum3);

    // Horizontal sum of the 4 elements in the combined sum register
    int32_t result = vaddvq_u32(total_sum);

    // Return the L2 squared distance as a float
    return static_cast<float>(result);
}
