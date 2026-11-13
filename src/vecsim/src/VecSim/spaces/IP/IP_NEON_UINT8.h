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
#include <arm_neon.h>

__attribute__((always_inline)) static inline void InnerProductOp(uint8x16_t &v1, uint8x16_t &v2,
                                                                 uint32x4_t &sum) {
    // Multiply and accumulate low 8 elements (first half)
    uint16x8_t prod_low = vmull_u8(vget_low_u8(v1), vget_low_u8(v2));

    // Multiply and accumulate high 8 elements (second half)
    uint16x8_t prod_high = vmull_u8(vget_high_u8(v1), vget_high_u8(v2));

    // Pairwise add adjacent elements to 32-bit accumulators
    sum = vpadalq_u16(sum, prod_low);
    sum = vpadalq_u16(sum, prod_high);
}

__attribute__((always_inline)) static inline void
InnerProductStep(uint8_t *&pVect1, uint8_t *&pVect2, uint32x4_t &sum) {
    // Load 16 uint8 elements (16 bytes) into NEON registers
    uint8x16_t v1 = vld1q_u8(pVect1);
    uint8x16_t v2 = vld1q_u8(pVect2);
    InnerProductOp(v1, v2, sum);

    pVect1 += 16;
    pVect2 += 16;
}

// static: the DOTPROD header defines this name too, with a different body.
template <unsigned char residual> // 0..63
static float UINT8_InnerProductImp(const void *pVect1v, const void *pVect2v, size_t dimension) {
    uint8_t *pVect1 = (uint8_t *)pVect1v;
    uint8_t *pVect2 = (uint8_t *)pVect2v;

    // Initialize multiple sum accumulators for better parallelism
    uint32x4_t sum0 = vdupq_n_u32(0);
    uint32x4_t sum1 = vdupq_n_u32(0);

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
        InnerProductOp(v1, v2, sum1);
        pVect1 += final_residual;
        pVect2 += final_residual;
    }

    // Process 64 elements at a time in the main loop
    const size_t num_of_chunks = dimension / 64;

    for (size_t i = 0; i < num_of_chunks; i++) {
        InnerProductStep(pVect1, pVect2, sum0);
        InnerProductStep(pVect1, pVect2, sum1);
        InnerProductStep(pVect1, pVect2, sum0);
        InnerProductStep(pVect1, pVect2, sum1);
    }

    constexpr size_t residual_chunks = residual / 16;

    if constexpr (residual_chunks > 0) {
        if constexpr (residual_chunks >= 1) {
            InnerProductStep(pVect1, pVect2, sum0);
        }
        if constexpr (residual_chunks >= 2) {
            InnerProductStep(pVect1, pVect2, sum1);
        }
        if constexpr (residual_chunks >= 3) {
            InnerProductStep(pVect1, pVect2, sum0);
        }
    }

    uint32x4_t total_sum = vaddq_u32(sum0, sum1);

    // Horizontal sum of the 4 elements in the combined sum register
    int32_t result = vaddvq_u32(total_sum);

    return static_cast<float>(result);
}

template <unsigned char residual> // 0..15
float UINT8_InnerProductSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - UINT8_InnerProductImp<residual>(pVect1v, pVect2v, dimension);
}

template <unsigned char residual> // 0..63
float UINT8_CosineSIMD_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float ip = UINT8_InnerProductImp<residual>(pVect1v, pVect2v, dimension);
    const float norm_v1 = load_unaligned<float>(static_cast<const uint8_t *>(pVect1v) + dimension);
    const float norm_v2 = load_unaligned<float>(static_cast<const uint8_t *>(pVect2v) + dimension);
    return 1.0f - ip / (norm_v1 * norm_v2);
}
