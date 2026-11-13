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
#include "VecSim/spaces/IP/IP_NEON_SQ4_SQ4.h" // SQ4TailNibbleMask
#include "VecSim/spaces/quantized_distance_helpers.h"
#include <arm_neon.h>

namespace {

/*
 * Symmetric SQ4-SQ4 inner product, as IP_NEON_SQ4_SQ4.h, with the code dot on vdot_u32: the low
 * nibbles and the high nibbles of both operands are each unpacked to bytes and dotted, which needs
 * no lane reordering because both operands share the packing. The uint32 lanes are exact up to
 * spaces::SQ4_MAX_EXACT_SIMD_DIM, which the dispatcher enforces.
 */
// Returns the raw inner product (not the distance); the L2 identity kernels share it.
template <unsigned char residual> // 0..31
float SQ4_SQ4_InnerProductSIMD32_NEON_DOTPROD_IMP(const void *pVect1v, const void *pVect2v,
                                                  size_t dimension) {
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    const uint8x16_t nibble = vdupq_n_u8(15);
    uint32x4_t sum0 = vdupq_n_u32(0);
    uint32x4_t sum1 = vdupq_n_u32(0);
    const size_t complete = dimension - residual;
    size_t i = 0;
    // One 16-byte load per operand covers 32 coordinates.
    for (; i < complete; i += 32) {
        const uint8x16_t xb = vld1q_u8(x + i / 2), yb = vld1q_u8(y + i / 2);
        sum0 = vdotq_u32(sum0, vandq_u8(xb, nibble), vandq_u8(yb, nibble));
        sum1 = vdotq_u32(sum1, vshrq_n_u8(xb, 4), vshrq_n_u8(yb, 4));
    }
    uint32x2_t tail = vdup_n_u32(0);
    if constexpr (residual >= 16) {
        const uint8x8_t xb = vld1_u8(x + i / 2), yb = vld1_u8(y + i / 2);
        tail = vdot_u32(tail, vand_u8(xb, vget_low_u8(nibble)), vand_u8(yb, vget_low_u8(nibble)));
        tail = vdot_u32(tail, vshr_n_u8(xb, 4), vshr_n_u8(yb, 4));
        i += 16;
    }
    if constexpr (residual % 16 > 0) {
        // One 8-byte load per operand from an even coordinate. The bytes past the last code are the
        // metadata that follows the codes in every stored blob (at least 12 bytes, so the load
        // stays inside it); masking x drops them and an odd dim's unused high nibble.
        constexpr unsigned rest = residual % 16;
        const uint8x8_t xb = vld1_u8(x + i / 2), yb = vld1_u8(y + i / 2);
        const uint8x8_t x_low = vand_u8(xb, vcreate_u8(SQ4TailNibbleMask(rest, false)));
        const uint8x8_t x_high =
            vand_u8(vshr_n_u8(xb, 4), vcreate_u8(SQ4TailNibbleMask(rest, true)));
        tail = vdot_u32(tail, x_low, vand_u8(yb, vget_low_u8(nibble)));
        tail = vdot_u32(tail, x_high, vshr_n_u8(yb, 4));
    }
    const uint32_t code_dot = vaddvq_u32(vaddq_u32(sum0, sum1)) + vaddv_u32(tail);
    return spaces::quantized::Stored_InnerProduct_FromCodeDot<vecsim_types::sq4>(
        x, y, dimension, static_cast<float>(code_dot));
}

template <unsigned char residual> // 0..31
float SQ4_SQ4_InnerProductSIMD32_NEON_DOTPROD(const void *pVect1v, const void *pVect2v,
                                              size_t dimension) {
    return 1.0f -
           SQ4_SQ4_InnerProductSIMD32_NEON_DOTPROD_IMP<residual>(pVect1v, pVect2v, dimension);
}

} // namespace
