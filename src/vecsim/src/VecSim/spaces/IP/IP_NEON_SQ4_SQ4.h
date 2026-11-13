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
#include "VecSim/types/sq4.h"
#include "VecSim/spaces/quantized_distance_helpers.h"
#include <arm_neon.h>

namespace {

/*
 * Symmetric SQ4-SQ4 inner product with the same identity as SQ8-SQ8:
 *
 *   IP(x, y) = min_x * sum_y + min_y * sum_x - dim * min_x * min_y
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *
 * Only the integer code dot Σ(qx_i * qy_i) is vectorized. Both operands pack coordinate i into
 * byte i / 2, the low nibble for even i and the high nibble for odd i, so the code dot is the sum
 * of the low-nibble products plus the sum of the high-nibble products and no lane reordering is
 * needed. The total is exact in uint32 up to spaces::SQ4_MAX_EXACT_SIMD_DIM, which the dispatcher
 * enforces. The packed codes are followed by unaligned FP32 metadata.
 */

// Byte-lane mask for a tail of `residual` (0..15) coordinates read as one 8-byte load from an even
// coordinate: byte j keeps its low nibble while coordinate 2j is in the tail (high == false) and
// its high nibble while coordinate 2j + 1 is (high == true). Each kept lane is 0x0f.
constexpr uint64_t SQ4TailNibbleMask(unsigned residual, bool high) {
    const unsigned kept = high ? residual / 2 : (residual + 1) / 2;
    uint64_t mask = 0;
    for (unsigned j = 0; j < kept; ++j) {
        mask |= uint64_t(0x0f) << (8 * j);
    }
    return mask;
}

// The masked tails here and in L2_NEON_SQ4_SQ4.h / IP_NEON_DOTPROD_SQ4_SQ4.h read up to 7 bytes of
// the metadata after the codes, so it must stay at least 8 bytes for both metrics.
static_assert(vecsim_types::sq4::storage_metadata_count<VecSimMetric_IP>() * sizeof(float) >= 8);
static_assert(vecsim_types::sq4::storage_metadata_count<VecSimMetric_L2>() * sizeof(float) >= 8);

// Code products of one 8-byte group (16 coordinates), added pairwise into four uint32 lanes. A
// byte's low and high nibble products are at most 225 each, so their sum fits a uint16 lane.
static inline uint32x4_t SQ4_SQ4_DotStep_NEON(uint32x4_t sum, uint8x8_t x_low, uint8x8_t x_high,
                                              uint8x8_t y) {
    const uint16x8_t low = vmull_u8(x_low, vand_u8(y, vdup_n_u8(15)));
    return vpadalq_u16(sum, vmlal_u8(low, x_high, vshr_n_u8(y, 4)));
}

// Returns the raw inner product (not the distance); the L2 identity kernels share it.
template <unsigned char residual> // 0..15
float SQ4_SQ4_InnerProductSIMD16_NEON_IMP(const void *pVect1v, const void *pVect2v,
                                          size_t dimension) {
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    const uint8x8_t nibble = vdup_n_u8(15);
    uint32x4_t sum0 = vdupq_n_u32(0);
    uint32x4_t sum1 = vdupq_n_u32(0);
    const size_t complete = dimension - residual;
    size_t i = 0;
    const uint8x16_t nibble16 = vdupq_n_u8(15);
    for (; i + 32 <= complete; i += 32) {
        // One 16-byte load per operand covers 32 coordinates; the nibble split runs on whole
        // registers and the widening multiplies take the low and high halves.
        const uint8x16_t xb = vld1q_u8(x + i / 2), yb = vld1q_u8(y + i / 2);
        const uint8x16_t xl = vandq_u8(xb, nibble16), xh = vshrq_n_u8(xb, 4);
        const uint8x16_t yl = vandq_u8(yb, nibble16), yh = vshrq_n_u8(yb, 4);
        const uint16x8_t lo =
            vmlal_u8(vmull_u8(vget_low_u8(xl), vget_low_u8(yl)), vget_low_u8(xh), vget_low_u8(yh));
        const uint16x8_t hi = vmlal_high_u8(vmull_high_u8(xl, yl), xh, yh);
        sum0 = vpadalq_u16(sum0, lo);
        sum1 = vpadalq_u16(sum1, hi);
    }
    if (i < complete) {
        const uint8x8_t x0 = vld1_u8(x + i / 2);
        sum0 =
            SQ4_SQ4_DotStep_NEON(sum0, vand_u8(x0, nibble), vshr_n_u8(x0, 4), vld1_u8(y + i / 2));
        i += 16;
    }
    if constexpr (residual > 0) {
        // One 8-byte load per operand from an even coordinate. The bytes past the last code are the
        // metadata that follows the codes in every stored blob (at least 12 bytes, so the load
        // stays inside it); masking x drops them and an odd dim's unused high nibble.
        const uint8x8_t xt = vld1_u8(x + i / 2);
        const uint8x8_t x_low = vand_u8(xt, vcreate_u8(SQ4TailNibbleMask(residual, false)));
        const uint8x8_t x_high =
            vand_u8(vshr_n_u8(xt, 4), vcreate_u8(SQ4TailNibbleMask(residual, true)));
        sum1 = SQ4_SQ4_DotStep_NEON(sum1, x_low, x_high, vld1_u8(y + i / 2));
    }
    const uint32_t code_dot = vaddvq_u32(vaddq_u32(sum0, sum1));
    return spaces::quantized::Stored_InnerProduct_FromCodeDot<vecsim_types::sq4>(
        x, y, dimension, static_cast<float>(code_dot));
}

template <unsigned char residual> // 0..15
float SQ4_SQ4_InnerProductSIMD16_NEON(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ4_SQ4_InnerProductSIMD16_NEON_IMP<residual>(pVect1v, pVect2v, dimension);
}

} // namespace
