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
#include <arm_sve.h>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

/*
 * Symmetric SQ4-SQ4 inner product with the same identity as SQ8-SQ8:
 *
 *   IP(x, y) = min_x * sum_y + min_y * sum_x - dim * min_x * min_y
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *
 * Only the integer code dot Σ(qx_i * qy_i) is vectorized, with svdot_u32 on the low nibbles and on
 * the high nibbles unpacked to bytes. Both operands pack coordinate i into byte i / 2 (the low
 * nibble for even i), so no lane reordering is needed. The uint32 lanes are exact up to
 * spaces::SQ4_MAX_EXACT_SIMD_DIM, which the dispatcher enforces.
 */

// Coordinates per SVE vector of packed bytes: the chunk CHOOSE_SVE_IMPLEMENTATION steps by.
static inline size_t SQ4_SQ4_SVE_CoordinatesPerVector() { return 2 * svcntb(); }

// Code dot of one full vector of packed bytes.
static inline void InnerProductStepSQ4_SQ4_SVE(const uint8_t *x, const uint8_t *y,
                                               svuint32_t &sum) {
    const svbool_t pg = svptrue_b8();
    const svuint8_t xb = svld1_u8(pg, x), yb = svld1_u8(pg, y);
    sum = svdot_u32(sum, svand_n_u8_x(pg, xb, 15), svand_n_u8_x(pg, yb, 15));
    sum = svdot_u32(sum, svlsr_n_u8_x(pg, xb, 4), svlsr_n_u8_x(pg, yb, 4));
}

// Returns the raw inner product (not the distance); the L2 identity kernels share it.
template <bool partial_chunk, unsigned char additional_steps>
float SQ4_SQ4_InnerProductSIMD_SVE_IMP(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    const size_t vl = svcntb();
    const size_t chunk = 2 * vl;
    svuint32_t sum0 = svdup_u32(0);
    svuint32_t sum1 = svdup_u32(0);
    svuint32_t sum2 = svdup_u32(0);
    svuint32_t sum3 = svdup_u32(0);
    size_t offset = 0; // in coordinates; always even before the tail, so x + offset / 2 is a byte
    // Bounds compared rather than divided, as in the SQ8 kernels: chunk is a runtime value.
    for (; offset + 4 * chunk <= dimension; offset += 4 * chunk) {
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2, y + offset / 2, sum0);
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2 + vl, y + offset / 2 + vl, sum1);
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2 + 2 * vl, y + offset / 2 + 2 * vl, sum2);
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2 + 3 * vl, y + offset / 2 + 3 * vl, sum3);
    }
    if constexpr (additional_steps > 0) {
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2, y + offset / 2, sum0);
        offset += chunk;
    }
    if constexpr (additional_steps > 1) {
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2, y + offset / 2, sum1);
        offset += chunk;
    }
    if constexpr (additional_steps > 2) {
        InnerProductStepSQ4_SQ4_SVE(x + offset / 2, y + offset / 2, sum2);
        offset += chunk;
    }
    if constexpr (partial_chunk) {
        const size_t remaining = dimension - offset;
        // The bytes holding any remaining coordinate, and those whose high nibble is one too: an
        // odd dim's last byte carries an unused high nibble. Loads zero the inactive lanes.
        const svbool_t pg = svptrue_b8();
        const svbool_t bytes_pg = svwhilelt_b8(uint64_t(0), uint64_t((remaining + 1) / 2));
        const svbool_t high_pg = svwhilelt_b8(uint64_t(0), uint64_t(remaining / 2));
        const svuint8_t xb = svld1_u8(bytes_pg, x + offset / 2);
        const svuint8_t yb = svld1_u8(bytes_pg, y + offset / 2);
        sum3 = svdot_u32(sum3, svand_n_u8_x(pg, xb, 15), svand_n_u8_x(pg, yb, 15));
        sum3 = svdot_u32(sum3, svlsr_n_u8_z(high_pg, xb, 4), svlsr_n_u8_x(pg, yb, 4));
    }
    const svbool_t pg32 = svptrue_b32();
    svuint32_t sum = svadd_u32_x(pg32, sum0, sum1);
    sum = svadd_u32_x(pg32, sum, sum2);
    sum = svadd_u32_x(pg32, sum, sum3);
    const float code_dot = static_cast<float>(svaddv_u32(pg32, sum));

    return spaces::quantized::Stored_InnerProduct_FromCodeDot<vecsim_types::sq4>(x, y, dimension,
                                                                                 code_dot);
}

template <bool partial_chunk, unsigned char additional_steps>
float SQ4_SQ4_InnerProductSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ4_SQ4_InnerProductSIMD_SVE_IMP<partial_chunk, additional_steps>(
                      pVect1v, pVect2v, dimension);
}

} // namespace
