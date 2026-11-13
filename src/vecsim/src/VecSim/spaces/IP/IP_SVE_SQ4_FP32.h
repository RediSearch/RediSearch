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
#include <arm_sve.h>
#include <cmath>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

/*
 * Asymmetric SQ4 inner product with the same identity as SQ8:
 *
 *   IP(x, y) ≈ Σ((min + delta * q_i) * y_i) = min * y_sum + delta * Σ(q_i * y_i)
 *
 * Storage packs coordinate i into byte i / 2, the low nibble for even i and the high nibble for
 * odd i, followed by unaligned FP32 metadata. The query blob carries y_sum after its dim floats.
 */

// Coordinates per step: one u32 lane per packed byte, two coordinates per lane.
static inline size_t SQ4_FP32_SVE_CoordinatesPerStep() { return 2 * svcntw(); }

// Helper: accumulate Σ(q_i * y_i) for the 2 * svcntw() coordinates held in svcntw() packed bytes
// (no dequantization). The low nibbles are the even coordinates and the high nibbles the odd ones;
// a de-interleaving load splits the query the same way, so no lane reordering is needed.
static inline void InnerProductStepSQ4_FP32(const uint8_t *x, const float *y, svfloat32_t &sum_even,
                                            svfloat32_t &sum_odd) {
    const svbool_t pg = svptrue_b32();
    const svuint32_t packed = svld1ub_u32(pg, x);
    const svfloat32x2_t query = svld2_f32(pg, y);
    sum_even = svmla_f32_x(pg, sum_even, svcvt_f32_u32_x(pg, svand_n_u32_x(pg, packed, 15)),
                           svget2_f32(query, 0));
    sum_odd = svmla_f32_x(pg, sum_odd, svcvt_f32_u32_x(pg, svlsr_n_u32_x(pg, packed, 4)),
                          svget2_f32(query, 1));
}

// Four accumulators and one final affine correction, as in SQ8. Full steps precede the tail, so
// every packed load starts at an even coordinate (a whole byte).
template <bool partial_chunk, unsigned char additional_steps>
float SQ4_FP32_InnerProductSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    const size_t vl = svcntw(); // packed bytes per step
    const svbool_t pg = svptrue_b32();
    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);
    size_t offset = 0; // in packed bytes; the step's first coordinate is 2 * offset
    const size_t main_bytes = (dimension / (8 * vl)) * 4 * vl;
    for (; offset < main_bytes; offset += 4 * vl) {
        InnerProductStepSQ4_FP32(x + offset, y + 2 * offset, sum0, sum1);
        InnerProductStepSQ4_FP32(x + offset + vl, y + 2 * (offset + vl), sum2, sum3);
        InnerProductStepSQ4_FP32(x + offset + 2 * vl, y + 2 * (offset + 2 * vl), sum0, sum1);
        InnerProductStepSQ4_FP32(x + offset + 3 * vl, y + 2 * (offset + 3 * vl), sum2, sum3);
    }
    if constexpr (additional_steps > 0) {
        InnerProductStepSQ4_FP32(x + offset, y + 2 * offset, sum0, sum1);
        offset += vl;
    }
    if constexpr (additional_steps > 1) {
        InnerProductStepSQ4_FP32(x + offset, y + 2 * offset, sum2, sum3);
        offset += vl;
    }
    if constexpr (additional_steps > 2) {
        InnerProductStepSQ4_FP32(x + offset, y + 2 * offset, sum0, sum1);
        offset += vl;
    }
    if constexpr (partial_chunk) {
        // (remaining + 1) / 2 bytes hold the tail. Their even coordinates are all in it, their odd
        // ones only for the first remaining / 2 bytes (an odd dim's last high nibble is padding).
        // The structure load reads y in pairs, so an odd tail's last pair reads the first query
        // metadata float, which odd_pg then drops.
        const size_t remaining = dimension - 2 * offset;
        const svbool_t even_pg = svwhilelt_b32(uint64_t(0), uint64_t((remaining + 1) / 2));
        const svbool_t odd_pg = svwhilelt_b32(uint64_t(0), uint64_t(remaining / 2));
        const svuint32_t packed = svld1ub_u32(even_pg, x + offset);
        const svfloat32x2_t query = svld2_f32(even_pg, y + 2 * offset);
        sum0 = svmla_f32_m(even_pg, sum0, svcvt_f32_u32_x(pg, svand_n_u32_x(pg, packed, 15)),
                           svget2_f32(query, 0));
        sum1 = svmla_f32_m(odd_pg, sum1, svcvt_f32_u32_x(pg, svlsr_n_u32_x(pg, packed, 4)),
                           svget2_f32(query, 1));
    }
    svfloat32_t sum = svadd_f32_x(pg, sum0, sum1);
    sum = svadd_f32_x(pg, sum, sum2);
    sum = svadd_f32_x(pg, sum, sum3);
    const float dot = svaddv_f32(pg, sum);
    const auto *params = x + sq4::data_bytes_count(dimension);
    const float min_val = load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + sq4::DELTA * sizeof(float));
    return 1.0f - (min_val * y[dimension + sq4::SUM_QUERY] + delta * dot);
}

} // namespace
