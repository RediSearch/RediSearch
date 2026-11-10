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
#include "VecSim/spaces/IP/IP_SVE_SQ4_FP32.h" // SQ4_FP32_SVE_CoordinatesPerStep
#include "VecSim/types/sq4.h"
#include "VecSim/utils/alignment.h"
#include <arm_sve.h>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

/*
 * Asymmetric SQ4-FP32 L2 squared distance via direct residual accumulation, as SQ8:
 *
 *   ||x - y||² = Σ(delta * q_i + (min - y_i))²
 *
 * Keeping min - y first (one fused multiply-add per coordinate) rounds at the scale of the
 * residual, not of min, when the data has a large common offset. Storage packs coordinate i into
 * byte i / 2, the low nibble for even i and the high nibble for odd i, followed by unaligned FP32
 * metadata.
 */

// Helper: accumulate Σ(diff_i²) for the 2 * svcntw() coordinates held in svcntw() packed bytes.
// The low nibbles are the even coordinates and the high nibbles the odd ones; a de-interleaving
// load splits the query the same way, so no lane reordering is needed.
static inline void L2StepSQ4_FP32_SVE(const uint8_t *x, const float *y, svfloat32_t &sum_even,
                                      svfloat32_t &sum_odd, svfloat32_t min_val_vec,
                                      svfloat32_t delta_vec) {
    const svbool_t pg = svptrue_b32();
    const svuint32_t packed = svld1ub_u32(pg, x);
    const svfloat32x2_t query = svld2_f32(pg, y);
    const svfloat32_t diff_even =
        svmla_f32_x(pg, svsub_f32_x(pg, min_val_vec, svget2_f32(query, 0)), delta_vec,
                    svcvt_f32_u32_x(pg, svand_n_u32_x(pg, packed, 15)));
    const svfloat32_t diff_odd =
        svmla_f32_x(pg, svsub_f32_x(pg, min_val_vec, svget2_f32(query, 1)), delta_vec,
                    svcvt_f32_u32_x(pg, svlsr_n_u32_x(pg, packed, 4)));
    sum_even = svmla_f32_x(pg, sum_even, diff_even, diff_even);
    sum_odd = svmla_f32_x(pg, sum_odd, diff_odd, diff_odd);
}

// Four accumulators, full steps first and a predicated tail last, as SQ8. Every full step is an
// even number of coordinates, so every packed load starts at a whole byte.
template <bool partial_chunk, unsigned char additional_steps>
float SQ4_FP32_L2SqrSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    using vecsim_types::sq4;
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const float *>(pVect2v);
    const size_t vl = svcntw(); // packed bytes per step
    const svbool_t pg = svptrue_b32();
    const auto *params = x + sq4::data_bytes_count(dimension);
    const float min_val = load_unaligned<float>(params + sq4::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + sq4::DELTA * sizeof(float));
    const svfloat32_t min_val_vec = svdup_f32(min_val);
    const svfloat32_t delta_vec = svdup_f32(delta);
    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);
    size_t offset = 0; // in packed bytes; the step's first coordinate is 2 * offset
    const size_t main_bytes = (dimension / (8 * vl)) * 4 * vl;
    for (; offset < main_bytes; offset += 4 * vl) {
        L2StepSQ4_FP32_SVE(x + offset, y + 2 * offset, sum0, sum1, min_val_vec, delta_vec);
        L2StepSQ4_FP32_SVE(x + offset + vl, y + 2 * (offset + vl), sum2, sum3, min_val_vec,
                           delta_vec);
        L2StepSQ4_FP32_SVE(x + offset + 2 * vl, y + 2 * (offset + 2 * vl), sum0, sum1, min_val_vec,
                           delta_vec);
        L2StepSQ4_FP32_SVE(x + offset + 3 * vl, y + 2 * (offset + 3 * vl), sum2, sum3, min_val_vec,
                           delta_vec);
    }
    if constexpr (additional_steps > 0) {
        L2StepSQ4_FP32_SVE(x + offset, y + 2 * offset, sum0, sum1, min_val_vec, delta_vec);
        offset += vl;
    }
    if constexpr (additional_steps > 1) {
        L2StepSQ4_FP32_SVE(x + offset, y + 2 * offset, sum2, sum3, min_val_vec, delta_vec);
        offset += vl;
    }
    if constexpr (additional_steps > 2) {
        L2StepSQ4_FP32_SVE(x + offset, y + 2 * offset, sum0, sum1, min_val_vec, delta_vec);
        offset += vl;
    }
    if constexpr (partial_chunk) {
        // As the IP tail: an odd tail's last query pair reads the first metadata float, which
        // odd_pg drops.
        const size_t remaining = dimension - 2 * offset;
        const svbool_t even_pg = svwhilelt_b32(uint64_t(0), uint64_t((remaining + 1) / 2));
        const svbool_t odd_pg = svwhilelt_b32(uint64_t(0), uint64_t(remaining / 2));
        const svuint32_t packed = svld1ub_u32(even_pg, x + offset);
        const svfloat32x2_t query = svld2_f32(even_pg, y + 2 * offset);
        const svfloat32_t diff_even =
            svmla_f32_x(pg, svsub_f32_x(pg, min_val_vec, svget2_f32(query, 0)), delta_vec,
                        svcvt_f32_u32_x(pg, svand_n_u32_x(pg, packed, 15)));
        const svfloat32_t diff_odd =
            svmla_f32_x(pg, svsub_f32_x(pg, min_val_vec, svget2_f32(query, 1)), delta_vec,
                        svcvt_f32_u32_x(pg, svlsr_n_u32_x(pg, packed, 4)));
        // Merging (_m): lanes outside the tail keep the running sums.
        sum3 = svmla_f32_m(even_pg, sum3, diff_even, diff_even);
        sum2 = svmla_f32_m(odd_pg, sum2, diff_odd, diff_odd);
    }
    svfloat32_t sum = svadd_f32_x(pg, sum0, sum1);
    sum = svadd_f32_x(pg, sum, sum2);
    sum = svadd_f32_x(pg, sum, sum3);
    return svaddv_f32(pg, sum);
}

} // namespace
