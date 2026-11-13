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
#include <arm_sve.h>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

using sq8 = vecsim_types::sq8;

/*
 * Asymmetric SQ8-FP32 L2 squared distance via direct residual accumulation:
 *
 *   ||x - y||² = Σ(dequant(x_i) - y_i)², where dequant(x_i) = min_val + delta * q_i
 */

// One SVE vector width of Σ(diff_i²). pVect1 = SQ8 storage, pVect2 = FP32 query.
static inline void L2StepSQ8_FP32_SVE(const uint8_t *pVect1, const float *pVect2, size_t offset,
                                      svfloat32_t &sum, svfloat32_t min_val_vec,
                                      svfloat32_t delta_vec) {
    svbool_t pg = svptrue_b32();

    svuint32_t v1_u32 = svld1ub_u32(pg, pVect1 + offset);
    svfloat32_t v1_f = svcvt_f32_u32_x(pg, v1_u32);
    svfloat32_t v2 = svld1_f32(pg, pVect2 + offset);

    // Subtract fused into the multiply-add; keeping min - y first is what preserves the residual.
    svfloat32_t min_minus_y = svsub_f32_x(pg, min_val_vec, v2);
    svfloat32_t diff = svmla_f32_x(pg, min_minus_y, delta_vec, v1_f);

    sum = svmla_f32_x(pg, sum, diff, diff);
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP32_L2SqrSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    size_t offset = 0;

    svbool_t pg = svptrue_b32();

    // Get the number of 32-bit elements per vector at runtime
    uint64_t chunk = svcntw();

    // Get quantization parameters from stored vector (after quantized data)
    const auto *params1 = pVect1 + dimension;
    const float min_val_scalar = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta_scalar = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const svfloat32_t min_val_vec = svdup_f32(min_val_scalar);
    const svfloat32_t delta_vec = svdup_f32(delta_scalar);

    // Multiple accumulators for ILP
    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);

    // Full vectors first, predicated tail last, bounds compared rather than divided: `chunk` is
    // runtime (svcntw), so `dimension % chunk` would emit a real udiv, and the chooser already
    // divided once to produce `partial_chunk`/`additional_steps`. Unlike the prefix-first sibling
    // SQ8 SVE kernels: on Graviton4 this shape is faster under SVE2 at every dimension and under
    // plain SVE only when the dimension is a multiple of the vector length (about 8% slower
    // otherwise); SVE2 wins dispatch when both tiers are present.
    // A bitmask cannot replace the modulo here: SVE vector length is a multiple of 128 bits,
    // not necessarily a power of two.
    const size_t chunk_size = 4 * chunk;
    for (; offset + chunk_size <= dimension; offset += chunk_size) {
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset, sum0, min_val_vec, delta_vec);
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset + chunk, sum1, min_val_vec, delta_vec);
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset + 2 * chunk, sum2, min_val_vec, delta_vec);
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset + 3 * chunk, sum3, min_val_vec, delta_vec);
    }

    // Handle remaining full-width steps (0-3), resolved at compile time.
    if constexpr (additional_steps > 0) {
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset, sum0, min_val_vec, delta_vec);
        offset += chunk;
    }
    if constexpr (additional_steps > 1) {
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset, sum1, min_val_vec, delta_vec);
        offset += chunk;
    }
    if constexpr (additional_steps > 2) {
        L2StepSQ8_FP32_SVE(pVect1, pVect2, offset, sum2, min_val_vec, delta_vec);
        offset += chunk;
    }

    // Predicated tail for the final partial vector.
    if constexpr (partial_chunk) {
        svbool_t pg_tail =
            svwhilelt_b32(static_cast<uint32_t>(offset), static_cast<uint32_t>(dimension));

        svuint32_t v1_u32 = svld1ub_u32(pg_tail, pVect1 + offset);
        svfloat32_t v1_f = svcvt_f32_u32_z(pg_tail, v1_u32);
        svfloat32_t v2 = svld1_f32(pg_tail, pVect2 + offset);

        svfloat32_t min_minus_y = svsub_f32_z(pg_tail, min_val_vec, v2);
        svfloat32_t diff = svmla_f32_z(pg_tail, min_minus_y, delta_vec, v1_f);

        // Merging (_m), not zeroing: sum3 already holds partial sums from the main loop, and _z
        // would zero every lane outside pg_tail. Only the accumulator needs this; the
        // temporaries above are fresh values.
        sum3 = svmla_f32_m(pg_tail, sum3, diff, diff);
    }

    // Combine the accumulators
    svfloat32_t sum = svadd_f32_z(pg, sum0, sum1);
    sum = svadd_f32_z(pg, sum, sum2);
    sum = svadd_f32_z(pg, sum, sum3);

    // Horizontal sum to get Σ(diff_i²)
    return svaddv_f32(pg, sum);
}
} // namespace
