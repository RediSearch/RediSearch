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
 * Optimized asymmetric SQ8 inner product using algebraic identity:
 *
 *   IP(x, y) = Σ(x_i * y_i)
 *            ≈ Σ((min + delta * q_i) * y_i)
 *            = min * Σy_i + delta * Σ(q_i * y_i)
 *            = min * y_sum + delta * quantized_dot_product
 *
 * where y_sum = Σy_i is precomputed and stored in the query blob.
 * This avoids dequantization in the hot loop - we only compute Σ(q_i * y_i).
 */

// Helper: compute Σ(q_i * y_i) for one SVE vector width (no dequantization)
// pVect1 = SQ8 storage (quantized values), pVect2 = FP32 query
static inline void InnerProductStepSQ8_FP32(const uint8_t *pVect1, const float *pVect2,
                                            size_t &offset, svfloat32_t &sum, const size_t chunk) {
    svbool_t pg = svptrue_b32();

    // Load uint8 elements and zero-extend to uint32
    svuint32_t v1_u32 = svld1ub_u32(pg, pVect1 + offset);

    // Convert uint32 to float32
    svfloat32_t v1_f = svcvt_f32_u32_x(pg, v1_u32);

    // Load float elements from query
    svfloat32_t v2 = svld1_f32(pg, pVect2 + offset);

    // Accumulate q_i * y_i (no dequantization!)
    sum = svmla_f32_x(pg, sum, v1_f, v2);

    offset += chunk;
}

// pVect1v = SQ8 storage, pVect2v = FP32 query
template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP32_InnerProductSIMD_SVE_IMP(const void *pVect1v, const void *pVect2v,
                                        size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const float *pVect2 = static_cast<const float *>(pVect2v);     // FP32 query
    size_t offset = 0;

    svbool_t pg = svptrue_b32();

    // Get the number of 32-bit elements per vector at runtime
    uint64_t chunk = svcntw();

    // Multiple accumulators for ILP
    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);

    // Handle partial chunk if needed
    if constexpr (partial_chunk) {
        size_t remaining = dimension % chunk;
        if (remaining > 0) {
            // Create predicate for the remaining elements
            svbool_t pg_partial =
                svwhilelt_b32(static_cast<uint32_t>(0), static_cast<uint32_t>(remaining));

            // Load uint8 elements and zero-extend to uint32
            svuint32_t v1_u32 = svld1ub_u32(pg_partial, pVect1 + offset);

            // Convert uint32 to float32
            svfloat32_t v1_f = svcvt_f32_u32_z(pg_partial, v1_u32);

            // Load float elements from query with predicate
            svfloat32_t v2 = svld1_f32(pg_partial, pVect2);

            // Compute q_i * y_i (no dequantization)
            sum0 = svmla_f32_z(pg_partial, sum0, v1_f, v2);

            offset += remaining;
        }
    }

    // Process 4 chunks at a time in the main loop
    auto chunk_size = 4 * chunk;
    const size_t number_of_chunks =
        (dimension - (partial_chunk ? dimension % chunk : 0)) / chunk_size;

    for (size_t i = 0; i < number_of_chunks; i++) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum0, chunk);
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum1, chunk);
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum2, chunk);
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum3, chunk);
    }

    // Handle remaining steps (0-3)
    if constexpr (additional_steps > 0) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum0, chunk);
    }
    if constexpr (additional_steps > 1) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum1, chunk);
    }
    if constexpr (additional_steps > 2) {
        InnerProductStepSQ8_FP32(pVect1, pVect2, offset, sum2, chunk);
    }

    // Combine the accumulators
    svfloat32_t sum = svadd_f32_z(pg, sum0, sum1);
    sum = svadd_f32_z(pg, sum, sum2);
    sum = svadd_f32_z(pg, sum, sum3);

    // Horizontal sum to get Σ(q_i * y_i)
    float quantized_dot = svaddv_f32(pg, sum);

    // Get quantization parameters from stored vector (after quantized data)
    const auto *params1 = pVect1 + dimension;
    const float min_val = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (stored after the dim floats)
    const float y_sum = pVect2[dimension + sq8::SUM_QUERY];

    // Apply the algebraic formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP32_InnerProductSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP32_InnerProductSIMD_SVE_IMP<partial_chunk, additional_steps>(
                      pVect1v, pVect2v, dimension);
}

template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP32_CosineSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    // Cosine distance = 1 - IP (vectors are pre-normalized)
    return SQ8_FP32_InnerProductSIMD_SVE<partial_chunk, additional_steps>(pVect1v, pVect2v,
                                                                          dimension);
}
} // namespace
