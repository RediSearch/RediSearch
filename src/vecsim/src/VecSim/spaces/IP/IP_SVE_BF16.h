/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <arm_sve.h>

inline void InnerProduct_Step(const bfloat16_t *vec1, const bfloat16_t *vec2, svfloat32_t &acc,
                              size_t &offset, const size_t chunk) {
    svbool_t all = svptrue_b16();

    // Load brain-half-precision vectors.
    svbfloat16_t v1 = svld1_bf16(all, vec1 + offset);
    svbfloat16_t v2 = svld1_bf16(all, vec2 + offset);
    // Compute multiplications and add to the accumulator
    acc = svbfdot(acc, v1, v2);

    // Move to next chunk
    offset += chunk;
}

template <bool partial_chunk, unsigned char additional_steps> // [t/f, 0..3]
float BF16_InnerProduct_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *vec1 = static_cast<const bfloat16_t *>(pVect1v);
    const auto *vec2 = static_cast<const bfloat16_t *>(pVect2v);
    const size_t chunk = svcnth(); // number of 16-bit elements in a register
    svfloat32_t acc1 = svdup_f32(0.0f);
    svfloat32_t acc2 = svdup_f32(0.0f);
    svfloat32_t acc3 = svdup_f32(0.0f);
    svfloat32_t acc4 = svdup_f32(0.0f);
    size_t offset = 0;

    // Process all full vectors
    const size_t full_iterations = dimension / chunk / 4;
    for (size_t iter = 0; iter < full_iterations; iter++) {
        InnerProduct_Step(vec1, vec2, acc1, offset, chunk);
        InnerProduct_Step(vec1, vec2, acc2, offset, chunk);
        InnerProduct_Step(vec1, vec2, acc3, offset, chunk);
        InnerProduct_Step(vec1, vec2, acc4, offset, chunk);
    }

    // Perform between 0 and 3 additional steps, according to `additional_steps` value
    if constexpr (additional_steps >= 1)
        InnerProduct_Step(vec1, vec2, acc1, offset, chunk);
    if constexpr (additional_steps >= 2)
        InnerProduct_Step(vec1, vec2, acc2, offset, chunk);
    if constexpr (additional_steps >= 3)
        InnerProduct_Step(vec1, vec2, acc3, offset, chunk);

    // Handle the tail with the residual predicate
    if constexpr (partial_chunk) {
        svbool_t pg = svwhilelt_b16_u64(offset, dimension);

        // Load brain-half-precision vectors.
        // Inactive elements are zeros, according to the docs
        svbfloat16_t v1 = svld1_bf16(pg, vec1 + offset);
        svbfloat16_t v2 = svld1_bf16(pg, vec2 + offset);
        // Compute multiplications and add to the accumulator.
        acc4 = svbfdot(acc4, v1, v2);
    }

    // Accumulate accumulators
    acc1 = svadd_f32_x(svptrue_b32(), acc1, acc3);
    acc2 = svadd_f32_x(svptrue_b32(), acc2, acc4);
    acc1 = svadd_f32_x(svptrue_b32(), acc1, acc2);

    // Reduce the accumulated sum.
    float result = svaddv_f32(svptrue_b32(), acc1);
    return 1.0f - result;
}
