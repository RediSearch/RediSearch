/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <arm_sve.h>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

inline void L2Sqr_Step(const float16_t *vec1, const float16_t *vec2, svfloat16_t &acc,
                       size_t &offset, const size_t chunk) {
    svbool_t all = svptrue_b16();

    svfloat16_t v1 = svld1_f16(all, vec1 + offset);
    svfloat16_t v2 = svld1_f16(all, vec2 + offset);
    // Compute difference in half precision.
    svfloat16_t diff = svsub_f16_x(all, v1, v2);
    // Square the differences and accumulate
    acc = svmla_f16_x(all, acc, diff, diff);
    offset += chunk;
}

template <bool partial_chunk, unsigned char additional_steps> // [t/f, 0..3]
float FP16_L2Sqr_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *vec1 = static_cast<const float16_t *>(pVect1v);
    const auto *vec2 = static_cast<const float16_t *>(pVect2v);
    const size_t chunk = svcnth(); // number of 16-bit elements in a register
    svbool_t all = svptrue_b16();
    svfloat16_t acc1 = svdup_f16(0.0f);
    svfloat16_t acc2 = svdup_f16(0.0f);
    svfloat16_t acc3 = svdup_f16(0.0f);
    svfloat16_t acc4 = svdup_f16(0.0f);
    size_t offset = 0;

    // Process all full vectors
    const size_t full_iterations = dimension / chunk / 4;
    for (size_t iter = 0; iter < full_iterations; iter++) {
        L2Sqr_Step(vec1, vec2, acc1, offset, chunk);
        L2Sqr_Step(vec1, vec2, acc2, offset, chunk);
        L2Sqr_Step(vec1, vec2, acc3, offset, chunk);
        L2Sqr_Step(vec1, vec2, acc4, offset, chunk);
    }

    // Perform between 0 and 3 additional steps, according to `additional_steps` value
    if constexpr (additional_steps >= 1)
        L2Sqr_Step(vec1, vec2, acc1, offset, chunk);
    if constexpr (additional_steps >= 2)
        L2Sqr_Step(vec1, vec2, acc2, offset, chunk);
    if constexpr (additional_steps >= 3)
        L2Sqr_Step(vec1, vec2, acc3, offset, chunk);

    // Handle partial chunk, if needed
    if constexpr (partial_chunk) {
        svbool_t pg = svwhilelt_b16_u64(offset, dimension);

        // Load half-precision vectors.
        svfloat16_t v1 = svld1_f16(pg, vec1 + offset);
        svfloat16_t v2 = svld1_f16(pg, vec2 + offset);
        // Compute difference in half precision.
        svfloat16_t diff = svsub_f16_x(pg, v1, v2);
        // Square the differences.
        // Use `m` suffix to keep the inactive elements as they are in `acc`
        acc4 = svmla_f16_m(pg, acc4, diff, diff);
    }

    // Accumulate accumulators
    acc1 = svadd_f16_x(all, acc1, acc3);
    acc2 = svadd_f16_x(all, acc2, acc4);
    acc1 = svadd_f16_x(all, acc1, acc2);

    // Reduce the accumulated sum.
    float result = svaddv_f16(all, acc1);
    return result;
}
} // namespace
