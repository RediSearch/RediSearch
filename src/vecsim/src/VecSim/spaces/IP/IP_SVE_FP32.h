/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/spaces/space_includes.h"

#include <arm_sve.h>

// SVE.cpp and SVE2.cpp both compile this header, under different -march flags. The
// anonymous namespace keeps each tier's bodies to itself; without it they are weak
// symbols that both objects define and link order picks the -march. Only the Choose_*
// entry points stay external. Dependencies above must stay outside the namespace.
namespace {

static inline void InnerProductStep(float *&pVect1, float *&pVect2, size_t &offset,
                                    svfloat32_t &sum, const size_t chunk) {
    svfloat32_t v1 = svld1_f32(svptrue_b32(), pVect1 + offset);
    svfloat32_t v2 = svld1_f32(svptrue_b32(), pVect2 + offset);

    sum = svmla_f32_x(svptrue_b32(), sum, v1, v2);

    offset += chunk;
}

template <bool partial_chunk, unsigned char additional_steps>
float FP32_InnerProductSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float *pVect1 = (float *)pVect1v;
    float *pVect2 = (float *)pVect2v;
    size_t offset = 0;

    uint64_t chunk = svcntw();

    svfloat32_t sum0 = svdup_f32(0.0f);
    svfloat32_t sum1 = svdup_f32(0.0f);
    svfloat32_t sum2 = svdup_f32(0.0f);
    svfloat32_t sum3 = svdup_f32(0.0f);

    auto chunk_size = 4 * chunk;
    const size_t number_of_chunks = dimension / chunk_size;
    for (size_t i = 0; i < number_of_chunks; i++) {
        InnerProductStep(pVect1, pVect2, offset, sum0, chunk);
        InnerProductStep(pVect1, pVect2, offset, sum1, chunk);
        InnerProductStep(pVect1, pVect2, offset, sum2, chunk);
        InnerProductStep(pVect1, pVect2, offset, sum3, chunk);
    }

    // Process remaining complete SVE vectors that didn't fit into the main loop
    // These are full vector operations (0-3 elements)
    if constexpr (additional_steps > 0) {
        if constexpr (additional_steps >= 1) {
            InnerProductStep(pVect1, pVect2, offset, sum0, chunk);
        }
        if constexpr (additional_steps >= 2) {
            InnerProductStep(pVect1, pVect2, offset, sum1, chunk);
        }
        if constexpr (additional_steps >= 3) {
            InnerProductStep(pVect1, pVect2, offset, sum3, chunk);
        }
    }

    // Process final tail elements that don't form a complete vector
    // This section handles the case when dimension is not evenly divisible by SVE vector length
    if constexpr (partial_chunk) {
        // Create a predicate mask where each lane is active only for the remaining elements
        svbool_t pg =
            svwhilelt_b32(static_cast<uint64_t>(offset), static_cast<uint64_t>(dimension));

        // Load vectors with predication
        svfloat32_t v1 = svld1_f32(pg, pVect1 + offset);
        svfloat32_t v2 = svld1_f32(pg, pVect2 + offset);
        sum3 = svmla_f32_m(pg, sum3, v1, v2);
    }

    sum0 = svadd_f32_x(svptrue_b32(), sum0, sum1);
    sum2 = svadd_f32_x(svptrue_b32(), sum2, sum3);
    // Perform vector addition in parallel
    svfloat32_t sum_all = svadd_f32_x(svptrue_b32(), sum0, sum2);
    // Single horizontal reduction at the end
    float result = svaddv_f32(svptrue_b32(), sum_all);
    return 1.0f - result;
}
} // namespace
