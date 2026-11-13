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

inline void L2SquareStep(double *&pVect1, double *&pVect2, size_t &offset, svfloat64_t &sum,
                         const size_t chunk) {
    // Load vectors
    svfloat64_t v1 = svld1_f64(svptrue_b64(), pVect1 + offset);
    svfloat64_t v2 = svld1_f64(svptrue_b64(), pVect2 + offset);

    // Calculate difference between vectors
    svfloat64_t diff = svsub_f64_x(svptrue_b64(), v1, v2);

    // Square the difference and accumulate: sum += diff * diff
    sum = svmla_f64_x(svptrue_b64(), sum, diff, diff);

    // Advance pointers by the vector length
    offset += chunk;
}

template <bool partial_chunk, unsigned char additional_steps>
double FP64_L2SqrSIMD_SVE(const void *pVect1v, const void *pVect2v, size_t dimension) {
    double *pVect1 = (double *)pVect1v;
    double *pVect2 = (double *)pVect2v;
    const size_t chunk = svcntd();
    size_t offset = 0;

    // Multiple accumulators to increase instruction-level parallelism
    svfloat64_t sum0 = svdup_f64(0.0);
    svfloat64_t sum1 = svdup_f64(0.0);
    svfloat64_t sum2 = svdup_f64(0.0);
    svfloat64_t sum3 = svdup_f64(0.0);

    // Process vectors in chunks, with unrolling for better pipelining
    auto chunk_size = 4 * chunk;
    size_t number_of_chunks = dimension / chunk_size;
    for (size_t i = 0; i < number_of_chunks; ++i) {
        // Process 4 chunks with separate accumulators
        L2SquareStep(pVect1, pVect2, offset, sum0, chunk);
        L2SquareStep(pVect1, pVect2, offset, sum1, chunk);
        L2SquareStep(pVect1, pVect2, offset, sum2, chunk);
        L2SquareStep(pVect1, pVect2, offset, sum3, chunk);
    }

    if constexpr (additional_steps >= 1) {
        L2SquareStep(pVect1, pVect2, offset, sum0, chunk);
    }
    if constexpr (additional_steps >= 2) {
        L2SquareStep(pVect1, pVect2, offset, sum1, chunk);
    }
    if constexpr (additional_steps >= 3) {
        L2SquareStep(pVect1, pVect2, offset, sum2, chunk);
    }

    if constexpr (partial_chunk) {
        svbool_t pg =
            svwhilelt_b64(static_cast<uint64_t>(offset), static_cast<uint64_t>(dimension));

        // Load vectors with predication
        svfloat64_t v1 = svld1_f64(pg, pVect1 + offset);
        svfloat64_t v2 = svld1_f64(pg, pVect2 + offset);

        // Calculate difference with predication (corrected)
        svfloat64_t diff = svsub_f64_x(pg, v1, v2);

        // Square the difference and accumulate with predication
        sum3 = svmla_f64_m(pg, sum3, diff, diff);
    }

    // Combine the partial sums
    sum0 = svadd_f64_x(svptrue_b64(), sum0, sum1);
    sum2 = svadd_f64_x(svptrue_b64(), sum2, sum3);
    svfloat64_t sum_all = svadd_f64_x(svptrue_b64(), sum0, sum2);
    double result = svaddv_f64(svptrue_b64(), sum_all);
    return result;
}
} // namespace
