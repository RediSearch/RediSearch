/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/spaces/space_includes.h"
#include <arm_sve.h>

namespace {

// SVE types are sizeless and cannot be array elements. Each candidate retains
// FP32_L2SqrSIMD_SVE's four accumulation chains and reduction tree for identical rounding.
#define GROUPED4_DECLARE_SUMS(N)                                                                   \
    svfloat32_t s##N##0 = svdup_f32(0), s##N##1 = svdup_f32(0), s##N##2 = svdup_f32(0),            \
                s##N##3 = svdup_f32(0)
#define GROUPED4_UPDATE(N, K, PG, SUBMODE, ACCUMODE)                                               \
    {                                                                                              \
        svfloat32_t x = svld1_f32(PG, p##N + offset);                                              \
        svfloat32_t diff = svsub_f32_##SUBMODE(PG, x, q);                                          \
        s##N##K = svmla_f32_##ACCUMODE(PG, s##N##K, diff, diff);                                   \
    }
#define GROUPED4_FULL_STEP(K)                                                                      \
    {                                                                                              \
        svfloat32_t q = svld1_f32(all, query + offset);                                            \
        GROUPED4_UPDATE(0, K, all, x, z);                                                          \
        GROUPED4_UPDATE(1, K, all, x, z);                                                          \
        GROUPED4_UPDATE(2, K, all, x, z);                                                          \
        GROUPED4_UPDATE(3, K, all, x, z);                                                          \
        offset += chunk;                                                                           \
    }
#define GROUPED4_REDUCE(N)                                                                         \
    out[N] = svaddv_f32(all, svadd_f32_x(all, svadd_f32_x(all, s##N##0, s##N##1),                  \
                                         svadd_f32_x(all, s##N##2, s##N##3)))

template <bool partial_chunk, unsigned char additional_steps>
void FP32_L2SqrSIMD_SVE_Grouped4(const float *p0, const float *p1, const float *p2, const float *p3,
                                 const float *query, size_t dimension, float *out) {
    const size_t chunk = svcntw();
    const svbool_t all = svptrue_b32();
    size_t offset = 0;
    GROUPED4_DECLARE_SUMS(0);
    GROUPED4_DECLARE_SUMS(1);
    GROUPED4_DECLARE_SUMS(2);
    GROUPED4_DECLARE_SUMS(3);
    for (size_t i = 0; i < dimension / (4 * chunk); ++i) {
        GROUPED4_FULL_STEP(0);
        GROUPED4_FULL_STEP(1);
        GROUPED4_FULL_STEP(2);
        GROUPED4_FULL_STEP(3);
    }
    if constexpr (additional_steps >= 1) {
        GROUPED4_FULL_STEP(0);
    }
    if constexpr (additional_steps >= 2) {
        GROUPED4_FULL_STEP(1);
    }
    if constexpr (additional_steps >= 3) {
        GROUPED4_FULL_STEP(2);
    }
    if constexpr (partial_chunk) {
        svbool_t pg = svwhilelt_b32(uint64_t(offset), uint64_t(dimension));
        svfloat32_t q = svld1_f32(pg, query + offset);
        GROUPED4_UPDATE(0, 3, pg, m, m);
        GROUPED4_UPDATE(1, 3, pg, m, m);
        GROUPED4_UPDATE(2, 3, pg, m, m);
        GROUPED4_UPDATE(3, 3, pg, m, m);
    }
    GROUPED4_REDUCE(0);
    GROUPED4_REDUCE(1);
    GROUPED4_REDUCE(2);
    GROUPED4_REDUCE(3);
}
#undef GROUPED4_DECLARE_SUMS
#undef GROUPED4_UPDATE
#undef GROUPED4_FULL_STEP
#undef GROUPED4_REDUCE

} // namespace
