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
#include "VecSim/types/float16.h"
#include <arm_sve.h>

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

/*
 * SVE2 asymmetric SQ8 (storage) <-> FP16 (query) inner product using the identity:
 *   IP(x, y) ~= min * y_sum + delta * Σ(q_i * y_i)
 *
 * SVE2-only fast path: the storage bytes (0..255, exact in FP16) and the FP16 query
 * lanes stay 16-bit, and the FP16->FP32 widening multiply-accumulate is done by the
 * FMLALB/FMLALT pair (svmlalb_f32 / svmlalt_f32). Each pair widens the even/odd
 * half-precision lanes to single precision and multiplies/accumulates in FP32 WITHOUT
 * intermediate rounding, so the per-lane products match the SVE svmla path exactly while
 * processing svcnth() lanes per step (twice the base-SVE svcntw() granularity) and halving
 * the number of loads and explicit conversions. The even/odd accumulator split groups the
 * FP32 additions differently than the base SVE kernel, so the reduced result is numerically
 * equivalent (well within the test tolerance) rather than bit-identical.
 */

// Helper: one svcnth()-wide FP16 step feeding an even/odd FP32 accumulator pair.
static inline void SQ8_FP16_InnerProductStep_SVE2(const uint8_t *pVect1, const float16 *pVect2,
                                                  size_t &offset, svfloat32_t &sum_even,
                                                  svfloat32_t &sum_odd, svbool_t pg16,
                                                  size_t chunk) {
    svuint16_t v1_u16 = svld1ub_u16(pg16, pVect1 + offset);
    svfloat16_t v1_f16 = svcvt_f16_u16_x(pg16, v1_u16);
    svfloat16_t q_f16 = svld1_f16(pg16, reinterpret_cast<const float16_t *>(pVect2 + offset));
    // FMLALB/FMLALT are unpredicated; inactive lanes were zeroed by the loads above so
    // their contribution is 0 and walking all lanes is safe.
    sum_even = svmlalb_f32(sum_even, v1_f16, q_f16);
    sum_odd = svmlalt_f32(sum_odd, v1_f16, q_f16);
    offset += chunk;
}

// pVect1v = SQ8 storage, pVect2v = FP16 query. Precondition: dim >= 16 (enforced by dispatcher).
template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP16_InnerProductSIMD_SVE2_IMP(const void *pVect1v, const void *pVect2v,
                                         size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const float16 *pVect2 = static_cast<const float16 *>(pVect2v);
    size_t offset = 0;
    const svbool_t pg16 = svptrue_b16();
    const size_t chunk = svcnth();

    svfloat32_t sum0e = svdup_f32(0.0f), sum0o = svdup_f32(0.0f);
    svfloat32_t sum1e = svdup_f32(0.0f), sum1o = svdup_f32(0.0f);
    svfloat32_t sum2e = svdup_f32(0.0f), sum2o = svdup_f32(0.0f);
    svfloat32_t sum3e = svdup_f32(0.0f), sum3o = svdup_f32(0.0f);

    // Partial chunk for dim % chunk FP16 lanes. Zeroing loads (_z convert) leave inactive
    // lanes at 0 so the unpredicated FMLALB/FMLALT below ignore them.
    if constexpr (partial_chunk) {
        size_t remaining = dimension % chunk;
        if (remaining > 0) {
            svbool_t pg_partial = svwhilelt_b16(uint64_t(0), uint64_t(remaining));
            svuint16_t v1_u16 = svld1ub_u16(pg_partial, pVect1 + offset);
            svfloat16_t v1_f16 = svcvt_f16_u16_z(pg_partial, v1_u16);
            svfloat16_t q_f16 =
                svld1_f16(pg_partial, reinterpret_cast<const float16_t *>(pVect2 + offset));
            sum0e = svmlalb_f32(sum0e, v1_f16, q_f16);
            sum0o = svmlalt_f32(sum0o, v1_f16, q_f16);
            offset += remaining;
        }
    }

    // Main loop: 4 steps per iteration, one even/odd accumulator pair per step.
    const size_t chunk_size = 4 * chunk;
    const size_t number_of_chunks =
        (dimension - (partial_chunk ? dimension % chunk : 0)) / chunk_size;
    for (size_t i = 0; i < number_of_chunks; i++) {
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum0e, sum0o, pg16, chunk);
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum1e, sum1o, pg16, chunk);
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum2e, sum2o, pg16, chunk);
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum3e, sum3o, pg16, chunk);
    }

    if constexpr (additional_steps > 0)
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum0e, sum0o, pg16, chunk);
    if constexpr (additional_steps > 1)
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum1e, sum1o, pg16, chunk);
    if constexpr (additional_steps > 2)
        SQ8_FP16_InnerProductStep_SVE2(pVect1, pVect2, offset, sum2e, sum2o, pg16, chunk);

    const svbool_t pg32 = svptrue_b32();
    svfloat32_t sum = svadd_f32_z(pg32, sum0e, sum0o);
    sum = svadd_f32_x(pg32, sum, svadd_f32_x(pg32, sum1e, sum1o));
    sum = svadd_f32_x(pg32, sum, svadd_f32_x(pg32, sum2e, sum2o));
    sum = svadd_f32_x(pg32, sum, svadd_f32_x(pg32, sum3e, sum3o));
    float quantized_dot = svaddv_f32(pg32, sum);

    const uint8_t *params_bytes = static_cast<const uint8_t *>(pVect1v) + dimension;
    const float min_val = load_unaligned<float>(params_bytes + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params_bytes + sq8::DELTA * sizeof(float));
    const uint8_t *query_meta_bytes =
        reinterpret_cast<const uint8_t *>(static_cast<const float16 *>(pVect2v) + dimension);
    const float y_sum = load_unaligned<float>(query_meta_bytes + sq8::SUM_QUERY * sizeof(float));

    return min_val * y_sum + delta * quantized_dot;
}

template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP16_InnerProductSIMD_SVE2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProductSIMD_SVE2_IMP<partial_chunk, additional_steps>(
                      pVect1v, pVect2v, dimension);
}

template <bool partial_chunk, unsigned char additional_steps>
float SQ8_FP16_CosineSIMD_SVE2(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD_SVE2<partial_chunk, additional_steps>(pVect1v, pVect2v,
                                                                           dimension);
}
