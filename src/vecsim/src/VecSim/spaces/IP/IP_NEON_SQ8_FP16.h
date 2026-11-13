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
#include <arm_neon.h>

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

/*
 * Asymmetric SQ8 (storage) <-> FP16 (query) inner product using algebraic identity:
 *   IP(x, y) ~= min * y_sum + delta * Σ(q_i * y_i)
 *
 * FP16 query lanes are widened to FP32 via vcvt_f32_f16 per 16-lane chunk.
 */

// Helper: 16 lanes per call, four FP32 accumulators (one per quarter).
static inline void SQ8_FP16_InnerProductStep_NEON_HP(const uint8_t *&pVect1, const float16 *&pVect2,
                                                     float32x4_t &sum0, float32x4_t &sum1,
                                                     float32x4_t &sum2, float32x4_t &sum3) {
    uint8x16_t v1_u8 = vld1q_u8(pVect1);
    // SQ8 values 0..255 are exact in FP16, so widen uint8 -> uint16 -> fp16 -> fp32.
    // This drops two integer-widening ops per chunk versus the uint8 -> u16 -> u32 -> f32
    // chain while producing bit-identical FP32 lane values.
    float16x8_t v1_h_lo = vcvtq_f16_u16(vmovl_u8(vget_low_u8(v1_u8)));
    float16x8_t v1_h_hi = vcvtq_f16_u16(vmovl_u8(vget_high_u8(v1_u8)));
    float32x4_t v1_0 = vcvt_f32_f16(vget_low_f16(v1_h_lo));
    float32x4_t v1_1 = vcvt_f32_f16(vget_high_f16(v1_h_lo));
    float32x4_t v1_2 = vcvt_f32_f16(vget_low_f16(v1_h_hi));
    float32x4_t v1_3 = vcvt_f32_f16(vget_high_f16(v1_h_hi));

    const float16_t *q = reinterpret_cast<const float16_t *>(pVect2);
    float16x8_t q_lo = vld1q_f16(q);
    float16x8_t q_hi = vld1q_f16(q + 8);
    float32x4_t v2_0 = vcvt_f32_f16(vget_low_f16(q_lo));
    float32x4_t v2_1 = vcvt_f32_f16(vget_high_f16(q_lo));
    float32x4_t v2_2 = vcvt_f32_f16(vget_low_f16(q_hi));
    float32x4_t v2_3 = vcvt_f32_f16(vget_high_f16(q_hi));

    sum0 = vfmaq_f32(sum0, v1_0, v2_0);
    sum1 = vfmaq_f32(sum1, v1_1, v2_1);
    sum2 = vfmaq_f32(sum2, v1_2, v2_2);
    sum3 = vfmaq_f32(sum3, v1_3, v2_3);

    pVect1 += 16;
    pVect2 += 16;
}

/*
 * FMLAL widening-FMA variant (FEAT_FHM / asimdfhm). Instead of widening both operands to FP32
 * and issuing vfmaq_f32, this multiplies the FP16 lanes directly into FP32 accumulators via
 * vfmlalq_low/high_f16, halving the conversion work: the SQ8 storage is widened uint8 -> fp16
 * (exact for 0..255) and the FP16 query is consumed in place, with no fp16 -> fp32 conversions.
 */
static inline void SQ8_FP16_InnerProductStep_NEON_FHM(const uint8_t *&pVect1,
                                                      const float16 *&pVect2, float32x4_t &sum0,
                                                      float32x4_t &sum1, float32x4_t &sum2,
                                                      float32x4_t &sum3) {
    uint8x16_t v1_u8 = vld1q_u8(pVect1);
    float16x8_t v1_h_lo = vcvtq_f16_u16(vmovl_u8(vget_low_u8(v1_u8)));
    float16x8_t v1_h_hi = vcvtq_f16_u16(vmovl_u8(vget_high_u8(v1_u8)));

    const float16_t *q = reinterpret_cast<const float16_t *>(pVect2);
    float16x8_t q_lo = vld1q_f16(q);
    float16x8_t q_hi = vld1q_f16(q + 8);

    // low_f16 handles lanes 0..3, high_f16 lanes 4..7 of each 8-lane FP16 register.
    sum0 = vfmlalq_low_f16(sum0, v1_h_lo, q_lo);
    sum1 = vfmlalq_high_f16(sum1, v1_h_lo, q_lo);
    sum2 = vfmlalq_low_f16(sum2, v1_h_hi, q_hi);
    sum3 = vfmlalq_high_f16(sum3, v1_h_hi, q_hi);

    pVect1 += 16;
    pVect2 += 16;
}

// pVect1v = SQ8 storage, pVect2v = FP16 query. Precondition: dim >= 16 (enforced by dispatcher).
// use_fhm selects the FMLAL widening-FMA main loop (requires FEAT_FHM at runtime); the residual
// tail is shared since it only reduces into the same FP32 accumulators.
template <unsigned char residual, bool use_fhm = false> // residual 0..15
float SQ8_FP16_InnerProductSIMD16_NEON_HP_IMP(const void *pVect1v, const void *pVect2v,
                                              size_t dimension) {
    const uint8_t *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const float16 *pVect2 = static_cast<const float16 *>(pVect2v);

    float32x4_t sum0 = vdupq_n_f32(0.0f);
    float32x4_t sum1 = vdupq_n_f32(0.0f);
    float32x4_t sum2 = vdupq_n_f32(0.0f);
    float32x4_t sum3 = vdupq_n_f32(0.0f);

    const size_t num_of_chunks = dimension / 16;
    for (size_t i = 0; i < num_of_chunks; i++) {
        if constexpr (use_fhm) {
            SQ8_FP16_InnerProductStep_NEON_FHM(pVect1, pVect2, sum0, sum1, sum2, sum3);
        } else {
            SQ8_FP16_InnerProductStep_NEON_HP(pVect1, pVect2, sum0, sum1, sum2, sum3);
        }
    }

    // Residual: up to three independent 4-lane sub-steps, leaving at most 3 elements
    // for scalar — mirrors the SQ8_FP32 NEON sister pattern.
    // vld1_f16 (4 FP16 = 8 bytes) is safe for any residual: FP16 metadata follows
    // the lane data so there is always enough headroom.
    constexpr unsigned char r = residual;
    if constexpr (r >= 4) {
        uint8x8_t v1_u8 = vld1_u8(pVect1);
        float32x4_t v1_a = vcvtq_f32_u32(vmovl_u16(vget_low_u16(vmovl_u8(v1_u8))));
        float32x4_t v2_a = vcvt_f32_f16(vld1_f16(reinterpret_cast<const float16_t *>(pVect2)));
        sum0 = vfmaq_f32(sum0, v1_a, v2_a);
        pVect1 += 4;
        pVect2 += 4;
    }
    if constexpr (r >= 8) {
        uint8x8_t v1_u8 = vld1_u8(pVect1);
        float32x4_t v1_b = vcvtq_f32_u32(vmovl_u16(vget_low_u16(vmovl_u8(v1_u8))));
        float32x4_t v2_b = vcvt_f32_f16(vld1_f16(reinterpret_cast<const float16_t *>(pVect2)));
        sum1 = vfmaq_f32(sum1, v1_b, v2_b);
        pVect1 += 4;
        pVect2 += 4;
    }
    if constexpr (r >= 12) {
        uint8x8_t v1_u8 = vld1_u8(pVect1);
        float32x4_t v1_c = vcvtq_f32_u32(vmovl_u16(vget_low_u16(vmovl_u8(v1_u8))));
        float32x4_t v2_c = vcvt_f32_f16(vld1_f16(reinterpret_cast<const float16_t *>(pVect2)));
        sum2 = vfmaq_f32(sum2, v1_c, v2_c);
        pVect1 += 4;
        pVect2 += 4;
    }
    constexpr unsigned char tail = r & 3;
    float scalar_dot = 0.0f;
    for (unsigned char k = 0; k < tail; ++k) {
        scalar_dot += static_cast<float>(pVect1[k]) * vecsim_types::FP16_to_FP32(pVect2[k]);
    }

    float32x4_t sum_lo = vaddq_f32(sum0, sum1);
    float32x4_t sum_hi = vaddq_f32(sum2, sum3);
    float quantized_dot = vaddvq_f32(vaddq_f32(sum_lo, sum_hi)) + scalar_dot;

    const uint8_t *params_bytes = static_cast<const uint8_t *>(pVect1v) + dimension;
    const float min_val = load_unaligned<float>(params_bytes + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params_bytes + sq8::DELTA * sizeof(float));
    const uint8_t *query_meta_bytes =
        reinterpret_cast<const uint8_t *>(static_cast<const float16 *>(pVect2v) + dimension);
    const float y_sum = load_unaligned<float>(query_meta_bytes + sq8::SUM_QUERY * sizeof(float));

    return min_val * y_sum + delta * quantized_dot;
}

template <unsigned char residual>
float SQ8_FP16_InnerProductSIMD16_NEON_HP(const void *pVect1v, const void *pVect2v,
                                          size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProductSIMD16_NEON_HP_IMP<residual>(pVect1v, pVect2v, dimension);
}

template <unsigned char residual>
float SQ8_FP16_CosineSIMD16_NEON_HP(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD16_NEON_HP<residual>(pVect1v, pVect2v, dimension);
}

// FMLAL (FEAT_FHM) variants — identical contract, FMLAL widening-FMA main loop.
template <unsigned char residual>
float SQ8_FP16_InnerProductSIMD16_NEON_FHM(const void *pVect1v, const void *pVect2v,
                                           size_t dimension) {
    return 1.0f -
           SQ8_FP16_InnerProductSIMD16_NEON_HP_IMP<residual, true>(pVect1v, pVect2v, dimension);
}

template <unsigned char residual>
float SQ8_FP16_CosineSIMD16_NEON_FHM(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_FP16_InnerProductSIMD16_NEON_FHM<residual>(pVect1v, pVect2v, dimension);
}
