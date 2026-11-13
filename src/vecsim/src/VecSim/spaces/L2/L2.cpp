/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "L2.h"
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/utils/alignment.h"
#include <iostream>

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;
using sq8 = vecsim_types::sq8;

/*
 * Asymmetric SQ8-FP32 L2 squared distance, accumulated directly per component:
 *   ||x - y||² = Σ(dequant(x_i) - y_i)²
 *   where dequant(x_i) = min_val + delta * q_i
 *
 * Accumulating residuals keeps each component's rounding at the scale of |x_i - y_i|. Expanding
 * to ||x||² + ||y||² - 2*IP(x, y) would round at the scale of the norms, which loses the
 * distance when x and y share a large common offset relative to their spread.
 *
 * The operand order in the loop is load-bearing and relies on FP addition not being
 * reassociated; this file must not be built with -ffast-math / -Ofast.
 *
 * pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val] [delta] [x_sum] [x_sum_squares]
 * pVect2 is query (FP32): [float values (dim)] [y_sum] [y_sum_squares]
 */
float SQ8_FP32_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    // Storage metadata follows a byte payload and is not necessarily float-aligned.
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const float *>(pVect2v);

    const auto *params1 = pVect1 + dimension;
    const float min_val = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        // Order matters. When the stored range sits far from zero, min_val and y_i are within a
        // factor of two of each other, so min_val - y_i is exact (Sterbenz) and rounding happens
        // only at the scale of the residual. Forming (min_val + delta*q_i) first would round at
        // the scale of min_val and wipe out small residuals. When min_val and y_i are not close,
        // the residual is large and either order is fine.
        float diff = delta * static_cast<float>(pVect1[i]) + (min_val - pVect2[i]);
        res += diff * diff;
    }
    return res;
}

/*
 * Optimized asymmetric SQ8-FP16 L2 squared distance using algebraic identity:
 *   ||x - y||² = Σx_i² - 2*IP(x, y) + Σy_i²
 *              = x_sum_squares - 2 * IP(x, y) + y_sum_squares
 *   where IP(x, y) = min * y_sum + delta * Σ(q_i * y_i) and FP16 query values are widened
 *   to FP32 inside SQ8_FP16_InnerProduct_Impl.
 *
 * pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val] [delta] [x_sum] [x_sum_squares]
 * pVect2 is query (FP16):  [float16 values (dim)] [y_sum] [y_sum_squares]
 */
float SQ8_FP16_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    // Get the raw inner product using the common implementation
    const float ip = SQ8_FP16_InnerProduct_Impl(pVect1v, pVect2v, dimension);

    // Get precomputed sum of squares from the storage and query blobs. The metadata sits at
    // byte offsets that are not guaranteed 4-byte aligned for odd `dimension`, so use
    // load_unaligned to avoid alignment UB.
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const float x_sum_sq =
        load_unaligned<float>(pVect1 + dimension + sq8::SUM_SQUARES * sizeof(float));
    const auto *pVect2 = static_cast<const float16 *>(pVect2v);
    const auto *query_meta_bytes = reinterpret_cast<const uint8_t *>(pVect2 + dimension);
    const float y_sum_sq =
        load_unaligned<float>(query_meta_bytes + sq8::SUM_SQUARES_QUERY * sizeof(float));

    // L2² = ||x||² + ||y||² - 2*IP(x, y)
    return x_sum_sq + y_sum_sq - 2.0f * ip;
}

float FP32_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    float *vec1 = (float *)pVect1v;
    float *vec2 = (float *)pVect2v;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        float t = vec1[i] - vec2[i];
        res += t * t;
    }
    return res;
}

double FP64_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    double *vec1 = (double *)pVect1v;
    double *vec2 = (double *)pVect2v;

    double res = 0;
    for (size_t i = 0; i < dimension; i++) {
        double t = vec1[i] - vec2[i];
        res += t * t;
    }
    return res;
}

template <bool is_little>
float BF16_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    bfloat16 *pVect1 = (bfloat16 *)pVect1v;
    bfloat16 *pVect2 = (bfloat16 *)pVect2v;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        float a = vecsim_types::bfloat16_to_float32<is_little>(pVect1[i]);
        float b = vecsim_types::bfloat16_to_float32<is_little>(pVect2[i]);
        float diff = a - b;
        res += diff * diff;
    }
    return res;
}

float BF16_L2Sqr_LittleEndian(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return BF16_L2Sqr<true>(pVect1v, pVect2v, dimension);
}

float BF16_L2Sqr_BigEndian(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return BF16_L2Sqr<false>(pVect1v, pVect2v, dimension);
}

float FP16_L2Sqr(const void *pVect1, const void *pVect2, size_t dimension) {
    auto *vec1 = (float16 *)pVect1;
    auto *vec2 = (float16 *)pVect2;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        float t = vecsim_types::FP16_to_FP32(vec1[i]) - vecsim_types::FP16_to_FP32(vec2[i]);
        res += t * t;
    }
    return res;
}

// Return type for the L2 functions.
// The type must hold `dimension * MAX_VAL(int_elem_t) * MAX_VAL(int_elem_t)`. For uint8 that
// is 65025 * dimension, which overflows a 32-bit int from dimension 33,026, and this is the
// kernel the chooser falls back to above that dimension, so UINT8_L2Sqr must be exact there.
template <typename int_elem_t>
using ret_t = long long;

// Difference type for the L2 functions.
// The type should be able to hold `MIN_VAL(int_elem_t)-MAX_VAL(int_elem_t)`, and should be signed
// to avoid unsigned arithmetic. This means that the difference type should be bigger than the
// size of the element type. We assert that in the implementation.
template <typename int_elem_t>
using diff_t = std::conditional_t<sizeof(int_elem_t) == 1, int16_t, int>;

template <typename int_elem_t>
static inline ret_t<int_elem_t> INTEGER_L2Sqr(const int_elem_t *pVect1, const int_elem_t *pVect2,
                                              size_t dimension) {
    static_assert(sizeof(ret_t<int_elem_t>) - sizeof(int_elem_t) * 2 >= sizeof(uint16_t));
    static_assert(std::is_signed_v<diff_t<int_elem_t>>);
    static_assert(sizeof(diff_t<int_elem_t>) >= 2 * sizeof(int_elem_t));

    ret_t<int_elem_t> res = 0;
    for (size_t i = 0; i < dimension; i++) {
        diff_t<int_elem_t> diff = pVect1[i] - pVect2[i];
        res += diff * diff;
    }
    return res;
}

float INT8_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const int8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const int8_t *>(pVect2v);
    return float(INTEGER_L2Sqr(pVect1, pVect2, dimension));
}

float UINT8_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);
    return float(INTEGER_L2Sqr(pVect1, pVect2, dimension));
}

// SQ8-to-SQ8 L2 squared distance (both vectors are uint8 quantized)
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
// [sum_of_squares (float)]
//  ||x - y||² = ||x||² + ||y||² - 2*IP(x, y)
//   where:
//     - ||x||² = sum_squares_x is precomputed and stored
//     - ||y||² = sum_squares_y is precomputed and stored
//     - IP(x, y) is computed using SQ8_SQ8_InnerProduct_Impl

float SQ8_SQ8_L2Sqr(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);

    // Get precomputed sum of squares from both vectors
    // Layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_of_squares]
    const float sum_sq_1 =
        load_unaligned<float>(pVect1 + dimension + sq8::SUM_SQUARES * sizeof(float));
    const float sum_sq_2 =
        load_unaligned<float>(pVect2 + dimension + sq8::SUM_SQUARES * sizeof(float));

    // Use the common inner product implementation
    const float ip = SQ8_SQ8_InnerProduct_Impl(pVect1v, pVect2v, dimension);

    // L2² = ||x||² + ||y||² - 2*IP(x, y)
    return sum_sq_1 + sum_sq_2 - 2.0f * ip;
}
