/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "IP.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/utils/alignment.h"

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;
using sq8 = vecsim_types::sq8;

/*
 * Optimized asymmetric SQ8-FP32 inner product using algebraic identity:
 *   IP(x, y) = Σ(x_i * y_i)
 *            ≈ Σ((min + delta * q_i) * y_i)
 *            = min * Σy_i + delta * Σ(q_i * y_i)
 *            = min * y_sum + delta * quantized_dot_product
 *
 * Uses 4x loop unrolling with multiple accumulators for ILP.
 * pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val] [delta] [x_sum] [x_sum_squares (L2
 * only)]
 * pVect2 is query (FP32): [float values (dim)] [y_sum] [y_sum_squares (L2 only)]
 *
 * Returns raw inner product value (not distance). Used by SQ8_FP32_InnerProduct, SQ8_FP32_Cosine,
 * SQ8_FP32_L2Sqr.
 */
float SQ8_FP32_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const float *>(pVect2v);

    // Use 4 accumulators for instruction-level parallelism
    float sum0 = 0, sum1 = 0, sum2 = 0, sum3 = 0;

    // Main loop: process 4 elements per iteration
    size_t i = 0;
    size_t dim4 = dimension & ~size_t(3); // dim4 is a multiple of 4
    for (; i < dim4; i += 4) {
        sum0 += static_cast<float>(pVect1[i + 0]) * pVect2[i + 0];
        sum1 += static_cast<float>(pVect1[i + 1]) * pVect2[i + 1];
        sum2 += static_cast<float>(pVect1[i + 2]) * pVect2[i + 2];
        sum3 += static_cast<float>(pVect1[i + 3]) * pVect2[i + 3];
    }

    // Handle remainder (0-3 elements)
    for (; i < dimension; i++) {
        sum0 += static_cast<float>(pVect1[i]) * pVect2[i];
    }

    // Combine accumulators
    float quantized_dot = (sum0 + sum1) + (sum2 + sum3);

    // Storage metadata follows a byte payload and is not necessarily float-aligned.
    const auto *params = pVect1 + dimension;
    const float min_val = load_unaligned<float>(params + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + sq8::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (pVect2 is FP32, stored after the dim floats)
    const float y_sum = pVect2[dimension + sq8::SUM_QUERY];

    // Apply formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

float SQ8_FP32_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP32_InnerProduct_Impl(pVect1v, pVect2v, dimension);
}

float SQ8_FP32_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_FP32_InnerProduct(pVect1v, pVect2v, dimension);
}

/*
 * Optimized asymmetric SQ8-FP16 inner product using algebraic identity:
 *   IP(x, y) = Σ(x_i * y_i)
 *            ≈ Σ((min + delta * q_i) * y_i)
 *            = min * Σy_i + delta * Σ(q_i * y_i)
 *            = min * y_sum + delta * quantized_dot_product
 *
 * Each FP16 query value is widened to FP32 before accumulation. SQ8 metadata and
 * query metadata are read as FP32.
 *
 * pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val] [delta] [x_sum] [x_sum_squares (L2
 * only)]
 * pVect2 is query (FP16):  [float16 values (dim)] [y_sum] [y_sum_squares (L2 only)]
 *
 * Returns raw inner product value (not distance). Used by SQ8_FP16_InnerProduct, SQ8_FP16_Cosine,
 * SQ8_FP16_L2Sqr.
 */
float SQ8_FP16_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const float16 *>(pVect2v);

    // Use 4 accumulators for instruction-level parallelism
    float sum0 = 0, sum1 = 0, sum2 = 0, sum3 = 0;

    // Main loop: process 4 elements per iteration
    size_t i = 0;
    size_t dim4 = dimension & ~size_t(3); // dim4 is a multiple of 4
    for (; i < dim4; i += 4) {
        sum0 += static_cast<float>(pVect1[i + 0]) * vecsim_types::FP16_to_FP32(pVect2[i + 0]);
        sum1 += static_cast<float>(pVect1[i + 1]) * vecsim_types::FP16_to_FP32(pVect2[i + 1]);
        sum2 += static_cast<float>(pVect1[i + 2]) * vecsim_types::FP16_to_FP32(pVect2[i + 2]);
        sum3 += static_cast<float>(pVect1[i + 3]) * vecsim_types::FP16_to_FP32(pVect2[i + 3]);
    }

    // Handle remainder (0-3 elements)
    for (; i < dimension; i++) {
        sum0 += static_cast<float>(pVect1[i]) * vecsim_types::FP16_to_FP32(pVect2[i]);
    }

    // Combine accumulators
    float quantized_dot = (sum0 + sum1) + (sum2 + sum3);

    // Get quantization parameters from stored vector (pVect1 is SQ8) and the precomputed
    // y_sum from the query blob. The metadata sits at byte offsets that are not guaranteed
    // 4-byte aligned for odd `dimension`, so use load_unaligned to avoid alignment UB.
    const auto *params_bytes = pVect1 + dimension;
    const float min_val = load_unaligned<float>(params_bytes + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params_bytes + sq8::DELTA * sizeof(float));
    const auto *query_meta_bytes = reinterpret_cast<const uint8_t *>(pVect2 + dimension);
    const float y_sum = load_unaligned<float>(query_meta_bytes + sq8::SUM_QUERY * sizeof(float));

    // Apply formula: IP = min * y_sum + delta * Σ(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

float SQ8_FP16_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_FP16_InnerProduct_Impl(pVect1v, pVect2v, dimension);
}

float SQ8_FP16_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_FP16_InnerProduct(pVect1v, pVect2v, dimension);
}

// SQ8-to-SQ8: Common inner product implementation that returns the raw inner product value
// (not distance). Used by both SQ8_SQ8_InnerProduct, SQ8_SQ8_Cosine, and SQ8_SQ8_L2Sqr.
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);

    // Compute inner product of quantized values: Σ(q1[i]*q2[i])
    float product = 0;
    for (size_t i = 0; i < dimension; i++) {
        product += pVect1[i] * pVect2[i];
    }

    // Metadata follows byte payloads and is not necessarily float-aligned.
    const auto *params1 = pVect1 + dimension;
    const float min_val1 = load_unaligned<float>(params1 + sq8::MIN_VAL * sizeof(float));
    const float delta1 = load_unaligned<float>(params1 + sq8::DELTA * sizeof(float));
    const float sum1 = load_unaligned<float>(params1 + sq8::SUM * sizeof(float));

    // Get quantization parameters from pVect2
    const auto *params2 = pVect2 + dimension;
    const float min_val2 = load_unaligned<float>(params2 + sq8::MIN_VAL * sizeof(float));
    const float delta2 = load_unaligned<float>(params2 + sq8::DELTA * sizeof(float));
    const float sum2 = load_unaligned<float>(params2 + sq8::SUM * sizeof(float));

    // Apply the algebraic formula using precomputed sums:
    // IP = min1*sum2 + min2*sum1 + delta1*delta2*Σ(q1[i]*q2[i]) - dim*min1*min2
    return min_val1 * sum2 + min_val2 * sum1 - static_cast<float>(dimension) * min_val1 * min_val2 +
           delta1 * delta2 * product;
}

// SQ8-to-SQ8: Both vectors are uint8 quantized with precomputed sum
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return 1.0f - SQ8_SQ8_InnerProduct_Impl(pVect1v, pVect2v, dimension);
}

// SQ8-to-SQ8: Both vectors are uint8 quantized and normalized with precomputed sum
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return SQ8_SQ8_InnerProduct(pVect1v, pVect2v, dimension);
}

float FP32_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension) {
    auto *vec1 = (float *)pVect1;
    auto *vec2 = (float *)pVect2;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        res += vec1[i] * vec2[i];
    }
    return 1.0f - res;
}

double FP64_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension) {
    auto *vec1 = (double *)pVect1;
    auto *vec2 = (double *)pVect2;

    double res = 0;
    for (size_t i = 0; i < dimension; i++) {
        res += vec1[i] * vec2[i];
    }
    return 1.0 - res;
}

template <bool is_little>
float BF16_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    auto *pVect1 = (bfloat16 *)pVect1v;
    auto *pVect2 = (bfloat16 *)pVect2v;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        float a = vecsim_types::bfloat16_to_float32<is_little>(pVect1[i]);
        float b = vecsim_types::bfloat16_to_float32<is_little>(pVect2[i]);
        res += a * b;
    }
    return 1.0f - res;
}

float BF16_InnerProduct_LittleEndian(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return BF16_InnerProduct<true>(pVect1v, pVect2v, dimension);
}

float BF16_InnerProduct_BigEndian(const void *pVect1v, const void *pVect2v, size_t dimension) {
    return BF16_InnerProduct<false>(pVect1v, pVect2v, dimension);
}

float FP16_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension) {
    auto *vec1 = (float16 *)pVect1;
    auto *vec2 = (float16 *)pVect2;

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        res += vecsim_types::FP16_to_FP32(vec1[i]) * vecsim_types::FP16_to_FP32(vec2[i]);
    }
    return 1.0f - res;
}

// Return type for the inner product functions.
// The type must hold `dimension * MAX_VAL(int_elem_t) * MAX_VAL(int_elem_t)`. For uint8 that
// is 65025 * dimension, which overflows a 32-bit int from dimension 33,026, and this is the
// kernel the chooser falls back to above that dimension, so UINT8_InnerProduct must be exact there.
template <typename int_elem_t>
using ret_t = long long;

template <typename int_elem_t>
static inline ret_t<int_elem_t>
INTEGER_InnerProductImp(const int_elem_t *pVect1, const int_elem_t *pVect2, size_t dimension) {
    static_assert(sizeof(ret_t<int_elem_t>) - sizeof(int_elem_t) * 2 >= sizeof(uint16_t));
    ret_t<int_elem_t> res = 0;
    for (size_t i = 0; i < dimension; i++) {
        res += pVect1[i] * pVect2[i];
    }
    return res;
}

float INT8_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const int8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const int8_t *>(pVect2v);
    return 1 - INTEGER_InnerProductImp(pVect1, pVect2, dimension);
}

float INT8_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const int8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const int8_t *>(pVect2v);
    // We expect the vectors' norm to be stored at the end of the vector.
    const float norm_v1 = load_unaligned<float>(pVect1 + dimension);
    const float norm_v2 = load_unaligned<float>(pVect2 + dimension);
    return 1.0f - float(INTEGER_InnerProductImp(pVect1, pVect2, dimension)) / (norm_v1 * norm_v2);
}

float UINT8_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);
    return 1 - INTEGER_InnerProductImp(pVect1, pVect2, dimension);
}

float UINT8_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);
    // We expect the vectors' norm to be stored at the end of the vector.
    const float norm_v1 = load_unaligned<float>(pVect1 + dimension);
    const float norm_v2 = load_unaligned<float>(pVect2 + dimension);
    return 1.0f - float(INTEGER_InnerProductImp(pVect1, pVect2, dimension)) / (norm_v1 * norm_v2);
}
