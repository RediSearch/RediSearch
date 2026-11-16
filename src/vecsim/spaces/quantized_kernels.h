/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include "vecsim/utils/alignment.h"
#include "vecsim/spaces/quantized_distance_helpers.h"

// SIMD translation units (functions/*.cpp) must not include this header; they call the exported
// scalar functions (e.g. SQ8_FP32_L2Sqr) instead.
//
// Scalar kernels for scalar quantized storage, generic over a StorageType that provides
// get_code(data, i), data_bytes_count(dim) and the MIN_VAL / DELTA / SUM / SUM_SQUARES metadata
// indices (stored after the codes) and SUM_QUERY (stored after the query values).
// StoredQueryL2 also requires metadata_type, matching the query value type.
// The float IP helpers use code_dot(a, b, dim) and code_dot_fp32(codes, y, dim).
//
// Stored layout: [codes (data_bytes_count(dim) bytes)] [min_val] [delta] [sum] [sum_squares (L2)]
// Query layout: [values (dim)] [y_sum] [y_sum_squares (L2 only)]
// SQ4 L2 uses only the query body; its two trailing FP32 slots are reserved and zero-initialized.
//
// The operand order in these bodies is load-bearing and relies on FP addition not being
// reassociated; they must not be built with -ffast-math / -Ofast.
namespace spaces {
namespace quantized {

/*
 * Asymmetric stored-FP32 inner product using the algebraic identity:
 *   IP(x, y) = sum((min + delta * q_i) * y_i) = min * y_sum + delta * sum(q_i * y_i)
 *
 * sum(q_i * y_i) comes from StorageType::code_dot_fp32.
 * Returns the raw inner product value (not distance).
 */
template <typename StorageType>
static inline float FP32_InnerProduct_Impl(const void *stored, const void *query, size_t dim) {
    const auto *pVect1 = static_cast<const uint8_t *>(stored);
    const auto *pVect2 = static_cast<const float *>(query);

    // Compute inner product of quantized values with the query: sum(q_i * y_i)
    const float quantized_dot = StorageType::code_dot_fp32(pVect1, pVect2, dim);

    // Storage metadata follows a byte payload and is not necessarily float-aligned.
    const auto *params = pVect1 + StorageType::data_bytes_count(dim);
    const float min_val = load_unaligned<float>(params + StorageType::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(params + StorageType::DELTA * sizeof(float));

    // Get precomputed y_sum from query blob (stored after the dim floats)
    const float y_sum = pVect2[dim + StorageType::SUM_QUERY];

    // Apply formula: IP = min * y_sum + delta * sum(q_i * y_i)
    return min_val * y_sum + delta * quantized_dot;
}

/*
 * Asymmetric stored-query L2 squared distance, accumulated directly per component:
 *   ||x - y||^2 = sum((dequant(x_i) - y_i)^2), where dequant(x_i) = min_val + delta * q_i
 *
 * Accumulating residuals keeps each component's rounding at the scale of |x_i - y_i|. Expanding
 * to ||x||^2 + ||y||^2 - 2*IP(x, y) would round at the scale of the norms, which loses the
 * distance when x and y share a large common offset relative to their spread.
 */
template <typename StorageType>
static inline typename StorageType::metadata_type StoredQueryL2(const void *stored,
                                                                const void *query, size_t dim) {
    using Real = typename StorageType::metadata_type;
    // Storage metadata follows a byte payload and is not necessarily aligned.
    const auto *const pVect1 = static_cast<const uint8_t *>(stored);
    const auto *const pVect2 = static_cast<const uint8_t *>(query);

    const auto *const params1 = pVect1 + StorageType::data_bytes_count(dim);
    const Real min_val = load_unaligned<Real>(params1 + StorageType::MIN_VAL * sizeof(Real));
    const Real delta = load_unaligned<Real>(params1 + StorageType::DELTA * sizeof(Real));

    Real res = 0;
    for (size_t i = 0; i < dim; i++) {
        // Subtract min and query first to preserve small residuals with a large common offset.
        const Real query_value = load_unaligned<Real>(pVect2 + i * sizeof(Real));
        const Real diff =
            delta * static_cast<Real>(StorageType::get_code(pVect1, i)) + (min_val - query_value);
        res += diff * diff;
    }
    return res;
}

template <typename StorageType>
static inline float FP32_L2Sqr(const void *stored, const void *query, size_t dim) {
    static_assert(std::is_same_v<typename StorageType::metadata_type, float>);
    return StoredQueryL2<StorageType>(stored, query, dim);
}

/*
 * Symmetric stored-stored inner product using precomputed sums:
 *   IP = min1*sum2 + min2*sum1 - dim*min1*min2 + delta1*delta2*sum(q1_i*q2_i)
 * Returns the raw inner product value (not distance).
 */
template <typename StorageType>
static inline float Stored_InnerProduct_Impl(const void *a, const void *b, size_t dim) {
    const auto *pVect1 = static_cast<const uint8_t *>(a);
    const auto *pVect2 = static_cast<const uint8_t *>(b);

    // Compute inner product of quantized values: sum(q1[i]*q2[i])
    const float product = StorageType::code_dot(pVect1, pVect2, dim);

    return Stored_InnerProduct_FromCodeDot<StorageType>(pVect1, pVect2, dim, product);
}

/*
 * Symmetric stored-stored L2 squared distance:
 *   ||x - y||^2 = ||x||^2 + ||y||^2 - 2*IP(x, y)
 * where ||x||^2 and ||y||^2 are the precomputed SUM_SQUARES metadata and IP(x, y) is computed by
 * Stored_InnerProduct_Impl.
 */
template <typename StorageType>
static inline float Stored_L2Sqr(const void *a, const void *b, size_t dim) {
    const auto *pVect1 = static_cast<const uint8_t *>(a);
    const auto *pVect2 = static_cast<const uint8_t *>(b);

    // Get precomputed sum of squares from both vectors
    const size_t data_bytes = StorageType::data_bytes_count(dim);
    const float sum_sq_1 =
        load_unaligned<float>(pVect1 + data_bytes + StorageType::SUM_SQUARES * sizeof(float));
    const float sum_sq_2 =
        load_unaligned<float>(pVect2 + data_bytes + StorageType::SUM_SQUARES * sizeof(float));

    // Use the common inner product implementation
    const float ip = Stored_InnerProduct_Impl<StorageType>(a, b, dim);

    // L2^2 = ||x||^2 + ||y||^2 - 2*IP(x, y)
    return sum_sq_1 + sum_sq_2 - 2.0f * ip;
}

} // namespace quantized
} // namespace spaces
