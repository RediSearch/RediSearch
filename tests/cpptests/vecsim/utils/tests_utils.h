/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstring>
#include <random>
#include <vector>
#include "VecSim/spaces/normalize/compute_norm.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/utils/alignment.h"

using sq8 = vecsim_types::sq8;

namespace test_utils {

// Assuming v is a memory allocation of size dim * sizeof(float)
static void populate_int8_vec(int8_t *v, size_t dim, int seed = 1234) {

    std::mt19937 gen(seed); // Mersenne Twister engine initialized with the fixed seed

    // uniform_int_distribution doesn't support int8,
    // Define a distribution range for int8_t
    std::uniform_int_distribution<int16_t> dis(INT8_MIN, INT8_MAX);

    for (size_t i = 0; i < dim; i++) {
        v[i] = static_cast<int8_t>(dis(gen));
    }
}
static void populate_uint8_vec(uint8_t *v, size_t dim, int seed = 1234) {

    std::mt19937 gen(seed); // Mersenne Twister engine initialized with the fixed seed

    // uniform_int_distribution doesn't support uint8,
    // Define a distribution range for uint8_t
    std::uniform_int_distribution<uint16_t> dis(0, UINT8_MAX);

    for (size_t i = 0; i < dim; i++) {
        v[i] = static_cast<uint8_t>(dis(gen));
    }
}

// Assuming v is a memory allocation of size dim * sizeof(float)
static void populate_float_vec(float *v, size_t dim, int seed = 1234, float min = -1.0f,
                               float max = 1.0f) {

    std::mt19937 gen(seed); // Mersenne Twister engine initialized with the fixed seed

    // Define a distribution range for float values
    std::uniform_real_distribution<float> dis(min, max);

    for (size_t i = 0; i < dim; i++) {
        v[i] = dis(gen);
    }
}

// Assuming v is a memory allocation of size dim * sizeof(float)
static void populate_float16_vec(vecsim_types::float16 *v, const size_t dim, int seed = 1234,
                                 float min = -1.0f, float max = 1.0f) {
    float v_f[dim];
    populate_float_vec(v_f, dim, seed, min, max);

    for (size_t i = 0; i < dim; i++) {
        v[i] = vecsim_types::FP32_to_FP16(v_f[i]);
    }
}

/*
 * SQ8-FP32 distance function without the algebraic optimizations
 * uses the regular dequantization formula:
 * IP = Σ((min + delta * q_i) * v_i)
 * pVect1 = SQ8 storage (quantized values + metadata)
 * pVect2 = FP32 query
 */
static float SQ8_FP32_NotOptimized_InnerProduct(const void *pVect1v, const void *pVect2v,
                                                size_t dimension) {

    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const auto *pVect2 = static_cast<const float *>(pVect2v);   // FP32 query

    // Get quantization parameters from pVect1 (SQ8 storage)
    const float min_val = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));
    // Compute inner product with dequantization
    float res = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        res += (pVect1[i] * delta + min_val) * pVect2[i];
    }
    return 1.0f - res;
}

/*
 * SQ8 Cosine distance function without the algebraic optimizations
 * For normalized vectors, cosine distance equals inner product distance.
 */
static float SQ8_FP32_NotOptimized_Cosine(const void *pVect1v, const void *pVect2v,
                                          size_t dimension) {
    return SQ8_FP32_NotOptimized_InnerProduct(pVect1v, pVect2v, dimension);
}

/*
 * SQ8_SQ8 distance function without the algebraic optimizations
 * uses the regular dequantization formula:
 * IP = Σ((min1 + delta1 * q1_i) * (min2 + delta2 * q2_i))
 * Used for testing the correctness of the optimized functions.
 *
 */
static float SQ8_SQ8_NotOptimized_InnerProduct(const void *pVect1v, const void *pVect2v,
                                               size_t dimension) {

    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);

    // Get quantization parameters from pVect1
    const float min_val1 = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta1 = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));

    // Get quantization parameters from pVect2
    const float min_val2 = load_unaligned<float>(pVect2 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta2 = load_unaligned<float>(pVect2 + dimension + sq8::DELTA * sizeof(float));

    // Compute inner product with dequantization
    float res = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        res += (pVect1[i] * delta1 + min_val1) * (pVect2[i] * delta2 + min_val2);
    }
    return 1.0f - res;
}

static float SQ8_SQ8_NotOptimized_Cosine(const void *pVect1v, const void *pVect2v,
                                         size_t dimension) {
    return SQ8_SQ8_NotOptimized_InnerProduct(pVect1v, pVect2v, dimension);
}

/*
    L2 distance function without the algebraic optimizations
    uses the regular dequantization formula:
    L2 = Σ((min1 + delta1 * q1_i) - (min2 + delta2 * q2_i))²
    Used for testing the correctness of the optimized functions.
*/
static float SQ8_SQ8_NotOptimized_L2Sqr(const void *pVect1v, const void *pVect2v,
                                        size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);

    // Extract metadata from the end of vectors
    // Layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_of_squares]
    const float min1 = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta1 = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));
    const float min2 = load_unaligned<float>(pVect2 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta2 = load_unaligned<float>(pVect2 + dimension + sq8::DELTA * sizeof(float));

    // Compute L2 distance with dequantization
    float res = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        float v1_dequantized = pVect1[i] * delta1 + min1;
        float v2_dequantized = pVect2[i] * delta2 + min2;
        float t = v1_dequantized - v2_dequantized;
        res += t * t;
    }
    return res;
}

/**
 * Quantize a float vector to SQ8 with precomputed sums.
 * Layout: [uint8_t values (dim)] [min (float)] [delta (float)] [sum (float)] [sum_squares (float)]
 * where sum = sum(x_r[i]) and sum_squares = sum(x_r[i]^2) over the reconstruction
 * x_r[i] = min + delta * qv[i]. The kernels are written in terms of x_r, so the metadata has to
 * describe x_r and not the input. See QuantPreprocessor::quantize.
 */
static void quantize_float_vec_to_sq8_with_metadata(const float *v, size_t dim, uint8_t *qv) {
    float min_val = v[0];
    float max_val = v[0];
    for (size_t i = 1; i < dim; i++) {
        min_val = std::min(min_val, v[i]);
        max_val = std::max(max_val, v[i]);
    }

    float delta = (max_val - min_val) / 255.0f;
    if (delta == 0)
        delta = 1.0f; // Avoid division by zero

    // Quantize, accumulating the bytes as exact integers.
    // 64-bit for the squares: 255^2 * dim passes UINT32_MAX just above dim 66051, and a
    // wrapped counter would corrupt the stored norm rather than round it. Mirrors
    // QuantPreprocessor::quantize.
    uint32_t q_sum = 0;
    uint64_t q_sum_squares = 0;
    for (size_t i = 0; i < dim; i++) {
        float normalized = (v[i] - min_val) / delta;
        normalized = std::max(0.0f, std::min(255.0f, normalized));
        const uint32_t q = static_cast<uint8_t>(std::round(normalized));
        qv[i] = static_cast<uint8_t>(q);
        q_sum += q;
        q_sum_squares += q * q;
    }

    // Derive in double, store FP32.
    const double d_min = min_val, d_delta = delta, d_dim = static_cast<double>(dim);
    const float sum = static_cast<float>(d_dim * d_min + d_delta * q_sum);
    const float square_sum = static_cast<float>(
        d_dim * d_min * d_min + 2.0 * d_min * d_delta * q_sum + d_delta * d_delta * q_sum_squares);

    auto *params = qv + dim;
    std::memcpy(params + sq8::MIN_VAL * sizeof(float), &min_val, sizeof(float));
    std::memcpy(params + sq8::DELTA * sizeof(float), &delta, sizeof(float));
    std::memcpy(params + sq8::SUM * sizeof(float), &sum, sizeof(float));
    std::memcpy(params + sq8::SUM_SQUARES * sizeof(float), &square_sum, sizeof(float));
}

// Preprocess fp32 query for SQ8 IP/Cosine/L2 space.
// Query layout: [float values (dim)] [sum (float)] [sum_squares (float)]
// Assuming v is a memory allocation of size (dim + sq8::query_metadata_count<VecSimMetric_L2>())
// defaults to L2 just for testing purposes.
static void preprocess_sq8_fp32_query(float *v, size_t dim) {
    float sum = 0.0f;
    float sum_squares = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        sum += v[i];
        sum_squares += v[i] * v[i];
    }
    v[dim + sq8::SUM_QUERY] = sum;
    v[dim + sq8::SUM_SQUARES_QUERY] = sum_squares;
}

// Assuming v is a memory allocation of size (dim + sq8::query_metadata_count<VecSimMetric_L2>())
static void populate_sq8_fp32_query(float *v, size_t dim, bool should_normalize = false,
                                    int seed = 1234, float min = -1.0f, float max = 1.0f) {
    populate_float_vec(v, dim, seed, min, max);
    if (should_normalize) {
        spaces::GetNormalizeFunc<float>()(v, dim);
    }
    preprocess_sq8_fp32_query(v, dim);
}

/*
 * SQ8-FP32 L2 squared distance function without the algebraic optimizations.
 * Uses the regular dequantization formula element-by-element:
 * L2² = Σ((y_i - (min + delta * q_i))²)
 * pVect1 = SQ8 storage (quantized values + metadata)
 * pVect2 = FP32 query
 */
static float SQ8_FP32_NotOptimized_L2Sqr(const void *pVect1v, const void *pVect2v,
                                         size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    const auto *pVect2 = static_cast<const float *>(pVect2v);   // FP32 query

    // Get quantization parameters from pVect1 (SQ8 storage)
    const float min_val = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));

    // Compute L2 squared with dequantization
    float res = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        float dequantized = pVect1[i] * delta + min_val;
        float diff = pVect2[i] - dequantized;
        res += diff * diff;
    }
    return res;
}
/*
 * SQ8-FP16 inner product distance reference implementation without algebraic optimizations.
 * Uses element-wise dequantization of the SQ8 storage and widens FP16 query values to FP32:
 * IP = Σ((min + delta * q_i) * FP16_to_FP32(y_i))
 * pVect1 = SQ8 storage (quantized values + metadata)
 * pVect2 = FP16 query (float16 values + FP32 metadata)
 */
static float SQ8_FP16_NotOptimized_InnerProduct(const void *pVect1v, const void *pVect2v,
                                                size_t dimension) {

    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v); // SQ8 storage
    // FP16 query buffer may be only 1-byte aligned (e.g. backed by std::vector<uint8_t>),
    // so access float16 values via memcpy on uint16_t to avoid alignment UB.
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v); // FP16 query

    // Storage metadata sits at byte offset `dimension` into the uint8 buffer and is not
    // guaranteed 4-byte aligned for odd `dimension`; use load_unaligned to avoid alignment UB.
    const float min_val = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));

    float res = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        uint16_t raw;
        std::memcpy(&raw, pVect2 + i * sizeof(vecsim_types::float16), sizeof(raw));
        res +=
            (pVect1[i] * delta + min_val) * vecsim_types::FP16_to_FP32(vecsim_types::float16{raw});
    }
    return 1.0f - res;
}

/*
 * SQ8-FP16 cosine reference. For normalized vectors, cosine equals inner product distance.
 */
static float SQ8_FP16_NotOptimized_Cosine(const void *pVect1v, const void *pVect2v,
                                          size_t dimension) {
    return SQ8_FP16_NotOptimized_InnerProduct(pVect1v, pVect2v, dimension);
}

/*
 * SQ8-FP16 L2 squared reference implementation without algebraic optimizations.
 * Mirrors the algebraic identity used by the optimized kernel:
 *   L2² = ||x_orig||² + ||y||² - 2 * Σ(dequant(x_i) * FP16_to_FP32(y_i))
 * where ||x_orig||² is the SUM_SQUARES precomputed from the *original* (pre-quantization)
 * floats and stored in the SQ8 metadata, and ||y||² is the SUM_SQUARES_QUERY computed
 * from the FP16-widened-to-FP32 query values. This intentionally differs from a pure
 * Σ(y - dequant(x))² by a quantization-error term that grows with dim, since the
 * production storage stores the pre-quantization norm.
 */
static float SQ8_FP16_NotOptimized_L2Sqr(const void *pVect1v, const void *pVect2v,
                                         size_t dimension) {
    const auto *pVect1 = static_cast<const uint8_t *>(pVect1v);
    // FP16 query buffer may be only 1-byte aligned; access float16 values via memcpy on
    // uint16_t to avoid alignment UB on strict-alignment targets.
    const auto *pVect2 = static_cast<const uint8_t *>(pVect2v);

    // Storage and query metadata sit at byte offsets that are not guaranteed 4-byte aligned
    // for odd `dimension`; use load_unaligned to avoid alignment UB.
    const float min_val = load_unaligned<float>(pVect1 + dimension + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(pVect1 + dimension + sq8::DELTA * sizeof(float));
    const float x_sum_sq =
        load_unaligned<float>(pVect1 + dimension + sq8::SUM_SQUARES * sizeof(float));
    const auto *query_meta_bytes = pVect2 + dimension * sizeof(vecsim_types::float16);
    const float y_sum_sq =
        load_unaligned<float>(query_meta_bytes + sq8::SUM_SQUARES_QUERY * sizeof(float));

    float ip = 0.0f;
    for (size_t i = 0; i < dimension; i++) {
        uint16_t raw;
        std::memcpy(&raw, pVect2 + i * sizeof(vecsim_types::float16), sizeof(raw));
        const float dequantized = pVect1[i] * delta + min_val;
        ip += dequantized * vecsim_types::FP16_to_FP32(vecsim_types::float16{raw});
    }
    return x_sum_sq + y_sum_sq - 2.0f * ip;
}

// Preprocess FP16 query for SQ8 IP/Cosine/L2 space.
// Query layout: [float16 values (dim)] [sum (float)] [sum_squares (float)]
// `buf` must be at least
//   dim * sizeof(float16) + sq8::query_metadata_count<VecSimMetric_L2>() * sizeof(float)
// bytes (defaults to L2 layout, sufficient for IP/Cosine).
// The metadata is computed from the FP16 values widened back to FP32, so it matches
// what the SQ8-FP16 distance kernels will accumulate.
// `buf` may be only 1-byte aligned (callers commonly back it with std::vector<uint8_t>),
// so FP16 values and FP32 metadata are accessed via memcpy to avoid alignment UB on
// strict-alignment targets.
static void preprocess_sq8_fp16_query(void *buf, size_t dim) {
    auto *bytes = static_cast<uint8_t *>(buf);
    float sum = 0.0f;
    float sum_squares = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        uint16_t raw;
        std::memcpy(&raw, bytes + i * sizeof(vecsim_types::float16), sizeof(raw));
        const float widened = vecsim_types::FP16_to_FP32(vecsim_types::float16{raw});
        sum += widened;
        sum_squares += widened * widened;
    }
    auto *metadata_bytes = bytes + dim * sizeof(vecsim_types::float16);
    std::memcpy(metadata_bytes + sq8::SUM_QUERY * sizeof(float), &sum, sizeof(float));
    std::memcpy(metadata_bytes + sq8::SUM_SQUARES_QUERY * sizeof(float), &sum_squares,
                sizeof(float));
}

// Populate an FP16 query buffer for SQ8 IP/Cosine/L2 space.
// `buf` must be at least
//   dim * sizeof(float16) + sq8::query_metadata_count<VecSimMetric_L2>() * sizeof(float)
// bytes. Generates float values, optionally normalizes them in FP32 (matching the
// FP32 query helper), converts to FP16, then computes FP32 metadata.
// `buf` may be only 1-byte aligned; FP16 stores go through memcpy on uint16_t to avoid
// alignment UB on strict-alignment targets.
static void populate_sq8_fp16_query(void *buf, size_t dim, bool should_normalize = false,
                                    int seed = 1234, float min = -1.0f, float max = 1.0f) {
    std::vector<float> tmp(dim);
    populate_float_vec(tmp.data(), dim, seed, min, max);
    if (should_normalize) {
        spaces::GetNormalizeFunc<float>()(tmp.data(), dim);
    }
    auto *bytes = static_cast<uint8_t *>(buf);
    for (size_t i = 0; i < dim; i++) {
        const uint16_t raw = vecsim_types::FP32_to_FP16(tmp[i]).val;
        std::memcpy(bytes + i * sizeof(vecsim_types::float16), &raw, sizeof(raw));
    }
    preprocess_sq8_fp16_query(buf, dim);
}

/**
 * Populate a float vector and quantize to SQ8 with precomputed sum and sum_squares.
 * Vector layout: [uint8_t values (dim)] [min (float)] [delta (float)] [sum (float)] [sum_squares
 * (float)]
 */
static void populate_float_vec_to_sq8_with_metadata(uint8_t *v, size_t dim,
                                                    bool should_normalize = false, int seed = 1234,
                                                    float min = -1.0f, float max = 1.0f) {
    std::vector<float> vec(dim);
    populate_float_vec(vec.data(), dim, seed, min, max);
    if (should_normalize) {
        spaces::GetNormalizeFunc<float>()(vec.data(), dim);
    }
    quantize_float_vec_to_sq8_with_metadata(vec.data(), dim, v);
}

template <typename datatype>
float integral_compute_norm(const datatype *vec, size_t dim) {
    return spaces::IntegralType_ComputeNorm<datatype>(vec, dim);
}

} // namespace test_utils
