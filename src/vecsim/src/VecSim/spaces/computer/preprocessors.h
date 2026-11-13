/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <type_traits>
#include <variant>

#include "VecSim/memory/vecsim_base.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/memory/memory_utils.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/utils/vecsim_stl.h"

class PreprocessorInterface : public VecsimBaseObject {
public:
    PreprocessorInterface(std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(allocator) {}
    // Combined preprocessing into both storage and query blobs. storage_alignment applies to any
    // newly allocated storage blob; query_alignment applies to any newly allocated query blob.
    // Implementations that allocate a single shared buffer for both must align it to satisfy both
    // requirements (use combineAlignments).
    virtual void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                            size_t &storage_blob_size, size_t &query_blob_size,
                            unsigned char storage_alignment,
                            unsigned char query_alignment) const = 0;
    virtual void preprocessForStorage(const void *original_blob, void *&storage_blob,
                                      size_t &input_blob_size,
                                      unsigned char storage_alignment) const = 0;
    virtual void preprocessQuery(const void *original_blob, void *&query_blob,
                                 size_t &input_blob_size, unsigned char query_alignment) const = 0;
    virtual void preprocessStorageInPlace(void *original_blob, size_t input_blob_size) const = 0;
};

template <typename DataType>
class CosinePreprocessor : public PreprocessorInterface {
public:
    // This preprocessor requires that storage_blob and query_blob have identical memory sizes
    // both before processing (as input) and after preprocessing completes.
    CosinePreprocessor(std::shared_ptr<VecSimAllocator> allocator, size_t dim,
                       size_t processed_bytes_count)
        : PreprocessorInterface(allocator), normalize_func(spaces::GetNormalizeFunc<DataType>()),
          dim(dim), processed_bytes_count(processed_bytes_count) {}

    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        // CosinePreprocessor produces equally-sized storage and query blobs.
        assert(storage_blob_size == query_blob_size);
        // see assert docs below
        assert(storage_blob == nullptr || storage_blob_size == processed_bytes_count);
        assert(query_blob == nullptr || query_blob_size == processed_bytes_count);

        // Case 1: Blobs are different (one might be null, or both are allocated and processed
        // separately).
        if (storage_blob != query_blob) {
            // If one of them is null, allocate memory for it and copy the original_blob to it.
            if (storage_blob == nullptr) {
                storage_blob =
                    this->allocator->allocate_aligned(processed_bytes_count, storage_alignment);
                memcpy(storage_blob, original_blob, storage_blob_size);
            } else if (query_blob == nullptr) {
                query_blob =
                    this->allocator->allocate_aligned(processed_bytes_count, query_alignment);
                memcpy(query_blob, original_blob, query_blob_size);
            }

            // Normalize both blobs.
            normalize_func(storage_blob, this->dim);
            normalize_func(query_blob, this->dim);
        } else { // Case 2: Blobs are the same (either both are null or processed in the same way).
            if (query_blob == nullptr) {
                // Single buffer must satisfy both the storage and the query alignment hint.
                const unsigned char shared_alignment =
                    spaces::combineAlignments(storage_alignment, query_alignment);
                query_blob =
                    this->allocator->allocate_aligned(processed_bytes_count, shared_alignment);
                memcpy(query_blob, original_blob, storage_blob_size);
                storage_blob = query_blob;
            }
            // normalize one of them (since they point to the same memory).
            normalize_func(query_blob, this->dim);
        }

        storage_blob_size = processed_bytes_count;
        query_blob_size = processed_bytes_count;
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        // The assert here verifies that if a blob was allocated by a previous preprocessor, its
        // size matches our expected processed size, allowing in-place normalization. Dynamic
        // resizing is intentionally not supported: handling it would require runtime size checks
        // and reallocation logic in a hot path, and no current caller needs it.
        assert(blob == nullptr || input_blob_size == processed_bytes_count);

        if (blob == nullptr) {
            blob = this->allocator->allocate_aligned(processed_bytes_count, storage_alignment);
            memcpy(blob, original_blob, input_blob_size);
        }
        normalize_func(blob, this->dim);
        input_blob_size = processed_bytes_count;
    }

    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        // see assert docs in preprocess
        assert(blob == nullptr || input_blob_size == processed_bytes_count);
        if (blob == nullptr) {
            blob = this->allocator->allocate_aligned(processed_bytes_count, alignment);
            memcpy(blob, original_blob, input_blob_size);
        }
        normalize_func(blob, this->dim);
        input_blob_size = processed_bytes_count;
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {
        assert(blob);
        assert(input_blob_size == this->processed_bytes_count);
        normalize_func(blob, this->dim);
    }

private:
    spaces::normalizeVector_f<DataType> normalize_func;
    const size_t dim;
    const size_t processed_bytes_count;
};

/*
 * QuantPreprocessor is a preprocessor that quantizes storage vectors from DataType to a
 * lower precision representation using OUTPUT_TYPE (uint8_t).
 * Query vectors remain as DataType for asymmetric distance computation.
 *
 * The quantized storage blob contains the quantized values along with metadata (min value,
 * scaling factor, and precomputed sums for reconstruction) in a single contiguous blob.
 * The quantization is done by finding the minimum and maximum values of the input vector,
 * and then scaling the values to fit in the range of [0, 255].
 *
 * Storage layout:
 * | quantized_values[dim] | min_val | delta | x_sum | (x_sum_squares for L2 only) |
 * where, writing x_r[i] = min_val + delta * a[i] for the value a blob actually represents:
 * x_sum        = Σx_r[i]:  sum of the reconstructed values,
 * x_sum_squares = Σx_r[i]²: sum of their squares.
 *
 * These describe x_r, not the input x. Every formula below is written in terms of x_r, because
 * that is what a quantized blob holds, so sums over the input would not satisfy them: the gap is
 * the quantization error, about 0.4% of ||x||², which is larger than the distance between two
 * similar vectors and made L2 come out negative.
 *
 * Storage metadata is always FP32 (independent of DataType) to match the asymmetric distance
 * kernels. The quantized blob size is:
 * - For L2:        dim * sizeof(OUTPUT_TYPE) + 4 * sizeof(float)
 * - For IP/Cosine: dim * sizeof(OUTPUT_TYPE) + 3 * sizeof(float)
 *
 * Reconstruction formulas:
 * Given quantized value q_i, the original value is reconstructed as:
 *   x_i ≈ min + delta * q_i
 *
 * Query processing:
 * The query vector is not quantized. It remains in DataType width (FP32 stays FP32, FP16 stays
 * FP16). Normalized L2 stores y' = y - mean; all other modes store y unchanged. We precompute and
 * store metric-specific FP32 values from the query body to accelerate asymmetric distance
 * computation:
 * - For IP/Cosine: y_sum = Σy_i (sum of query values)
 * - For L2: y_sum = Σy_i (sum of query values), y_sum_squares = Σy_i² (sum of squared query values)
 *
 * Query blob layout:
 * - For IP/Cosine: | query_values[dim] | y_sum |
 * - For L2:        | query_values[dim] | y_sum | y_sum_squares |
 *
 * Query metadata is always FP32. The query blob size is:
 * - For IP/Cosine: dim * sizeof(DataType) + 1 * sizeof(float)
 * - For L2:        dim * sizeof(DataType) + 2 * sizeof(float)
 *
 * Note: when DataType is float16 the metadata region may not be 4-byte aligned; both writes
 * and reads of metadata must therefore go through memcpy.
 *
 * === Asymmetric distance (storage x quantized, query y in DataType) ===
 *
 * For IP/Cosine:
 *   IP(x, y) = Σ(x_i * y_i)
 *            ≈ Σ((min + delta * q_i) * y_i)
 *            = min * Σy_i + delta * Σ(q_i * y_i)
 *            = min * y_sum + delta * quantized_dot_product
 *   where y_sum = Σy_i is precomputed and stored in the query blob.
 *
 * For L2:
 *   ||x_r - y||² = Σx_r[i]² - 2*Σ(x_r[i] * y_i) + Σy_i²
 *                = x_sum_squares - 2 * IP(x_r, y) + y_sum_squares
 *   where:
 *     - x_sum_squares = Σx_r[i]² is precomputed and stored in the storage blob. A query is not
 *       quantized, so y_sum_squares is Σy_i² over the query itself.
 *     - IP(x, y) is computed using the formula above
 *     - y_sum_squares = Σy_i² is precomputed and stored in the query blob
 *   For normalized L2, x and y in this formula are the centered values x' and y'; their distance
 *   is identical to the distance between the original vectors.
 *
 * === Symmetric distance (both x and y are quantized) ===
 *
 * For IP/Cosine:
 *   IP(x, y) = Σ((min_x + delta_x * qx_i) * (min_y + delta_y * qy_i))
 *            = dim * min_x * min_y
 *              + min_x * delta_y * Σqy_i + min_y * delta_x * Σqx_i
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *            = dim * min_x * min_y
 *              + min_x * (sum_y - dim * min_y) + min_y * (sum_x - dim * min_x)
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *            = min_x * sum_y + min_y * sum_x - dim * min_x * min_y
 *              + delta_x * delta_y * Σ(qx_i * qy_i)
 *   where:
 *     - sum_x, sum_y are precomputed sums of the values represented by each blob
 *     - Σqx_i = (sum_x - dim * min_x) / delta_x  (exact, since sum_x is derived from Σqx_i)
 *     - Σqy_i = (sum_y - dim * min_y) / delta_y
 *
 * For L2:
 *   ||x - y||² = sum_sq_x + sum_sq_y - 2 * IP(x, y)
 *   where sum_sq_x, sum_sq_y are precomputed sums of the squared represented values.
 */
// Input types accepted by QuantPreprocessor. Opt-in via std::same_as so unrelated types
// (e.g. integers, double, bfloat16) are rejected at the template head with a named constraint.
template <typename T>
concept QuantInput = std::same_as<T, float> || std::same_as<T, vecsim_types::float16>;

// Convert a single input element to FP32 for accumulation/comparison. Identity for float,
// FP16 -> FP32 widening for vecsim_types::float16.
template <QuantInput T>
static inline float to_fp32(T x) {
    if constexpr (std::is_same_v<T, vecsim_types::float16>) {
        return vecsim_types::FP16_to_FP32(x);
    } else {
        return x;
    }
}

template <QuantInput T>
static inline T from_fp32(float x) {
    if constexpr (std::is_same_v<T, vecsim_types::float16>) {
        return vecsim_types::FP32_to_FP16(x);
    } else {
        return x;
    }
}

template <QuantInput DataType, VecSimMetric Metric, bool WithNorm = false>
class QuantPreprocessor : public PreprocessorInterface {
public:
    // Center L2 queries in FP32 even when the original input is FP16.
    using QueryType = std::conditional_t<WithNorm && Metric == VecSimMetric_L2, float, DataType>;

private:
    using OUTPUT_TYPE = uint8_t;
    using MetadataType = float; // SQ8 metadata is always FP32 (see class doc).
    using sq8 = vecsim_types::sq8;

    static_assert(Metric == VecSimMetric_L2 || Metric == VecSimMetric_IP ||
                      Metric == VecSimMetric_Cosine,
                  "QuantPreprocessor only supports L2, IP and Cosine metrics");
    static_assert(!WithNorm || Metric != VecSimMetric_Cosine,
                  "WithNorm does not support Cosine metric.");

    // Helper function to perform quantization. This function is used by the storage preprocessing
    // methods.
    void quantize(const DataType *input, OUTPUT_TYPE *quantized) const {
        assert(input && quantized);

        float x_mean_ip = 0.0f; // only used for WithNorm
        auto [min_val, max_val] = find_min_max(input, x_mean_ip);

        // Calculate scaling factor (typed as MetadataType because they end up as metadata).
        const MetadataType diff = (max_val - min_val);
        const MetadataType delta = (diff == 0.0f) ? MetadataType{1} : diff / MetadataType{255};
        const MetadataType inv_delta = MetadataType{1} / delta;

        // Saturating conversion. Casting a float to uint8_t is undefined when the truncated value
        // does not fit, and std::round does not help: it returns 300.0 for 300.0 and NaN for NaN,
        // both of which then convert with no defined result. On x86 an out-of-range value becomes
        // the integer indefinite 0x80000000, so a component above the range silently quantizes to
        // 0, the bottom of the scale. Bounding first makes every input defined, and `+ 0.5` then
        // truncating matches std::round for non-negative values without its libm call.
        //
        // Only the conversion is guarded here. The FP32 arithmetic feeding it keeps whatever
        // overflow behavior it had; see the tech debt ticket for the intermediate cases.
        const auto to_byte = [](MetadataType scaled) -> OUTPUT_TYPE {
            if (!(scaled > MetadataType{0})) {
                return OUTPUT_TYPE{0}; // zero, negative, -inf, or NaN
            }
            if (scaled >= MetadataType{255}) {
                return OUTPUT_TYPE{255}; // includes +inf
            }
            return static_cast<OUTPUT_TYPE>(scaled + MetadataType{0.5});
        };

        // Sum the quantized bytes, not the input values. Every kernel term is written in terms of
        // the reconstruction x_r[i] = min + delta * a[i], so the stored sums have to describe x_r
        // as well. Summing the input instead leaves a systematic mismatch of about 0.4% of ||x||^2,
        // which is larger than a small true distance and made L2 come out negative: for two
        // near-duplicate vectors at dim 128 the reconstruction distance is 2.93e-04 and the kernels
        // returned -1.49e-01. The byte sums are exact integers, so the derivation below is the only
        // place rounding enters.
        //
        // 4 independent accumulators each, so the unrolled loop keeps four dependency chains.
        uint32_t s0{}, s1{}, s2{}, s3{};
        // 64-bit: 65025 * 65536 leaves under 1% of UINT32_MAX, and overflow here would corrupt
        // the metadata rather than round it. This is the write path, once per vector.
        uint64_t q0{}, q1{}, q2{}, q3{}; // only used for L2

        size_t i = 0;
        // round dim down to the nearest multiple of 4
        size_t dim_round_down = this->dim & ~size_t(3);

        // Quantize the values
        for (; i < dim_round_down; i += 4) {
            // Load once (widened to FP32 if DataType is FP16).
            const float x0 = transformed_value(input, i);
            const float x1 = transformed_value(input, i + 1);
            const float x2 = transformed_value(input, i + 2);
            const float x3 = transformed_value(input, i + 3);
            // We know (input - min) => 0
            // If min == max, all values are the same and should be quantized to 0.
            // reconstruction will yield the same original value for all vectors.
            const OUTPUT_TYPE a0 = to_byte((x0 - min_val) * inv_delta);
            const OUTPUT_TYPE a1 = to_byte((x1 - min_val) * inv_delta);
            const OUTPUT_TYPE a2 = to_byte((x2 - min_val) * inv_delta);
            const OUTPUT_TYPE a3 = to_byte((x3 - min_val) * inv_delta);
            quantized[i] = a0;
            quantized[i + 1] = a1;
            quantized[i + 2] = a2;
            quantized[i + 3] = a3;

            s0 += a0;
            s1 += a1;
            s2 += a2;
            s3 += a3;

            if constexpr (Metric == VecSimMetric_L2) {
                q0 += uint64_t{a0} * a0;
                q1 += uint64_t{a1} * a1;
                q2 += uint64_t{a2} * a2;
                q3 += uint64_t{a3} * a3;
            }
        }

        // Tail: 0..3 remaining elements (still the same pass, just finishing work).
        uint32_t q_sum = (s0 + s1) + (s2 + s3);
        uint64_t q_sum_squares{};
        if constexpr (Metric == VecSimMetric_L2) {
            q_sum_squares = (q0 + q1) + (q2 + q3);
        }

        for (; i < this->dim; ++i) {
            const float x = transformed_value(input, i);
            const OUTPUT_TYPE a = to_byte((x - min_val) * inv_delta);
            quantized[i] = a;
            q_sum += a;
            if constexpr (Metric == VecSimMetric_L2) {
                q_sum_squares += uint64_t{a} * a;
            }
        }

        // Derive the reconstruction sums from the exact byte sums, in double so the expansion does
        // not lose the cross term, then store FP32 as before. The slot layout and types are
        // unchanged; only what the numbers describe changes.
        //   sum         = sum(x_r[i])   = dim*min + delta*q_sum
        //   sum_squares = sum(x_r[i]^2) = dim*min^2 + 2*min*delta*q_sum + delta^2*q_sum_squares
        const double d_min = min_val, d_delta = delta, d_dim = static_cast<double>(this->dim);
        const MetadataType sum = static_cast<MetadataType>(d_dim * d_min + d_delta * q_sum);
        MetadataType sum_squares{};
        if constexpr (Metric == VecSimMetric_L2) {
            sum_squares =
                static_cast<MetadataType>(d_dim * d_min * d_min + 2.0 * d_min * d_delta * q_sum +
                                          d_delta * d_delta * q_sum_squares);
        }

        // Metadata uses MetadataType. Use memcpy because the metadata offset
        // (dim * sizeof(uint8_t)) is not guaranteed to be sizeof(MetadataType)-aligned.
        void *meta_dst = quantized + this->dim;
        MetadataType buf[5] = {min_val, delta, sum};
        size_t n = 3;
        if constexpr (Metric == VecSimMetric_L2)
            buf[n++] = sum_squares;
        if constexpr (WithNorm && Metric == VecSimMetric_IP)
            buf[n++] = x_mean_ip;
        memcpy(meta_dst, buf, n * sizeof(MetadataType));
    }

    // Computes and writes query metadata (FP32) in a single pass over the query values.
    // Without norm:
    //   For IP/Cosine: writes y_sum = Σy_i
    //   For L2: writes y_sum = Σy_i and y_sum_squares = Σy_i²
    // With norm and Metric == IP: additionally appends y_mean_ip = Σ(mean_i * original_y_i).
    // Normalized L2 needs no extra metadata; the mean terms cancel exactly during centering.
    // For normalized L2, values contains y - mean while original_input contains y.
    // The output pointer addresses the metadata region after the query body and may not be
    // 4-byte aligned (e.g. FP16 query body with odd dim), so writes go through memcpy.
    void assign_query_metadata(const QueryType *values, const DataType *original_input,
                               void *output_metadata) const {

        // Accumulators are FP32 to preserve precision for FP16 inputs.
        // 4 independent accumulators for sum
        float s0{}, s1{}, s2{}, s3{};
        // 4 independent accumulators for sum of squares (only used for L2)
        float q0{}, q1{}, q2{}, q3{};
        // 4 independent accumulators for y_mean_ip (only used for WithNorm)
        float m0{}, m1{}, m2{}, m3{};

        size_t i = 0;
        // round dim down to the nearest multiple of 4
        size_t dim_round_down = this->dim & ~size_t(3);

        for (; i < dim_round_down; i += 4) {
            const float y0 = to_fp32<QueryType>(values[i]);
            const float y1 = to_fp32<QueryType>(values[i + 1]);
            const float y2 = to_fp32<QueryType>(values[i + 2]);
            const float y3 = to_fp32<QueryType>(values[i + 3]);

            s0 += y0;
            s1 += y1;
            s2 += y2;
            s3 += y3;

            if constexpr (Metric == VecSimMetric_L2) {
                q0 += y0 * y0;
                q1 += y1 * y1;
                q2 += y2 * y2;
                q3 += y3 * y3;
            }

            if constexpr (WithNorm && Metric == VecSimMetric_IP) {
                m0 += mean[i] * to_fp32<DataType>(original_input[i]);
                m1 += mean[i + 1] * to_fp32<DataType>(original_input[i + 1]);
                m2 += mean[i + 2] * to_fp32<DataType>(original_input[i + 2]);
                m3 += mean[i + 3] * to_fp32<DataType>(original_input[i + 3]);
            }
        }

        // Sum/sum_squares/y_mean_ip become metadata, so they are MetadataType.
        MetadataType sum = (s0 + s1) + (s2 + s3);
        MetadataType sum_squares = (q0 + q1) + (q2 + q3);
        MetadataType y_mean_ip = (m0 + m1) + (m2 + m3);

        // Tail: handle remaining elements
        for (; i < this->dim; ++i) {
            const float y = to_fp32<QueryType>(values[i]);
            sum += y;
            if constexpr (Metric == VecSimMetric_L2) {
                sum_squares += y * y;
            }
            if constexpr (WithNorm && Metric == VecSimMetric_IP) {
                y_mean_ip += mean[i] * to_fp32<DataType>(original_input[i]);
            }
        }

        // Metadata uses MetadataType. Use memcpy because the metadata offset (after the query
        // body of dim * sizeof(QueryType)) is not guaranteed to be sizeof(MetadataType)-aligned
        // when DataType is float16 and dim is odd.
        MetadataType buf[3] = {sum};
        size_t n = 1;
        if constexpr (Metric == VecSimMetric_L2)
            buf[n++] = sum_squares;
        if constexpr (WithNorm && Metric == VecSimMetric_IP)
            buf[n++] = y_mean_ip;
        memcpy(output_metadata, buf, n * sizeof(MetadataType));
    }

public:
    // Install the final mean before any vectors are stored, with queries excluded by the caller.
    void setMean(std::span<const float> values) noexcept
        requires(WithNorm)
    {
        assert(values.size() == mean.size());
        std::copy(values.begin(), values.end(), mean.begin());
    }

    // Standard constructor (WithNorm == false): no mean vector.
    QuantPreprocessor(std::shared_ptr<VecSimAllocator> allocator, size_t dim)
        requires(!WithNorm)
        : PreprocessorInterface(allocator), dim(dim),
          storage_bytes_count(sq8::storage_bytes_count<Metric>(dim)),
          query_bytes_count(dim * sizeof(QueryType) +
                            sq8::query_metadata_count<Metric>() * sizeof(MetadataType)) {}

    // WithNorm constructor: accepts a pre-computed mean vector (FP32, length == dim).
    QuantPreprocessor(std::shared_ptr<VecSimAllocator> allocator, size_t dim,
                      const vecsim_stl::vector<float> &mean_vec)
        requires(WithNorm)
        : PreprocessorInterface(allocator), mean(mean_vec), dim(dim),
          storage_bytes_count(sq8::storage_bytes_count<Metric, WithNorm>(dim)),
          query_bytes_count(dim * sizeof(QueryType) +
                            sq8::query_metadata_count<Metric, WithNorm>() * sizeof(MetadataType)) {
        assert(this->mean.size() == dim && "mean vector size must equal dim");
    }

    /**
     * Preprocesses the original blob into separate storage and query blobs.
     *
     * Storage vectors are quantized to uint8_t values, with metadata (min, delta, sum, and
     * sum_squares for L2) appended for distance reconstruction.
     *
     * Query vectors use QueryType for asymmetric distance computation, with a precomputed
     * sum (for IP/Cosine) or sum of squares (for L2) appended for efficient distance calculation.
     *
     * Possible scenarios (currently only CASE 1 is implemented):
     * - CASE 1: STORAGE BLOB AND QUERY BLOB NEED ALLOCATION (storage_blob == query_blob == nullptr)
     * - CASE 2: STORAGE BLOB EXISTS (storage_blob != nullptr)
     *   - CASE 2A: STORAGE BLOB EXISTS and its size is insufficient
     * (storage_blob_size < required_size) - reallocate storage
     *   - CASE 2B: STORAGE AND QUERY SHARE MEMORY (storage_blob == query_blob != nullptr) -
     * reallocate storage
     *   - CASE 2C: SEPARATE STORAGE AND QUERY BLOBS (storage_blob != query_blob) - quantize storage
     * in-place
     */
    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        // CASE 1: STORAGE BLOB NEEDS ALLOCATION - the only implemented case
        assert(!storage_blob && "CASE 1: storage_blob must be nullptr");
        assert(!query_blob && "CASE 1: query_blob must be nullptr");

        // storage_blob_size and query_blob_size must point to different memory slots.
        assert(&storage_blob_size != &query_blob_size);

        // CASE 2A: STORAGE BLOB EXISTS and its size is insufficient - not implemented
        // storage_blob && storage_blob_size < required_size
        // CASE 2B: STORAGE EXISTS AND EQUALS QUERY BLOB - not implemented
        // storage_blob && storage_blob == query_blob
        // (if we want to handle this, we need to separate the blobs)
        // CASE 2C: SEPARATE STORAGE AND QUERY BLOBS - not implemented
        // storage_blob && storage_blob != query_blob
        // We can quantize the storage blob in-place (if we already checked storage_blob_size is
        // sufficient)

        preprocessForStorage(original_blob, storage_blob, storage_blob_size, storage_alignment);
        preprocessQuery(original_blob, query_blob, query_blob_size, query_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        assert(!blob && "storage_blob must be nullptr");

        blob = this->allocator->allocate_aligned(storage_bytes_count, storage_alignment);
        // Cast to appropriate types
        const DataType *input = static_cast<const DataType *>(original_blob);
        OUTPUT_TYPE *quantized = static_cast<OUTPUT_TYPE *>(blob);
        quantize(input, quantized);

        input_blob_size = storage_bytes_count;
    }

    /**
     * Preprocesses the query vector for asymmetric distance computation.
     *
     * The query blob contains QueryType values followed by FP32 precomputed values. Normalized L2
     * stores centered query values (y - mean), so its distance kernel directly computes
     * ||(x - mean) - (y - mean)||² without cancellation-prone correction terms. Other modes keep
     * the original query values.
     *
     * - For IP/Cosine: y_sum = Σy_i (sum of query values)
     * - For L2: y_sum = Σy_i (sum of query values), y_sum_squares = Σy_i² (sum of squared query
     *                                                                      values)
     *
     * Query blob layout:
     * - For IP/Cosine: | query_values[dim] | y_sum |
     * - For L2:        | query_values[dim] | y_sum | y_sum_squares |
     *
     * Query blob size:
     * - For IP/Cosine: dim * sizeof(QueryType) + 1 * sizeof(float)
     * - For L2:        dim * sizeof(QueryType) + 2 * sizeof(float)
     */
    void preprocessQuery(const void *original_blob, void *&blob, size_t &query_blob_size,
                         unsigned char alignment) const override {
        assert(!blob && "query_blob must be nullptr");

        // Allocate aligned memory for the query blob
        blob = this->allocator->allocate_aligned(this->query_bytes_count, alignment);
        const size_t body_bytes = this->dim * sizeof(QueryType);
        const DataType *input = static_cast<const DataType *>(original_blob);
        QueryType *query_values = static_cast<QueryType *>(blob);
        if constexpr (WithNorm && Metric == VecSimMetric_L2) {
            for (size_t i = 0; i < this->dim; ++i) {
                query_values[i] = to_fp32<DataType>(input[i]) - this->mean[i];
            }
        } else {
            memcpy(query_values, original_blob, body_bytes);
        }

        // Compute and write FP32 query metadata after the query body. The metadata offset is
        // body_bytes, which is not guaranteed to be 4-byte aligned for FP16 query bodies.
        void *metadata_dst = static_cast<uint8_t *>(blob) + body_bytes;
        assign_query_metadata(query_values, input, metadata_dst);

        query_blob_size = this->query_bytes_count;
    }

    void preprocessStorageInPlace(void *original_blob, size_t input_blob_size) const override {
        assert(original_blob);
        assert(input_blob_size >= storage_bytes_count &&
               "Input buffer too small for in-place quantization");

        quantize(static_cast<const DataType *>(original_blob),
                 static_cast<OUTPUT_TYPE *>(original_blob));
    }

private:
    inline float transformed_value(const DataType *input, size_t i) const {
        float value = to_fp32<DataType>(input[i]);
        if constexpr (WithNorm) {
            value -= mean[i];
        }
        return value;
    }

    std::pair<float, float> find_min_max(const DataType *input, float &x_mean_ip) const {
        if constexpr (!WithNorm) {
            auto [min_it, max_it] = std::minmax_element(input, input + dim);
            return {to_fp32<DataType>(*min_it), to_fp32<DataType>(*max_it)};
        } else {
            const float first_input_value = to_fp32<DataType>(input[0]);
            float value = first_input_value - mean[0];
            float min_val = value;
            float max_val = value;
            // x_mean_ip only feeds the IP correction term (see DistanceCalculatorWithNorm); the
            // normalized L2 correction cancels the mean terms exactly, so skip the extra work.
            if constexpr (Metric == VecSimMetric_IP)
                x_mean_ip = first_input_value * mean[0];

            for (size_t i = 1; i < dim; ++i) {
                const float input_value = to_fp32<DataType>(input[i]);
                value = input_value - mean[i];
                min_val = std::min(min_val, value);
                max_val = std::max(max_val, value);
                if constexpr (Metric == VecSimMetric_IP)
                    x_mean_ip += input_value * mean[i];
            }
            return {min_val, max_val};
        }
    }

    // Mean vector for WithNorm=true; zero-size placeholder otherwise (no runtime overhead).
    [[no_unique_address]] std::conditional_t<WithNorm, vecsim_stl::vector<float>, std::monostate>
        mean;

    const size_t dim;
    const size_t storage_bytes_count;
    const size_t query_bytes_count;
};
