/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstdint>
#include "VecSim/types/quantized_metadata.h"

namespace vecsim_types {

// Two scalar-quantized coordinates per byte, followed by the same metadata as SQ8.
struct sq4 : quantized_metadata {
    using value_type = uint8_t;
    static constexpr uint8_t max_code = 15;
    static constexpr unsigned bits_per_code = 4;

    static constexpr size_t data_bytes_count(size_t dim) { return dim / 2 + dim % 2; }

    static uint8_t get_code(const uint8_t *data, size_t index) {
        return (data[index / 2] >> ((index % 2) * bits_per_code)) & max_code;
    }

    // Coordinates are stored in ascending order. Starting each byte with an assignment allows
    // in-place preprocessing and leaves the unused high nibble zero for odd dimensions.
    static void set_code(uint8_t *data, size_t index, uint8_t code) {
        if (index % 2 == 0) {
            data[index / 2] = code & max_code;
        } else {
            data[index / 2] = (data[index / 2] & max_code) | ((code & max_code) << bits_per_code);
        }
    }

    // Dot product of two packed code arrays: sum(q1_i * q2_i). Each byte is loaded once and both
    // nibbles are used. The 64-bit integer sum is exact for any dimension.
    // Used by scalar SQ4-to-SQ4 IP and L2 kernels; SIMD kernels compute the dot product separately.
    static float code_dot(const uint8_t *a, const uint8_t *b, size_t dim) {
        const size_t full_bytes = dim / 2;
        uint64_t sum = 0;
        for (size_t j = 0; j < full_bytes; j++) {
            const uint64_t a_byte = a[j], b_byte = b[j];
            sum += (a_byte & max_code) * (b_byte & max_code) +
                   (a_byte >> bits_per_code) * (b_byte >> bits_per_code);
        }
        if (dim % 2) {
            // Odd dim: the last byte holds only a low nibble.
            sum += uint64_t(a[full_bytes] & max_code) * uint64_t(b[full_bytes] & max_code);
        }
        return static_cast<float>(sum);
    }

    // Dot product of a packed code array with FP32 values: sum(q_i * y_i). Each byte is loaded
    // once; the low nibble pairs with y[2j] and the high nibble with y[2j + 1], in two independent
    // accumulators for instruction-level parallelism.
    static float code_dot_fp32(const uint8_t *codes, const float *y, size_t dim) {
        const size_t full_bytes = dim / 2;
        float sum_low = 0, sum_high = 0;
        for (size_t j = 0; j < full_bytes; j++) {
            const uint8_t byte = codes[j];
            sum_low += static_cast<float>(byte & max_code) * y[2 * j];
            sum_high += static_cast<float>(byte >> bits_per_code) * y[2 * j + 1];
        }
        if (dim % 2) {
            // Odd dim: the last byte holds only a low nibble.
            sum_low += static_cast<float>(codes[full_bytes] & max_code) * y[dim - 1];
        }
        return sum_low + sum_high;
    }

    template <VecSimMetric Metric, bool WithNorm = false>
    static constexpr size_t storage_bytes_count(size_t dim) {
        return data_bytes_count(dim) + storage_metadata_count<Metric, WithNorm>() * sizeof(float);
    }
};

} // namespace vecsim_types
