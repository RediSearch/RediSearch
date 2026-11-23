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
#include <limits>
#include "vecsim/types/quantized_metadata.h"

namespace vecsim_types {

// SQ8 bytes followed by double reconstruction metadata; distinct from the FP32 SQ8 layout.
struct sq8_fp64 : quantized_metadata {
    using value_type = uint8_t;
    using metadata_type = double;
    static constexpr uint8_t max_code = std::numeric_limits<uint8_t>::max();
    static constexpr size_t data_bytes_count(size_t dim);
    static uint8_t get_code(const uint8_t *data, size_t index);
    static void set_code(uint8_t *data, size_t index, uint8_t code);
    static double prepare_delta(double delta);
    static double encode(double offset, double prepared);
    template <VecSimMetric Metric, bool WithNorm = false>
    static constexpr size_t storage_metadata_count();
    template <VecSimMetric Metric, bool WithNorm = false>
    static constexpr size_t query_metadata_count();
    template <VecSimMetric Metric, bool WithNorm = false>
    static constexpr size_t storage_bytes_count(size_t dim);
    template <VecSimMetric Metric>
    static constexpr size_t query_bytes_count(size_t dim);
};

constexpr size_t sq8_fp64::data_bytes_count(size_t dim) { return dim; }

inline uint8_t sq8_fp64::get_code(const uint8_t *data, size_t index) { return data[index]; }

inline void sq8_fp64::set_code(uint8_t *data, size_t index, uint8_t code) { data[index] = code; }

inline double sq8_fp64::prepare_delta(double delta) { return delta; }

// For tiny delta, 1 / delta can overflow; dividing directly avoids that.
inline double sq8_fp64::encode(double offset, double prepared) { return offset / prepared; }

template <VecSimMetric Metric, bool WithNorm>
constexpr size_t sq8_fp64::storage_metadata_count() {
    static_assert(!WithNorm, "FP64 quantization does not support mean training");
    return 2;
}

template <VecSimMetric Metric, bool WithNorm>
constexpr size_t sq8_fp64::query_metadata_count() {
    static_assert(!WithNorm, "FP64 quantization does not support mean training");
    return 0;
}

template <VecSimMetric Metric, bool WithNorm>
constexpr size_t sq8_fp64::storage_bytes_count(size_t dim) {
    return data_bytes_count(dim) + storage_metadata_count<Metric, WithNorm>() * sizeof(double);
}

template <VecSimMetric Metric>
constexpr size_t sq8_fp64::query_bytes_count(size_t dim) {
    return (dim + query_metadata_count<Metric>()) * sizeof(double);
}

} // namespace vecsim_types
