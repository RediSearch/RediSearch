/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "compute_norm.h"
#include <cmath>
#include <cstring>
#include <vector>

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

namespace spaces {

template <typename DataType>
static inline void normalizeVector_imp(void *vec, const size_t dim) {
    DataType *input_vector = (DataType *)vec;
    // Cast to double to avoid float overflow.
    double sum = 0;

    for (size_t i = 0; i < dim; i++) {
        sum += (double)input_vector[i] * (double)input_vector[i];
    }
    DataType norm = sqrt(sum);

    for (size_t i = 0; i < dim; i++) {
        input_vector[i] = input_vector[i] / norm;
    }
}

template <bool is_little>
static inline void bfloat16_normalizeVector(void *vec, const size_t dim) {
    bfloat16 *input_vector = (bfloat16 *)vec;

    std::vector<float> f32_tmp(dim);

    float sum = 0;

    for (size_t i = 0; i < dim; i++) {
        float val = vecsim_types::bfloat16_to_float32<is_little>(input_vector[i]);
        f32_tmp[i] = val;
        sum += val * val;
    }

    float norm = sqrt(sum);

    for (size_t i = 0; i < dim; i++) {
        input_vector[i] = vecsim_types::float_to_bf16(f32_tmp[i] / norm);
    }
}

static inline void float16_normalizeVector(void *vec, const size_t dim) {
    float16 *input_vector = (float16 *)vec;

    std::vector<float> f32_tmp(dim);

    float sum = 0;

    for (size_t i = 0; i < dim; i++) {
        float val = vecsim_types::FP16_to_FP32(input_vector[i]);
        f32_tmp[i] = val;
        sum += val * val;
    }

    float norm = sqrt(sum);

    for (size_t i = 0; i < dim; i++) {
        input_vector[i] = vecsim_types::FP32_to_FP16(f32_tmp[i] / norm);
    }
}

template <typename DataType>
static inline void integer_normalizeVector(void *vec, const size_t dim) {
    DataType *input_vector = static_cast<DataType *>(vec);

    float norm = IntegralType_ComputeNorm<DataType>(input_vector, dim);

    // The norm follows a one-byte element payload and may be unaligned for odd dimensions.
    std::memcpy(input_vector + dim, &norm, sizeof(norm));
}

} // namespace spaces
