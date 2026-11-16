/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "vecsim/spaces/sq8_fp64.h"
#include "vecsim/spaces/quantized_kernels.h"
#include "vecsim/types/sq8_fp64.h"
#include "vecsim/utils/alignment.h"

namespace spaces {
namespace {
double metadata(const void *blob, size_t dim, size_t index) {
    return load_unaligned<double>(static_cast<const uint8_t *>(blob) +
                                  vecsim_types::sq8_fp64::data_bytes_count(dim) +
                                  index * sizeof(double));
}

double storedL2(const void *left, const void *right, size_t dim) {
    const double left_delta = metadata(left, dim, vecsim_types::sq8_fp64::DELTA);
    const double right_delta = metadata(right, dim, vecsim_types::sq8_fp64::DELTA);
    // Put the larger delta first so a constant vector's delta cannot erase a smaller step.
    const bool reverse = left_delta < right_delta;
    const auto *const a = static_cast<const uint8_t *>(reverse ? right : left);
    const auto *const b = static_cast<const uint8_t *>(reverse ? left : right);
    const double min_a = metadata(a, dim, vecsim_types::sq8_fp64::MIN_VAL);
    const double delta_a = reverse ? right_delta : left_delta;
    const double min_b = metadata(b, dim, vecsim_types::sq8_fp64::MIN_VAL);
    const double delta_b = reverse ? left_delta : right_delta;
    double result = 0.0;
    for (size_t i = 0; i < dim; ++i) {
        const double residual =
            (min_a - min_b) + (delta_a - delta_b) * a[i] + delta_b * (int(a[i]) - int(b[i]));
        result += residual * residual;
    }
    return result;
}

template <bool Asymmetric>
double innerProduct(const void *left, const void *right, size_t dim) {
    const auto *const a = static_cast<const uint8_t *>(left);
    const auto *const b = static_cast<const uint8_t *>(right);
    const double min_a = metadata(left, dim, vecsim_types::sq8_fp64::MIN_VAL);
    const double delta_a = metadata(left, dim, vecsim_types::sq8_fp64::DELTA);
    const double min_b = Asymmetric ? 0.0 : metadata(right, dim, vecsim_types::sq8_fp64::MIN_VAL);
    const double delta_b = Asymmetric ? 0.0 : metadata(right, dim, vecsim_types::sq8_fp64::DELTA);
    double result = 0.0;
    for (size_t i = 0; i < dim; ++i) {
        const double y =
            Asymmetric ? load_unaligned<double>(b + i * sizeof(double)) : min_b + delta_b * b[i];
        result += (min_a + delta_a * a[i]) * y;
    }
    return 1.0 - result;
}
} // namespace

double L2_SQ8FP64(const void *left, const void *right, size_t dim) {
    return storedL2(left, right, dim);
}
double L2_SQ8FP64_FP64(const void *left, const void *right, size_t dim) {
    return quantized::StoredQueryL2<vecsim_types::sq8_fp64>(left, right, dim);
}
double IP_SQ8FP64(const void *left, const void *right, size_t dim) {
    return innerProduct<false>(left, right, dim);
}
double IP_SQ8FP64_FP64(const void *left, const void *right, size_t dim) {
    return innerProduct<true>(left, right, dim);
}
} // namespace spaces
