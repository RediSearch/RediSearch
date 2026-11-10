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
#include "VecSim/utils/alignment.h"

namespace spaces {
namespace quantized {

// Complete the symmetric stored-stored inner product from the integer code dot and stored sums.
// This ISA-neutral helper is shared by scalar and SIMD kernels. Internal linkage keeps each
// translation unit's body under its own ISA flags instead of merging target-specific definitions.
// The expression's operation order is part of the numerical contract: do not reassociate it or
// compile with -ffast-math / -Ofast.
template <typename StorageType>
static inline float Stored_InnerProduct_FromCodeDot(const uint8_t *a, const uint8_t *b, size_t dim,
                                                    float code_dot) {
    // Metadata follows byte payloads and is not necessarily float-aligned.
    const auto *params1 = a + StorageType::data_bytes_count(dim);
    const float min_val1 = load_unaligned<float>(params1 + StorageType::MIN_VAL * sizeof(float));
    const float delta1 = load_unaligned<float>(params1 + StorageType::DELTA * sizeof(float));
    const float sum1 = load_unaligned<float>(params1 + StorageType::SUM * sizeof(float));
    const auto *params2 = b + StorageType::data_bytes_count(dim);
    const float min_val2 = load_unaligned<float>(params2 + StorageType::MIN_VAL * sizeof(float));
    const float delta2 = load_unaligned<float>(params2 + StorageType::DELTA * sizeof(float));
    const float sum2 = load_unaligned<float>(params2 + StorageType::SUM * sizeof(float));

    return min_val1 * sum2 + min_val2 * sum1 - static_cast<float>(dim) * min_val1 * min_val2 +
           delta1 * delta2 * code_dot;
}

} // namespace quantized
} // namespace spaces
