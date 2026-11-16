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

namespace vecsim_types {

// Stored layout: packed Bits-wide codes, then the FP32 scale and the FP32 mean-error delta.
template <unsigned Bits>
struct tq {
    static_assert(Bits == 2 || Bits == 3);
    static constexpr size_t data_bytes_count(size_t dim) { return (dim * Bits + 7) / 8; }
    static constexpr size_t storage_bytes_count(size_t dim) {
        return data_bytes_count(dim) + 2 * sizeof(float);
    }
};

using tq2 = tq<2>;
using tq3 = tq<3>;

} // namespace vecsim_types
