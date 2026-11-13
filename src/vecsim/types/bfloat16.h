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
#include <cstring>
#include <cmath>

namespace vecsim_types {
struct bfloat16 {
    uint16_t val;
    bfloat16() = default;
    explicit constexpr bfloat16(uint16_t val) : val(val) {}
    operator uint16_t() const { return val; }
};

static inline bfloat16 float_to_bf16(const float ff) {
    uint32_t *p_f32 = (uint32_t *)&ff;
    uint32_t f32 = *p_f32;
    uint32_t lsb = (f32 >> 16) & 1;
    uint32_t round = lsb + 0x7FFF;
    f32 += round;
    return bfloat16(f32 >> 16);
}

template <bool is_little = true>
inline float bfloat16_to_float32(bfloat16 val) {
    size_t constexpr bytes_offset = is_little ? 1 : 0;
    float result = 0;
    bfloat16 *p_result = (bfloat16 *)&result + bytes_offset;
    *p_result = val;
    return result;
}

} // namespace vecsim_types
