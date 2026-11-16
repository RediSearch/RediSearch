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
#include <algorithm>
#include <compare>
namespace vecsim_types {
struct float16 {
    uint16_t val;
    float16() = default;
    explicit constexpr float16(uint16_t val) : val(val) {}
    operator uint16_t() const { return val; }
};

inline float _interpret_as_float(uint32_t num) {
    void *num_ptr = &num;
    return *(float *)num_ptr;
}

inline int32_t _interpret_as_int(float num) {
    void *num_ptr = &num;
    return *(int32_t *)num_ptr;
}

static inline float FP16_to_FP32(float16 input) {
    // https://gist.github.com/2144712
    // Fabian "ryg" Giesen.

    const uint32_t shifted_exp = 0x7c00u << 13; // exponent mask after shift

    int32_t o = ((int32_t)(input & 0x7fffu)) << 13; // exponent/mantissa bits
    int32_t exp = shifted_exp & o;                  // just the exponent
    o += (int32_t)(127 - 15) << 23;                 // exponent adjust

    int32_t infnan_val = o + ((int32_t)(128 - 16) << 23);
    int32_t zerodenorm_val =
        _interpret_as_int(_interpret_as_float(o + (1u << 23)) - _interpret_as_float(113u << 23));
    int32_t reg_val = (exp == 0) ? zerodenorm_val : o;

    int32_t sign_bit = ((int32_t)(input & 0x8000u)) << 16;
    return _interpret_as_float(((exp == shifted_exp) ? infnan_val : reg_val) | sign_bit);
}

// Comparison operators that delegate to FP32 semantics. Required because the implicit
// conversion to uint16_t would otherwise compare the raw bit pattern, which is not a valid
// ordering for IEEE 754 values (the sign bit is the MSB, so negatives appear "larger" than
// positives, and same-signed magnitudes compare correctly only for non-negative values).
// These exact-match overloads take precedence over the implicit uint16_t conversion path.
inline std::partial_ordering operator<=>(float16 a, float16 b) {
    return FP16_to_FP32(a) <=> FP16_to_FP32(b);
}
inline bool operator==(float16 a, float16 b) { return FP16_to_FP32(a) == FP16_to_FP32(b); }

static inline float16 FP32_to_FP16(float input) {
    // via Fabian "ryg" Giesen.
    // https://gist.github.com/2156668
    uint32_t sign_mask = 0x80000000u;
    int32_t o;

    uint32_t fint = _interpret_as_int(input);
    uint32_t sign = fint & sign_mask;
    fint ^= sign;

    // NOTE all the integer compares in this function can be safely
    // compiled into signed compares since all operands are below
    // 0x80000000. Important if you want fast straight SSE2 code (since
    // there's no unsigned PCMPGTD).

    // Inf or NaN (all exponent bits set)
    // NaN->qNaN and Inf->Inf
    // unconditional assignment here, will override with right value for
    // the regular case below.
    uint32_t f32infty = 255u << 23;
    o = (fint > f32infty) ? 0x7e00u : 0x7c00u;

    // (De)normalized number or zero
    // update fint unconditionally to save the blending; we don't need it
    // anymore for the Inf/NaN case anyway.

    const uint32_t round_mask = ~0xfffu;
    const uint32_t magic = 15u << 23;

    // Shift exponent down, denormalize if necessary.
    // NOTE This represents half-float denormals using single
    // precision denormals.  The main reason to do this is that
    // there's no shift with per-lane variable shifts in SSE*, which
    // we'd otherwise need. It has some funky side effects though:
    // - This conversion will actually respect the FTZ (Flush To Zero)
    //   flag in MXCSR - if it's set, no half-float denormals will be
    //   generated. I'm honestly not sure whether this is good or
    //   bad. It's definitely interesting.
    // - If the underlying HW doesn't support denormals (not an issue
    //   with Intel CPUs, but might be a problem on GPUs or PS3 SPUs),
    //   you will always get flush-to-zero behavior. This is bad,
    //   unless you're on a CPU where you don't care.
    // - Denormals tend to be slow. FP32 denormals are rare in
    //   practice outside of things like recursive filters in DSP -
    //   not a typical half-float application. Whether FP16 denormals
    //   are rare in practice, I don't know. Whatever slow path your
    //   HW may or may not have for denormals, this may well hit it.
    float fscale = _interpret_as_float(fint & round_mask) * _interpret_as_float(magic);
    fscale = std::min(fscale, _interpret_as_float((31u << 23) - 0x1000u));
    int32_t fint2 = _interpret_as_int(fscale) - round_mask;

    if (fint < f32infty)
        o = fint2 >> 13; // Take the bits!

    return float16(o | (sign >> 16));
}

} // namespace vecsim_types
