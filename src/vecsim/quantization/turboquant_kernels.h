/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "cpu_features_macros.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#if defined(CPU_FEATURES_ARCH_AARCH64) && defined(__ARM_NEON)
#include "cpuinfo_aarch64.h"
#include <arm_neon.h>
#endif

namespace vecsim::quantization::turboquant_detail {

using PackedDot = float (*)(const unsigned char *, const unsigned char *, const float *, size_t);
using RowDot = float (*)(const float *, const float *, size_t);
using PackedPairDot = float (*)(const unsigned char *, const unsigned char *, const float *, size_t,
                                float, float, const float *);

inline float loadFloat(const unsigned char *bytes) {
    float value;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

template <unsigned Bits>
inline unsigned loadPackedBlock(const unsigned char *bytes) {
    static_assert(Bits == 2 || Bits == 3);
    unsigned packed = unsigned(bytes[0]) | (unsigned(bytes[1]) << 8);
    if constexpr (Bits == 3) {
        packed |= unsigned(bytes[2]) << 16;
    }
    return packed;
}

template <unsigned Bits>
inline unsigned loadCode(const unsigned char *bytes, size_t coordinate) {
    const size_t start = coordinate * Bits;
    const unsigned shift = start % 8;
    unsigned packed = bytes[start / 8];
    if (shift + Bits > 8) {
        packed |= unsigned(bytes[start / 8 + 1]) << 8;
    }
    return (packed >> shift) & ((1U << Bits) - 1);
}

template <unsigned Bits>
inline float packedDot(const unsigned char *codes, const unsigned char *query,
                       const float *centroids, size_t dim) {
    float sum = 0;
    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        const unsigned packed = loadPackedBlock<Bits>(codes + i / 8 * Bits);
        for (unsigned lane = 0; lane < 8; ++lane) {
            const unsigned code = (packed >> (lane * Bits)) & ((1U << Bits) - 1);
            sum += loadFloat(query + (i + lane) * sizeof(float)) * centroids[code];
        }
    }
    for (; i < dim; ++i) {
        sum += loadFloat(query + i * sizeof(float)) * centroids[loadCode<Bits>(codes, i)];
    }
    return sum;
}

// Both inputs remain packed. A null mean denotes the uncentered estimator.
template <unsigned Bits>
inline float packedPairDot(const unsigned char *a, const unsigned char *b, const float *centroids,
                           size_t dim, float norm_a, float norm_b, const float *rotated_mean) {
    float sum = 0;
    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        const unsigned packed_a = loadPackedBlock<Bits>(a + i / 8 * Bits);
        const unsigned packed_b = loadPackedBlock<Bits>(b + i / 8 * Bits);
        for (unsigned lane = 0; lane < 8; ++lane) {
            const unsigned shift = lane * Bits;
            const float za = norm_a * centroids[(packed_a >> shift) & ((1U << Bits) - 1)];
            const float zb = norm_b * centroids[(packed_b >> shift) & ((1U << Bits) - 1)];
            sum += za * zb;
            if (rotated_mean)
                sum += (za + zb) * rotated_mean[i + lane];
        }
    }
    for (; i < dim; ++i) {
        const float za = norm_a * centroids[loadCode<Bits>(a, i)];
        const float zb = norm_b * centroids[loadCode<Bits>(b, i)];
        sum += za * zb;
        if (rotated_mean)
            sum += (za + zb) * rotated_mean[i];
    }
    return sum;
}

#if defined(CPU_FEATURES_ARCH_AARCH64) && defined(__ARM_NEON)
inline float32x4_t loadQuery4(const unsigned char *bytes) {
    float32x4_t value;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

template <unsigned Bits>
inline float32x4_t lookupCentroids(uint32x4_t codes, const uint8x16x2_t &table) {
    // Each code selects the four consecutive bytes of its FP32 centroid.
    const auto indices =
        vreinterpretq_u8_u32(vaddq_u32(vmulq_n_u32(codes, 0x04040404U), vdupq_n_u32(0x03020100U)));
    if constexpr (Bits == 2) {
        return vreinterpretq_f32_u8(vqtbl1q_u8(table.val[0], indices));
    } else {
        return vreinterpretq_f32_u8(vqtbl2q_u8(table, indices));
    }
}

template <unsigned Bits>
inline float packedDotNEON(const unsigned char *codes, const unsigned char *query,
                           const float *centroids, size_t dim) {
    uint8x16x2_t table;
    table.val[0] = vreinterpretq_u8_f32(vld1q_f32(centroids));
    table.val[1] = vdupq_n_u8(0);
    if constexpr (Bits == 3) {
        table.val[1] = vreinterpretq_u8_f32(vld1q_f32(centroids + 4));
    }
    const int32_t shifts0[] = {0, -int(Bits), -int(2 * Bits), -int(3 * Bits)};
    const int32_t shifts1[] = {-int(4 * Bits), -int(5 * Bits), -int(6 * Bits), -int(7 * Bits)};
    const int32x4_t shift0 = vld1q_s32(shifts0), shift1 = vld1q_s32(shifts1);
    const uint32x4_t mask = vdupq_n_u32((1U << Bits) - 1);
    float32x4_t sum0 = vdupq_n_f32(0), sum1 = vdupq_n_f32(0);
    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        const uint32x4_t packed = vdupq_n_u32(loadPackedBlock<Bits>(codes + i / 8 * Bits));
        const auto c0 = lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed, shift0), mask), table);
        const auto c1 = lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed, shift1), mask), table);
        // Make the validated Release FMA arithmetic explicit in both native kernels.
        sum0 = vfmaq_f32(sum0, loadQuery4(query + i * sizeof(float)), c0);
        sum1 = vfmaq_f32(sum1, loadQuery4(query + (i + 4) * sizeof(float)), c1);
    }
    float sum = vaddvq_f32(vaddq_f32(sum0, sum1));
    for (; i < dim; ++i) {
        sum += loadFloat(query + i * sizeof(float)) * centroids[loadCode<Bits>(codes, i)];
    }
    return sum;
}

template <unsigned Bits>
inline float packedPairDotNEON(const unsigned char *a, const unsigned char *b,
                               const float *centroids, size_t dim, float norm_a, float norm_b,
                               const float *rotated_mean) {
    uint8x16x2_t table;
    table.val[0] = vreinterpretq_u8_f32(vld1q_f32(centroids));
    table.val[1] = vdupq_n_u8(0);
    if constexpr (Bits == 3)
        table.val[1] = vreinterpretq_u8_f32(vld1q_f32(centroids + 4));
    const int32_t shifts0[] = {0, -int(Bits), -int(2 * Bits), -int(3 * Bits)};
    const int32_t shifts1[] = {-int(4 * Bits), -int(5 * Bits), -int(6 * Bits), -int(7 * Bits)};
    const auto shift0 = vld1q_s32(shifts0), shift1 = vld1q_s32(shifts1);
    const auto mask = vdupq_n_u32((1U << Bits) - 1);
    float32x4_t sum0 = vdupq_n_f32(0), sum1 = vdupq_n_f32(0);
    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        const auto packed_a = vdupq_n_u32(loadPackedBlock<Bits>(a + i / 8 * Bits));
        const auto packed_b = vdupq_n_u32(loadPackedBlock<Bits>(b + i / 8 * Bits));
        const auto a0 = vmulq_n_f32(
            lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed_a, shift0), mask), table), norm_a);
        const auto a1 = vmulq_n_f32(
            lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed_a, shift1), mask), table), norm_a);
        const auto b0 = vmulq_n_f32(
            lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed_b, shift0), mask), table), norm_b);
        const auto b1 = vmulq_n_f32(
            lookupCentroids<Bits>(vandq_u32(vshlq_u32(packed_b, shift1), mask), table), norm_b);
        sum0 = vaddq_f32(sum0, vmulq_f32(a0, b0));
        sum1 = vaddq_f32(sum1, vmulq_f32(a1, b1));
        if (rotated_mean) {
            sum0 = vaddq_f32(sum0, vmulq_f32(vaddq_f32(a0, b0), vld1q_f32(rotated_mean + i)));
            sum1 = vaddq_f32(sum1, vmulq_f32(vaddq_f32(a1, b1), vld1q_f32(rotated_mean + i + 4)));
        }
    }
    float sum = vaddvq_f32(vaddq_f32(sum0, sum1));
    for (; i < dim; ++i) {
        const float za = norm_a * centroids[loadCode<Bits>(a, i)];
        const float zb = norm_b * centroids[loadCode<Bits>(b, i)];
        sum += za * zb;
        if (rotated_mean)
            sum += (za + zb) * rotated_mean[i];
    }
    return sum;
}

inline float rowDotNEON(const float *row, const float *query, size_t dim) {
    float32x4_t sum0 = vdupq_n_f32(0), sum1 = vdupq_n_f32(0);
    float32x4_t sum2 = vdupq_n_f32(0), sum3 = vdupq_n_f32(0);
    size_t i = 0;
    for (; i + 16 <= dim; i += 16) {
        sum0 = vaddq_f32(sum0, vmulq_f32(vld1q_f32(row + i), vld1q_f32(query + i)));
        sum1 = vaddq_f32(sum1, vmulq_f32(vld1q_f32(row + i + 4), vld1q_f32(query + i + 4)));
        sum2 = vaddq_f32(sum2, vmulq_f32(vld1q_f32(row + i + 8), vld1q_f32(query + i + 8)));
        sum3 = vaddq_f32(sum3, vmulq_f32(vld1q_f32(row + i + 12), vld1q_f32(query + i + 12)));
    }
    for (; i + 4 <= dim; i += 4) {
        sum0 = vaddq_f32(sum0, vmulq_f32(vld1q_f32(row + i), vld1q_f32(query + i)));
    }
    float sum = vaddvq_f32(vaddq_f32(vaddq_f32(sum0, sum1), vaddq_f32(sum2, sum3)));
    for (; i < dim; ++i) {
        sum += row[i] * query[i];
    }
    return sum;
}
#endif

} // namespace vecsim::quantization::turboquant_detail
