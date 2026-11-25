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
#include <immintrin.h>

// TQ2/TQ3 packed inner products with AVX2 and FMA. Each step expands one block of 8 codes into
// 8 lanes and looks up their FP32 centroids with one in-register permute (4 or 8 centroids fit
// one __m256). dim is a multiple of 64, so the loops run on whole 16-code pairs of blocks.
namespace vecsim::quantization::turboquant_detail {

// A private copy of the packed-block reader: inline helpers shared with the baseline build would
// be compiled here with AVX2 enabled and could be picked by the linker for the scalar path.
template <unsigned Bits>
inline unsigned loadPackedBlockAVX2(const unsigned char *bytes) {
    unsigned packed = unsigned(bytes[0]) | (unsigned(bytes[1]) << 8);
    if constexpr (Bits == 3)
        packed |= unsigned(bytes[2]) << 16;
    return packed;
}

template <unsigned Bits>
inline __m256 centroidTableAVX2(const float *centroids) {
    // TQ2 has only 4 centroids; codes never index the upper lanes.
    return Bits == 2 ? _mm256_castps128_ps256(_mm_loadu_ps(centroids)) : _mm256_loadu_ps(centroids);
}

template <unsigned Bits>
inline __m256 lookupAVX2(const unsigned char *block, __m256i shifts, __m256i mask, __m256 table) {
    const __m256i packed = _mm256_set1_epi32(int(loadPackedBlockAVX2<Bits>(block)));
    const __m256i codes = _mm256_and_si256(_mm256_srlv_epi32(packed, shifts), mask);
    return _mm256_permutevar8x32_ps(table, codes);
}

inline float horizontalSumAVX2(__m256 v) {
    const __m128 sum4 = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
    const __m128 sum2 = _mm_add_ps(sum4, _mm_movehl_ps(sum4, sum4));
    return _mm_cvtss_f32(_mm_add_ss(sum2, _mm_movehdup_ps(sum2)));
}

template <unsigned Bits>
float packedDotAVX2(const unsigned char *codes, const unsigned char *query, const float *centroids,
                    size_t dim) {
    const __m256 table = centroidTableAVX2<Bits>(centroids);
    const __m256i shifts =
        _mm256_setr_epi32(0, Bits, 2 * Bits, 3 * Bits, 4 * Bits, 5 * Bits, 6 * Bits, 7 * Bits);
    const __m256i mask = _mm256_set1_epi32((1 << Bits) - 1);
    const auto *q = reinterpret_cast<const float *>(query);
    __m256 sum0 = _mm256_setzero_ps(), sum1 = _mm256_setzero_ps();
    for (size_t i = 0; i < dim; i += 16) {
        const unsigned char *block = codes + i / 8 * Bits;
        sum0 = _mm256_fmadd_ps(_mm256_loadu_ps(q + i), lookupAVX2<Bits>(block, shifts, mask, table),
                               sum0);
        sum1 = _mm256_fmadd_ps(_mm256_loadu_ps(q + i + 8),
                               lookupAVX2<Bits>(block + Bits, shifts, mask, table), sum1);
    }
    return horizontalSumAVX2(_mm256_add_ps(sum0, sum1));
}

template <unsigned Bits>
float packedPairDotAVX2(const unsigned char *a, const unsigned char *b, const float *centroids,
                        size_t dim, float norm_a, float norm_b, const float *rotated_mean) {
    const __m256 table = centroidTableAVX2<Bits>(centroids);
    const __m256i shifts =
        _mm256_setr_epi32(0, Bits, 2 * Bits, 3 * Bits, 4 * Bits, 5 * Bits, 6 * Bits, 7 * Bits);
    const __m256i mask = _mm256_set1_epi32((1 << Bits) - 1);
    const __m256 scale_a = _mm256_set1_ps(norm_a), scale_b = _mm256_set1_ps(norm_b);
    __m256 sum0 = _mm256_setzero_ps(), sum1 = _mm256_setzero_ps();
    for (size_t i = 0; i < dim; i += 8) {
        const size_t offset = i / 8 * Bits;
        const __m256 za = _mm256_mul_ps(lookupAVX2<Bits>(a + offset, shifts, mask, table), scale_a);
        const __m256 zb = _mm256_mul_ps(lookupAVX2<Bits>(b + offset, shifts, mask, table), scale_b);
        sum0 = _mm256_fmadd_ps(za, zb, sum0);
        sum1 = _mm256_fmadd_ps(_mm256_add_ps(za, zb), _mm256_loadu_ps(rotated_mean + i), sum1);
    }
    return horizontalSumAVX2(_mm256_add_ps(sum0, sum1));
}

} // namespace vecsim::quantization::turboquant_detail
