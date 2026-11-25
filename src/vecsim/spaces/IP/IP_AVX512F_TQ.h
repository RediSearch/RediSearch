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

// TQ2/TQ3 packed inner products with AVX-512F. Each step expands two blocks of 8 codes into 16
// lanes and looks up their FP32 centroids with one in-register permute. dim is a multiple of 64,
// so the loops run on whole 32-code (query) or 16-code (pair) steps.
namespace vecsim::quantization::turboquant_detail {

// A private copy of the packed-block reader: inline helpers shared with the baseline build would
// be compiled here with AVX512 enabled and could be picked by the linker for the scalar path.
template <unsigned Bits>
inline unsigned loadPackedBlockAVX512(const unsigned char *bytes) {
    unsigned packed = unsigned(bytes[0]) | (unsigned(bytes[1]) << 8);
    if constexpr (Bits == 3)
        packed |= unsigned(bytes[2]) << 16;
    return packed;
}

template <unsigned Bits>
inline __m512 centroidTableAVX512(const float *centroids) {
    // Codes index at most 4 (TQ2) or 8 (TQ3) lanes, so the rest of the table is never read.
    return Bits == 2 ? _mm512_castps128_ps512(_mm_loadu_ps(centroids))
                     : _mm512_castps256_ps512(_mm256_loadu_ps(centroids));
}

// Lanes 0-7 hold the codes of `block`, lanes 8-15 those of the next block.
template <unsigned Bits>
inline __m512 lookupAVX512(const unsigned char *block, __m512i shifts, __m512i mask, __m512 table) {
    const __m256i low = _mm256_set1_epi32(int(loadPackedBlockAVX512<Bits>(block)));
    const __m256i high = _mm256_set1_epi32(int(loadPackedBlockAVX512<Bits>(block + Bits)));
    const __m512i packed = _mm512_inserti64x4(_mm512_castsi256_si512(low), high, 1);
    const __m512i codes = _mm512_and_si512(_mm512_srlv_epi32(packed, shifts), mask);
    return _mm512_permutexvar_ps(codes, table);
}

template <unsigned Bits>
inline __m512i laneShiftsAVX512() {
    return _mm512_setr_epi32(0, Bits, 2 * Bits, 3 * Bits, 4 * Bits, 5 * Bits, 6 * Bits, 7 * Bits, 0,
                             Bits, 2 * Bits, 3 * Bits, 4 * Bits, 5 * Bits, 6 * Bits, 7 * Bits);
}

template <unsigned Bits>
float packedDotAVX512(const unsigned char *codes, const unsigned char *query,
                      const float *centroids, size_t dim) {
    const __m512 table = centroidTableAVX512<Bits>(centroids);
    const __m512i shifts = laneShiftsAVX512<Bits>();
    const __m512i mask = _mm512_set1_epi32((1 << Bits) - 1);
    const auto *q = reinterpret_cast<const float *>(query);
    __m512 sum0 = _mm512_setzero_ps(), sum1 = _mm512_setzero_ps();
    for (size_t i = 0; i < dim; i += 32) {
        const unsigned char *block = codes + i / 8 * Bits;
        sum0 = _mm512_fmadd_ps(_mm512_loadu_ps(q + i),
                               lookupAVX512<Bits>(block, shifts, mask, table), sum0);
        sum1 = _mm512_fmadd_ps(_mm512_loadu_ps(q + i + 16),
                               lookupAVX512<Bits>(block + 2 * Bits, shifts, mask, table), sum1);
    }
    return _mm512_reduce_add_ps(_mm512_add_ps(sum0, sum1));
}

template <unsigned Bits>
float packedPairDotAVX512(const unsigned char *a, const unsigned char *b, const float *centroids,
                          size_t dim, float norm_a, float norm_b, const float *rotated_mean) {
    const __m512 table = centroidTableAVX512<Bits>(centroids);
    const __m512i shifts = laneShiftsAVX512<Bits>();
    const __m512i mask = _mm512_set1_epi32((1 << Bits) - 1);
    const __m512 scale_a = _mm512_set1_ps(norm_a), scale_b = _mm512_set1_ps(norm_b);
    __m512 sum0 = _mm512_setzero_ps(), sum1 = _mm512_setzero_ps();
    for (size_t i = 0; i < dim; i += 16) {
        const size_t offset = i / 8 * Bits;
        const __m512 za =
            _mm512_mul_ps(lookupAVX512<Bits>(a + offset, shifts, mask, table), scale_a);
        const __m512 zb =
            _mm512_mul_ps(lookupAVX512<Bits>(b + offset, shifts, mask, table), scale_b);
        sum0 = _mm512_fmadd_ps(za, zb, sum0);
        sum1 = _mm512_fmadd_ps(_mm512_add_ps(za, zb), _mm512_loadu_ps(rotated_mean + i), sum1);
    }
    return _mm512_reduce_add_ps(_mm512_add_ps(sum0, sum1));
}

} // namespace vecsim::quantization::turboquant_detail
