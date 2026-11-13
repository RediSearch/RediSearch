/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "space_includes.h"

template <__mmask8 mask> // (2^n)-1, where n is in 1..7 (1, 3, ..., 127)
static inline __m256 my_mm256_maskz_loadu_ps(const float *p) {
    // Load 8 floats (assuming this is safe to do)
    __m256 data = _mm256_loadu_ps(p);
    // Set the mask for the loaded data (set 0 if a bit is 0)
    __m256 masked_data = _mm256_blend_ps(_mm256_setzero_ps(), data, mask);

    return masked_data;
}

template <__mmask8 mask> // (2^n)-1, where n is in 1..3 (1, 3, 7)
static inline __m256d my_mm256_maskz_loadu_pd(const double *p) {
    // Load 4 doubles (assuming this is safe to do)
    __m256d data = _mm256_loadu_pd(p);
    // Set the mask for the loaded data (set 0 if a bit is 0)
    __m256d masked_data = _mm256_blend_pd(_mm256_setzero_pd(), data, mask);

    return masked_data;
}

static inline float my_mm256_reduce_add_ps(__m256 x) {
    float PORTABLE_ALIGN32 TmpRes[8];
    _mm256_store_ps(TmpRes, x);
    return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] +
           TmpRes[7];
}

// As my_mm256_reduce_add_ps, but folded in-register in three steps instead of spilling 8 floats
// and summing them with 7 dependent scalar adds. That fixed cost dominates at small dimensions.
// Reassociating changes the low bits, so this is added alongside the original rather than
// replacing it -- the original's other callers would each need their own benchmarking.
static inline float my_mm256_reduce_add_ps_tree(__m256 x) {
    __m128 sum128 = _mm_add_ps(_mm256_castps256_ps128(x), _mm256_extractf128_ps(x, 1));
    sum128 = _mm_add_ps(sum128, _mm_movehl_ps(sum128, sum128));
    sum128 = _mm_add_ss(sum128, _mm_shuffle_ps(sum128, sum128, 0x1));
    return _mm_cvtss_f32(sum128);
}
