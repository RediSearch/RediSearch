/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "bm_spaces.h"

class BM_VecSimSpaces_FP64 : public BM_VecSimSpaces<double> {};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON implementation for ARMv8-a
#ifdef OPT_NEON
bool neon_supported = opt.asimd; // ARMv8-a always supports NEON
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, NEON, 8, neon_supported);
#endif
// SVE implementation
#ifdef OPT_SVE
bool sve_supported = opt.sve; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, SVE, 8, sve_supported);
#endif
// SVE2 implementation
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2; // Check for SVE2 support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, SVE2, 8, sve2_supported);
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512 functions
#ifdef OPT_AVX512F
bool avx512f_supported = opt.avx512f;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, AVX512F, 8, avx512f_supported);
#endif // AVX512F

// AVX functions
#ifdef OPT_AVX
bool avx_supported = opt.avx;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, AVX, 8, avx_supported);
#endif // AVX

// SSE functions
#ifdef OPT_SSE
bool sse_supported = opt.sse;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP64, FP64, SSE, 8, sse_supported);
#endif // SSE

#endif // x86_64

// Naive algorithms
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP64, FP64, InnerProduct, 8);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP64, FP64, L2Sqr, 8);
// Naive

BENCHMARK_MAIN();
