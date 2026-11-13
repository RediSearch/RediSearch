/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "VecSim/types/bfloat16.h"
#include "bm_spaces.h"

class BM_VecSimSpaces_BF16 : public BM_VecSimSpaces<vecsim_types::bfloat16> {
    vecsim_types::bfloat16 DoubleToType(double val) override {
        return vecsim_types::float_to_bf16(val);
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON implementation for ARMv8-a
#ifdef OPT_NEON_BF16
bool neon_supported = opt.bf16;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_BF16, BF16, NEON_BF16, 32, neon_supported);
#endif
#ifdef OPT_SVE_BF16
bool sve_supported = opt.svebf16; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_BF16, BF16, SVE_BF16, 32, sve_supported);
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512_BF16 functions
#ifdef OPT_AVX512_BF16_VL
bool avx512_bf16_vl_supported = opt.avx512_bf16 && opt.avx512vl;
INITIALIZE_BENCHMARKS_SET_IP(BM_VecSimSpaces_BF16, BF16, AVX512BF16_VL, 32,
                             avx512_bf16_vl_supported);
#endif // AVX512_BF16

// AVX512 functions
#ifdef OPT_AVX512_BW_VBMI2
bool avx512_bw_vbmi2_supported = opt.avx512bw && opt.avx512vbmi2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_BF16, BF16, AVX512BW_VBMI2, 32,
                                avx512_bw_vbmi2_supported);
#endif // AVX512F

// AVX functions
#ifdef OPT_AVX2
bool avx2_supported = opt.avx2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_BF16, BF16, AVX2, 32, avx2_supported);
#endif // AVX

// SSE functions
#ifdef OPT_SSE3
bool sse3_supported = opt.sse3;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_BF16, BF16, SSE3, 32, sse3_supported);
#endif // SSE

#endif // x86_64

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_BF16, BF16, InnerProduct_LittleEndian, 32);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_BF16, BF16, L2Sqr_LittleEndian, 32);
BENCHMARK_MAIN();
