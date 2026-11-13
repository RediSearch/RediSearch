/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "VecSim/types/float16.h"
#include "bm_spaces.h"

class BM_VecSimSpaces_FP16 : public BM_VecSimSpaces<vecsim_types::float16> {
    vecsim_types::float16 DoubleToType(double val) override {
        return vecsim_types::FP32_to_FP16(val);
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON implementation for ARMv8-a
#ifdef OPT_NEON_HP
bool neon_supported = opt.asimdhp; // ARMv8-a always supports NEON
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16, FP16, NEON_HP, 32, neon_supported);
#endif
#ifdef OPT_SVE
bool sve_supported = opt.sve; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16, FP16, SVE, 32, sve_supported);
#endif
// SVE2 implementation
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2; // Check for SVE2 support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16, FP16, SVE2, 32, sve2_supported);
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// OPT_AVX512FP16 functions
#ifdef OPT_AVX512_FP16_VL

class BM_VecSimSpaces_FP16_adv : public BM_VecSimSpaces<_Float16> {};

bool avx512fp16_vl_supported = opt.avx512_fp16 && opt.avx512vl;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16_adv, FP16, AVX512FP16_VL, 32,
                                avx512fp16_vl_supported);

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP16_adv, FP16, InnerProduct, 32);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP16_adv, FP16, L2Sqr, 32);
#endif // OPT_AVX512_FP16_VL

// OPT_AVX512F functions
#ifdef OPT_AVX512F
bool avx512f_supported = opt.avx512f;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16, FP16, AVX512F, 32, avx512f_supported);
#endif // OPT_AVX512F
// AVX functions
#ifdef OPT_F16C
bool avx512_bw_f16c_supported = opt.f16c && opt.fma3 && opt.avx;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_FP16, FP16, F16C, 32, avx512_bw_f16c_supported);
#endif // OPT_F16C

#endif // x86_64

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP16, FP16, InnerProduct, 32);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_FP16, FP16, L2Sqr, 32);
BENCHMARK_MAIN();
