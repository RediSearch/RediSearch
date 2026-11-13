/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "bm_spaces.h"
#include "VecSim/types/float16.h"
#include "utils/tests_utils.h"

using sq8 = vecsim_types::sq8;
using float16 = vecsim_types::float16;

/**
 * SQ8-to-FP16 benchmarks: SQ8 quantized storage with FP16 query.
 * Registers the naive (scalar) baseline plus per-ISA SIMD variants (x86: AVX-512 / AVX2+FMA /
 * AVX2 / SSE4 — gated on the matching OPT_* defines and runtime CPU features). ARM kernels (NEON_HP
 * / SVE / SVE2) are registered below.
 */
class BM_VecSimSpaces_SQ8_FP16 : public benchmark::Fixture {
protected:
    std::mt19937 rng;
    size_t dim;
    // The naive benchmark macro calls `SQ8_FP16_<metric>(v1, v2, dim)`, and the kernel signature
    // is `(SQ8_storage, FP16_query, dim)`. v1 is therefore the SQ8 storage, v2 the FP16 query.
    uint8_t *v1;
    float16 *v2;

public:
    BM_VecSimSpaces_SQ8_FP16() { rng.seed(47); }
    ~BM_VecSimSpaces_SQ8_FP16() = default;

    void SetUp(const ::benchmark::State &state) {
        dim = state.range(0);
        size_t quantized_size =
            dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
        v1 = new uint8_t[quantized_size];
        test_utils::populate_float_vec_to_sq8_with_metadata(v1, dim, true, 1234);
        // Allocate as float16[] so v2 is alignof(float16)-aligned for the SQ8_FP16 kernel's
        // typed loads. Add extra float16 slots to cover the trailing FP32 metadata bytes.
        size_t query_count =
            dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
        v2 = new float16[query_count];
        test_utils::populate_sq8_fp16_query(v2, dim, true, 123);
    }
    void TearDown(const ::benchmark::State &state) {
        delete[] v1;
        delete[] v2;
    }
};

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX-512F is sufficient — _mm512_cvtph_ps is part of AVX-512F, no F16C/VNNI/BW/VL needed.
#ifdef OPT_AVX512F
bool avx512f_supported = opt.avx512f;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX512F, 16, avx512f_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX512F, 16,
                                 avx512f_supported);
#endif

#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
bool avx2_fma3_f16c_supported = opt.avx2 && opt.fma3 && opt.f16c;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX2_FMA, 16,
                                avx2_fma3_f16c_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX2_FMA, 16,
                                 avx2_fma3_f16c_supported);
#endif

#ifdef OPT_AVX2
bool avx2_f16c_supported = opt.avx2 && opt.f16c;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX2, 16, avx2_f16c_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, AVX2, 16, avx2_f16c_supported);
#endif

#ifdef OPT_SSE4
bool sse4_f16c_supported = opt.sse4_1 && opt.f16c && opt.avx;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SSE4, 16, sse4_f16c_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SSE4, 16, sse4_f16c_supported);
#endif
#endif // OPT_F16C
#endif // x86_64

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features arm_opt = cpu_features::GetAarch64Info().features;

#ifdef OPT_SVE2
bool sve2_supported = arm_opt.sve2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SVE2, 16, sve2_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SVE2, 16, sve2_supported);
#endif

#ifdef OPT_SVE
bool sve_supported = arm_opt.sve;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SVE, 16, sve_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, SVE, 16, sve_supported);
#endif

#ifdef OPT_NEON_HP
bool neon_hp_supported = arm_opt.asimdhp;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, NEON_HP, 16, neon_hp_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, NEON_HP, 16,
                                 neon_hp_supported);
#endif

#ifdef OPT_NEON_FHM
bool neon_fhm_supported = arm_opt.asimdhp && arm_opt.asimdfhm;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, NEON_FHM, 16,
                                neon_fhm_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, NEON_FHM, 16,
                                 neon_fhm_supported);
#endif
#endif // CPU_FEATURES_ARCH_AARCH64

// Naive (scalar) baseline — always registered as the comparison anchor.

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, InnerProduct, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, Cosine, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP16, SQ8_FP16, L2Sqr, 16);

BENCHMARK_MAIN();
