/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "bm_spaces.h"
#include "utils/tests_utils.h"

using sq8 = vecsim_types::sq8;

class BM_VecSimSpaces_SQ8_FP32 : public benchmark::Fixture {
protected:
    std::mt19937 rng;
    size_t dim;
    uint8_t *v1;
    float *v2;

public:
    BM_VecSimSpaces_SQ8_FP32() { rng.seed(47); }
    ~BM_VecSimSpaces_SQ8_FP32() = default;

    void SetUp(const ::benchmark::State &state) {
        dim = state.range(0);
        size_t quantized_size =
            dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
        v1 = new uint8_t[quantized_size];
        test_utils::populate_float_vec_to_sq8_with_metadata(v1, dim, true, 1234);
        size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
        v2 = new float[query_size];
        test_utils::populate_sq8_fp32_query(v2, dim, true, 123);
    }
    void TearDown(const ::benchmark::State &state) {
        delete[] v1;
        delete[] v2;
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON implementation for ARMv8-a
#ifdef OPT_NEON
bool neon_supported = opt.asimd; // ARMv8-a always supports NEON
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, NEON, 16, neon_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, NEON, 16, neon_supported);
#endif
// SVE implementation
#ifdef OPT_SVE
bool sve_supported = opt.sve; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SVE, 16, sve_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SVE, 16, sve_supported);
#endif
// SVE2 implementation
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2; // Check for SVE2 support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SVE2, 16, sve2_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SVE2, 16, sve2_supported);
#endif
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512_F_BW_VL_VNNI functions
#ifdef OPT_AVX512_F_BW_VL_VNNI
bool avx512_f_bw_vl_vnni_supported = opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX512F_BW_VL_VNNI, 16,
                                avx512_f_bw_vl_vnni_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX512F_BW_VL_VNNI, 16,
                                 avx512_f_bw_vl_vnni_supported);
#endif // AVX512_F_BW_VL_VNNI

#ifdef OPT_AVX2_FMA
bool avx2_fma3_supported = opt.avx2 && opt.fma3;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX2_FMA, 16,
                                avx2_fma3_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX2_FMA, 16,
                                 avx2_fma3_supported);
#endif // AVX2_FMA

#ifdef OPT_AVX2
// AVX2 functions
bool avx2_supported = opt.avx2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX2, 16, avx2_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, AVX2, 16, avx2_supported);
#endif // AVX2

// SSE4 functions
#ifdef OPT_SSE4
bool sse4_supported = opt.sse4_1;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SSE4, 16, sse4_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, SSE4, 16, sse4_supported);
#endif // SSE4
#endif // x86_64

// Naive algorithms

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, InnerProduct, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, Cosine, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_FP32, SQ8_FP32, L2Sqr, 16);

// Naive

BENCHMARK_MAIN();
