/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <benchmark/benchmark.h>
#include <random>
#include <cstring>
#include "utils/tests_utils.h"
#include "bm_spaces.h"

class BM_VecSimSpaces_Integers_UINT8 : public benchmark::Fixture {
protected:
    std::mt19937 rng;
    size_t dim;
    uint8_t *v1, *v2;

public:
    BM_VecSimSpaces_Integers_UINT8() { rng.seed(47); }
    ~BM_VecSimSpaces_Integers_UINT8() = default;

    void SetUp(const ::benchmark::State &state) {
        dim = state.range(0);
        // Allocate vector with extra space for cosine calculations
        v1 = new uint8_t[dim + sizeof(float)];
        v2 = new uint8_t[dim + sizeof(float)];
        test_utils::populate_uint8_vec(v1, dim, 123);
        test_utils::populate_uint8_vec(v2, dim, 1234);

        // Store the norm in the extra space for cosine calculations
        *(float *)(v1 + dim) = test_utils::integral_compute_norm(v1, dim);
        *(float *)(v2 + dim) = test_utils::integral_compute_norm(v2, dim);
    }
    void TearDown(const ::benchmark::State &state) {
        delete v1;
        delete v2;
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_Integers_UINT8, UINT8, SVE2, 32, sve2_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_Integers_UINT8, UINT8, SVE2, 32, sve2_supported);
#endif
#ifdef OPT_SVE
bool sve_supported = opt.sve; // Check for SVE support
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_Integers_UINT8, UINT8, SVE, 32, sve_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_Integers_UINT8, UINT8, SVE, 32, sve_supported);
#endif
#ifdef OPT_NEON_DOTPROD
// NEON_DOTPROD functions
bool neon_dotprod_supported = opt.asimddp;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_Integers_UINT8, UINT8, NEON_DOTPROD, 64,
                                neon_dotprod_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_Integers_UINT8, UINT8, NEON_DOTPROD, 64,
                                 neon_dotprod_supported);
#endif
// NEON functions
#ifdef OPT_NEON
bool neon_supported = opt.asimd;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_Integers_UINT8, UINT8, NEON, 64, neon_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_Integers_UINT8, UINT8, NEON, 64, neon_supported);
#endif
#endif

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512_F_BW_VL_VNNI functions
#ifdef OPT_AVX512_F_BW_VL_VNNI
bool avx512_f_bw_vl_vnni_supported = opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_Integers_UINT8, UINT8, AVX512F_BW_VL_VNNI, 32,
                                avx512_f_bw_vl_vnni_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_Integers_UINT8, UINT8, AVX512F_BW_VL_VNNI, 32,
                                 avx512_f_bw_vl_vnni_supported);
#endif // AVX512_F_BW_VL_VNNI

#endif // x86_64

INITIALIZE_NAIVE_BM(BM_VecSimSpaces_Integers_UINT8, UINT8, InnerProduct, 32);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_Integers_UINT8, UINT8, Cosine, 32);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_Integers_UINT8, UINT8, L2Sqr, 32);
BENCHMARK_MAIN();
