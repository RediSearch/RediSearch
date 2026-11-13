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

/**
 * SQ8-to-SQ8 benchmarks: Both vectors are uint8 quantized with dequantization applied to both.
 */
class BM_VecSimSpaces_SQ8_SQ8 : public benchmark::Fixture {
protected:
    std::mt19937 rng;
    size_t dim;
    uint8_t *v1;
    uint8_t *v2;

public:
    BM_VecSimSpaces_SQ8_SQ8() { rng.seed(47); }
    ~BM_VecSimSpaces_SQ8_SQ8() = default;

    void SetUp(const ::benchmark::State &state) {
        dim = state.range(0);
        // Allocate both vectors with extra space for min, delta, sum, and sum_squares
        // Vector layout: [uint8_t values (dim)] [min (float)] [delta (float)] [sum (float)]
        // [sum_squares (float)]
        size_t quantized_size =
            dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
        v1 = new uint8_t[quantized_size];
        v2 = new uint8_t[quantized_size];
        test_utils::populate_float_vec_to_sq8_with_metadata(v1, dim, true, 123);
        test_utils::populate_float_vec_to_sq8_with_metadata(v2, dim, true, 1234);
    }
    void TearDown(const ::benchmark::State &state) {
        delete[] v1;
        delete[] v2;
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON SQ8-to-SQ8 functions
#ifdef OPT_NEON
bool neon_supported = opt.asimd;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, NEON, 64, neon_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, NEON, 64, neon_supported);
#endif // NEON
// NEON DOTPROD SQ8-to-SQ8 functions
#ifdef OPT_NEON_DOTPROD
bool neon_dotprod_supported = opt.asimddp;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, NEON_DOTPROD, 64,
                                neon_dotprod_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, NEON_DOTPROD, 64,
                                 neon_dotprod_supported);
#endif // NEON_DOTPROD
// SVE SQ8-to-SQ8 functions
#ifdef OPT_SVE
bool sve_supported = opt.sve;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, SVE, 16, sve_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, SVE, 16, sve_supported);
#endif // SVE
// SVE2 SQ8-to-SQ8 functions
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, SVE2, 16, sve2_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, SVE2, 16, sve2_supported);
#endif // SVE2
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512_F_BW_VL_VNNI SQ8-to-SQ8 functions
#ifdef OPT_AVX512_F_BW_VL_VNNI
bool avx512_f_bw_vl_vnni_supported = opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, AVX512F_BW_VL_VNNI, 64,
                                avx512_f_bw_vl_vnni_supported);
INITIALIZE_BENCHMARKS_SET_Cosine(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, AVX512F_BW_VL_VNNI, 64,
                                 avx512_f_bw_vl_vnni_supported);

#endif // AVX512_F_BW_VL_VNNI
#endif // x86_64

// Naive SQ8-to-SQ8 algorithms
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, InnerProduct, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, Cosine, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ8_SQ8, SQ8_SQ8, L2Sqr, 16);

BENCHMARK_MAIN();
