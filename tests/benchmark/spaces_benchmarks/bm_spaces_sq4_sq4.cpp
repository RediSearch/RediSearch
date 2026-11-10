/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "bm_spaces.h"
#include <vector>
#include "VecSim/spaces/computer/preprocessors.h"
#include "VecSim/types/sq4.h"

using sq4 = vecsim_types::sq4;

/**
 * SQ4-to-SQ4 benchmarks: both vectors are packed 4-bit storage blobs from the production
 * preprocessor. The L2 layout carries every field the IP kernels read, so one pair serves both
 * metrics.
 */
class BM_VecSimSpaces_SQ4_SQ4 : public benchmark::Fixture {
protected:
    std::shared_ptr<VecSimAllocator> allocator;
    size_t dim;
    void *v1 = nullptr;
    void *v2 = nullptr;

public:
    BM_VecSimSpaces_SQ4_SQ4() = default;
    ~BM_VecSimSpaces_SQ4_SQ4() = default;

    void SetUp(const ::benchmark::State &state) {
        allocator = VecSimAllocator::newVecsimAllocator();
        dim = state.range(0);
        std::mt19937 rng(47);
        std::uniform_real_distribution<float> uniform(-1.0f, 1.0f);
        std::vector<float> first(dim), second(dim);
        for (size_t i = 0; i < dim; ++i) {
            first[i] = uniform(rng);
            second[i] = uniform(rng);
        }
        QuantPreprocessor<float, VecSimMetric_L2, false, sq4> pp(allocator, dim);
        size_t first_size = dim * sizeof(float), second_size = dim * sizeof(float);
        pp.preprocessForStorage(first.data(), v1, first_size, 0);
        pp.preprocessForStorage(second.data(), v2, second_size, 0);
    }
    void TearDown(const ::benchmark::State &state) {
        allocator->free_allocation(v1);
        allocator->free_allocation(v2);
        v1 = v2 = nullptr;
        allocator.reset();
    }
};

#ifdef CPU_FEATURES_ARCH_AARCH64
cpu_features::Aarch64Features opt = cpu_features::GetAarch64Info().features;

// NEON SQ4-to-SQ4 functions
#ifdef OPT_NEON
bool neon_supported = opt.asimd;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, NEON, 16, neon_supported);
#endif // NEON
// NEON DOTPROD SQ4-to-SQ4 functions
#ifdef OPT_NEON_DOTPROD
bool neon_dotprod_supported = opt.asimddp;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, NEON_DOTPROD, 32,
                                neon_dotprod_supported);
#endif // NEON_DOTPROD
// SVE SQ4-to-SQ4 functions
#ifdef OPT_SVE
bool sve_supported = opt.sve;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, SVE, 16, sve_supported);
#endif // SVE
// SVE2 SQ4-to-SQ4 functions
#ifdef OPT_SVE2
bool sve2_supported = opt.sve2;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, SVE2, 16, sve2_supported);
#endif // SVE2
#endif // AARCH64

#ifdef CPU_FEATURES_ARCH_X86_64
cpu_features::X86Features opt = cpu_features::GetX86Info().features;

// AVX512_F_BW_VL_VNNI SQ4-to-SQ4 functions
#ifdef OPT_AVX512_F_BW_VL_VNNI
bool avx512_f_bw_vl_vnni_supported = opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni;
INITIALIZE_BENCHMARKS_SET_L2_IP(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, AVX512F_BW_VL_VNNI, 64,
                                avx512_f_bw_vl_vnni_supported);
#endif // AVX512_F_BW_VL_VNNI
#endif // x86_64

// Naive SQ4-to-SQ4 algorithms
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, InnerProduct, 16);
INITIALIZE_NAIVE_BM(BM_VecSimSpaces_SQ4_SQ4, SQ4_SQ4, L2Sqr, 16);

BENCHMARK_MAIN();
