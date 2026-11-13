/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <array>
#include <cstdint>
#include <cstring>
#include <utility>
#include <random>
#include <cmath>
#include <limits>
#include <vector>
#include <string>

#include "gtest/gtest.h"
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/spaces/L2/L2.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/spaces/L2_space.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/spaces/functions/AVX512F.h"
#include "VecSim/spaces/functions/AVX.h"
#include "VecSim/spaces/functions/SSE.h"
#include "VecSim/spaces/functions/AVX512BW_VBMI2.h"
#include "VecSim/spaces/functions/AVX512BF16_VL.h"
#include "VecSim/spaces/functions/AVX512FP16_VL.h"
#include "VecSim/spaces/functions/AVX512F_BW_VL_VNNI.h"
#include "VecSim/spaces/functions/AVX2.h"
#include "VecSim/spaces/functions/AVX2_F16C.h"
#include "VecSim/spaces/functions/AVX2_FMA.h"
#include "VecSim/spaces/functions/AVX2_FMA_F16C.h"
#include "VecSim/spaces/functions/SSE3.h"
#include "VecSim/spaces/functions/SSE4.h"
#include "VecSim/spaces/functions/SSE4_F16C.h"
#include "VecSim/spaces/functions/F16C.h"
#include "VecSim/spaces/functions/NEON.h"
#include "VecSim/spaces/functions/NEON_DOTPROD.h"
#include "VecSim/spaces/functions/NEON_HP.h"
#include "VecSim/spaces/functions/NEON_FHM.h"
#include "VecSim/spaces/functions/NEON_BF16.h"
#include "VecSim/spaces/functions/SVE.h"
#include "VecSim/spaces/functions/SVE_BF16.h"
#include "VecSim/spaces/functions/SVE2.h"
#include "tests_utils.h"

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;
using sq8 = vecsim_types::sq8;
using namespace spaces;

class SpacesTest : public ::testing::Test {

protected:
    SpacesTest() {}

    ~SpacesTest() override {}

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(SpacesTest, float_l2_no_optimization_func_test) {
    size_t dim = 5;

    float a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = float(i + 1.5);
        b[i] = float(i + 1.5);
    }

    float dist = FP32_L2Sqr((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 0.0);
}

TEST_F(SpacesTest, double_l2_no_optimization_func_test) {
    size_t dim = 5;

    double a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = double(i + 1.5);
        b[i] = double(i + 1.5);
    }

    double dist = FP64_L2Sqr((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 0.0);
}

TEST_F(SpacesTest, bf16_l2_no_optimization_func_test) {
    size_t dim = 4;

    bfloat16 a[dim], b[dim];
    float sanity_a[dim], sanity_b[dim];
    for (size_t i = 0; i < dim; i++) {
        // multiplication of 0.25 have no rounding error when converted to bfloat16
        sanity_a[i] = 0.5f + i * 0.25f;
        a[i] = vecsim_types::float_to_bf16(sanity_a[i]);
        sanity_b[i] = (float)i * 0.25f;
        b[i] = vecsim_types::float_to_bf16(sanity_b[i]);
    }

    float dist = BF16_L2Sqr_LittleEndian((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, FP32_L2Sqr((const void *)sanity_a, (const void *)sanity_b, dim));
}

TEST_F(SpacesTest, fp16_l2_no_optimization_func_test) {
    size_t dim = 4;

    float16 a[dim], b[dim];
    float sanity_a[dim], sanity_b[dim];
    for (size_t i = 0; i < dim; i++) {
        // multiplication of 0.25 have no rounding error when converted to bfloat16
        sanity_a[i] = 0.5f + i * 0.25f;
        a[i] = vecsim_types::FP32_to_FP16(sanity_a[i]);
        sanity_b[i] = (float)i * 0.25f;
        b[i] = vecsim_types::FP32_to_FP16(sanity_b[i]);
    }

    float dist = FP16_L2Sqr((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, FP32_L2Sqr((const void *)sanity_a, (const void *)sanity_b, dim));
}

TEST_F(SpacesTest, int8_l2_no_optimization_func_test) {
    size_t dim = 5;

    int8_t a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = (i + 1);
        b[i] = (i + 2);
    }

    float dist = INT8_L2Sqr((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 5.0);
}

TEST_F(SpacesTest, uint8_l2_no_optimization_func_test) {
    size_t dim = 5;

    uint8_t a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = (i + 1);
        b[i] = (i + 2);
    }

    float dist = UINT8_L2Sqr((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 5.0);
}

/* ======================== IP NO OPT ======================== */

TEST_F(SpacesTest, float_ip_no_optimization_func_test) {
    size_t dim = 5;

    float a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = float(i + 1.5);
        b[i] = float(i + 1.5);
    }

    spaces::GetNormalizeFunc<float>()(a, dim);
    spaces::GetNormalizeFunc<float>()(b, dim);

    float dist = FP32_InnerProduct((const void *)a, (const void *)b, dim);
    ASSERT_FLOAT_EQ(dist, 0.0f);
}

TEST_F(SpacesTest, double_ip_no_optimization_func_test) {
    size_t dim = 5;

    double a[dim], b[dim];
    for (size_t i = 0; i < dim; i++) {
        a[i] = double(i + 1.5);
        b[i] = double(i + 1.5);
    }

    spaces::GetNormalizeFunc<double>()(a, dim);
    spaces::GetNormalizeFunc<double>()(b, dim);

    double dist = FP64_InnerProduct((const void *)a, (const void *)b, dim);
    ASSERT_NEAR(dist, 0.0, 0.00000001);
}

TEST_F(SpacesTest, bf16_normalize_test) {
    size_t dim = 4;

    bfloat16 a[dim];
    float sanity_a[dim];
    for (size_t i = 0; i < dim; i++) {
        // unit vector
        sanity_a[i] = float(4);
        a[i] = vecsim_types::float_to_bf16(sanity_a[i]);
    }

    spaces::GetNormalizeFunc<bfloat16>()(a, dim);
    spaces::GetNormalizeFunc<float>()(sanity_a, dim);
    // Convert assuming little endian system
    for (size_t i = 0; i < dim; i++) {
        ASSERT_EQ(vecsim_types::bfloat16_to_float32(a[i]), sanity_a[i])
            << " bf16 normalization failed for i = " << i;
        ASSERT_EQ(vecsim_types::bfloat16_to_float32(a[i]), 0.5)
            << " bf16 normalization failed for i = " << i;
    }
}

TEST_F(SpacesTest, fp16_normalize_test) {
    size_t dim = 4;

    float16 a[dim];
    float sanity_a[dim];
    for (size_t i = 0; i < dim; i++) {
        // unit vector
        sanity_a[i] = float(4);
        a[i] = vecsim_types::FP32_to_FP16(sanity_a[i]);
    }

    spaces::GetNormalizeFunc<float16>()(a, dim);
    spaces::GetNormalizeFunc<float>()(sanity_a, dim);
    for (size_t i = 0; i < dim; i++) {
        ASSERT_EQ(vecsim_types::FP16_to_FP32(a[i]), sanity_a[i])
            << " fp16 normalization failed for i = " << i;
        ASSERT_EQ(vecsim_types::FP16_to_FP32(a[i]), 0.5)
            << " fp16 normalization failed for i = " << i;
    }
}

TEST_F(SpacesTest, bf16_ip_no_optimization_func_test) {
    size_t dim = 4;

    bfloat16 a[dim], b[dim];
    float sanity_a[dim], sanity_b[dim];
    for (size_t i = 0; i < dim; i++) {
        // multiplication of 0.25 have no rounding error when converted to bfloat16
        sanity_a[i] = 0.5f + i * 0.25f;
        a[i] = vecsim_types::float_to_bf16(sanity_a[i]);
        sanity_b[i] = (float)i * 0.25f;
        b[i] = vecsim_types::float_to_bf16(sanity_b[i]);
    }

    float dist = BF16_InnerProduct_LittleEndian((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, FP32_InnerProduct((const void *)sanity_a, (const void *)sanity_b, dim));
}

TEST_F(SpacesTest, fp16_ip_no_optimization_func_test) {
    size_t dim = 4;

    float16 a[dim], b[dim];
    float sanity_a[dim], sanity_b[dim];
    for (size_t i = 0; i < dim; i++) {
        // multiplication of 0.25 have no rounding error when converted to bfloat16
        sanity_a[i] = 0.5f + i * 0.25f;
        a[i] = vecsim_types::FP32_to_FP16(sanity_a[i]);
        sanity_b[i] = (float)i * 0.25f;
        b[i] = vecsim_types::FP32_to_FP16(sanity_b[i]);
    }

    float dist = FP16_InnerProduct((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, FP32_InnerProduct((const void *)sanity_a, (const void *)sanity_b, dim));
}

TEST_F(SpacesTest, int8_ip_no_optimization_func_test) {
    size_t dim = 4;
    int8_t a[] = {1, 0, 0, 0};
    int8_t b[] = {1, 0, 0, 0};

    float dist = INT8_InnerProduct((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 0.0);
}

TEST_F(SpacesTest, uint8_ip_no_optimization_func_test) {
    size_t dim = 4;
    uint8_t a[] = {1, 0, 0, 0};
    uint8_t b[] = {1, 0, 0, 0};

    float dist = UINT8_InnerProduct((const void *)a, (const void *)b, dim);
    ASSERT_EQ(dist, 0.0);
}

/* ======================== Cosine NO OPT ======================== */

TEST_F(SpacesTest, int8_Cosine_no_optimization_func_test) {
    size_t dim = 4;
    // create a vector with extra space for the norm
    int8_t v1[dim + sizeof(float)];
    int8_t v2[dim + sizeof(float)];

    test_utils::populate_int8_vec(v1, dim, 123);
    test_utils::populate_int8_vec(v2, dim, 123);

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1, dim);
    const float norm_v2 = test_utils::integral_compute_norm(v2, dim);
    std::memcpy(v1 + dim, &norm_v1, sizeof(norm_v1));
    std::memcpy(v2 + dim, &norm_v2, sizeof(norm_v2));

    float dist = INT8_Cosine((const void *)v1, (const void *)v2, dim);
    ASSERT_NEAR(dist, 0.0, 0.000001);
}

TEST_F(SpacesTest, uint8_Cosine_no_optimization_func_test) {
    size_t dim = 4;
    // create a vector with extra space for the norm
    uint8_t v1[dim + sizeof(float)];
    uint8_t v2[dim + sizeof(float)];

    test_utils::populate_uint8_vec(v1, dim, 123);
    test_utils::populate_uint8_vec(v2, dim, 123);

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1, dim);
    const float norm_v2 = test_utils::integral_compute_norm(v2, dim);
    std::memcpy(v1 + dim, &norm_v1, sizeof(norm_v1));
    std::memcpy(v2 + dim, &norm_v2, sizeof(norm_v2));

    float dist = UINT8_Cosine((const void *)v1, (const void *)v2, dim);
    ASSERT_NEAR(dist, 0.0, 0.000001);
}

/* ======================== Tests SQ8 ========================= */

TEST_F(SpacesTest, SQ8_FP32_ip_no_optimization_norm_func_test) {
    size_t dim = 5;

    // Create V1 fp32 query with precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_orig(query_size);
    test_utils::populate_sq8_fp32_query(v1_orig.data(), dim, true, 1234);

    // Create V2 as SQ8 quantized vector with different seed
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_FP32_NotOptimized_InnerProduct(v2_compressed.data(), v1_orig.data(), dim);

    float dist = SQ8_FP32_InnerProduct((const void *)v2_compressed.data(),
                                       (const void *)v1_orig.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01) << "SQ8_FP32_InnerProduct failed to match expected distance";
}

TEST_F(SpacesTest, SQ8_FP32_l2sqr_no_optimization_func_test) {
    size_t dim = 5;

    // Create V1 fp32 query with precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_orig(query_size);
    test_utils::populate_sq8_fp32_query(v1_orig.data(), dim, false, 1234);

    // Create V2 as SQ8 quantized vector with different seed
    // Storage layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_squares]
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, false, 5678);

    float baseline =
        test_utils::SQ8_FP32_NotOptimized_L2Sqr(v2_compressed.data(), v1_orig.data(), dim);

    float dist =
        SQ8_FP32_L2Sqr((const void *)v2_compressed.data(), (const void *)v1_orig.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01) << "SQ8_FP32_L2Sqr failed to match expected distance";
}

TEST_F(SpacesTest, SQ8_FP32_odd_dim_unaligned_metadata_test) {
    for (const size_t dim : {1UL, 5UL, 7UL, 15UL}) {
        const size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
        std::vector<float> query(query_size);
        test_utils::populate_sq8_fp32_query(query.data(), dim, false, 1234);

        const size_t storage_size =
            dim + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
        std::vector<uint8_t> allocation(storage_size + alignof(float));
        auto *storage = allocation.data();
        while (reinterpret_cast<std::uintptr_t>(storage) % alignof(float) != 0) {
            ++storage;
        }
        test_utils::populate_float_vec_to_sq8_with_metadata(storage, dim, false, 5678);

        const auto *metadata = storage + dim;
        ASSERT_NE(reinterpret_cast<std::uintptr_t>(metadata) % alignof(float), 0u);

        const float expected_ip =
            test_utils::SQ8_FP32_NotOptimized_InnerProduct(storage, query.data(), dim);
        const float expected_l2 =
            test_utils::SQ8_FP32_NotOptimized_L2Sqr(storage, query.data(), dim);

        EXPECT_NEAR(SQ8_FP32_InnerProduct(storage, query.data(), dim), expected_ip, 0.01)
            << "scalar IP with dim " << dim;
        EXPECT_NEAR(SQ8_FP32_L2Sqr(storage, query.data(), dim), expected_l2, 0.01)
            << "scalar L2 with dim " << dim;
        EXPECT_NEAR(IP_SQ8_FP32_GetDistFunc(dim, nullptr)(storage, query.data(), dim), expected_ip,
                    0.01)
            << "dispatched IP with dim " << dim;
        EXPECT_NEAR(L2_SQ8_FP32_GetDistFunc(dim, nullptr)(storage, query.data(), dim), expected_l2,
                    0.01)
            << "dispatched L2 with dim " << dim;
    }
}

/* ==================== MOD-17526 regression tests ====================
 * The SQ8-FP32 L2 kernels used to compute L2^2 via the ||x||^2 + ||y||^2 - 2*IP identity, which
 * cancels catastrophically in FP32 when both vectors share a large common offset.
 *
 * These check against an independent double-precision reference rather than another kernel
 * (pre-fix, scalar and SIMD were wrong together), and use relative rather than absolute
 * tolerances, since a ~0.01 absolute bound is what let the bug hide.
 */

namespace {

// Ground truth: dequantizes the storage by hand and accumulates in double. Independent of the
// production kernels and of test_utils::SQ8_FP32_NotOptimized_L2Sqr, which accumulates in float.
double SQ8_FP32_L2Sqr_DoubleReference(const uint8_t *storage, const float *query, size_t dim) {
    const float min_val = load_unaligned<float>(storage + dim + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(storage + dim + sq8::DELTA * sizeof(float));
    double res = 0.0;
    for (size_t i = 0; i < dim; i++) {
        const double dequantized =
            static_cast<double>(min_val) + static_cast<double>(delta) * storage[i];
        const double diff = static_cast<double>(query[i]) - dequantized;
        res += diff * diff;
    }
    return res;
}

// Checks the scalar kernel, the dispatched kernel, and every SIMD variant in this binary against
// `expected`, within `rel_tol` scaled by max(expected, 1.0).
void ExpectSQ8_FP32_L2SqrNear(const uint8_t *storage, const float *query, size_t dim,
                              double expected, double rel_tol, const std::string &context) {
    const double scale = std::max(expected, 1.0);
    auto check = [&](double actual, const char *label) {
        EXPECT_NEAR(actual, expected, rel_tol * scale)
            << context << " [" << label << "]: actual=" << actual << ", expected=" << expected;
    };

    check(SQ8_FP32_L2Sqr(storage, query, dim), "scalar");
    check(L2_SQ8_FP32_GetDistFunc(dim, nullptr)(storage, query, dim), "dispatched");

    auto optimization = getCpuOptimizationFeatures();

    // The chooser only reaches the x86 kernels at dim >= 8, and the AVX2 residual path reads a
    // full 32-byte vector (my_mm256_maskz_loadu_ps is an unconditional load), which overreads a
    // smaller query blob. aarch64 has no such floor, so those tiers stay ungated to match
    // production.
    const bool x86_simd_reachable = dim >= 8;
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (x86_simd_reachable && optimization.avx512f && optimization.avx512bw &&
        optimization.avx512vl && optimization.avx512vnni) {
        check(Choose_SQ8_FP32_L2_implementation_AVX512F_BW_VL_VNNI(dim)(storage, query, dim),
              "AVX512F_BW_VL_VNNI");
    }
#endif
#ifdef OPT_AVX2_FMA
    if (x86_simd_reachable && optimization.avx2 && optimization.fma3) {
        check(Choose_SQ8_FP32_L2_implementation_AVX2_FMA(dim)(storage, query, dim), "AVX2_FMA");
    }
#endif
#ifdef OPT_AVX2
    if (x86_simd_reachable && optimization.avx2) {
        check(Choose_SQ8_FP32_L2_implementation_AVX2(dim)(storage, query, dim), "AVX2");
    }
#endif
#ifdef OPT_SSE4
    if (x86_simd_reachable && optimization.sse4_1) {
        check(Choose_SQ8_FP32_L2_implementation_SSE4(dim)(storage, query, dim), "SSE4");
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        check(Choose_SQ8_FP32_L2_implementation_SVE2(dim)(storage, query, dim), "SVE2");
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        check(Choose_SQ8_FP32_L2_implementation_SVE(dim)(storage, query, dim), "SVE");
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        check(Choose_SQ8_FP32_L2_implementation_NEON(dim)(storage, query, dim), "NEON");
    }
#endif
}

// Builds the blobs from explicit float vectors, kept separate from generation so a caller can
// derive a shifted pair by adding a constant to the same values. Re-sampling
// std::uniform_real_distribution over a shifted [min,max] with the same seed does NOT give "the
// same vector plus a constant", which is what a translation-invariance test needs.
struct SQ8_FP32_L2_TestVectors {
    std::vector<float> query;     // [float values (dim)] [sum] [sum_squares]
    std::vector<uint8_t> storage; // [uint8_t values (dim)] [min] [delta] [sum] [sum_squares]
};

SQ8_FP32_L2_TestVectors
BuildSQ8_FP32_L2_TestVectorsFromValues(const std::vector<float> &query_values,
                                       const std::vector<float> &storage_values) {
    const size_t dim = query_values.size();
    SQ8_FP32_L2_TestVectors v;
    v.query.resize(dim + sq8::query_metadata_count<VecSimMetric_L2>());
    std::copy(query_values.begin(), query_values.end(), v.query.begin());
    test_utils::preprocess_sq8_fp32_query(v.query.data(), dim);

    v.storage.resize(dim * sizeof(uint8_t) +
                     sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float));
    test_utils::quantize_float_vec_to_sq8_with_metadata(storage_values.data(), dim,
                                                        v.storage.data());
    return v;
}

// One base (query, storage) pair, plus the same pair with `offset` added to every element.
std::pair<SQ8_FP32_L2_TestVectors, SQ8_FP32_L2_TestVectors>
BuildSQ8_FP32_L2_ShiftedPair(size_t dim, float spread, float offset, int seed) {
    std::vector<float> query_values(dim), storage_values(dim);
    test_utils::populate_float_vec(query_values.data(), dim, seed, -spread, spread);
    test_utils::populate_float_vec(storage_values.data(), dim, seed + 1, -spread, spread);

    std::vector<float> shifted_query(dim), shifted_storage(dim);
    for (size_t i = 0; i < dim; i++) {
        shifted_query[i] = query_values[i] + offset;
        shifted_storage[i] = storage_values[i] + offset;
    }

    return {BuildSQ8_FP32_L2_TestVectorsFromValues(query_values, storage_values),
            BuildSQ8_FP32_L2_TestVectorsFromValues(shifted_query, shifted_storage)};
}

} // namespace

TEST_F(SpacesTest, SQ8_FP32_L2Sqr_MOD17526_TranslationInvariance) {
    // L2 must satisfy L2(x, y) == L2(x + C, y + C) for any constant offset C. The old
    // identity-based kernel violated this for large C relative to the vectors' spread; the
    // direct-residual kernel must satisfy it (up to FP32 rounding, hence the relative tolerance).
    const float spread = 1.0f;
    const float large_offset = 100000.0f; // offset/spread == 100000, far past the ~4000 threshold
                                          // where the old identity started to catastrophically
                                          // cancel in FP32.
    // Mixes exact multiples of the SIMD chunk widths with awkward remainders, so the
    // large-offset case also runs through the residual/masked-lane tails.
    for (const size_t dim : {128UL, 129UL, 145UL, 512UL, 513UL, 527UL, 768UL}) {
        auto [no_offset, with_offset] =
            BuildSQ8_FP32_L2_ShiftedPair(dim, spread, large_offset, 1234);

        const double expected_no_offset =
            SQ8_FP32_L2Sqr_DoubleReference(no_offset.storage.data(), no_offset.query.data(), dim);
        const double expected_with_offset = SQ8_FP32_L2Sqr_DoubleReference(
            with_offset.storage.data(), with_offset.query.data(), dim);

        // The reference itself must be translation-invariant. Not exact equality: float32
        // spacing near 1e5 is coarser than near 0, which can shift a value to an adjacent
        // quantization code.
        ASSERT_NEAR(expected_no_offset, expected_with_offset,
                    1e-2 * std::max(expected_no_offset, 1.0))
            << "dim " << dim << ": independent reference is not translation-invariant";

        ExpectSQ8_FP32_L2SqrNear(no_offset.storage.data(), no_offset.query.data(), dim,
                                 expected_no_offset, 1e-3, "dim " + std::to_string(dim) + ", C=0");

        // `min - y` is exact in FP32 by Sterbenz (both large, within a factor of 2), so no
        // large-scale cancellation remains and the tolerance stays as tight as the C=0 case.
        ExpectSQ8_FP32_L2SqrNear(
            with_offset.storage.data(), with_offset.query.data(), dim, expected_with_offset, 1e-3,
            "dim " + std::to_string(dim) + ", C=" + std::to_string(large_offset));
    }
}

TEST_F(SpacesTest, SQ8_FP32_L2Sqr_MOD17526_TicketReproShape) {
    // The ticket's literal repro: x=[100000,100008], y=[100000,100000]. True L2^2 = (100000 -
    // 100000)^2 + (100008 - 100000)^2 = 64. The old ||x||^2+||y||^2-2*IP identity computed this
    // as a huge value minus a nearly-equal huge value in FP32 and could return 0, a negative
    // number, or other unbounded garbage instead of 64.
    const size_t dim = 2;
    const float storage_values[dim] = {100000.0f, 100008.0f};
    const float query_values[dim] = {100000.0f, 100000.0f};

    std::vector<float> query(dim + sq8::query_metadata_count<VecSimMetric_L2>());
    std::copy(query_values, query_values + dim, query.begin());
    test_utils::preprocess_sq8_fp32_query(query.data(), dim);

    std::vector<uint8_t> storage(dim * sizeof(uint8_t) +
                                 sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float));
    test_utils::quantize_float_vec_to_sq8_with_metadata(storage_values, dim, storage.data());

    ExpectSQ8_FP32_L2SqrNear(storage.data(), query.data(), dim, /*expected=*/64.0, 1e-4,
                             "MOD-17526 ticket repro shape");
}

TEST_F(SpacesTest, SQ8_FP32_L2Sqr_MOD17526_ScalarAssociativityRegression) {
    // Pins a scalar-only ordering bug: `(min_val + delta*q) - y` rounds the dequantized value at
    // the large offset's precision before subtracting, discarding the residual, while
    // `delta*q + (min_val - y)` keeps it. With min=100000, delta=8/255, q=1 the dequantized value
    // is 100000.031372549..., which FP32 rounds to exactly y=100000.03125, so the buggy order
    // yields diff=0 instead of ~0.0001225.
    const size_t dim = 8;
    // storage: min=100000 (index 0), max=100008 (index 7) => delta = 8/255, so index 1
    // quantizes to q=1 (dequantizes to 100000 + 8/255 = 100000.031372549...).
    const float storage_values[dim] = {
        100000.0f, 100000.0f + 8.0f / 255.0f, 100000.0f, 100000.0f, 100000.0f, 100000.0f, 100000.0f,
        100008.0f};
    // query matches storage exactly everywhere except index 1, so every other element
    // contributes exactly 0 and the whole result isolates the associativity bug at index 1.
    const float query_values[dim] = {100000.0f, 100000.03125f, 100000.0f, 100000.0f,
                                     100000.0f, 100000.0f,     100000.0f, 100008.0f};

    std::vector<float> query(dim + sq8::query_metadata_count<VecSimMetric_L2>());
    std::copy(query_values, query_values + dim, query.begin());
    test_utils::preprocess_sq8_fp32_query(query.data(), dim);

    std::vector<uint8_t> storage(dim * sizeof(uint8_t) +
                                 sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float));
    test_utils::quantize_float_vec_to_sq8_with_metadata(storage_values, dim, storage.data());

    const double expected = SQ8_FP32_L2Sqr_DoubleReference(storage.data(), query.data(), dim);
    ASSERT_GT(expected, 0.0) << "test construction should produce a nonzero true distance";
    // Absolute, not relative: the true value is ~1.5e-8 and the bug returns exactly 0.
    EXPECT_NEAR(SQ8_FP32_L2Sqr(storage.data(), query.data(), dim), expected, 1e-9)
        << "scalar kernel: expected=" << expected;
}

TEST_F(SpacesTest, SQ8_FP32_L2Sqr_MOD17526_RegularVectorSanity) {
    // Sanity check that the fix didn't regress the common case: ordinary small-magnitude
    // vectors, no large shared offset, realistic embedding-sized dims.
    for (const size_t dim : {128UL, 768UL}) {
        auto [v, _unused] =
            BuildSQ8_FP32_L2_ShiftedPair(dim, /*spread=*/1.0f, /*offset=*/0.0f, 4242);
        const double expected =
            SQ8_FP32_L2Sqr_DoubleReference(v.storage.data(), v.query.data(), dim);
        ExpectSQ8_FP32_L2SqrNear(v.storage.data(), v.query.data(), dim, expected, 1e-3,
                                 "dim " + std::to_string(dim) + ", regular vectors");
    }
}

/* ======================== Tests SQ8-FP16 ========================= */

TEST_F(SpacesTest, SQ8_FP16_ip_no_optimization_norm_func_test) {
    size_t dim = 5;

    // Create V1 fp16 query with precomputed sum and sum_squares
    // Query layout: [float16 values (dim)] [sum (float)] [sum_squares (float)]
    // Allocate as std::vector<float16> so v1_query.data() is alignof(float16)-aligned, as
    // required by the SQ8_FP16 production kernels' typed float16* loads. Add extra float16
    // slots to cover the trailing FP32 metadata bytes.
    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    // Create V2 as SQ8 quantized vector with different seed
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_InnerProduct(v2_compressed.data(), v1_query.data(), dim);

    float dist = SQ8_FP16_InnerProduct((const void *)v2_compressed.data(),
                                       (const void *)v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01) << "SQ8_FP16_InnerProduct failed to match expected distance";
}

TEST_F(SpacesTest, SQ8_FP16_cosine_no_optimization_norm_func_test) {
    size_t dim = 5;

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_Cosine(v2_compressed.data(), v1_query.data(), dim);

    float dist =
        SQ8_FP16_Cosine((const void *)v2_compressed.data(), (const void *)v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01) << "SQ8_FP16_Cosine failed to match expected distance";
}

TEST_F(SpacesTest, SQ8_FP16_l2sqr_no_optimization_func_test) {
    size_t dim = 5;

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, false, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, false, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_L2Sqr(v2_compressed.data(), v1_query.data(), dim);

    float dist =
        SQ8_FP16_L2Sqr((const void *)v2_compressed.data(), (const void *)v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01) << "SQ8_FP16_L2Sqr failed to match expected distance";
}

TEST_F(SpacesTest, SQ8_FP16_l2sqr_odd_dim_unaligned_metadata_test) {
    constexpr size_t dim = 5;
    constexpr size_t storage_bytes =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    static_assert(sizeof(float) % sizeof(float16) == 0);
    constexpr size_t query_values_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * sizeof(float) / sizeof(float16);

    alignas(float) std::array<uint8_t, storage_bytes> storage{};
    alignas(float) std::array<float16, query_values_count> query{};

    for (size_t i = 0; i < dim; i++) {
        storage[i] = static_cast<uint8_t>(i + 1);
    }

    auto store_float = [](uint8_t *dst, float value) { std::memcpy(dst, &value, sizeof(value)); };

    constexpr float min_val = 0.0f;
    constexpr float delta = 1.0f;
    constexpr float storage_sum = 15.0f;
    constexpr float storage_sum_squares = 55.0f;
    uint8_t *storage_meta = storage.data() + dim;
    store_float(storage_meta + sq8::MIN_VAL * sizeof(float), min_val);
    store_float(storage_meta + sq8::DELTA * sizeof(float), delta);
    store_float(storage_meta + sq8::SUM * sizeof(float), storage_sum);
    store_float(storage_meta + sq8::SUM_SQUARES * sizeof(float), storage_sum_squares);

    for (size_t i = 0; i < dim; i++) {
        query[i] = vecsim_types::FP32_to_FP16(static_cast<float>(i + 2));
    }

    constexpr float query_sum = 20.0f;
    constexpr float query_sum_squares = 90.0f;
    uint8_t *query_meta = reinterpret_cast<uint8_t *>(query.data() + dim);
    store_float(query_meta + sq8::SUM_QUERY * sizeof(float), query_sum);
    store_float(query_meta + sq8::SUM_SQUARES_QUERY * sizeof(float), query_sum_squares);

    const auto *storage_sum_squares_addr = storage_meta + sq8::SUM_SQUARES * sizeof(float);
    const auto *query_sum_squares_addr = query_meta + sq8::SUM_SQUARES_QUERY * sizeof(float);
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(storage_sum_squares_addr) % alignof(float), 0u);
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(query_sum_squares_addr) % alignof(float), 0u);

    const float dist = SQ8_FP16_L2Sqr(storage.data(), query.data(), dim);

    ASSERT_FLOAT_EQ(dist, 5.0f);
}

/* ======================== Test Getters ======================== */

TEST_F(SpacesTest, GetDistFuncInvalidMetricFP32) {
    EXPECT_THROW(
        (spaces::GetDistFunc<float, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricFP64) {
    EXPECT_THROW(
        (spaces::GetDistFunc<double, double>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricBF16) {
    EXPECT_THROW((spaces::GetDistFunc<bfloat16, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10,
                                                       nullptr)),
                 std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricFP16) {
    EXPECT_THROW(
        (spaces::GetDistFunc<float16, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricINT8) {
    EXPECT_THROW(
        (spaces::GetDistFunc<int8_t, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricUINT8) {
    EXPECT_THROW(
        (spaces::GetDistFunc<uint8_t, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricSQ8) {
    // SQ8 to SQ8 (symmetric)
    EXPECT_THROW(
        (spaces::GetDistFunc<sq8, float>((VecSimMetric)(VecSimMetric_Cosine + 1), 10, nullptr)),
        std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricSQ8ToFloat) {
    // SQ8 to float (asymmetric)
    EXPECT_THROW((spaces::GetDistFunc<sq8, float, float>((VecSimMetric)(VecSimMetric_Cosine + 1),
                                                         10, nullptr)),
                 std::invalid_argument);
}
TEST_F(SpacesTest, GetDistFuncInvalidMetricSQ8ToFP16) {
    // SQ8 storage with FP16 query (asymmetric)
    EXPECT_THROW((spaces::GetDistFunc<sq8, float, float16>((VecSimMetric)(VecSimMetric_Cosine + 1),
                                                           10, nullptr)),
                 std::invalid_argument);
}

// Positive tests for GetDistFunc - verify correct function is returned
TEST_F(SpacesTest, GetDistFuncSQ8Symmetric) {
    // SQ8 to SQ8 (symmetric) - should return SQ8_SQ8 functions
    size_t dim = 128;
    auto l2_func = spaces::GetDistFunc<sq8, float>(VecSimMetric_L2, dim, nullptr);
    auto ip_func = spaces::GetDistFunc<sq8, float>(VecSimMetric_IP, dim, nullptr);
    auto cosine_func = spaces::GetDistFunc<sq8, float>(VecSimMetric_Cosine, dim, nullptr);
    ASSERT_EQ(l2_func, L2_SQ8_SQ8_GetDistFunc(dim, nullptr));
    ASSERT_EQ(ip_func, IP_SQ8_SQ8_GetDistFunc(dim, nullptr));
    ASSERT_EQ(cosine_func, Cosine_SQ8_SQ8_GetDistFunc(dim, nullptr));
}

TEST_F(SpacesTest, GetDistFuncSQ8Asymmetric) {
    // SQ8 to float (asymmetric) - should return SQ8 functions
    size_t dim = 128;
    auto l2_func = spaces::GetDistFunc<sq8, float, float>(VecSimMetric_L2, dim, nullptr);
    auto ip_func = spaces::GetDistFunc<sq8, float, float>(VecSimMetric_IP, dim, nullptr);
    auto cosine_func = spaces::GetDistFunc<sq8, float, float>(VecSimMetric_Cosine, dim, nullptr);
    ASSERT_EQ(l2_func, L2_SQ8_FP32_GetDistFunc(dim, nullptr));
    ASSERT_EQ(ip_func, IP_SQ8_FP32_GetDistFunc(dim, nullptr));
    ASSERT_EQ(cosine_func, Cosine_SQ8_FP32_GetDistFunc(dim, nullptr));
}

TEST_F(SpacesTest, GetDistFuncSQ8FP16Asymmetric) {
    // SQ8 storage with FP16 query (asymmetric) - should return SQ8_FP16 functions.
    // Per-ISA dispatcher walk coverage lives in the SQ8_FP16 SpacesOptimizationTest below.
    size_t dim = 128;
    auto l2_func = spaces::GetDistFunc<sq8, float, float16>(VecSimMetric_L2, dim, nullptr);
    auto ip_func = spaces::GetDistFunc<sq8, float, float16>(VecSimMetric_IP, dim, nullptr);
    auto cosine_func = spaces::GetDistFunc<sq8, float, float16>(VecSimMetric_Cosine, dim, nullptr);
    ASSERT_EQ(l2_func, L2_SQ8_FP16_GetDistFunc(dim, nullptr));
    ASSERT_EQ(ip_func, IP_SQ8_FP16_GetDistFunc(dim, nullptr));
    ASSERT_EQ(cosine_func, Cosine_SQ8_FP16_GetDistFunc(dim, nullptr));

    // dim < 16 takes the scalar early-return in every SQ8_FP16 dispatcher (no SIMD tier).
    size_t small_dim = 8;
    ASSERT_EQ(L2_SQ8_FP16_GetDistFunc(small_dim, nullptr), SQ8_FP16_L2Sqr);
    ASSERT_EQ(IP_SQ8_FP16_GetDistFunc(small_dim, nullptr), SQ8_FP16_InnerProduct);
    ASSERT_EQ(Cosine_SQ8_FP16_GetDistFunc(small_dim, nullptr), SQ8_FP16_Cosine);
}

#ifdef CPU_FEATURES_ARCH_X86_64
#ifdef OPT_SSE
// Regression test for MOD-16730: the FP32 L2 SSE kernel's residual % 4 == 3 path used
// _mm_loadr_ps (movaps), which faults on non-16-byte-aligned addresses. Vectors are not
// guaranteed such alignment: VecSimAllocator::allocate() returns malloc + 8 (allocation
// header), and the dispatcher sets no alignment hint when dim % 4 != 0. This test feeds the
// kernel buffers at that exact placement (16-aligned base + 8).
TEST_F(SpacesTest, FP32_L2Sqr_SSE_misaligned_residual3) {
    constexpr size_t dim = 19; // dim % 16 == 3 -> residual 3 path
    alignas(16) static char raw1[16 + dim * sizeof(float)];
    alignas(16) static char raw2[16 + dim * sizeof(float)];
    float *v1 = reinterpret_cast<float *>(raw1 + 8); // address == 8 (mod 16)
    float *v2 = reinterpret_cast<float *>(raw2 + 8);
    for (size_t i = 0; i < dim; i++) {
        v1[i] = float(i);
        v2[i] = float(i) + 1.5f;
    }
    float baseline = FP32_L2Sqr(v1, v2, dim);
    dist_func_t<float> arch_opt_func = spaces::Choose_FP32_L2_implementation_SSE(dim);
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim));
}
#endif // OPT_SSE

TEST_F(SpacesTest, smallDimChooser) {
    // Verify that dimensions below each type's SIMD threshold get the no optimization function.
    // FP64 is optimized from dim >= 4.
    for (size_t dim = 1; dim < 4; dim++) {
        ASSERT_EQ(L2_FP64_GetDistFunc(dim), FP64_L2Sqr);
        ASSERT_EQ(IP_FP64_GetDistFunc(dim), FP64_InnerProduct);
    }
    // FP32 and FP16 are optimized from dim >= 8 (FP16 only on machines with F16C; for
    // 8 <= dim < 32 the chosen FP16 function depends on the available features, so we only
    // assert the range that is naive regardless of features).
    for (size_t dim = 1; dim < 8; dim++) {
        ASSERT_EQ(L2_FP32_GetDistFunc(dim), FP32_L2Sqr);
        ASSERT_EQ(IP_FP32_GetDistFunc(dim), FP32_InnerProduct);
        ASSERT_EQ(L2_FP16_GetDistFunc(dim), FP16_L2Sqr);
        ASSERT_EQ(IP_FP16_GetDistFunc(dim), FP16_InnerProduct);
    }
    // BF16, INT8 and UINT8 are optimized from dim >= 32.
    for (size_t dim = 1; dim < 32; dim++) {
        ASSERT_EQ(L2_BF16_GetDistFunc(dim), BF16_L2Sqr_LittleEndian);
        ASSERT_EQ(L2_INT8_GetDistFunc(dim), INT8_L2Sqr);
        ASSERT_EQ(L2_UINT8_GetDistFunc(dim), UINT8_L2Sqr);
        ASSERT_EQ(IP_BF16_GetDistFunc(dim), BF16_InnerProduct_LittleEndian);
        ASSERT_EQ(IP_INT8_GetDistFunc(dim), INT8_InnerProduct);
        ASSERT_EQ(IP_UINT8_GetDistFunc(dim), UINT8_InnerProduct);
        ASSERT_EQ(Cosine_INT8_GetDistFunc(dim), INT8_Cosine);
        ASSERT_EQ(Cosine_UINT8_GetDistFunc(dim), UINT8_Cosine);
    }
}
#endif

TEST_F(SpacesTest, FP16NativeDispatcherDimensionCap) {
    auto optimization = getCpuOptimizationFeatures();
    unsigned char alignment = 0;

    static_assert(spaces::FP16_MAX_UNIT_IP_SIMD_DIM == 65504);
    static_assert(spaces::FP16_MAX_UNIT_L2_SIMD_DIM == 16376);

#if defined(CPU_FEATURES_ARCH_X86_64) && defined(OPT_AVX512_FP16_VL)
    if (optimization.avx512_fp16 && optimization.avx512vl) {
        const size_t ip_cap = spaces::FP16_MAX_UNIT_IP_SIMD_DIM;
        const size_t l2_cap = spaces::FP16_MAX_UNIT_L2_SIMD_DIM;
        ASSERT_EQ(IP_FP16_GetDistFunc(ip_cap, &alignment, &optimization),
                  Choose_FP16_IP_implementation_AVX512FP16_VL(ip_cap));
        ASSERT_EQ(L2_FP16_GetDistFunc(l2_cap, &alignment, &optimization),
                  Choose_FP16_L2_implementation_AVX512FP16_VL(l2_cap));
        ASSERT_NE(IP_FP16_GetDistFunc(ip_cap + 1, &alignment, &optimization),
                  Choose_FP16_IP_implementation_AVX512FP16_VL(ip_cap + 1));
        ASSERT_NE(L2_FP16_GetDistFunc(l2_cap + 1, &alignment, &optimization),
                  Choose_FP16_L2_implementation_AVX512FP16_VL(l2_cap + 1));
    }
#elif defined(CPU_FEATURES_ARCH_AARCH64)
    const size_t ip_cap = spaces::FP16_MAX_UNIT_IP_SIMD_DIM;
    const size_t l2_cap = spaces::FP16_MAX_UNIT_L2_SIMD_DIM;
#ifdef OPT_SVE2
    if (optimization.sve2) {
        ASSERT_EQ(IP_FP16_GetDistFunc(ip_cap, &alignment, &optimization),
                  Choose_FP16_IP_implementation_SVE2(ip_cap));
        ASSERT_EQ(L2_FP16_GetDistFunc(l2_cap, &alignment, &optimization),
                  Choose_FP16_L2_implementation_SVE2(l2_cap));
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        auto sve_optimization = optimization;
        sve_optimization.sve2 = 0;
        ASSERT_EQ(IP_FP16_GetDistFunc(ip_cap, &alignment, &sve_optimization),
                  Choose_FP16_IP_implementation_SVE(ip_cap));
        ASSERT_EQ(L2_FP16_GetDistFunc(l2_cap, &alignment, &sve_optimization),
                  Choose_FP16_L2_implementation_SVE(l2_cap));
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        auto neon_optimization = optimization;
        neon_optimization.sve = neon_optimization.sve2 = 0;
        ASSERT_EQ(IP_FP16_GetDistFunc(ip_cap, &alignment, &neon_optimization),
                  Choose_FP16_IP_implementation_NEON_HP(ip_cap));
        ASSERT_EQ(L2_FP16_GetDistFunc(l2_cap, &alignment, &neon_optimization),
                  Choose_FP16_L2_implementation_NEON_HP(l2_cap));
    }
#endif
    ASSERT_EQ(IP_FP16_GetDistFunc(ip_cap + 1, &alignment, &optimization), FP16_InnerProduct);
    ASSERT_EQ(L2_FP16_GetDistFunc(l2_cap + 1, &alignment, &optimization), FP16_L2Sqr);
#endif
}

// The cap is evaluated once by the dispatcher. The selected distance function therefore has no
// added hot-path instructions, while high-dimensional unit-bounded inputs avoid a native-fp16
// total that exceeds 65,504.
TEST_F(SpacesTest, FP16HighDimensionDispatcherAvoidsHalfOverflow) {
    const size_t ip_dim = spaces::FP16_MAX_UNIT_IP_SIMD_DIM + 32;
    const float16 one = vecsim_types::FP32_to_FP16(1.0f);
    std::vector<float16> ip_v1(ip_dim, one);
    std::vector<float16> ip_v2(ip_dim, one);
    auto ip = IP_FP16_GetDistFunc(ip_dim);
    ASSERT_EQ(ip(ip_v1.data(), ip_v2.data(), ip_dim), 1.0f - static_cast<float>(ip_dim));

    const size_t l2_dim = spaces::FP16_MAX_UNIT_L2_SIMD_DIM + 8;
    const float16 minus_one = vecsim_types::FP32_to_FP16(-1.0f);
    std::vector<float16> l2_v1(l2_dim, one);
    std::vector<float16> l2_v2(l2_dim, minus_one);
    auto l2 = L2_FP16_GetDistFunc(l2_dim);
    ASSERT_EQ(l2(l2_v1.data(), l2_v2.data(), l2_dim), 4.0f * static_cast<float>(l2_dim));
}

#if defined(CPU_FEATURES_ARCH_X86_64) && defined(OPT_AVX512_FP16_VL)
TEST_F(SpacesTest, AVX512FP16InnerProductSubtractsInFP32) {
    const auto optimization = getCpuOptimizationFeatures();
    if (!optimization.avx512_fp16 || !optimization.avx512vl) {
        GTEST_SKIP() << "AVX512FP16+VL is unavailable";
    }

    constexpr size_t dim = 32;
    const float16 zero = vecsim_types::FP32_to_FP16(0.0f);
    std::vector<float16> v1(dim, zero);
    std::vector<float16> v2(dim, zero);
    v1[0] = vecsim_types::FP32_to_FP16(-1.0f / 4096.0f);
    v2[0] = vecsim_types::FP32_to_FP16(1.0f);

    auto ip = IP_FP16_GetDistFunc(dim, nullptr, &optimization);
    ASSERT_EQ(ip, Choose_FP16_IP_implementation_AVX512FP16_VL(dim));
    ASSERT_EQ(ip(v1.data(), v2.data(), dim), 1.0f + 1.0f / 4096.0f);
}
#endif

/* ======================== Test SIMD Functions ======================== */

// In this following tests we assume that compiler supports all X86 optimizations, so if we have
// some hardware flag enabled, we check that the corresponding optimization function was chosen.

class FP32SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(FP32SpacesOptimizationTest, FP32L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    float v[dim];
    float v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = (float)i;
        v2[i] = (float)(i + 1.5);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(float) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(float) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = FP32_L2Sqr(v, v2, dim);
// CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        // Unset avx512f flag, so we'll choose the next optimization (AVX).
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX
    if (optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_AVX(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        // Unset avx flag as well, so we'll choose the next optimization (SSE).
        optimization.avx = 0;
    }
#endif
#ifdef OPT_SSE
    if (optimization.sse) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_SSE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        // Unset sse flag as well, so we'll choose the next option (default).
        optimization.sse = 0;
    }
#endif

// CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif

    unsigned char alignment = 0;
    arch_opt_func = L2_FP32_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP32_L2Sqr) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(FP32SpacesOptimizationTest, FP32InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    float v[dim];
    float v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = (float)i;
        v2[i] = (float)(i + 1.5);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(float) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(float) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = FP32_InnerProduct(v, v2, dim);

// CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX
    if (optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_AVX(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        optimization.avx = 0;
    }
#endif
#ifdef OPT_SSE
    if (optimization.sse) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_SSE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        optimization.sse = 0;
    }
#endif

// CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP32_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif

    unsigned char alignment = 0;
    arch_opt_func = IP_FP32_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP32_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

INSTANTIATE_TEST_SUITE_P(FP32OptFuncs, FP32SpacesOptimizationTest,
                         testing::Range(8UL, 32 * 2UL + 1));

class FP64SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(FP64SpacesOptimizationTest, FP64L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    double v[dim];
    double v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = (double)i;
        v2[i] = (double)(i + 1.5);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(double) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(double) : 0;
    };

    dist_func_t<double> arch_opt_func;
    double baseline = FP64_L2Sqr(v, v2, dim);
#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX
    if (optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_AVX(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        optimization.avx = 0;
    }
#endif
#ifdef OPT_SSE
    if (optimization.sse) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_SSE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        optimization.sse = 0;
    }
#endif

// CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "NOEN with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif

    unsigned char alignment = 0;
    arch_opt_func = L2_FP64_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP64_L2Sqr) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(FP64SpacesOptimizationTest, FP64InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    double v[dim];
    double v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = (double)i;
        v2[i] = (double)(i + 1.5);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(double) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(double) : 0;
    };

    dist_func_t<double> arch_opt_func;
    double baseline = FP64_InnerProduct(v, v2, dim);
#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX
    if (optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_AVX(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        optimization.avx = 0;
    }
#endif
#ifdef OPT_SSE
    if (optimization.sse) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_SSE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        optimization.sse = 0;
    }
#endif
// CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP64_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        EXPECT_EQ(baseline, arch_opt_func(v, v2, dim)) << "NEON with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif

    unsigned char alignment = 0;
    arch_opt_func = IP_FP64_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP64_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

INSTANTIATE_TEST_SUITE_P(FP64OptFuncs, FP64SpacesOptimizationTest,
                         testing::Range(4UL, 16 * 2UL + 1));

class BF16SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(BF16SpacesOptimizationTest, BF16InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    bfloat16 v[dim];
    bfloat16 v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = vecsim_types::float_to_bf16((float)i);
        v2[i] = vecsim_types::float_to_bf16(((float)i + 1.5f));
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(bfloat16) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(bfloat16) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = BF16_InnerProduct_LittleEndian(v, v2, dim);
#ifdef OPT_AVX512_BF16_VL
    if (optimization.avx512_bf16 && optimization.avx512vl) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_AVX512BF16_VL(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512_bf16 = optimization.avx512vl = 0;
    }
#endif
#ifdef OPT_AVX512_BW_VBMI2
    if (optimization.avx512bw && optimization.avx512vbmi2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_AVX512BW_VBMI2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512bw = optimization.avx512vbmi2 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE3
    if (optimization.sse3) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_SSE3(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        optimization.sse3 = 0;
    }
#endif
#ifdef OPT_SVE_BF16
    if (optimization.svebf16) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_SVE_BF16(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE_BF16 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "SVE_BF16 with dim " << dim;
        optimization.svebf16 = 0;
    }
#endif
#ifdef OPT_NEON_BF16
    if (optimization.bf16) {
        unsigned char alignment = 0;
        arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_IP_implementation_NEON_BF16(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "NEON_BF16 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "NEON_BF16 with dim " << dim;
        optimization.bf16 = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = IP_BF16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, BF16_InnerProduct_LittleEndian)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(BF16SpacesOptimizationTest, BF16L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    bfloat16 v[dim];
    bfloat16 v2[dim];
    for (size_t i = 0; i < dim; i++) {
        v[i] = vecsim_types::float_to_bf16((float)i);
        v2[i] = vecsim_types::float_to_bf16(((float)i + 1.5f));
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(bfloat16) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(bfloat16) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = BF16_L2Sqr_LittleEndian(v, v2, dim);
#ifdef OPT_AVX512_BW_VBMI2
    if (optimization.avx512bw && optimization.avx512vbmi2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_L2_implementation_AVX512BW_VBMI2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        optimization.avx512bw = optimization.avx512vbmi2 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_L2_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "AVX with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE3
    if (optimization.sse3) {
        unsigned char alignment = 0;
        arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_L2_implementation_SSE3(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SSE with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        optimization.sse3 = 0;
    }
#endif
#ifdef OPT_SVE_BF16
    if (optimization.svebf16) {
        unsigned char alignment = 0;
        arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_L2_implementation_SVE_BF16(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "SVE_BF16 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "SVE_BF16 with dim " << dim;
        optimization.svebf16 = 0;
    }
#endif
#ifdef OPT_NEON_BF16
    if (optimization.bf16) {
        unsigned char alignment = 0;
        arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_BF16_L2_implementation_NEON_BF16(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "NEON_BF16 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "NEON_BF16 with dim " << dim;
        optimization.bf16 = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = L2_BF16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, BF16_L2Sqr_LittleEndian)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

INSTANTIATE_TEST_SUITE_P(BF16OptFuncs, BF16SpacesOptimizationTest,
                         testing::Range(32UL, 32 * 2UL + 1));

class FP16SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(FP16SpacesOptimizationTest, FP16InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    float16 v1[dim], v2[dim];
    float v1_fp32[dim], v2_fp32[dim];
    for (size_t i = 0; i < dim; i++) {
        v1_fp32[i] = (float)i;
        v1[i] = vecsim_types::FP32_to_FP16(v1_fp32[i]);
        v2_fp32[i] = (float)i + 1.5f;
        v2[i] = vecsim_types::FP32_to_FP16(v2_fp32[i]);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(float16) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(float16) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = FP16_InnerProduct(v1, v2, dim);
    ASSERT_EQ(baseline, FP32_InnerProduct(v1_fp32, v2_fp32, dim)) << "Baseline check " << dim;
#if defined(CPU_FEATURES_ARCH_X86_64)
    // Turn off advanced fp16 flags. They will be tested in the next test.
    optimization.avx512_fp16 = optimization.avx512vl = 0;
#ifdef OPT_AVX512F
    // The AVX512F FP16 tier requires at least 16 elements; below that the dispatcher falls
    // through to the next tier.
    if (optimization.avx512f && dim >= 16) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
    }
    optimization.avx512f = 0;
#endif
#ifdef OPT_F16C
    if (optimization.f16c && optimization.fma3 && optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_F16C(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "F16C with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "F16C with dim " << dim;
        optimization.f16c = optimization.fma3 = optimization.avx = 0;
    }
#endif
#elif defined(CPU_FEATURES_ARCH_AARCH64)
    // Turn off advanced fp16 flags. They will be tested in the next test.
    optimization.sve = optimization.sve2 = optimization.asimdhp = 0;
#endif
    unsigned char alignment = 0;
    arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP16_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(FP16SpacesOptimizationTest, FP16L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    float16 v1[dim], v2[dim];
    float v1_fp32[dim], v2_fp32[dim];
    for (size_t i = 0; i < dim; i++) {
        v1_fp32[i] = (float)i;
        v1[i] = vecsim_types::FP32_to_FP16(v1_fp32[i]);
        v2_fp32[i] = (float)i + 1.5f;
        v2[i] = vecsim_types::FP32_to_FP16(v2_fp32[i]);
    }

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(float16) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(float16) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = FP16_L2Sqr(v1, v2, dim);
    ASSERT_EQ(baseline, FP32_L2Sqr(v1_fp32, v2_fp32, dim)) << "Baseline check " << dim;
#if defined(CPU_FEATURES_ARCH_X86_64)
    // Turn off advanced fp16 flags. They will be tested in the next test.
    optimization.avx512_fp16 = optimization.avx512vl = 0;
#ifdef OPT_AVX512F
    // The AVX512F FP16 tier requires at least 16 elements; below that the dispatcher falls
    // through to the next tier.
    if (optimization.avx512f && dim >= 16) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_L2_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
    }
    optimization.avx512f = 0;
#endif
#ifdef OPT_F16C
    if (optimization.f16c && optimization.fma3 && optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_L2_implementation_F16C(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "F16C with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "F16C with dim " << dim;
        optimization.f16c = optimization.fma3 = optimization.avx = 0;
    }
#endif
#elif defined(CPU_FEATURES_ARCH_AARCH64)
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE2 with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        unsigned char alignment = 0;
        arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_L2_implementation_NEON_HP(dim))
            << "Unexpected distance function chosen for dim OPT_NEON_HP " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_HP with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_HP with dim " << dim;
        optimization.asimdhp = 0;
    }
#endif
#endif
    unsigned char alignment = 0;
    arch_opt_func = L2_FP16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, FP16_L2Sqr) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "F16C with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

INSTANTIATE_TEST_SUITE_P(FP16OptFuncs, FP16SpacesOptimizationTest,
                         testing::Range(8UL, 32 * 2UL + 1));

/** Since we are handling floats, the order of summation affect on the final result.
 * This is very significant when the entries are half precision floats, since the accumulated
 * error is much higher than in single precision floats.
 * In the following tests the error between the naive calculation to SIMD optimization function
 * is allowed to be up to 1%. If we wanted to be accurate, we could have done the baseline
 * calculations accumulating the results in a SIMD size vector and reduce the final result to float,
 * but this is too complicated for the scope of this test.
 * Special attention should be given to the implementation of the SIMD reduce function for float16,
 * that has different logic than the float32 and float64 reduce functions.
 * For more info, refer to intel's intrinsics guide.
 */
#if defined(OPT_AVX512_FP16_VL) || defined(CPU_FEATURES_ARCH_AARCH64)
class FP16SpacesOptimizationTestAdvanced : public testing::TestWithParam<size_t> {};

TEST_P(FP16SpacesOptimizationTestAdvanced, FP16InnerProductTestAdv) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    float16 v1[dim], v2[dim];

    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(-0.99, 0.99);

#if defined(CPU_FEATURES_ARCH_AARCH64) && defined(__GNUC__) && (__GNUC__ < 13)
    // https://github.com/pytorch/executorch/issues/6844
    __fp16 baseline = 0;
#else
    _Float16 baseline = 0;
#endif

    for (size_t i = 0; i < dim; i++) {
        float val1 = (dis(gen));
        float val2 = (dis(gen));
        v1[i] = vecsim_types::FP32_to_FP16((val1));
        v2[i] = vecsim_types::FP32_to_FP16((val2));

        baseline += static_cast<decltype(baseline)>(val1) * static_cast<decltype(baseline)>(val2);
    }
    baseline = decltype(baseline)(1) - baseline;

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(float16) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(float16) : 0;
    };

    dist_func_t<float> arch_opt_func;

#ifdef OPT_AVX512_FP16_VL
    if (optimization.avx512_fp16 && optimization.avx512vl) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_AVX512FP16_VL(dim))
            << "Unexpected distance function chosen for dim " << dim;
        float dist = arch_opt_func(v1, v2, dim);
        float f_baseline = baseline;
        float error = std::abs((dist / f_baseline) - 1);
        // Alow 1% error
        ASSERT_LE(error, 0.01) << "AVX512 with dim " << dim << ", baseline: " << f_baseline
                               << ", dist: " << dist;
        ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        float dist = arch_opt_func(v1, v2, dim);
        float f_baseline = baseline;
        float error = std::abs((dist / f_baseline) - 1);
        // Alow 1% error
        ASSERT_LE(error, 0.01) << "SVE2 with dim " << dim << ", baseline: " << f_baseline
                               << ", dist: " << dist;
        // ASSERT_EQ(alignment, expected_alignment(512, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "SVE2 with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        float dist = arch_opt_func(v1, v2, dim);
        float f_baseline = baseline;
        float error = std::abs((dist / f_baseline) - 1);
        // Alow 1% error
        ASSERT_LE(error, 0.01) << "SVE with dim " << dim << ", baseline: " << f_baseline
                               << ", dist: " << dist;
        // ASSERT_EQ(alignment, expected_alignment(512, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "SVE with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        unsigned char alignment = 0;
        arch_opt_func = IP_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_FP16_IP_implementation_NEON_HP(dim))
            << "Unexpected distance function chosen for dim " << dim;
        float dist = arch_opt_func(v1, v2, dim);
        float f_baseline = baseline;
        float error = std::abs((dist / f_baseline) - 1);
        // Alow 1% error
        ASSERT_LE(error, 0.01) << "NEON_HP with dim " << dim << ", baseline: " << f_baseline
                               << ", dist: " << dist;
        // ASSERT_EQ(alignment, expected_alignment(512, dim)) << "NEON_HP with dim " << dim;
        ASSERT_EQ(alignment, 0) << "NEON_HP with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.asimdhp = 0;
    }
#endif
}

#ifdef OPT_AVX512_FP16_VL
TEST_P(FP16SpacesOptimizationTestAdvanced, FP16L2SqrTestAdv) {
    auto optimization = cpu_features::GetX86Info().features;
    if (optimization.avx512_fp16 && optimization.avx512vl) {
        size_t dim = GetParam();
        float16 v1[dim], v2[dim];

        std::mt19937 gen(42);
        std::uniform_real_distribution<float> dis(-0.99f, 0.99f);

        _Float16 baseline = 0;
        for (size_t i = 0; i < dim; i++) {
            float val1 = (dis(gen));
            float val2 = (dis(gen));
            v1[i] = vecsim_types::FP32_to_FP16((val1));
            v2[i] = vecsim_types::FP32_to_FP16((val2));

            _Float16 diff = static_cast<_Float16>(val1) - static_cast<_Float16>(val2);
            baseline += diff * diff;
        }

        dist_func_t<float> arch_opt_func;
        arch_opt_func = Choose_FP16_L2_implementation_AVX512FP16_VL(dim);
        float dist = arch_opt_func(v1, v2, dim);
        float f_baseline = baseline;
        float error = std::abs((dist / f_baseline) - 1);
        // Alow 1% error
        ASSERT_LE(error, 0.01) << "AVX512 with dim " << dim << ", baseline: " << f_baseline
                               << ", dist: " << dist;
    }
}
#endif

// Start from a 32 multiplier
INSTANTIATE_TEST_SUITE_P(, FP16SpacesOptimizationTestAdvanced,
                         testing::Range(512UL, 512 + 32UL + 1));

#endif // defined(OPT_AVX512_FP16_VL) || defined(CPU_FEATURES_ARCH_AARCH64)

class INT8SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(INT8SpacesOptimizationTest, INT8L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    int8_t v1[dim];
    int8_t v2[dim];
    test_utils::populate_int8_vec(v1, dim, 123);
    test_utils::populate_int8_vec(v2, dim, 1234);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(int8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(int8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = INT8_L2Sqr(v1, v2, dim);
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_L2_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_L2_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_DOTPROD with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON_DOTPROD with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimd = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = L2_INT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, INT8_L2Sqr) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(INT8SpacesOptimizationTest, INT8InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    int8_t v1[dim];
    int8_t v2[dim];
    test_utils::populate_int8_vec(v1, dim, 123);
    test_utils::populate_int8_vec(v2, dim, 1234);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(int8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(int8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = INT8_InnerProduct(v1, v2, dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim OPT_NEON_DOTPROD " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_DOTPROD with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON_DOTPROD with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = IP_INT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, INT8_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(INT8SpacesOptimizationTest, INT8CosineTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    int8_t v1[dim + sizeof(float)];
    int8_t v2[dim + sizeof(float)];
    test_utils::populate_int8_vec(v1, dim, 123);
    test_utils::populate_int8_vec(v2, dim, 1234);

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1, dim);
    const float norm_v2 = test_utils::integral_compute_norm(v2, dim);
    std::memcpy(v1 + dim, &norm_v1, sizeof(norm_v1));
    std::memcpy(v2 + dim, &norm_v2, sizeof(norm_v2));

    dist_func_t<float> arch_opt_func;
    float baseline = INT8_Cosine(v1, v2, dim);
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        // We don't align int8 vectors with cosine distance
        ASSERT_EQ(alignment, 0) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_Cosine_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_Cosine_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_Cosine_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim OPT_NEON_DOTPROD " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_DOTPROD with dim " << dim;
        ASSERT_EQ(alignment, 0) << "NEON_DOTPROD with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_INT8_Cosine_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = Cosine_INT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, INT8_Cosine) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

INSTANTIATE_TEST_SUITE_P(INT8OptFuncs, INT8SpacesOptimizationTest,
                         testing::Range(32UL, 32 * 2UL + 1));

class UINT8SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(UINT8SpacesOptimizationTest, UINT8L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    uint8_t v1[dim];
    uint8_t v2[dim];
    test_utils::populate_uint8_vec(v1, dim, 123);
    test_utils::populate_uint8_vec(v2, dim, 1234);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(uint8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(uint8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = UINT8_L2Sqr(v1, v2, dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_L2_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimd = 0;
    }
#endif

#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_L2_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = L2_UINT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, UINT8_L2Sqr) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(UINT8SpacesOptimizationTest, UINT8InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    uint8_t v1[dim];
    uint8_t v2[dim];
    test_utils::populate_uint8_vec(v1, dim, 123);
    test_utils::populate_uint8_vec(v2, dim, 1234);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(uint8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(uint8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = UINT8_InnerProduct(v1, v2, dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_IP_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim NEON_DOTPROD " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_DOTPROD with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(0, dim)) << "NEON_DOTPROD with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_IP_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
    unsigned char alignment = 0;
    arch_opt_func = IP_UINT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, UINT8_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(UINT8SpacesOptimizationTest, UINT8CosineTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();
    uint8_t v1[dim + sizeof(float)];
    uint8_t v2[dim + sizeof(float)];
    test_utils::populate_uint8_vec(v1, dim, 123);
    test_utils::populate_uint8_vec(v2, dim, 1234);

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1, dim);
    const float norm_v2 = test_utils::integral_compute_norm(v2, dim);
    std::memcpy(v1 + dim, &norm_v1, sizeof(norm_v1));
    std::memcpy(v2 + dim, &norm_v2, sizeof(norm_v2));

    dist_func_t<float> arch_opt_func;
    float baseline = UINT8_Cosine(v1, v2, dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_Cosine_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_Cosine_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_Cosine_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim OPT_NEON_DOTPROD " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "NEON_DOTPROD with dim " << dim;
        // We don't align uint8 vectors with cosine distance
        ASSERT_EQ(alignment, 0) << "NEON with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_Cosine_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim OPT_NEON " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_UINT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "AVX512 with dim " << dim;
        // We don't align uint8 vectors with cosine distance
        ASSERT_EQ(alignment, 0) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif

    unsigned char alignment = 0;
    arch_opt_func = Cosine_UINT8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, UINT8_Cosine) << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1, v2, dim)) << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}
TEST_P(UINT8SpacesOptimizationTest, UINT8_full_range_test) {
    auto optimization = getCpuOptimizationFeatures();
    constexpr size_t dim = 512;

    uint8_t v1[dim + sizeof(float)];
    uint8_t v2[dim + sizeof(float)];

    // v1: 0..255 followed by 255..0
    for (size_t i = 0; i < 256; i++) {
        v1[i] = static_cast<uint8_t>(i);
        v1[256 + i] = static_cast<uint8_t>(255 - i);
    }

    // v2: 255..0 followed by 0..255
    for (size_t i = 0; i < 256; i++) {
        v2[i] = static_cast<uint8_t>(255 - i);
        v2[256 + i] = static_cast<uint8_t>(i);
    }

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1, dim);
    const float norm_v2 = test_utils::integral_compute_norm(v2, dim);
    std::memcpy(v1 + dim, &norm_v1, sizeof(norm_v1));
    std::memcpy(v2 + dim, &norm_v2, sizeof(norm_v2));

    float baseline_l2 = UINT8_L2Sqr(v1, v2, dim);
    float baseline_ip = UINT8_InnerProduct(v1, v2, dim);
    float baseline_cosine = UINT8_Cosine(v1, v2, dim);

    dist_func_t<float> arch_opt_func;

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;

        arch_opt_func = Choose_UINT8_L2_implementation_SVE2(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1, v2, dim)) << "L2 SVE2 with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_SVE2(dim);
        ASSERT_EQ(baseline_ip, arch_opt_func(v1, v2, dim)) << "IP SVE2 with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_SVE2(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1, v2, dim)) << "Cosine SVE2 with dim " << dim;

        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;

        arch_opt_func = Choose_UINT8_L2_implementation_SVE(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1, v2, dim)) << "L2 SVE with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_SVE(dim);
        ASSERT_EQ(baseline_ip, arch_opt_func(v1, v2, dim)) << "IP SVE with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_SVE(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1, v2, dim)) << "Cosine SVE with dim " << dim;

        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;

        arch_opt_func = Choose_UINT8_L2_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1, v2, dim)) << "L2 AVX512 with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_EQ(baseline_ip, arch_opt_func(v1, v2, dim)) << "IP AVX512 with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1, v2, dim)) << "Cosine AVX512 with dim " << dim;

        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = optimization.avx512bw = optimization.avx512vl =
            optimization.avx512vnni = 0;
    }
#endif
}

// Worst-case bytes: all-255 against all-0 is the largest possible L2 total and all-255 against
// itself the largest possible IP and cosine total, both 255 * 255 * dim. At
// spaces::UINT8_MAX_EXACT_SIMD_DIM that is the largest total the kernels' 32-bit accumulators hold,
// and one dimension later it passes INT32_MAX, which is what the dispatcher cap exists to keep out.
// Tiers are called directly so a host with SVE still exercises its NEON kernels.
TEST_P(UINT8SpacesOptimizationTest, UINT8_max_value_test) {
    auto optimization = getCpuOptimizationFeatures();
    const size_t dim = GetParam();

    std::vector<uint8_t> v1(dim + sizeof(float), 255);
    std::vector<uint8_t> v2(dim + sizeof(float), 0);

    // write the norm at the end of the vector
    const float norm_v1 = test_utils::integral_compute_norm(v1.data(), dim);
    std::memcpy(v1.data() + dim, &norm_v1, sizeof(norm_v1));

    float baseline_l2 = UINT8_L2Sqr(v1.data(), v2.data(), dim);
    float baseline_ip = UINT8_InnerProduct(v1.data(), v1.data(), dim);
    float baseline_cosine = UINT8_Cosine(v1.data(), v1.data(), dim);

    // NEON and SVE compute 1.0f - float(sum), while the scalar and AVX512 kernels subtract in
    // integers and convert once. Both are exact in integers, which is what the cap protects, but
    // the two forms can land one float ULP apart at these totals: dimension 32,960 against all-255
    // bytes is such a case, 128 apart at a magnitude of 2.1e9. So IP is compared within one ULP,
    // while L2 and cosine, which convert the total and do no further integer arithmetic, are exact.
    const float ip_ulp =
        std::nextafterf(std::fabs(baseline_ip), std::numeric_limits<float>::infinity()) -
        std::fabs(baseline_ip);

    dist_func_t<float> arch_opt_func;

#ifdef OPT_SVE2
    if (optimization.sve2) {
        arch_opt_func = Choose_UINT8_L2_implementation_SVE2(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1.data(), v2.data(), dim))
            << "L2 SVE2 with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_SVE2(dim);
        ASSERT_NEAR(baseline_ip, arch_opt_func(v1.data(), v1.data(), dim), ip_ulp)
            << "IP SVE2 with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_SVE2(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1.data(), v1.data(), dim))
            << "Cosine SVE2 with dim " << dim;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        arch_opt_func = Choose_UINT8_L2_implementation_SVE(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1.data(), v2.data(), dim))
            << "L2 SVE with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_SVE(dim);
        ASSERT_NEAR(baseline_ip, arch_opt_func(v1.data(), v1.data(), dim), ip_ulp)
            << "IP SVE with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_SVE(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1.data(), v1.data(), dim))
            << "Cosine SVE with dim " << dim;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        arch_opt_func = Choose_UINT8_L2_implementation_NEON_DOTPROD(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1.data(), v2.data(), dim))
            << "L2 NEON_DOTPROD with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_NEON_DOTPROD(dim);
        ASSERT_NEAR(baseline_ip, arch_opt_func(v1.data(), v1.data(), dim), ip_ulp)
            << "IP NEON_DOTPROD with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_NEON_DOTPROD(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1.data(), v1.data(), dim))
            << "Cosine NEON_DOTPROD with dim " << dim;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        arch_opt_func = Choose_UINT8_L2_implementation_NEON(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1.data(), v2.data(), dim))
            << "L2 NEON with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_NEON(dim);
        ASSERT_NEAR(baseline_ip, arch_opt_func(v1.data(), v1.data(), dim), ip_ulp)
            << "IP NEON with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_NEON(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1.data(), v1.data(), dim))
            << "Cosine NEON with dim " << dim;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        arch_opt_func = Choose_UINT8_L2_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_EQ(baseline_l2, arch_opt_func(v1.data(), v2.data(), dim))
            << "L2 AVX512 with dim " << dim;
        arch_opt_func = Choose_UINT8_IP_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_NEAR(baseline_ip, arch_opt_func(v1.data(), v1.data(), dim), ip_ulp)
            << "IP AVX512 with dim " << dim;
        arch_opt_func = Choose_UINT8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim);
        ASSERT_EQ(baseline_cosine, arch_opt_func(v1.data(), v1.data(), dim))
            << "Cosine AVX512 with dim " << dim;
    }
#endif
}

INSTANTIATE_TEST_SUITE_P(UINT8OptFuncs, UINT8SpacesOptimizationTest,
                         testing::Range(32UL, 64 * 2UL + 1));

// Dimensions just below spaces::UINT8_MAX_EXACT_SIMD_DIM, where the accumulated total is nearly all
// a 32-bit accumulator holds. 33,025 is the cap itself and the others are residual-bearing, so each
// tier is exercised at its own tail handling there. The [32, 128] range above cannot reach this:
// its totals are four orders of magnitude smaller.
INSTANTIATE_TEST_SUITE_P(UINT8OptFuncsNearCap, UINT8SpacesOptimizationTest,
                         testing::Values(32960UL, 32993UL, 33023UL, 33024UL, 33025UL));

// The dispatcher cap. Above spaces::UINT8_MAX_EXACT_SIMD_DIM every uint8 and SQ8_SQ8 chooser must
// hand back its scalar kernel, whose ret_t is long long and so stays exact. The SQ8_SQ8 kernels
// call the shared uint8 helper directly and never pass through a uint8 chooser, so they carry the
// same guard. That the SIMD kernels are still handed out at the cap is covered by
// UINT8OptFuncsNearCap above.
TEST_F(SpacesTest, UINT8_DispatcherCapFallback) {
    constexpr size_t cap = spaces::UINT8_MAX_EXACT_SIMD_DIM;
    constexpr size_t above = cap + 1;
    constexpr uint64_t max_term = 255ULL * 255ULL;
    constexpr uint64_t int32_max = std::numeric_limits<int32_t>::max();
    auto optimization = getCpuOptimizationFeatures();
    unsigned char alignment = 0;

    // The cap must sit exactly where a signed 32-bit total runs out, so pin it arithmetically
    // rather than trusting the constant.
    ASSERT_LE(max_term * cap, int32_max);
    ASSERT_GT(max_term * above, int32_max);

    ASSERT_EQ(L2_UINT8_GetDistFunc(above, &alignment, &optimization), UINT8_L2Sqr);
    ASSERT_EQ(IP_UINT8_GetDistFunc(above, &alignment, &optimization), UINT8_InnerProduct);
    ASSERT_EQ(Cosine_UINT8_GetDistFunc(above, &alignment, &optimization), UINT8_Cosine);
    ASSERT_EQ(L2_SQ8_SQ8_GetDistFunc(above, &alignment, &optimization), SQ8_SQ8_L2Sqr);
    ASSERT_EQ(IP_SQ8_SQ8_GetDistFunc(above, &alignment, &optimization), SQ8_SQ8_InnerProduct);
    ASSERT_EQ(Cosine_SQ8_SQ8_GetDistFunc(above, &alignment, &optimization), SQ8_SQ8_Cosine);

    // And the scalar kernels must be exact where they take over, which is what makes the fallback
    // safe rather than merely different.
    std::vector<uint8_t> v1(above + sizeof(float), 255);
    std::vector<uint8_t> v2(above + sizeof(float), 0);
    const int64_t total = static_cast<int64_t>(max_term * above);
    ASSERT_EQ(static_cast<float>(total), UINT8_L2Sqr(v1.data(), v2.data(), above));
    ASSERT_EQ(static_cast<float>(1 - total), UINT8_InnerProduct(v1.data(), v1.data(), above));
}

class SQ8_FP32_SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(SQ8_FP32_SpacesOptimizationTest, SQ8_FP32_L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create V1 fp32 query with precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_orig(query_size);
    test_utils::populate_sq8_fp32_query(v1_orig.data(), dim, false, 1234);

    // Create V2 as SQ8 quantized vector with different seed
    // Storage layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_squares]
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, false, 456);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(uint8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(uint8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_FP32_L2Sqr(v2_compressed.data(), v1_orig.data(), dim);
// Test different optimizations based on CPU features
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        // ASSERT_EQ(alignment, expected_alignment(512, dim)) << "AVX512 with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        // ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        // ASSERT_EQ(alignment, expected_alignment(256, dim)) << "AVX with dim " << dim;
        // Unset avx flag as well, so we'll choose the next optimization (SSE).
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SSE with dim " << dim;
        // ASSERT_EQ(alignment, expected_alignment(128, dim)) << "SSE with dim " << dim;
        // Unset sse flag as well, so we'll choose the next optimization (default).
        optimization.sse4_1 = 0;
    }
#endif

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve2 flag as well, so we'll choose the next option (default).
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset sve flag as well, so we'll choose the next option (default).
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "NEON with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
        // Unset optimizations flag, so we'll choose the next optimization.
        optimization.asimd = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP32_L2Sqr)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(SQ8_FP32_SpacesOptimizationTest, SQ8_FP32_InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create original vectors with precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_orig(query_size);
    test_utils::populate_sq8_fp32_query(v1_orig.data(), dim, true, 1234);
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 456);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(uint8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(uint8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_FP32_InnerProduct(v2_compressed.data(), v1_orig.data(), dim);

// Test different optimizations based on CPU features
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE
    if (optimization.sse4_1) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SSE with dim " << dim;
        optimization.sse4_1 = 0;
    }
#endif
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
            << "NEON with dim " << dim;
        optimization.asimd = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = IP_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP32_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_orig.data(), dim), 0.01)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Instantiate the test suite with dimensions to test
INSTANTIATE_TEST_SUITE_P(SQ8_FP32_Test, SQ8_FP32_SpacesOptimizationTest,
                         testing::Range(8UL, 32 * 2UL + 1));

TEST_P(SQ8_FP32_SpacesOptimizationTest, SQ8_FP32_CosineTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create original vectors - v1 needs extra space for precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_orig(query_size);
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_quantized(quantized_size);

    test_utils::populate_sq8_fp32_query(v1_orig.data(), dim, true, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, false, 456);

    auto expected_alignment = [](size_t reg_bit_size, size_t dim) {
        size_t elements_in_reg = reg_bit_size / sizeof(uint8_t) / 8;
        return (dim % elements_in_reg == 0) ? elements_in_reg * sizeof(uint8_t) : 0;
    };

    dist_func_t<float> arch_opt_func;
    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline = SQ8_FP32_Cosine(v2_quantized.data(), v1_orig.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "NEON with dim " << dim;
        optimization.asimd = 0;
    }
#endif

// Test different optimizations based on CPU features
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "AVX with dim " << dim;
        optimization.avx2 = 0;
    }
#endif

#ifdef OPT_SSE
    if (optimization.sse4_1) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP32_Cosine_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
            << "SSE with dim " << dim;
        optimization.sse4_1 = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP32_Cosine)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_quantized.data(), v1_orig.data(), dim), 0.01)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Test self-distance: distance to itself should be 0 for cosine (normalized vectors)
TEST(SQ8_FP32_EdgeCases, SelfDistanceCosine) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = (dim + sq8::query_metadata_count<VecSimMetric_L2>());
    std::vector<float> v_orig(query_size);
    test_utils::populate_sq8_fp32_query(v_orig.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_quantized.data(), dim, true, 1234);

    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline = SQ8_FP32_Cosine(v_quantized.data(), v_orig.data(), dim);

    // Self-distance for cosine should be close to 0
    ASSERT_NEAR(baseline, 0.0f, 0.001f) << "Self-distance should be ~0 for cosine";

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sse4_1 = 0;
    }
#endif

    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
    auto result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
    ASSERT_NEAR(baseline, result, 0.00001) << "No optimization self-distance should match baseline";
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Test self-distance: distance to itself should be 0 for L2
TEST(SQ8_FP32_EdgeCases, SelfDistanceL2) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    // Create fp32 query with precomputed sum and sum_squares
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = (dim + sq8::query_metadata_count<VecSimMetric_L2>());
    std::vector<float> v_orig(query_size);
    test_utils::populate_sq8_fp32_query(v_orig.data(), dim, false, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_quantized.data(), dim, false, 1234);

    float baseline = SQ8_FP32_L2Sqr(v_quantized.data(), v_orig.data(), dim);

    // Self-distance for L2 should be close to 0 (due to quantization effects, small errors are
    // expected)
    ASSERT_NEAR(baseline, 0.0f, 0.1f) << "Self-distance should be ~0 for L2";

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx512f = 0;
    }
#endif
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sse4_1 = 0;
    }
#endif

    unsigned char alignment = 0;
    auto arch_opt_func = L2_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
    auto result = arch_opt_func(v_quantized.data(), v_orig.data(), dim);
    ASSERT_NEAR(baseline, result, 0.00001) << "No optimization self-distance should match baseline";
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Test symmetry: dist(v1, v2) == dist(v2, v1)
// For asymmetric SQ8_FP32, symmetry means: dist(sq8_1, fp32_2) == dist(sq8_2, fp32_1)
TEST(SQ8_FP32_EdgeCases, CosineSymmetryTest) {
    size_t dim = 128;
    auto optimization = getCpuOptimizationFeatures();
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1_fp32(query_size);
    test_utils::populate_sq8_fp32_query(v1_fp32.data(), dim, true, 1234);
    std::vector<float> v2_fp32(query_size);
    test_utils::populate_sq8_fp32_query(v2_fp32.data(), dim, true, 456);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 1234);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 456);
    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline_1 = SQ8_FP32_Cosine(v2_quantized.data(), v1_fp32.data(), dim);
    float baseline_2 = SQ8_FP32_Cosine(v1_quantized.data(), v2_fp32.data(), dim);
    ASSERT_NEAR(baseline_1, baseline_2, 0.001f) << "Cosine should be symmetric";

    unsigned char alignment = 0;

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v2_quantized.data(), v1_fp32.data(), dim);
        float cos_21 = arch_opt_func(v1_quantized.data(), v2_fp32.data(), dim);
        ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Optimized cosine should be symmetric";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v2_quantized.data(), v1_fp32.data(), dim);
        float cos_21 = arch_opt_func(v1_quantized.data(), v2_fp32.data(), dim);
        ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Optimized cosine should be symmetric";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v2_quantized.data(), v1_fp32.data(), dim);
        float cos_21 = arch_opt_func(v1_quantized.data(), v2_fp32.data(), dim);
        ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Optimized cosine should be symmetric";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v2_quantized.data(), v1_fp32.data(), dim);
        float cos_21 = arch_opt_func(v1_quantized.data(), v2_fp32.data(), dim);
        ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Optimized cosine should be symmetric";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v2_quantized.data(), v1_fp32.data(), dim);
        float cos_21 = arch_opt_func(v1_quantized.data(), v2_fp32.data(), dim);
        ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Optimized cosine should be symmetric";
        optimization.avx512f = 0;
    }
#endif
    auto cosine_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, nullptr);
    float cos_12 = cosine_func(v2_quantized.data(), v1_fp32.data(), dim);
    float cos_21 = cosine_func(v1_quantized.data(), v2_fp32.data(), dim);
    ASSERT_NEAR(cos_12, cos_21, 0.001f) << "Cosine should be symmetric";
}

// Test with zero vector
TEST(SQ8_FP32_EdgeCases, CosineZeroVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v_zero(query_size, 0.0f);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_nonzero_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_nonzero_quantized.data(), dim, true);

    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline = SQ8_FP32_Cosine(v_nonzero_quantized.data(), v_zero.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_nonzero_quantized.data(), v_zero.data(), dim);

    ASSERT_EQ(result, baseline) << "Zero vector Cosine should match baseline";
}

// Test with constant quantized vector (all same values - edge case where delta = 0)
TEST(SQ8_FP32_EdgeCases, CosineConstantVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;

    // Create a random query vector (preprocessed for FP32->SQ8 cosine)
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v_query(query_size);
    test_utils::populate_float_vec(v_query.data(), dim);
    test_utils::preprocess_sq8_fp32_query(v_query.data(), dim);

    // Create a constant quantized vector (all same values)
    // This tests the edge case where delta = 0 (or set to 1.0 to avoid division by zero)
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_const_quantized(quantized_size);
    std::vector<float> v_const(dim, 0.5f);
    spaces::GetNormalizeFunc<float>()(v_const.data(), dim);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_const.data(), dim,
                                                        v_const_quantized.data());

    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline = SQ8_FP32_Cosine(v_const_quantized.data(), v_query.data(), dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_const_quantized.data(), v_query.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f)
        << "Constant quantized vector Cosine should match baseline";
}

// Test with extreme values (-1 and 1 only)
TEST(SQ8_FP32_EdgeCases, CosineExtremeValuesTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    // Query layout: [float values (dim)] [sum] [sum_squares]
    size_t query_size = dim + sq8::query_metadata_count<VecSimMetric_L2>();
    std::vector<float> v1(query_size), v2(dim);

    // Alternating extreme values
    for (size_t i = 0; i < dim; i++) {
        v1[i] = (i % 2 == 0) ? 1.0f : -1.0f;
        v2[i] = (i % 3 == 0) ? 1.0f : -1.0f;
    }
    test_utils::preprocess_sq8_fp32_query(v1.data(), dim);
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v2.data(), dim, v2_quantized.data());

    // Arguments: (SQ8_storage, FP32_query, dim)
    float baseline = SQ8_FP32_Cosine(v2_quantized.data(), v1.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_FP32_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v2_quantized.data(), v1.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Extreme values Cosine should match baseline";
}

/* ======================== Tests SQ8_FP16 (parameterized) ========================= */

// Parameterized tests that verify the scalar SQ8_FP16 kernels against the not-optimized
// baseline across multiple dimensions, including odd dimensions and SIMD-boundary residues.
// The SIMD-tier dispatcher coverage lives in SQ8_FP16_SpacesOptimizationTest below; this
// suite intentionally exercises the scalar reference directly to keep it as a fixed baseline
// the SIMD tiers are compared against.
class SQ8_FP16_NoOptimizationSpacesTest : public testing::TestWithParam<size_t> {};

TEST_P(SQ8_FP16_NoOptimizationSpacesTest, SQ8_FP16_L2SqrTest) {
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, false, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, false, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_L2Sqr(v2_compressed.data(), v1_query.data(), dim);
    float dist = SQ8_FP16_L2Sqr(v2_compressed.data(), v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01f) << "SQ8_FP16_L2Sqr mismatch for dim " << dim;
}

TEST_P(SQ8_FP16_NoOptimizationSpacesTest, SQ8_FP16_InnerProductTest) {
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_InnerProduct(v2_compressed.data(), v1_query.data(), dim);
    float dist = SQ8_FP16_InnerProduct(v2_compressed.data(), v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01f) << "SQ8_FP16_InnerProduct mismatch for dim " << dim;
}

TEST_P(SQ8_FP16_NoOptimizationSpacesTest, SQ8_FP16_CosineTest) {
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_FP16_NotOptimized_Cosine(v2_compressed.data(), v1_query.data(), dim);
    float dist = SQ8_FP16_Cosine(v2_compressed.data(), v1_query.data(), dim);

    ASSERT_NEAR(dist, baseline, 0.01f) << "SQ8_FP16_Cosine mismatch for dim " << dim;
}

// Cover small dims, odd dims, SIMD-boundary residues for upcoming AVX2 / AVX512 / SVE / NEON
// register widths (8/16/32/64 elements per register for SQ8 storage).
INSTANTIATE_TEST_SUITE_P(SQ8_FP16_NoOpt, SQ8_FP16_NoOptimizationSpacesTest,
                         testing::Values(1, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 47, 48, 49, 63, 64,
                                         65, 127, 128));

/* ======================== SQ8_FP16 SIMD optimisation tests ========================= */

// Walks down the x86 ISA tiers (AVX-512 → AVX2+FMA → AVX2 → SSE4 → scalar) and asserts
// that {IP,Cosine,L2}_SQ8_FP16_GetDistFunc returns the expected Choose_* symbol and that
// its output matches the scalar baseline within 0.01.
class SQ8_FP16_SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(SQ8_FP16_SpacesOptimizationTest, SQ8_FP16_L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, false, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, false, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_FP16_L2Sqr(v2_compressed.data(), v1_query.data(), dim);

#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
    // F16C is required by every non-AVX-512 SQ8↔FP16 tier (vcvtph2ps), so the guard is hoisted
    // around all three — matches the dispatcher layout in L2_space.cpp.
#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2+FMA with dim " << dim;
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2 with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1 && optimization.f16c && optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SSE4 with dim " << dim;
        optimization.sse4_1 = 0;
    }
#endif
#endif // OPT_F16C

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_FHM
    if (optimization.asimdhp && optimization.asimdfhm) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_NEON_FHM(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_FHM with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_FHM with dim " << dim;
        optimization.asimdfhm = 0;
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_L2_implementation_NEON_HP(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_HP with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_HP with dim " << dim;
        optimization.asimdhp = 0;
    }
#endif
#endif // CPU_FEATURES_ARCH_AARCH64

    unsigned char alignment = 0;
    arch_opt_func = L2_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP16_L2Sqr)
        << "Unexpected scalar fallback function for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
        << "Scalar fallback with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(SQ8_FP16_SpacesOptimizationTest, SQ8_FP16_InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_FP16_InnerProduct(v2_compressed.data(), v1_query.data(), dim);

#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
    // F16C is required by every non-AVX-512 SQ8↔FP16 tier (vcvtph2ps), so the guard is hoisted
    // around all three — matches the dispatcher layout in IP_space.cpp.
#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2+FMA with dim " << dim;
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2 with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1 && optimization.f16c && optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SSE4 with dim " << dim;
        optimization.sse4_1 = 0;
    }
#endif
#endif // OPT_F16C

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_FHM
    if (optimization.asimdhp && optimization.asimdfhm) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_NEON_FHM(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_FHM with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_FHM with dim " << dim;
        optimization.asimdfhm = 0;
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_IP_implementation_NEON_HP(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_HP with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_HP with dim " << dim;
        optimization.asimdhp = 0;
    }
#endif
#endif // CPU_FEATURES_ARCH_AARCH64

    unsigned char alignment = 0;
    arch_opt_func = IP_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP16_InnerProduct)
        << "Unexpected scalar fallback function for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
        << "Scalar fallback with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(SQ8_FP16_SpacesOptimizationTest, SQ8_FP16_CosineTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v1_query(query_count);
    test_utils::populate_sq8_fp16_query(v1_query.data(), dim, true, 1234);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v2_compressed(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_compressed.data(), dim, true, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_FP16_Cosine(v2_compressed.data(), v1_query.data(), dim);

#ifdef OPT_AVX512F
    if (optimization.avx512f) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_AVX512F(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif
    // F16C is required by every non-AVX-512 SQ8↔FP16 tier (vcvtph2ps), so the guard is hoisted
    // around all three — matches the dispatcher layout in IP_space.cpp.
#ifdef OPT_F16C
#ifdef OPT_AVX2_FMA
    if (optimization.avx2 && optimization.fma3 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_AVX2_FMA(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2+FMA with dim " << dim;
        optimization.fma3 = 0;
    }
#endif
#ifdef OPT_AVX2
    if (optimization.avx2 && optimization.f16c) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_AVX2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "AVX2 with dim " << dim;
        optimization.avx2 = 0;
    }
#endif
#ifdef OPT_SSE4
    if (optimization.sse4_1 && optimization.f16c && optimization.avx) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_SSE4(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SSE4 with dim " << dim;
        optimization.sse4_1 = 0;
    }
#endif
#endif // OPT_F16C

#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "SVE with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_FHM
    if (optimization.asimdhp && optimization.asimdfhm) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_NEON_FHM(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_FHM with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_FHM with dim " << dim;
        optimization.asimdfhm = 0;
    }
#endif
#ifdef OPT_NEON_HP
    if (optimization.asimdhp) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_FP16_Cosine_implementation_NEON_HP(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
            << "NEON_HP with dim " << dim;
        ASSERT_EQ(alignment, 0) << "No alignment NEON_HP with dim " << dim;
        optimization.asimdhp = 0;
    }
#endif
#endif // CPU_FEATURES_ARCH_AARCH64

    unsigned char alignment = 0;
    arch_opt_func = Cosine_SQ8_FP16_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_FP16_Cosine)
        << "Unexpected scalar fallback function for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v2_compressed.data(), v1_query.data(), dim), 0.01)
        << "Scalar fallback with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Dim range [16, 32] covers every residual class for the 16-element chunk used by every tier.
INSTANTIATE_TEST_SUITE_P(SQ8_FP16_SIMD, SQ8_FP16_SpacesOptimizationTest,
                         testing::Range(16UL, 16 * 2UL + 1));

// Higher dimensions surface multi-iteration loop bugs (pointer stride, do-while termination
// off-by-one) that the [16, 32] range does not exercise because the AVX-512 inner loop runs at
// most twice in that range. 48 and 112 specifically hit the AVX-512 three-chunk tail
// (remaining == 48, i.e. (dim / 16) % 4 == 3): 48 with zero main-loop iterations, 112 with one.
INSTANTIATE_TEST_SUITE_P(SQ8_FP16_SIMD_HighDim, SQ8_FP16_SpacesOptimizationTest,
                         testing::Values(48UL, 64UL, 112UL, 128UL, 256UL, 512UL, 1024UL));

/* ======================== Tests SQ8_FP16 (edge cases) ========================= */

// Zero FP16 query against a non-zero SQ8 storage. IP must be exactly 1.0 (1 - 0),
// L2² must equal Σ dequantized². Math correctness on adversarial inputs is verified
// against the scalar reference; SIMD tier coverage with branchless kernels is provided
// separately by SQ8_FP16_SpacesOptimizationTest.
TEST(SQ8_FP16_EdgeCases, ZeroQueryTest) {
    size_t dim = 64;

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v_zero_query(query_count, float16{0});
    // Metadata bits are zero (sum = 0, sum_squares = 0); FP16 zero is bit-pattern 0.

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_nonzero_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_nonzero_quantized.data(), dim, false,
                                                        1234);

    float ip_baseline = test_utils::SQ8_FP16_NotOptimized_InnerProduct(v_nonzero_quantized.data(),
                                                                       v_zero_query.data(), dim);
    float ip = SQ8_FP16_InnerProduct(v_nonzero_quantized.data(), v_zero_query.data(), dim);
    ASSERT_NEAR(ip, ip_baseline, 0.01f) << "Zero-query SQ8_FP16_InnerProduct mismatch";
    ASSERT_NEAR(ip, 1.0f, 0.01f) << "Zero-query IP must equal 1.0 (1 - 0)";

    float l2_baseline = test_utils::SQ8_FP16_NotOptimized_L2Sqr(v_nonzero_quantized.data(),
                                                                v_zero_query.data(), dim);
    float l2 = SQ8_FP16_L2Sqr(v_nonzero_quantized.data(), v_zero_query.data(), dim);
    ASSERT_NEAR(l2, l2_baseline, 0.01f) << "Zero-query SQ8_FP16_L2Sqr mismatch";
}

// Constant SQ8 storage (all values identical => delta = 0). Storage quantizer sets delta to 1.0
// to avoid div-by-zero, so verify the kernels still match the dequantization baseline.
TEST(SQ8_FP16_EdgeCases, ConstantStorageTest) {
    size_t dim = 64;

    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v_query(query_count);
    test_utils::populate_sq8_fp16_query(v_query.data(), dim, false, 4321);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_const_quantized(quantized_size);
    std::vector<float> v_const(dim, 0.5f);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_const.data(), dim,
                                                        v_const_quantized.data());

    float ip_baseline = test_utils::SQ8_FP16_NotOptimized_InnerProduct(v_const_quantized.data(),
                                                                       v_query.data(), dim);
    float ip = SQ8_FP16_InnerProduct(v_const_quantized.data(), v_query.data(), dim);
    ASSERT_NEAR(ip, ip_baseline, 0.01f) << "Constant-storage SQ8_FP16_InnerProduct mismatch";

    float l2_baseline =
        test_utils::SQ8_FP16_NotOptimized_L2Sqr(v_const_quantized.data(), v_query.data(), dim);
    float l2 = SQ8_FP16_L2Sqr(v_const_quantized.data(), v_query.data(), dim);
    ASSERT_NEAR(l2, l2_baseline, 0.01f) << "Constant-storage SQ8_FP16_L2Sqr mismatch";
}

// Mixed-sign FP16 query (alternating positive/negative values) verifies sign handling
// in the FP16->FP32 widening path and in the algebraic identity used by the kernels.
TEST(SQ8_FP16_EdgeCases, MixedSignQueryTest) {
    size_t dim = 64;

    // Build an alternating +0.75 / -0.75 FP16 query manually so we don't depend on RNG sign mix.
    // Allocated as std::vector<float16> so v_query.data() is alignof(float16)-aligned for
    // the SQ8_FP16 production kernel.
    size_t query_count =
        dim + sq8::query_metadata_count<VecSimMetric_L2>() * (sizeof(float) / sizeof(float16));
    std::vector<float16> v_query(query_count);
    for (size_t i = 0; i < dim; i++) {
        v_query[i] = vecsim_types::FP32_to_FP16((i % 2 == 0) ? 0.75f : -0.75f);
    }
    test_utils::preprocess_sq8_fp16_query(v_query.data(), dim);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_quantized.data(), dim, false, 9876);

    float ip_baseline =
        test_utils::SQ8_FP16_NotOptimized_InnerProduct(v_quantized.data(), v_query.data(), dim);
    float ip = SQ8_FP16_InnerProduct(v_quantized.data(), v_query.data(), dim);
    ASSERT_NEAR(ip, ip_baseline, 0.01f) << "Mixed-sign SQ8_FP16_InnerProduct mismatch";

    float cos_baseline =
        test_utils::SQ8_FP16_NotOptimized_Cosine(v_quantized.data(), v_query.data(), dim);
    float cos = SQ8_FP16_Cosine(v_quantized.data(), v_query.data(), dim);
    ASSERT_NEAR(cos, cos_baseline, 0.01f) << "Mixed-sign SQ8_FP16_Cosine mismatch";

    float l2_baseline =
        test_utils::SQ8_FP16_NotOptimized_L2Sqr(v_quantized.data(), v_query.data(), dim);
    float l2 = SQ8_FP16_L2Sqr(v_quantized.data(), v_query.data(), dim);
    ASSERT_NEAR(l2, l2_baseline, 0.01f) << "Mixed-sign SQ8_FP16_L2Sqr mismatch";
}

/* ======================== Tests SQ8_SQ8 ========================= */

TEST_F(SpacesTest, SQ8_SQ8_ip_no_optimization_func_test) {
    size_t dim = 5;

    // Create SQ8 quantized versions of both vectors
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 5678);

    float baseline = test_utils::SQ8_SQ8_NotOptimized_InnerProduct(v1_quantized.data(),
                                                                   v2_quantized.data(), dim);

    unsigned char alignment = 0;
#ifdef CPU_FEATURES_ARCH_AARCH64
    // Make sure we don't use any optimization (because there is no size optimization for arm)
    auto optimization = getCpuOptimizationFeatures();
    optimization.sve = optimization.sve2 = 0;
    auto arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
#else
    auto arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
#endif
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    // Checks that the function with the optimized math equivalence returns similar result.
    // Use ASSERT_NEAR due to floating-point differences between naive and algebraic formulas.
    ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.001)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_F(SpacesTest, SQ8_SQ8_Cosine_no_optimization_func_test) {
    size_t dim = 5;

    // Create SQ8 quantized versions of both vectors
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 5678);

    float baseline =
        test_utils::SQ8_SQ8_NotOptimized_Cosine(v1_quantized.data(), v2_quantized.data(), dim);

    unsigned char alignment = 0;
#ifdef CPU_FEATURES_ARCH_AARCH64
    // Make sure we don't use any optimization (because there is no size optimization for arm)
    auto optimization = getCpuOptimizationFeatures();
    optimization.sve = optimization.sve2 = 0;
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
#else
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
#endif
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_Cosine)
        << "Unexpected distance function chosen for dim " << dim;
    // Checks that the function with the optimized math equivalence returns the same result.
    // min1*sum2 + min2*sum1 + delta1*delta2*Σ(q1[i]*q2[i]) - dim*min1*min2
    ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.001)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_F(SpacesTest, SQ8_SQ8_L2_no_optimization_func_test) {
    size_t dim = 5;

    // Create SQ8 quantized versions of both vectors
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, false, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, false, 5678);

    float baseline =
        test_utils::SQ8_SQ8_NotOptimized_L2Sqr(v1_quantized.data(), v2_quantized.data(), dim);
    unsigned char alignment = 0;
#ifdef CPU_FEATURES_ARCH_AARCH64
    // Make sure we don't use any optimization (because there is no size optimization for arm)
    auto optimization = getCpuOptimizationFeatures();
    optimization.sve = optimization.sve2 = 0;
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
#else
    // Get distance function with nullptr alignment to cover that code path
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
#endif
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_L2Sqr)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.001f)
        << "SQ8_SQ8_L2Sqr failed to match expected distance";
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

class SQ8_SQ8_SpacesOptimizationTest : public testing::TestWithParam<size_t> {};

TEST_P(SQ8_SQ8_SpacesOptimizationTest, SQ8_SQ8_InnerProductTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create SQ8 quantized versions of both vectors
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_SQ8_InnerProduct(v1_quantized.data(), v2_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_IP_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_IP_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp && dim >= 64) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_IP_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "NEON_DOTPROD with dim " << dim;
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_IP_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "NEON with dim " << dim;
        optimization.asimd = 0;
    }
#endif

#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_IP_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = IP_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_InnerProduct)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim))
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(SQ8_SQ8_SpacesOptimizationTest, SQ8_SQ8_CosineTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create quantized vectors
    // Size: dim (uint8_t) + min_val (float) + delta (float) + sum (float) + sum_squares (float)
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_SQ8_Cosine(v1_quantized.data(), v2_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_Cosine_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_Cosine_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp && dim >= 64) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_Cosine_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "NEON_DOTPROD with dim " << dim;
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_Cosine_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "NEON with dim " << dim;
        optimization.asimd = 0;
    }
#endif

#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_Cosine_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.01)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_Cosine)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_EQ(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim))
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

TEST_P(SQ8_SQ8_SpacesOptimizationTest, SQ8_SQ8_L2SqrTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = GetParam();

    // Create SQ8 quantized versions of both vectors
    // Layout: [uint8_t values (dim)] [min_val] [delta] [sum] [sum_of_squares]
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, false, 1234);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, false, 5678);

    dist_func_t<float> arch_opt_func;
    float baseline = SQ8_SQ8_L2Sqr(v1_quantized.data(), v2_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_L2_implementation_SVE2(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
            << "SVE2 with dim " << dim;
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_L2_implementation_SVE(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
            << "SVE with dim " << dim;
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp && dim >= 64) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_L2_implementation_NEON_DOTPROD(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
            << "NEON_DOTPROD with dim " << dim;
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_L2_implementation_NEON(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
            << "NEON with dim " << dim;
        optimization.asimd = 0;
    }
#endif

#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        ASSERT_EQ(arch_opt_func, Choose_SQ8_SQ8_L2_implementation_AVX512F_BW_VL_VNNI(dim))
            << "Unexpected distance function chosen for dim " << dim;
        ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
            << "AVX512 with dim " << dim;
        optimization.avx512f = 0;
    }
#endif

    // Test default implementation
    unsigned char alignment = 0;
    arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(arch_opt_func, SQ8_SQ8_L2Sqr)
        << "Unexpected distance function chosen for dim " << dim;
    ASSERT_NEAR(baseline, arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim), 0.02)
        << "No optimization with dim " << dim;
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Note: This suite intentionally uses a larger dimension range (64–128) than SQ8OptFuncs.
// It is designed to exercise SQ8–SQ8 cosine implementations, including SIMD paths
// that are only enabled or meaningfully stressed for dimensions >= 64.
INSTANTIATE_TEST_SUITE_P(SQ8_SQ8OptFuncs, SQ8_SQ8_SpacesOptimizationTest,
                         testing::Range(64UL, 64 * 2UL + 1));

// Test self-distance: distance to itself should be 0 for cosine (normalized vectors)
TEST(SQ8_SQ8_EdgeCases, SelfDistanceCosine) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_quantized.data(), dim, true);

    float baseline = SQ8_SQ8_Cosine(v_quantized.data(), v_quantized.data(), dim);

    // Self-distance for cosine should be close to 0
    ASSERT_NEAR(baseline, 0.0f, 0.001f) << "Self-distance should be ~0 for cosine";

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx512f = 0;
    }
#endif

    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(baseline, arch_opt_func(v_quantized.data(), v_quantized.data(), dim))
        << "No optimization self-distance should match baseline";
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Test symmetry: dist(v1, v2) == dist(v2, v1)
TEST(SQ8_SQ8_EdgeCases, CosineSymmetryTest) {
    size_t dim = 128;
    auto optimization = getCpuOptimizationFeatures();
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, true, 456, -1.0f,
                                                        1.0f);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, true, 123, -1.0f,
                                                        1.0f);

    unsigned char alignment = 0;

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float cos_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(cos_12, cos_21) << "Optimized cosine should be symmetric";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float cos_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(cos_12, cos_21) << "Optimized cosine should be symmetric";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float cos_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(cos_12, cos_21) << "Optimized cosine should be symmetric";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float cos_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(cos_12, cos_21) << "Optimized cosine should be symmetric";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float cos_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float cos_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(cos_12, cos_21) << "Optimized cosine should be symmetric";
        optimization.avx512f = 0;
    }
#endif
    auto cosine_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float cos_12 = cosine_func(v1_quantized.data(), v2_quantized.data(), dim);
    float cos_21 = cosine_func(v2_quantized.data(), v1_quantized.data(), dim);
    ASSERT_EQ(cos_12, cos_21) << "Cosine should be symmetric";
}

// Test with zero vector
TEST(SQ8_SQ8_EdgeCases, CosineZeroVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v_zero(dim, 0.0f);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_zero_quantized(quantized_size);
    std::vector<uint8_t> v_nonzero_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_zero.data(), dim,
                                                        v_zero_quantized.data());
    test_utils::populate_float_vec_to_sq8_with_metadata(v_nonzero_quantized.data(), dim, true);

    float baseline = SQ8_SQ8_Cosine(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector IP should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Zero vector Cosine should match baseline";
}

// Test with constant vector (all same values)
TEST(SQ8_SQ8_EdgeCases, CosineConstantVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v_const(dim, 0.5f);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_const_quantized(quantized_size);
    std::vector<uint8_t> v_random_quantized(quantized_size);
    spaces::GetNormalizeFunc<float>()(v_const.data(), dim);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_const.data(), dim,
                                                        v_const_quantized.data());
    test_utils::populate_float_vec_to_sq8_with_metadata(v_random_quantized.data(), dim, true);

    float baseline = SQ8_SQ8_Cosine(v_const_quantized.data(), v_random_quantized.data(), dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector Cosine should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Constant vector Cosine should match baseline";
}

// Test with extreme values (-1 and 1 only)
TEST(SQ8_SQ8_EdgeCases, CosineExtremeValuesTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v1(dim), v2(dim);

    // Alternating extreme values
    for (size_t i = 0; i < dim; i++) {
        v1[i] = (i % 2 == 0) ? 1.0f : -1.0f;
        v2[i] = (i % 3 == 0) ? 1.0f : -1.0f;
    }

    spaces::GetNormalizeFunc<float>()(v1.data(), dim);
    spaces::GetNormalizeFunc<float>()(v2.data(), dim);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v1.data(), dim, v1_quantized.data());
    test_utils::quantize_float_vec_to_sq8_with_metadata(v2.data(), dim, v2_quantized.data());

    float baseline = SQ8_SQ8_Cosine(v1_quantized.data(), v2_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized extreme values Cosine should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = Cosine_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Extreme values Cosine should match baseline";
}

// Test self-distance: distance to itself should be 0 for L2
TEST(SQ8_SQ8_EdgeCases, SelfDistanceL2) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v_quantized.data(), dim, false);

    float baseline = SQ8_SQ8_L2Sqr(v_quantized.data(), v_quantized.data(), dim);

    // Self-distance for L2 should be close to 0 (due to quantization effects, small errors are
    // expected)
    ASSERT_NEAR(baseline, 0.0f, 0.1f) << "Self-distance should be ~0 for L2";

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_quantized.data(), v_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized self-distance should match baseline";
        optimization.avx512f = 0;
    }
#endif

    unsigned char alignment = 0;
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
    ASSERT_EQ(baseline, arch_opt_func(v_quantized.data(), v_quantized.data(), dim))
        << "No optimization self-distance should match baseline";
    ASSERT_EQ(alignment, 0) << "No optimization with dim " << dim;
}

// Test symmetry: dist(v1, v2) == dist(v2, v1) for L2
TEST(SQ8_SQ8_EdgeCases, L2SymmetryTest) {
    size_t dim = 128;
    auto optimization = getCpuOptimizationFeatures();
    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::populate_float_vec_to_sq8_with_metadata(v1_quantized.data(), dim, false, 456, -1.0f,
                                                        1.0f);
    test_utils::populate_float_vec_to_sq8_with_metadata(v2_quantized.data(), dim, false, 123, -1.0f,
                                                        1.0f);

    unsigned char alignment = 0;

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float l2_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float l2_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(l2_12, l2_21) << "Optimized L2 should be symmetric";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float l2_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float l2_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(l2_12, l2_21) << "Optimized L2 should be symmetric";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float l2_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float l2_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(l2_12, l2_21) << "Optimized L2 should be symmetric";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float l2_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float l2_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(l2_12, l2_21) << "Optimized L2 should be symmetric";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float l2_12 = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        float l2_21 = arch_opt_func(v2_quantized.data(), v1_quantized.data(), dim);
        ASSERT_EQ(l2_12, l2_21) << "Optimized L2 should be symmetric";
        optimization.avx512f = 0;
    }
#endif
    auto l2_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float l2_12 = l2_func(v1_quantized.data(), v2_quantized.data(), dim);
    float l2_21 = l2_func(v2_quantized.data(), v1_quantized.data(), dim);
    ASSERT_EQ(l2_12, l2_21) << "L2 should be symmetric";
}

// Test with zero vector for L2
TEST(SQ8_SQ8_EdgeCases, L2ZeroVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v_zero(dim, 0.0f);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_zero_quantized(quantized_size);
    std::vector<uint8_t> v_nonzero_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_zero.data(), dim,
                                                        v_zero_quantized.data());
    test_utils::populate_float_vec_to_sq8_with_metadata(v_nonzero_quantized.data(), dim, false);

    float baseline = SQ8_SQ8_L2Sqr(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector L2 should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector L2 should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector L2 should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector L2 should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized zero vector L2 should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_zero_quantized.data(), v_nonzero_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Zero vector L2 should match baseline";
}

// Test with constant vector (all same values) for L2
TEST(SQ8_SQ8_EdgeCases, L2ConstantVectorTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v_const(dim, 0.5f);

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v_const_quantized(quantized_size);
    std::vector<uint8_t> v_random_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v_const.data(), dim,
                                                        v_const_quantized.data());
    test_utils::populate_float_vec_to_sq8_with_metadata(v_random_quantized.data(), dim, false);

    float baseline = SQ8_SQ8_L2Sqr(v_const_quantized.data(), v_random_quantized.data(), dim);
#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector L2 should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector L2 should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector L2 should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector L2 should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f)
            << "Optimized constant vector L2 should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v_const_quantized.data(), v_random_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Constant vector L2 should match baseline";
}

// Test with extreme values (-1 and 1 only) for L2
TEST(SQ8_SQ8_EdgeCases, L2ExtremeValuesTest) {
    auto optimization = getCpuOptimizationFeatures();
    size_t dim = 128;
    std::vector<float> v1(dim), v2(dim);

    // Alternating extreme values
    for (size_t i = 0; i < dim; i++) {
        v1[i] = (i % 2 == 0) ? 1.0f : -1.0f;
        v2[i] = (i % 3 == 0) ? 1.0f : -1.0f;
    }

    size_t quantized_size =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    std::vector<uint8_t> v1_quantized(quantized_size);
    std::vector<uint8_t> v2_quantized(quantized_size);
    test_utils::quantize_float_vec_to_sq8_with_metadata(v1.data(), dim, v1_quantized.data());
    test_utils::quantize_float_vec_to_sq8_with_metadata(v2.data(), dim, v2_quantized.data());

    float baseline = SQ8_SQ8_L2Sqr(v1_quantized.data(), v2_quantized.data(), dim);

#ifdef OPT_SVE2
    if (optimization.sve2) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized extreme values L2 should match baseline";
        optimization.sve2 = 0;
    }
#endif
#ifdef OPT_SVE
    if (optimization.sve) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized extreme values L2 should match baseline";
        optimization.sve = 0;
    }
#endif
#ifdef OPT_NEON_DOTPROD
    if (optimization.asimddp) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized extreme values L2 should match baseline";
        optimization.asimddp = 0;
    }
#endif
#ifdef OPT_NEON
    if (optimization.asimd) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized extreme values L2 should match baseline";
        optimization.asimd = 0;
    }
#endif
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (optimization.avx512f && optimization.avx512bw && optimization.avx512vl &&
        optimization.avx512vnni) {
        unsigned char alignment = 0;
        auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, &optimization);
        float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);
        ASSERT_NEAR(result, baseline, 0.01f) << "Optimized extreme values L2 should match baseline";
        optimization.avx512f = 0;
    }
#endif
    unsigned char alignment = 0;
    auto arch_opt_func = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
    float result = arch_opt_func(v1_quantized.data(), v2_quantized.data(), dim);

    ASSERT_NEAR(result, baseline, 0.01f) << "Extreme values L2 should match baseline";
}

// Assert the exact alignment-hint values published by the SQ8 distance dispatchers.
// The hint refers to the SQ8 (first / storage) operand per the GetDistFunc contract documented
// in spaces/spaces.h. These tests guard against silent regressions of the per-kernel hints used
// by the preprocessor pipeline to align the storage blob.
#ifdef CPU_FEATURES_ARCH_X86_64
TEST_F(SpacesTest, SQ8_FP32_DispatcherAlignmentHints) {
    // dim divisible by 16 (and therefore 8 and 4) so every x86 path sets a non-zero hint.
    constexpr size_t dim = 64;
    auto features = getCpuOptimizationFeatures();

    auto check = [&](const char *kind,
                     spaces::dist_func_t<float> (*get)(size_t, unsigned char *, const void *)) {
        auto opt = features;
#ifdef OPT_AVX512_F_BW_VL_VNNI
        if (opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni) {
            unsigned char alignment = 0;
            (void)get(dim, &alignment, &opt);
            ASSERT_EQ(alignment, 16u) << kind << ": AVX512 SQ8_FP32 hint should be 16";
            opt.avx512f = 0;
        }
#endif
#ifdef OPT_AVX2_FMA
        if (opt.avx2 && opt.fma3) {
            unsigned char alignment = 0;
            (void)get(dim, &alignment, &opt);
            ASSERT_EQ(alignment, 8u) << kind << ": AVX2_FMA SQ8_FP32 hint should be 8";
            opt.fma3 = 0;
        }
#endif
#ifdef OPT_AVX2
        if (opt.avx2) {
            unsigned char alignment = 0;
            (void)get(dim, &alignment, &opt);
            ASSERT_EQ(alignment, 8u) << kind << ": AVX2 SQ8_FP32 hint should be 8";
            opt.avx2 = 0;
        }
#endif
#ifdef OPT_SSE4
        if (opt.sse4_1) {
            unsigned char alignment = 0;
            (void)get(dim, &alignment, &opt);
            ASSERT_EQ(alignment, 4u) << kind << ": SSE4 SQ8_FP32 hint should be 4";
            opt.sse4_1 = 0;
        }
#endif
        // No-optimization path must leave the hint at 0.
        unsigned char alignment = 0;
        (void)get(dim, &alignment, &opt);
        ASSERT_EQ(alignment, 0u) << kind << ": no-optimization hint should be 0";
    };

    check("IP", &spaces::IP_SQ8_FP32_GetDistFunc);
    check("L2", &spaces::L2_SQ8_FP32_GetDistFunc);
    check("Cosine", &spaces::Cosine_SQ8_FP32_GetDistFunc);
}

TEST_F(SpacesTest, SQ8_SQ8_DispatcherAlignmentHints) {
    // dim divisible by 32 so the AVX512 SQ8_SQ8 path sets the hint (otherwise it stays at 0).
    constexpr size_t dim = 64;
    auto features = getCpuOptimizationFeatures();

    auto check = [&](const char *kind,
                     spaces::dist_func_t<float> (*get)(size_t, unsigned char *, const void *)) {
        auto opt = features;
#ifdef OPT_AVX512_F_BW_VL_VNNI
        if (opt.avx512f && opt.avx512bw && opt.avx512vl && opt.avx512vnni) {
            unsigned char alignment = 0;
            (void)get(dim, &alignment, &opt);
            ASSERT_EQ(alignment, 32u) << kind << ": AVX512 SQ8_SQ8 hint should be 32";
            opt.avx512f = 0;
        }
#endif
        // No-optimization path must leave the hint at 0.
        unsigned char alignment = 0;
        (void)get(dim, &alignment, &opt);
        ASSERT_EQ(alignment, 0u) << kind << ": no-optimization hint should be 0";
    };

    check("IP", &spaces::IP_SQ8_SQ8_GetDistFunc);
    check("L2", &spaces::L2_SQ8_SQ8_GetDistFunc);
    check("Cosine", &spaces::Cosine_SQ8_SQ8_GetDistFunc);
}
#endif // CPU_FEATURES_ARCH_X86_64

namespace {
// The stored sum_squares is ||x_r||^2 by construction, so read it rather than recomputing.
float sq8_norm_sq(const std::vector<uint8_t> &blob, size_t dim) {
    return load_unaligned<float>(blob.data() + dim + sq8::SUM_SQUARES * sizeof(float));
}

std::vector<uint8_t> sq8_quantize_l2(const std::vector<float> &v) {
    const size_t dim = v.size();
    std::vector<uint8_t> blob(dim * sizeof(uint8_t) + 4 * sizeof(float));
    test_utils::quantize_float_vec_to_sq8_with_metadata(v.data(), dim, blob.data());
    return blob;
}
} // namespace

namespace {
// The reconstruction the kernels compute distances over: x_r[i] = min + delta * a[i]. Reference
// values are accumulated in double so the comparison is against the algebra, not against another
// FP32 implementation with the same rounding.
double ReferenceL2SqrOverReconstruction(const uint8_t *a, const uint8_t *b, size_t dim) {
    const float min_a = load_unaligned<float>(a + dim + sq8::MIN_VAL * sizeof(float));
    const float delta_a = load_unaligned<float>(a + dim + sq8::DELTA * sizeof(float));
    const float min_b = load_unaligned<float>(b + dim + sq8::MIN_VAL * sizeof(float));
    const float delta_b = load_unaligned<float>(b + dim + sq8::DELTA * sizeof(float));
    double acc = 0.0;
    for (size_t i = 0; i < dim; i++) {
        const double d = (static_cast<double>(min_a) + static_cast<double>(delta_a) * a[i]) -
                         (static_cast<double>(min_b) + static_cast<double>(delta_b) * b[i]);
        acc += d * d;
    }
    return acc;
}
} // namespace

// The stored sums describe the reconstruction, not the input. Summing the input instead left a
// systematic mismatch of about 0.4% of ||x||^2, which is larger than the distance between two
// near-duplicate vectors, so L2 came back negative: at dim 128 the reconstruction distance is
// 2.93e-04 and the kernels returned -1.49e-01. Near-duplicates are the case that exposes it,
// because the true distance is small enough for the mismatch to dominate.
TEST_F(SpacesTest, SQ8_SQ8_L2_is_non_negative_and_matches_reconstruction) {
    std::mt19937 gen(20260820);
    std::uniform_real_distribution<float> value_gen(-1.0f, 1.0f);
    std::uniform_real_distribution<float> nudge_gen(-1e-3f, 1e-3f);

    for (const size_t dim : {4UL, 15UL, 64UL, 128UL, 512UL}) {
        std::vector<float> x(dim), y(dim);
        for (size_t i = 0; i < dim; i++) {
            x[i] = value_gen(gen);
            y[i] = x[i] + nudge_gen(gen);
        }
        const std::vector<uint8_t> qx = sq8_quantize_l2(x);
        const std::vector<uint8_t> qy = sq8_quantize_l2(y);

        const double expected = ReferenceL2SqrOverReconstruction(qx.data(), qy.data(), dim);

        // The error floor is FP32 cancellation in sum_sq_x + sum_sq_y - 2*IP, so it scales with the
        // norms, not with the distance. A flat bound is thousands of times the distance it guards
        // at small dimensions, which would let a kernel returning zero pass.
        const float tol = 8.0f * std::numeric_limits<float>::epsilon() *
                          (sq8_norm_sq(qx, dim) + sq8_norm_sq(qy, dim));
        unsigned char alignment = 0;
        auto dispatched = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);

        for (const auto &probe :
             {std::make_pair("scalar", SQ8_SQ8_L2Sqr), std::make_pair("dispatched", dispatched)}) {
            const float got = probe.second(qx.data(), qy.data(), dim);
            // Bounded by the noise floor, not by zero: this change does not make the result
            // provably non-negative. The two sides of the subtraction are computed by different
            // routes, and a blob against itself lands just below zero at dim 512.
            EXPECT_GE(got, -tol) << probe.first << " negative beyond the noise floor, dim " << dim;
            EXPECT_NEAR(got, expected, tol)
                << probe.first << " does not match the reconstruction, dim " << dim;
        }
    }
}

// The asymmetric path is how the defect reached production unnoticed: its tests run dimensions 1,
// 5, 7 and 15 against an absolute tolerance of 0.01, and 0.4% of ||x||^2 at dim 5 is about 0.007,
// which fits underneath. Storage is quantized and the query is not, so this checks
// sum(x_r^2) + sum(y^2) - 2*sum(x_r*y) against a double reference over the same two sides.
TEST_F(SpacesTest, SQ8_FP32_L2_matches_reconstruction_against_float_query) {
    std::mt19937 gen(20260822);
    std::uniform_real_distribution<float> value_gen(-1.0f, 1.0f);
    std::uniform_real_distribution<float> nudge_gen(-1e-3f, 1e-3f);

    for (const size_t dim : {5UL, 15UL, 64UL, 128UL, 512UL}) {
        std::vector<float> x(dim), y(dim);
        for (size_t i = 0; i < dim; i++) {
            x[i] = value_gen(gen);
            y[i] = x[i] + nudge_gen(gen); // near-duplicate: small true distance exposes the offset
        }
        const std::vector<uint8_t> qx = sq8_quantize_l2(x);

        // The query carries its own sum of squares, over the query itself, since it is not
        // quantized. Reference is the distance between the reconstruction and that query.
        std::vector<float> query(dim + 2, 0.0f);
        double ref = 0.0, y_sum_sq = 0.0;
        const float min_x = load_unaligned<float>(qx.data() + dim + sq8::MIN_VAL * sizeof(float));
        const float delta_x = load_unaligned<float>(qx.data() + dim + sq8::DELTA * sizeof(float));
        for (size_t i = 0; i < dim; i++) {
            query[i] = y[i];
            y_sum_sq += static_cast<double>(y[i]) * y[i];
            const double d = (static_cast<double>(min_x) + static_cast<double>(delta_x) * qx[i]) -
                             static_cast<double>(y[i]);
            ref += d * d;
        }
        // Both query slots matter: the asymmetric inner product is
        // min*sum(y) + delta*sum(a[i]*y[i]), so zeroing SUM_QUERY silently drops the min*sum(y)
        // term. Fill them the way the production query preprocessor does.
        test_utils::preprocess_sq8_fp32_query(query.data(), dim);

        const float tol = 8.0f * std::numeric_limits<float>::epsilon() *
                          (sq8_norm_sq(qx, dim) + static_cast<float>(y_sum_sq));
        unsigned char alignment = 0;
        auto dispatched = L2_SQ8_FP32_GetDistFunc(dim, &alignment, nullptr);
        for (const auto &probe :
             {std::make_pair("scalar", SQ8_FP32_L2Sqr), std::make_pair("dispatched", dispatched)}) {
            // storage first, query second, as the other asymmetric tests call it.
            const float got = probe.second(qx.data(), query.data(), dim);
            EXPECT_GE(got, -tol) << probe.first << " negative beyond the noise floor, dim " << dim;
            EXPECT_NEAR(got, ref, tol)
                << probe.first << " does not match the reconstruction, dim " << dim;
        }
    }
}

// A blob against itself. Exact zero is not claimed here: the two sides of
// sum_sq_x + sum_sq_y - 2*IP are computed by different routes and round differently, so this pins
// the magnitude rather than the bit pattern.
TEST_F(SpacesTest, SQ8_SQ8_L2_self_distance_is_near_zero) {
    std::mt19937 gen(20260821);
    std::uniform_real_distribution<float> value_gen(-3.0f, 5.0f);

    for (const size_t dim : {3UL, 8UL, 64UL, 512UL}) {
        std::vector<float> x(dim);
        double norm_sq = 0.0;
        for (size_t i = 0; i < dim; i++) {
            x[i] = value_gen(gen);
            norm_sq += static_cast<double>(x[i]) * x[i];
        }
        const std::vector<uint8_t> q = sq8_quantize_l2(x);
        const float tol = static_cast<float>(1e-5 * norm_sq);

        EXPECT_NEAR(SQ8_SQ8_L2Sqr(q.data(), q.data(), dim), 0.0f, tol)
            << "scalar kernel, dim " << dim;
        unsigned char alignment = 0;
        auto dispatched = L2_SQ8_SQ8_GetDistFunc(dim, &alignment, nullptr);
        EXPECT_NEAR(dispatched(q.data(), q.data(), dim), 0.0f, tol)
            << "dispatched kernel, dim " << dim;
    }
}
