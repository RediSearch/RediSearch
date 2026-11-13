/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/spaces/L2/L2.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/spaces/L2_space.h"
#include "VecSim/types/sq4.h"
#include "VecSim/types/sq8.h"
#include "VecSim/spaces/functions/NEON.h"
#include "VecSim/spaces/functions/NEON_DOTPROD.h"
#include "VecSim/spaces/functions/SVE.h"
#include "VecSim/spaces/functions/SVE2.h"
#include "VecSim/spaces/functions/AVX2.h"
#include "VecSim/spaces/functions/AVX2_FMA.h"
#include "VecSim/spaces/functions/AVX512F_BW_VL_VNNI.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace {
using vecsim_types::sq4;
using vecsim_types::sq8;

static_assert(sq8::max_code == 255);
static_assert(sq8::data_bytes_count(7) == 7);
static_assert(sq8::storage_bytes_count<VecSimMetric_IP>(7) == 19);
static_assert(sq8::storage_bytes_count<VecSimMetric_L2, true>(7) == 23);
static_assert(sq4::data_bytes_count(0) == 0);
static_assert(sq4::data_bytes_count(7) == 4);
static_assert(sq4::storage_bytes_count<VecSimMetric_IP, true>(7) == 20);
static_assert(sq4::storage_bytes_count<VecSimMetric_L2>(7) == 20);

// Offset the blob by one byte to exercise unaligned metadata reads as well as odd payload sizes.
struct PackedVector {
    std::vector<uint8_t> bytes;
    std::vector<float> reconstructed;

    PackedVector(const std::vector<uint8_t> &codes, float minimum, float delta)
        : bytes(1 + sq4::storage_bytes_count<VecSimMetric_L2>(codes.size()), 0xff),
          reconstructed(codes.size()) {
        float metadata[4] = {minimum, delta, 0.0f, 0.0f};
        for (size_t i = 0; i < codes.size(); ++i) {
            sq4::set_code(data(), i, codes[i]);
            reconstructed[i] = minimum + delta * codes[i];
            metadata[sq4::SUM] += reconstructed[i];
            metadata[sq4::SUM_SQUARES] += reconstructed[i] * reconstructed[i];
        }
        std::memcpy(data() + sq4::data_bytes_count(codes.size()), metadata, sizeof(metadata));
    }

    uint8_t *data() { return bytes.data() + 1; }
};

float referenceDistance(VecSimMetric metric, const std::vector<float> &x,
                        const std::vector<float> &y) {
    float result = 0.0f;
    for (size_t i = 0; i < x.size(); ++i) {
        if (metric == VecSimMetric_IP) {
            result += x[i] * y[i];
        } else {
            const float diff = x[i] - y[i];
            result += diff * diff;
        }
    }
    return metric == VecSimMetric_IP ? 1.0f - result : result;
}
} // namespace

TEST(SQ4Distances, PackedLayoutAndSQ8Compatibility) {
    uint8_t packed[8];
    std::memset(packed, 0xff, sizeof(packed));
    for (size_t i = 0; i < 16; ++i) {
        sq4::set_code(packed, i, i);
        if (i % 2 == 0) {
            EXPECT_EQ(packed[i / 2] & 0xf0, 0);
        }
    }
    for (size_t i = 0; i < 16; ++i) {
        EXPECT_EQ(sq4::get_code(packed, i), i);
        if (i % 2 == 0) {
            EXPECT_EQ(packed[i / 2], i | ((i + 1) << 4));
        }
    }
    uint8_t unpacked[256];
    for (size_t i = 0; i < 256; ++i) {
        sq8::set_code(unpacked, i, i);
        EXPECT_EQ(sq8::get_code(unpacked, i), i);
        EXPECT_EQ(unpacked[i], i);
    }
}

TEST(SQ4Distances, DispatchMatchesExplicitReconstruction) {
    for (size_t dim : {0u, 1u, 2u, 3u, 4u, 7u, 16u, 31u, 32u, 65u}) {
        SCOPED_TRACE(dim);
        std::vector<uint8_t> x_codes(dim), y_codes(dim);
        std::vector<float> query(dim + sq4::query_metadata_count<VecSimMetric_L2>(), 0.0f);
        std::vector<float> query_values(dim);
        for (size_t i = 0; i < dim; ++i) {
            x_codes[i] = (i * 7) % 16;
            y_codes[i] = (i * 3 + 15) % 16;
            query[i] = query_values[i] = static_cast<float>(static_cast<int>(i % 13) - 6) / 8;
            query[dim + sq4::SUM_QUERY] += query[i];
            query[dim + sq4::SUM_SQUARES_QUERY] += query[i] * query[i];
        }
        PackedVector x(x_codes, -1.25f, 0.125f);
        PackedVector y(y_codes, 0.75f, 0.0625f);
        if (dim % 2) {
            EXPECT_EQ(x.data()[sq4::data_bytes_count(dim) - 1] & 0xf0, 0);
            EXPECT_EQ(y.data()[sq4::data_bytes_count(dim) - 1] & 0xf0, 0);
        }
        for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
            unsigned char alignment = 255;
            auto symmetric = spaces::GetDistFunc<sq4, float>(metric, dim, &alignment);
            EXPECT_EQ(alignment, 0);
            auto asymmetric = spaces::GetDistFunc<sq4, float, float>(metric, dim, &alignment);
            EXPECT_EQ(alignment, 0);
            EXPECT_FLOAT_EQ(symmetric(x.data(), y.data(), dim),
                            referenceDistance(metric, x.reconstructed, y.reconstructed));
            EXPECT_FLOAT_EQ(asymmetric(x.data(), query.data(), dim),
                            referenceDistance(metric, x.reconstructed, query_values));
            EXPECT_EQ(symmetric, (spaces::GetDistFunc<sq4, float>(metric, dim, nullptr)));
            EXPECT_EQ(asymmetric, (spaces::GetDistFunc<sq4, float, float>(metric, dim, nullptr)));
            if (metric == VecSimMetric_L2) {
                EXPECT_FLOAT_EQ(symmetric(x.data(), x.data(), dim), 0.0f);
                EXPECT_FLOAT_EQ(symmetric(x.data(), y.data(), dim),
                                symmetric(y.data(), x.data(), dim));
            }
        }
    }
}

TEST(SQ4Distances, ConstantVectorsAndOddPadding) {
    std::vector<uint8_t> codes(5, 0);
    PackedVector x(codes, 2.0f, 0.0f);
    PackedVector y(codes, -3.0f, 0.0f);
    std::vector<float> query(5 + sq4::query_metadata_count<VecSimMetric_L2>(), -3.0f);
    query[5 + sq4::SUM_QUERY] = -15.0f;
    query[5 + sq4::SUM_SQUARES_QUERY] = 45.0f;
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
        auto symmetric = spaces::GetDistFunc<sq4, float>(metric, 5, nullptr);
        auto asymmetric = spaces::GetDistFunc<sq4, float, float>(metric, 5, nullptr);
        const float expected = metric == VecSimMetric_IP ? 31.0f : 125.0f;
        EXPECT_FLOAT_EQ(symmetric(x.data(), y.data(), 5), expected);
        EXPECT_FLOAT_EQ(asymmetric(x.data(), query.data(), 5), expected);
        // The unused nibble is outside the vector, even if a foreign writer leaves it nonzero.
        x.data()[sq4::data_bytes_count(5) - 1] |= 0xf0;
        EXPECT_FLOAT_EQ(symmetric(x.data(), y.data(), 5), expected);
        EXPECT_FLOAT_EQ(asymmetric(x.data(), query.data(), 5), expected);
    }
}

TEST(SQ4Distances, FP32L2PreservesResidualsAtLargeCommonOffset) {
    PackedVector x({0, 1, 2}, 1048576.0f, 0.03125f);
    PackedVector y({0, 0, 0}, 1048576.0f, 0.0f);
    std::vector<float> query(3 + sq4::query_metadata_count<VecSimMetric_L2>(), 1048576.0f);
    query[3 + sq4::SUM_QUERY] = 3.0f * 1048576.0f;
    query[3 + sq4::SUM_SQUARES_QUERY] = 3.0f * 1048576.0f * 1048576.0f;
    const float expected = 5.0f * 0.03125f * 0.03125f;
    EXPECT_FLOAT_EQ(SQ4_FP32_L2Sqr(x.data(), query.data(), 3), expected);
}

TEST(SQ4Distances, FP32L2PackedPairsPreserveCoordinateOrder) {
    for (size_t dim : {0u, 1u, 2u, 3u, 7u, 16u, 31u, 32u, 33u, 65u, 767u, 768u, 769u}) {
        for (const auto &[minimum, delta] :
             {std::pair{-0.75f, 0.1f}, std::pair{1048576.0f, 0.03125f}}) {
            SCOPED_TRACE(::testing::Message() << "dim=" << dim << " min=" << minimum);
            std::vector<uint8_t> codes(dim);
            std::vector<float> query(dim);
            float expected = 0.0f;
            for (size_t i = 0; i < dim; ++i) {
                codes[i] = (i * 7 + 3) % 16;
                query[i] = minimum + 0.13f * (int(i % 17) - 8);
                // Use the original codes, independently of the packed blob, and the previous
                // scalar kernel's operand and reduction order.
                const float diff = delta * float(codes[i]) + (minimum - query[i]);
                expected += diff * diff;
            }
            PackedVector stored(codes, minimum, delta);
            if (dim % 2) {
                // A foreign writer's nonzero padding must not become another coordinate.
                stored.data()[dim / 2] |= 0xf0;
            }
            const float actual = SQ4_FP32_L2Sqr(stored.data(), query.data(), dim);
            EXPECT_EQ(std::bit_cast<uint32_t>(actual), std::bit_cast<uint32_t>(expected));
        }
    }
}

TEST(SQ4Distances, RejectsUnsupportedMetric) {
    EXPECT_THROW((spaces::GetDistFunc<sq4, float>(VecSimMetric_Cosine, 3, nullptr)),
                 std::invalid_argument);
    EXPECT_THROW((spaces::GetDistFunc<sq4, float, float>(VecSimMetric_Cosine, 3, nullptr)),
                 std::invalid_argument);
}

namespace {
// Double-precision reference over the decoded vectors.
double refDistance(VecSimMetric metric, const std::vector<double> &x,
                   const std::vector<double> &y) {
    double result = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        result += metric == VecSimMetric_IP ? x[i] * y[i] : (x[i] - y[i]) * (x[i] - y[i]);
    }
    return metric == VecSimMetric_IP ? 1.0 - result : result;
}

std::vector<float> makeQuery(const std::vector<float> &values) {
    const size_t dim = values.size();
    std::vector<float> query(dim + sq4::query_metadata_count<VecSimMetric_L2>(), 0.0f);
    for (size_t i = 0; i < dim; ++i) {
        query[i] = values[i];
        query[dim + sq4::SUM_QUERY] += values[i];
        query[dim + sq4::SUM_SQUARES_QUERY] += values[i] * values[i];
    }
    return query;
}

// Stored metadata slot of a packed vector, as the distance kernels read it.
double storedMeta(const uint8_t *blob, size_t dim, size_t slot) {
    float value;
    std::memcpy(&value, blob + sq4::data_bytes_count(dim) + slot * sizeof(float), sizeof(value));
    return value;
}

// Stored-to-stored IP identity over the stored metadata, evaluated in double:
//   IP = min_x * sum_y + min_y * sum_x - dim * min_x * min_y + delta_x * delta_y * sum(qx * qy)
double identityIP(const uint8_t *x, const uint8_t *y, size_t dim) {
    double code_dot = 0.0;
    for (size_t i = 0; i < dim; ++i) {
        code_dot += double(sq4::get_code(x, i)) * sq4::get_code(y, i);
    }
    const double min_x = storedMeta(x, dim, sq4::MIN_VAL), dx = storedMeta(x, dim, sq4::DELTA);
    const double min_y = storedMeta(y, dim, sq4::MIN_VAL), dy = storedMeta(y, dim, sq4::DELTA);
    return min_x * storedMeta(y, dim, sq4::SUM) + min_y * storedMeta(x, dim, sq4::SUM) -
           double(dim) * min_x * min_y + dx * dy * code_dot;
}

// Per-element reconstruction of the SQ4-FP32 inner product distance.
float reconstructedIP(const uint8_t *x, const float *y, size_t dim, float min_val, float delta) {
    float dot = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        dot += (min_val + delta * sq4::get_code(x, i)) * y[i];
    }
    return 1.0f - dot;
}
} // namespace

TEST(SQ4Distances, OddDimMatchesReference) {
    std::mt19937 rng(1234);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    for (size_t dim : {1u, 15u, 16u, 17u, 64u, 767u, 768u}) {
        SCOPED_TRACE(dim);
        std::vector<uint8_t> x_codes(dim), y_codes(dim);
        std::vector<float> values(dim);
        for (size_t i = 0; i < dim; ++i) {
            x_codes[i] = code_dist(rng);
            y_codes[i] = code_dist(rng);
            values[i] = val_dist(rng);
        }
        PackedVector x(x_codes, -0.5f, 1.0f / 15);
        PackedVector y(y_codes, 0.25f, 1.0f / 30);
        auto query = makeQuery(values);
        std::vector<double> xd(dim), yd(dim), qd(dim);
        for (size_t i = 0; i < dim; ++i) {
            qd[i] = values[i];
        }
        // Decode with the exact float min/delta stored in the blobs, then compute in double.
        const float minx = -0.5f, dx = 1.0f / 15, miny = 0.25f, dy = 1.0f / 30;
        for (size_t i = 0; i < dim; ++i) {
            xd[i] = double(minx) + double(dx) * sq4::get_code(x.data(), i);
            yd[i] = double(miny) + double(dy) * sq4::get_code(y.data(), i);
        }
        auto check = [](float actual, double ref) {
            EXPECT_NEAR(actual, ref, 1e-4 * std::max(1.0, std::abs(ref)));
        };
        check(SQ4_SQ4_InnerProduct(x.data(), y.data(), dim),
              1.0 - identityIP(x.data(), y.data(), dim));
        check(SQ4_SQ4_L2Sqr(x.data(), y.data(), dim), refDistance(VecSimMetric_L2, xd, yd));
        check(SQ4_FP32_InnerProduct(x.data(), query.data(), dim),
              refDistance(VecSimMetric_IP, xd, qd));
        check(SQ4_FP32_L2Sqr(x.data(), query.data(), dim), refDistance(VecSimMetric_L2, xd, qd));
    }
}

TEST(SQ4Distances, FP32IPEqualsReconstruction) {
    constexpr size_t dim = 768;
    std::mt19937 rng(99);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    std::vector<uint8_t> codes(dim);
    std::vector<float> values(dim);
    for (size_t i = 0; i < dim; ++i) {
        codes[i] = code_dist(rng);
        values[i] = val_dist(rng);
    }
    const float min_val = -0.75f, delta = 0.1f;
    PackedVector x(codes, min_val, delta);
    auto query = makeQuery(values);
    const float ref = reconstructedIP(x.data(), query.data(), dim, min_val, delta);
    EXPECT_NEAR(SQ4_FP32_InnerProduct(x.data(), query.data(), dim), ref,
                1e-5 * std::max(1.0f, std::abs(ref)));
}

namespace {
// The dispatcher hands out SQ4-FP32 SIMD kernels from this dim up: any dim on AArch64, eight
// coordinates on x86 (as SQ8, for the AVX2 tail's full 8-float query load).
#ifdef CPU_FEATURES_ARCH_X86_64
constexpr size_t SQ4_FP32_SIMD_MIN_DIM = 8;
#else
constexpr size_t SQ4_FP32_SIMD_MIN_DIM = 0;
#endif
} // namespace

TEST(SQ4Distances, DispatchIsScalarWithZeroAlignment) {
    for (size_t dim : {7u, 8u, 15u, 16u, 63u, 64u, 768u}) {
        SCOPED_TRACE(dim);
        // SQ4-FP32 has SIMD kernels from SQ4_FP32_SIMD_MIN_DIM (see the SQ4SIMD tests), below
        // that scalar.
        for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
            SCOPED_TRACE(metric);
            unsigned char alignment = 255;
            const auto fp32 = spaces::GetDistFunc<sq4, float, float>(metric, dim, &alignment);
            if (dim < SQ4_FP32_SIMD_MIN_DIM) {
                EXPECT_EQ(fp32, metric == VecSimMetric_IP ? SQ4_FP32_InnerProduct : SQ4_FP32_L2Sqr);
            }
            EXPECT_EQ(alignment, 0);
            alignment = 255;
            const auto direct = metric == VecSimMetric_IP
                                    ? spaces::IP_SQ4_FP32_GetDistFunc(dim, &alignment)
                                    : spaces::L2_SQ4_FP32_GetDistFunc(dim, &alignment);
            EXPECT_EQ(direct, fp32);
            EXPECT_EQ(alignment, 0);
        }
        // SQ4-SQ4 has SIMD kernels on AArch64 and on x86 with AVX2 or VNNI from dim 64 (both
        // metrics: L2 is the stored-sum identity over the IP code dot), see the SQ4SIMD tests.
        // Elsewhere scalar.
        for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
            SCOPED_TRACE(metric);
            unsigned char alignment = 255;
            const auto stored = spaces::GetDistFunc<sq4, float>(metric, dim, &alignment);
            EXPECT_EQ(alignment, 0);
            const auto scalar = metric == VecSimMetric_IP ? SQ4_SQ4_InnerProduct : SQ4_SQ4_L2Sqr;
#if defined(CPU_FEATURES_ARCH_X86_64)
            if (dim < 64) {
                EXPECT_EQ(stored, scalar);
            }
            // Clearing all CPU features must return scalar even on SIMD-capable hosts.
            decltype(spaces::getCpuOptimizationFeatures()) no_simd{};
            EXPECT_EQ(metric == VecSimMetric_IP
                          ? spaces::IP_SQ4_SQ4_GetDistFunc(dim, nullptr, &no_simd)
                          : spaces::L2_SQ4_SQ4_GetDistFunc(dim, nullptr, &no_simd),
                      scalar);
#elif !defined(CPU_FEATURES_ARCH_AARCH64)
            EXPECT_EQ(stored, scalar);
#endif
            alignment = 255;
            const auto direct = metric == VecSimMetric_IP
                                    ? spaces::IP_SQ4_SQ4_GetDistFunc(dim, &alignment)
                                    : spaces::L2_SQ4_SQ4_GetDistFunc(dim, &alignment);
            EXPECT_EQ(direct, stored);
            EXPECT_EQ(alignment, 0);
        }
    }
}

// SIMD SQ4-FP32 tiers. Each tier is reached through the dispatcher by clearing the feature flags
// of the tiers above it, and its choosers are also called directly.
namespace {
using Features = decltype(spaces::getCpuOptimizationFeatures());
using Chooser = spaces::dist_func_t<float> (*)(size_t);

struct SimdTier {
    const char *name;
    Features features;
    Chooser chooseIP;
    Chooser chooseL2;

    Chooser choose(VecSimMetric metric) const {
        return metric == VecSimMetric_IP ? chooseIP : chooseL2;
    }
};

std::vector<SimdTier> sq4FP32Tiers() {
    std::vector<SimdTier> tiers;
    [[maybe_unused]] Features features = spaces::getCpuOptimizationFeatures();
#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        tiers.push_back({"SVE2", features, spaces::Choose_SQ4_FP32_IP_implementation_SVE2,
                         spaces::Choose_SQ4_FP32_L2_implementation_SVE2});
    }
#endif
    features.sve2 = 0;
#ifdef OPT_SVE
    if (features.sve) {
        tiers.push_back({"SVE", features, spaces::Choose_SQ4_FP32_IP_implementation_SVE,
                         spaces::Choose_SQ4_FP32_L2_implementation_SVE});
    }
#endif
    features.sve = 0;
#ifdef OPT_NEON
    if (features.asimd) {
        tiers.push_back({"NEON", features, spaces::Choose_SQ4_FP32_IP_implementation_NEON,
                         spaces::Choose_SQ4_FP32_L2_implementation_NEON});
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        tiers.push_back({"AVX512F_BW_VL_VNNI", features,
                         spaces::Choose_SQ4_FP32_IP_implementation_AVX512F_BW_VL_VNNI,
                         spaces::Choose_SQ4_FP32_L2_implementation_AVX512F_BW_VL_VNNI});
    }
#endif
    features.avx512f = 0;
#ifdef OPT_AVX2_FMA
    if (features.avx2 && features.fma3) {
        tiers.push_back({"AVX2_FMA", features, spaces::Choose_SQ4_FP32_IP_implementation_AVX2_FMA,
                         spaces::Choose_SQ4_FP32_L2_implementation_AVX2_FMA});
    }
#endif
#endif
    return tiers;
}

spaces::dist_func_t<float> scalarFP32(VecSimMetric metric) {
    return metric == VecSimMetric_IP ? SQ4_FP32_InnerProduct : SQ4_FP32_L2Sqr;
}

spaces::dist_func_t<float> dispatchFP32(VecSimMetric metric, size_t dim, unsigned char *alignment,
                                        const Features *features) {
    return metric == VecSimMetric_IP ? spaces::IP_SQ4_FP32_GetDistFunc(dim, alignment, features)
                                     : spaces::L2_SQ4_FP32_GetDistFunc(dim, alignment, features);
}

size_t storageBytes(VecSimMetric metric, size_t dim) {
    return metric == VecSimMetric_IP ? sq4::storage_bytes_count<VecSimMetric_IP>(dim)
                                     : sq4::storage_bytes_count<VecSimMetric_L2>(dim);
}

size_t queryBytes(VecSimMetric metric, size_t dim) {
    const size_t metadata = metric == VecSimMetric_IP
                                ? sq4::query_metadata_count<VecSimMetric_IP>()
                                : sq4::query_metadata_count<VecSimMetric_L2>();
    return (dim + metadata) * sizeof(float);
}

// Exact-size storage blob of `metric` at `offset` bytes into its buffer. For odd dims the unused
// high nibble is set, so a kernel that lets it into the distance fails.
struct OffsetVector {
    std::vector<uint8_t> bytes;
    size_t offset;

    OffsetVector(VecSimMetric metric, const std::vector<uint8_t> &codes, float minimum, float delta,
                 size_t offset)
        : bytes(offset + storageBytes(metric, codes.size()), 0xff), offset(offset) {
        const size_t dim = codes.size();
        float metadata[4] = {minimum, delta, 0.0f, 0.0f};
        for (size_t i = 0; i < dim; ++i) {
            sq4::set_code(data(), i, codes[i]);
            const float xi = minimum + delta * codes[i];
            metadata[sq4::SUM] += xi;
            metadata[sq4::SUM_SQUARES] += xi * xi;
        }
        if (dim % 2) {
            data()[sq4::data_bytes_count(dim) - 1] |= 0xf0;
        }
        std::memcpy(data() + sq4::data_bytes_count(dim), metadata,
                    size() - sq4::data_bytes_count(dim));
    }

    uint8_t *data() { return bytes.data() + offset; }
    size_t size() const { return bytes.size() - offset; }
};

// SIMD-versus-scalar bound: 1e-5 of the magnitude of the summed terms. The result itself can
// cancel to near zero, so a bound relative to it would be too tight for IP.
//   IP: |min| * sum|y_i| + |delta| * sum(code_i * |y_i|)
//   L2: sum((|delta * code_i| + |min - y_i|)^2)
double fp32Tolerance(VecSimMetric metric, const uint8_t *x, const float *y, size_t dim) {
    const double min_val = storedMeta(x, dim, sq4::MIN_VAL);
    const double delta = storedMeta(x, dim, sq4::DELTA);
    double magnitude = 0.0;
    for (size_t i = 0; i < dim; ++i) {
        const double code = sq4::get_code(x, i);
        if (metric == VecSimMetric_IP) {
            magnitude +=
                std::abs(min_val) * std::abs(y[i]) + std::abs(delta) * code * std::abs(y[i]);
        } else {
            const double term = std::abs(delta * code) + std::abs(min_val - y[i]);
            magnitude += term * term;
        }
    }
    return 1e-5 * std::max(1.0, magnitude);
}

#ifdef __linux__
// Places an exact-size object immediately before a PROT_NONE page: any read past it faults.
class GuardedBuffer {
    void *mapping = MAP_FAILED;
    size_t mapping_size = 0;
    uint8_t *object = nullptr;

public:
    explicit GuardedBuffer(size_t bytes) {
        const long page_size = sysconf(_SC_PAGESIZE);
        if (page_size <= 0) {
            return;
        }
        const size_t page = size_t(page_size);
        const size_t accessible = std::max(size_t(1), (bytes + page - 1) / page) * page;
        mapping_size = accessible + page;
        mapping =
            mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mapping == MAP_FAILED) {
            return;
        }
        auto *boundary = static_cast<uint8_t *>(mapping) + accessible;
        if (mprotect(boundary, page, PROT_NONE) == 0) {
            object = boundary - bytes;
        }
    }
    ~GuardedBuffer() {
        if (mapping != MAP_FAILED) {
            munmap(mapping, mapping_size);
        }
    }
    GuardedBuffer(const GuardedBuffer &) = delete;
    GuardedBuffer &operator=(const GuardedBuffer &) = delete;
    uint8_t *data() { return object; }
};
#endif

// Per tier: dims 1..130 and 767..769, blob offsets 0..7, query offsets 0 and 4 bytes. Checks the
// dispatched function and alignment, then the chooser kernel (also below the dispatch minimum)
// against scalar.
void fp32MatchesScalar(VecSimMetric metric) {
    const auto tiers = sq4FP32Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-FP32 tier on this machine";
    }
    std::vector<size_t> dims;
    for (size_t dim = 1; dim <= 130; ++dim) {
        dims.push_back(dim);
    }
    dims.insert(dims.end(), {767u, 768u, 769u});
    std::mt19937 rng(0x5134);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        for (size_t dim : dims) {
            SCOPED_TRACE(dim);
            unsigned char alignment = 255;
            const auto dispatched = dispatchFP32(metric, dim, &alignment, &tier.features);
            const auto chosen = tier.choose(metric);
            const auto kernel = chosen(dim);
            EXPECT_EQ(dispatched, dim >= SQ4_FP32_SIMD_MIN_DIM ? kernel : scalarFP32(metric));
            EXPECT_EQ(alignment, 0);
            std::vector<uint8_t> codes(dim);
            std::vector<float> values(dim);
            for (size_t i = 0; i < dim; ++i) {
                codes[i] = code_dist(rng);
                values[i] = val_dist(rng);
            }
            const auto query = makeQuery(values);
            const float min_val = val_dist(rng), delta = (1.0f + val_dist(rng)) / 15;
            for (size_t query_offset : {0u, 1u}) {
                SCOPED_TRACE(query_offset * sizeof(float));
                std::vector<float> query_buffer(query_offset + query.size());
                std::copy(query.begin(), query.end(), query_buffer.begin() + query_offset);
                const float *y = query_buffer.data() + query_offset;
                for (size_t offset = 0; offset < 8; ++offset) {
                    SCOPED_TRACE(offset);
                    OffsetVector x(metric, codes, min_val, delta, offset);
                    const float scalar = scalarFP32(metric)(x.data(), y, dim);
                    const float simd = kernel(x.data(), y, dim);
                    EXPECT_LE(std::abs(double(simd) - scalar),
                              fp32Tolerance(metric, x.data(), y, dim));
                }
            }
        }
    }
}

// Negative control for fp32MatchesScalar's bound: a kernel that reads the nibbles in swapped order
// or the metadata one float late must miss it by more than 100 times. The query is the stored
// vector plus small noise, so swapping the nibbles changes the distance systematically rather than
// by chance.
void fp32ToleranceRejectsCorruption(VecSimMetric metric) {
    const auto tiers = sq4FP32Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-FP32 tier on this machine";
    }
    std::mt19937 rng(0x5135);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        for (size_t dim : {16u, 45u, 768u}) {
            SCOPED_TRACE(dim);
            // A minimum away from zero keeps the stored sum (the late-read delta) away from zero.
            const float min_val = 0.75f + 0.25f * val_dist(rng);
            const float delta = (1.5f + 0.5f * val_dist(rng)) / 15;
            std::vector<uint8_t> codes(dim);
            std::vector<float> values(dim);
            for (size_t i = 0; i < dim; ++i) {
                codes[i] = code_dist(rng);
                values[i] = min_val + delta * codes[i] + 0.05f * val_dist(rng);
            }
            const auto query = makeQuery(values);
            OffsetVector x(metric, codes, min_val, delta, 0);
            const auto kernel = tier.choose(metric)(dim);
            const double tolerance = fp32Tolerance(metric, x.data(), query.data(), dim);
            const float scalar = scalarFP32(metric)(x.data(), query.data(), dim);
            EXPECT_LE(std::abs(double(kernel(x.data(), query.data(), dim)) - scalar), tolerance);

            OffsetVector swapped = x;
            for (size_t b = 0; b < sq4::data_bytes_count(dim); ++b) {
                const uint8_t byte = swapped.data()[b];
                swapped.data()[b] = uint8_t(byte >> 4) | uint8_t(byte << 4);
            }
            EXPECT_GT(std::abs(double(kernel(swapped.data(), query.data(), dim)) - scalar),
                      100 * tolerance);

            OffsetVector shifted = x;
            uint8_t *metadata = shifted.data() + sq4::data_bytes_count(dim);
            const size_t metadata_bytes = shifted.size() - sq4::data_bytes_count(dim);
            std::memmove(metadata, metadata + sizeof(float), metadata_bytes - sizeof(float));
            EXPECT_GT(std::abs(double(kernel(shifted.data(), query.data(), dim)) - scalar),
                      100 * tolerance);
        }
    }
}
} // namespace

TEST(SQ4SIMD, FP32IPMatchesScalar) { fp32MatchesScalar(VecSimMetric_IP); }
TEST(SQ4SIMD, FP32L2MatchesScalar) { fp32MatchesScalar(VecSimMetric_L2); }

TEST(SQ4SIMD, FP32IPToleranceRejectsCorruption) { fp32ToleranceRejectsCorruption(VecSimMetric_IP); }
TEST(SQ4SIMD, FP32L2ToleranceRejectsCorruption) { fp32ToleranceRejectsCorruption(VecSimMetric_L2); }

// FP32L2PreservesResidualsAtLargeCommonOffset for every tier: with a common offset of 1e4 (float
// spacing 2^-10 there) and residuals up to 1e-2, forming min + delta * q before subtracting y
// would round away a large part of each residual, so the kernels must keep min - y first.
TEST(SQ4SIMD, FP32L2LargeCommonOffset) {
    const auto tiers = sq4FP32Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-FP32 tier on this machine";
    }
    const float min_val = 1e4f, delta = 1e-2f / 15, spacing = 1.0f / 1024;
    for (size_t dim : {16u, 17u, 31u, 32u, 45u, 64u, 129u, 768u, 769u}) {
        SCOPED_TRACE(dim);
        std::vector<uint8_t> codes(dim);
        std::vector<float> values(dim);
        double expected = 0.0;
        float reconstructed_first = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            codes[i] = (i * 7) % 16;
            // min - y is 0, 1 or 2 float spacings, exact by Sterbenz.
            values[i] = min_val - float(i % 3) * spacing;
            const double residual = double(delta) * codes[i] + (double(min_val) - values[i]);
            expected += residual * residual;
            const float diff = (min_val + delta * codes[i]) - values[i];
            reconstructed_first += diff * diff;
        }
        const double tolerance = 1e-4 * expected;
        // The bound below discriminates: reconstructing first misses it.
        ASSERT_GT(std::abs(reconstructed_first - expected), tolerance);
        const auto query = makeQuery(values);
        OffsetVector x(VecSimMetric_L2, codes, min_val, delta, 1);
        EXPECT_NEAR(SQ4_FP32_L2Sqr(x.data(), query.data(), dim), expected, tolerance);
        for (const auto &tier : tiers) {
            SCOPED_TRACE(tier.name);
            EXPECT_NEAR(tier.chooseL2(dim)(x.data(), query.data(), dim), expected, tolerance);
            EXPECT_NEAR(spaces::L2_SQ4_FP32_GetDistFunc(dim, nullptr, &tier.features)(
                            x.data(), query.data(), dim),
                        expected, tolerance);
        }
    }
}

#ifdef __linux__
// Storage blobs and query blobs of both metrics end exactly at a PROT_NONE page, so a kernel that
// reads past the packed codes plus metadata, or past the query's floats plus metadata, faults.
TEST(SQ4SIMD, ExactBuffersBeforeGuard) {
    const auto tiers = sq4FP32Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-FP32 tier on this machine";
    }
    std::vector<size_t> dims;
    for (size_t dim = 0; dim < 32; ++dim) {
        dims.push_back(dim);
    }
    dims.insert(dims.end(), {127u, 128u, 129u, 767u, 768u, 769u, 1536u, 65536u});
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
        SCOPED_TRACE(metric);
        for (size_t dim : dims) {
            SCOPED_TRACE(dim);
            std::vector<uint8_t> codes(dim);
            std::vector<float> values(dim);
            for (size_t i = 0; i < dim; ++i) {
                codes[i] = (i * 7) % 16;
                values[i] = 0.75f + 0.0625f * ((i * 7) % 16);
            }
            OffsetVector x(metric, codes, -1.25f, 0.125f, 0);
            const auto query = makeQuery(values);
            const size_t query_bytes = queryBytes(metric, dim);
            GuardedBuffer guarded_x(x.size()), guarded_query(query_bytes);
            ASSERT_NE(guarded_x.data(), nullptr);
            ASSERT_NE(guarded_query.data(), nullptr);
            std::memcpy(guarded_x.data(), x.data(), x.size());
            std::memcpy(guarded_query.data(), query.data(), query_bytes);
            const auto *y = reinterpret_cast<const float *>(guarded_query.data());
            // Double-precision oracle over the reconstructed coordinates.
            double dot = 0.0, norm_x = 0.0, norm_y = 0.0, l2 = 0.0;
            for (size_t i = 0; i < dim; ++i) {
                const double xi = -1.25 + 0.125 * codes[i];
                dot += xi * values[i];
                norm_x += xi * xi;
                norm_y += double(values[i]) * values[i];
                l2 += (xi - values[i]) * (xi - values[i]);
            }
            // IP keeps the prototype's bound; L2 uses the equivalence bound.
            const double expected = metric == VecSimMetric_IP ? 1.0 - dot : l2;
            const double tolerance = metric == VecSimMetric_IP
                                         ? 32 * std::numeric_limits<float>::epsilon() *
                                               (1.0 + std::sqrt(norm_x * norm_y))
                                         : fp32Tolerance(metric, guarded_x.data(), y, dim);
            for (const auto &tier : tiers) {
                SCOPED_TRACE(tier.name);
                EXPECT_NEAR(tier.choose(metric)(dim)(guarded_x.data(), y, dim), expected,
                            tolerance);
            }
        }
    }
}
#endif

// SIMD SQ4-SQ4 tiers, reached through the dispatcher by clearing the feature flags of the tiers
// above each, and through their choosers directly. The L2 kernels are the stored-sum identity over
// the integer code dot, so L2 has the same tiers and dispatch minimum as IP, including the NEON dot
// product tier. On x86 AVX512F_BW_VL_VNNI takes precedence over AVX2; AVX2 needs no FMA.
namespace {
// Stored-to-stored forms: IP, and the L2 identity ||x||^2 + ||y||^2 - 2 * IP(x, y) over the stored
// metadata.
enum class SQ4Form { IP, L2 };

VecSimMetric metricOf(SQ4Form form) {
    return form == SQ4Form::IP ? VecSimMetric_IP : VecSimMetric_L2;
}

struct SQ4Tier {
    const char *name;
    Features features;
    Chooser chooseIP;
    Chooser chooseL2;
    // The dispatcher hands out this tier's kernels (both metrics) from this dim up.
    size_t min_dim;

    Chooser choose(SQ4Form form) const { return form == SQ4Form::IP ? chooseIP : chooseL2; }
    size_t minDim() const { return min_dim; }
};

std::vector<SQ4Tier> sq4SQ4Tiers() {
    std::vector<SQ4Tier> tiers;
    [[maybe_unused]] Features features = spaces::getCpuOptimizationFeatures();
#ifdef CPU_FEATURES_ARCH_AARCH64
#ifdef OPT_SVE2
    if (features.sve2) {
        tiers.push_back({"SVE2", features, spaces::Choose_SQ4_SQ4_IP_implementation_SVE2,
                         spaces::Choose_SQ4_SQ4_L2_implementation_SVE2, 0});
    }
#endif
    features.sve2 = 0;
#ifdef OPT_SVE
    if (features.sve) {
        tiers.push_back({"SVE", features, spaces::Choose_SQ4_SQ4_IP_implementation_SVE,
                         spaces::Choose_SQ4_SQ4_L2_implementation_SVE, 0});
    }
#endif
    features.sve = 0;
#ifdef OPT_NEON_DOTPROD
    if (features.asimddp) {
        tiers.push_back({"NEON_DOTPROD", features,
                         spaces::Choose_SQ4_SQ4_IP_implementation_NEON_DOTPROD,
                         spaces::Choose_SQ4_SQ4_L2_implementation_NEON_DOTPROD, 16});
    }
#endif
    features.asimddp = 0;
#ifdef OPT_NEON
    if (features.asimd) {
        tiers.push_back({"NEON", features, spaces::Choose_SQ4_SQ4_IP_implementation_NEON,
                         spaces::Choose_SQ4_SQ4_L2_implementation_NEON, 16});
    }
#endif
#endif
#ifdef CPU_FEATURES_ARCH_X86_64
#ifdef OPT_AVX512_F_BW_VL_VNNI
    if (features.avx512f && features.avx512bw && features.avx512vl && features.avx512vnni) {
        tiers.push_back({"AVX512F_BW_VL_VNNI", features,
                         spaces::Choose_SQ4_SQ4_IP_implementation_AVX512F_BW_VL_VNNI,
                         spaces::Choose_SQ4_SQ4_L2_implementation_AVX512F_BW_VL_VNNI, 64});
    }
#endif
#ifdef OPT_AVX2
    if (features.avx2) {
        // Force the AVX2-only gate, with FMA and every AVX512 flag cleared.
        Features avx2_only{};
        avx2_only.avx2 = 1;
        tiers.push_back({"AVX2", avx2_only, spaces::Choose_SQ4_SQ4_IP_implementation_AVX2,
                         spaces::Choose_SQ4_SQ4_L2_implementation_AVX2, 64});
    }
#endif
#endif
    return tiers;
}

spaces::dist_func_t<float> scalarSQ4(SQ4Form form) {
    return form == SQ4Form::IP ? SQ4_SQ4_InnerProduct : SQ4_SQ4_L2Sqr;
}

spaces::dist_func_t<float> dispatchSQ4(SQ4Form form, size_t dim, unsigned char *alignment,
                                       const Features *features) {
    switch (form) {
    case SQ4Form::IP:
        return spaces::IP_SQ4_SQ4_GetDistFunc(dim, alignment, features);
    case SQ4Form::L2:
        return spaces::L2_SQ4_SQ4_GetDistFunc(dim, alignment, features);
    }
    return nullptr;
}

// Magnitude of the summed terms of the stored-to-stored IP identity:
//   |min_x| * sum|y_i| + |min_y| * sum|x_i| + dim * |min_x * min_y| + |dx * dy| * sum(qx * qy)
double sq4IPMagnitude(const uint8_t *x, const uint8_t *y, size_t dim) {
    const double min_x = storedMeta(x, dim, sq4::MIN_VAL), dx = storedMeta(x, dim, sq4::DELTA);
    const double min_y = storedMeta(y, dim, sq4::MIN_VAL), dy = storedMeta(y, dim, sq4::DELTA);
    double abs_x = 0.0, abs_y = 0.0, code_dot = 0.0;
    for (size_t i = 0; i < dim; ++i) {
        const double qx = sq4::get_code(x, i), qy = sq4::get_code(y, i);
        abs_x += std::abs(min_x + dx * qx);
        abs_y += std::abs(min_y + dy * qy);
        code_dot += qx * qy;
    }
    return std::abs(min_x) * abs_y + std::abs(min_y) * abs_x +
           double(dim) * std::abs(min_x * min_y) + std::abs(dx * dy) * code_dot;
}

// SIMD-versus-scalar bound for two stored vectors: 1e-5 of the magnitude of the summed terms.
//   IP: sq4IPMagnitude
//   L2: |sum_sq_x| + |sum_sq_y| + 2 * sq4IPMagnitude
double sq4Tolerance(SQ4Form form, const uint8_t *x, const uint8_t *y, size_t dim) {
    double magnitude = sq4IPMagnitude(x, y, dim);
    if (form == SQ4Form::L2) {
        magnitude = std::abs(storedMeta(x, dim, sq4::SUM_SQUARES)) +
                    std::abs(storedMeta(y, dim, sq4::SUM_SQUARES)) + 2 * magnitude;
    }
    return 1e-5 * std::max(1.0, magnitude);
}

// Double-precision stored-to-stored reference: the IP and L2 identities over the stored metadata
// (what the scalar kernels compute).
double sq4Reference(SQ4Form form, const uint8_t *x, const uint8_t *y, size_t dim) {
    if (form == SQ4Form::IP) {
        return 1.0 - identityIP(x, y, dim);
    }
    return storedMeta(x, dim, sq4::SUM_SQUARES) + storedMeta(y, dim, sq4::SUM_SQUARES) -
           2 * identityIP(x, y, dim);
}

// Per tier: dims 1..130 and 767..769, offsets 0..7 for each operand. Checks the dispatched function
// and alignment, then the chooser kernel (also below the dispatch minimum) against scalar.
void sq4MatchesScalar(SQ4Form form) {
    const VecSimMetric metric = metricOf(form);
    const auto tiers = sq4SQ4Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-SQ4 tier on this machine";
    }
    std::vector<size_t> dims;
    for (size_t dim = 1; dim <= 130; ++dim) {
        dims.push_back(dim);
    }
    dims.insert(dims.end(), {767u, 768u, 769u});
    std::mt19937 rng(0x5144);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        for (size_t dim : dims) {
            SCOPED_TRACE(dim);
            unsigned char alignment = 255;
            const auto dispatched = dispatchSQ4(form, dim, &alignment, &tier.features);
            const auto kernel = tier.choose(form)(dim);
            EXPECT_EQ(dispatched, dim >= tier.minDim() ? kernel : scalarSQ4(form));
            EXPECT_EQ(alignment, 0);
            std::vector<uint8_t> x_codes(dim), y_codes(dim);
            for (size_t i = 0; i < dim; ++i) {
                x_codes[i] = code_dist(rng);
                y_codes[i] = code_dist(rng);
            }
            const float min_x = val_dist(rng), dx = (1.0f + val_dist(rng)) / 15;
            const float min_y = val_dist(rng), dy = (1.0f + val_dist(rng)) / 15;
            std::vector<OffsetVector> ys;
            for (size_t offset = 0; offset < 8; ++offset) {
                ys.emplace_back(metric, y_codes, min_y, dy, offset);
            }
            for (size_t x_offset = 0; x_offset < 8; ++x_offset) {
                SCOPED_TRACE(x_offset);
                OffsetVector x(metric, x_codes, min_x, dx, x_offset);
                for (auto &y : ys) {
                    SCOPED_TRACE(y.offset);
                    const float scalar = scalarSQ4(form)(x.data(), y.data(), dim);
                    const float simd = kernel(x.data(), y.data(), dim);
                    EXPECT_LE(std::abs(double(simd) - scalar),
                              sq4Tolerance(form, x.data(), y.data(), dim));
                }
            }
        }
    }
}

// Negative control for sq4MatchesScalar's bound: a kernel that reads the nibbles in swapped order
// or the metadata one float late must miss it by more than 100 times. y is x's codes moved by at
// most one level, so both corruptions change the distance systematically rather than by chance.
void sq4ToleranceRejectsCorruption(SQ4Form form) {
    const VecSimMetric metric = metricOf(form);
    const auto tiers = sq4SQ4Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-SQ4 tier on this machine";
    }
    std::mt19937 rng(0x5145);
    std::uniform_int_distribution<int> code_dist(0, 15);
    std::uniform_int_distribution<int> step_dist(-1, 1);
    std::uniform_real_distribution<float> val_dist(-1.0f, 1.0f);
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        for (size_t dim : {16u, 45u, 768u}) {
            SCOPED_TRACE(dim);
            std::vector<uint8_t> x_codes(dim), y_codes(dim);
            for (size_t i = 0; i < dim; ++i) {
                x_codes[i] = code_dist(rng);
                y_codes[i] = std::clamp(x_codes[i] + step_dist(rng), 0, 15);
            }
            // Positive minimums keep the stored sum (the late-read delta) away from zero.
            const float min_x = 0.1f + 0.05f * val_dist(rng),
                        dx = (1.5f + 0.5f * val_dist(rng)) / 15;
            const float min_y = 0.1f + 0.05f * val_dist(rng),
                        dy = dx * (1.0f + 0.1f * val_dist(rng));
            OffsetVector x(metric, x_codes, min_x, dx, 0);
            OffsetVector y(metric, y_codes, min_y, dy, 0);
            const auto kernel = tier.choose(form)(dim);
            const double tolerance = sq4Tolerance(form, x.data(), y.data(), dim);
            const float scalar = scalarSQ4(form)(x.data(), y.data(), dim);
            EXPECT_LE(std::abs(double(kernel(x.data(), y.data(), dim)) - scalar), tolerance);

            OffsetVector swapped = x;
            for (size_t b = 0; b < sq4::data_bytes_count(dim); ++b) {
                const uint8_t byte = swapped.data()[b];
                swapped.data()[b] = uint8_t(byte >> 4) | uint8_t(byte << 4);
            }
            EXPECT_GT(std::abs(double(kernel(swapped.data(), y.data(), dim)) - scalar),
                      100 * tolerance);

            OffsetVector shifted = x;
            uint8_t *metadata = shifted.data() + sq4::data_bytes_count(dim);
            const size_t metadata_bytes = shifted.size() - sq4::data_bytes_count(dim);
            std::memmove(metadata, metadata + sizeof(float), metadata_bytes - sizeof(float));
            EXPECT_GT(std::abs(double(kernel(shifted.data(), y.data(), dim)) - scalar),
                      100 * tolerance);
        }
    }
}
} // namespace

TEST(SQ4SIMD, SQ4IPMatchesScalar) { sq4MatchesScalar(SQ4Form::IP); }
TEST(SQ4SIMD, SQ4L2MatchesScalar) { sq4MatchesScalar(SQ4Form::L2); }

TEST(SQ4SIMD, SQ4IPToleranceRejectsCorruption) { sq4ToleranceRejectsCorruption(SQ4Form::IP); }
TEST(SQ4SIMD, SQ4L2ToleranceRejectsCorruption) { sq4ToleranceRejectsCorruption(SQ4Form::L2); }

// The integer code dot is exact while 225 * dim fits a uint32. Above SQ4_MAX_EXACT_SIMD_DIM the
// dispatcher hands back the scalar kernel on every tier; at it, the SIMD kernels are still handed
// out and are exact for all-15 codes, whose total is the largest a uint32 holds.
// On x86 that total also exceeds INT32_MAX, so it checks that the signed int32 SIMD lanes are
// reduced without signed wrap.
TEST(SQ4_SQ4, DispatchFallsBackAboveExactDim) {
    constexpr size_t cap = spaces::SQ4_MAX_EXACT_SIMD_DIM;
    constexpr uint64_t max_term = 15 * 15;
    constexpr uint64_t uint32_max = std::numeric_limits<uint32_t>::max();
    ASSERT_LE(max_term * cap, uint32_max);
    ASSERT_GT(max_term * (cap + 1), uint32_max);

    unsigned char alignment = 255;
    EXPECT_EQ(spaces::IP_SQ4_SQ4_GetDistFunc(cap + 1, &alignment), SQ4_SQ4_InnerProduct);
    EXPECT_EQ(alignment, 0);
    EXPECT_EQ((spaces::GetDistFunc<sq4, float>(VecSimMetric_IP, cap + 1, nullptr)),
              SQ4_SQ4_InnerProduct);
    alignment = 255;
    EXPECT_EQ(spaces::L2_SQ4_SQ4_GetDistFunc(cap + 1, &alignment), SQ4_SQ4_L2Sqr);
    EXPECT_EQ(alignment, 0);

    const auto tiers = sq4SQ4Tiers();
    if (tiers.empty()) {
        return;
    }
    // All codes 15 with min 0, delta 1 and sum 0: the distance is 1 - float(225 * cap).
    std::vector<uint8_t> blob(sq4::storage_bytes_count<VecSimMetric_IP>(cap), 0);
    std::memset(blob.data(), 0xff, sq4::data_bytes_count(cap));
    blob[sq4::data_bytes_count(cap) - 1] = cap % 2 ? 0x0f : 0xff;
    const float metadata[3] = {0.0f, 1.0f, 0.0f};
    std::memcpy(blob.data() + sq4::data_bytes_count(cap), metadata, sizeof(metadata));
    const float expected = 1.0f - static_cast<float>(uint32_t(max_term * cap));
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        alignment = 255;
        EXPECT_EQ(spaces::IP_SQ4_SQ4_GetDistFunc(cap + 1, &alignment, &tier.features),
                  SQ4_SQ4_InnerProduct);
        EXPECT_EQ(alignment, 0);
        const auto kernel = tier.chooseIP(cap);
        EXPECT_EQ(spaces::IP_SQ4_SQ4_GetDistFunc(cap, nullptr, &tier.features), kernel);
        EXPECT_EQ(kernel(blob.data(), blob.data(), cap), expected);
    }

    // L2 is the identity over the same code dot: with sum_squares 0 as well, it is -2 * float(225 *
    // cap).
    blob.resize(sq4::storage_bytes_count<VecSimMetric_L2>(cap), 0);
    const float expected_l2 = -2.0f * static_cast<float>(uint32_t(max_term * cap));
    for (const auto &tier : tiers) {
        SCOPED_TRACE(tier.name);
        alignment = 255;
        EXPECT_EQ(spaces::L2_SQ4_SQ4_GetDistFunc(cap + 1, &alignment, &tier.features),
                  SQ4_SQ4_L2Sqr);
        EXPECT_EQ(alignment, 0);
        const auto kernel = tier.chooseL2(cap);
        EXPECT_EQ(spaces::L2_SQ4_SQ4_GetDistFunc(cap, nullptr, &tier.features), kernel);
        EXPECT_EQ(kernel(blob.data(), blob.data(), cap), expected_l2);
    }
}

#ifdef __linux__
// Both stored operands end exactly at a PROT_NONE page, for both metrics and every tier, with the
// odd-dim padding nibble set: a kernel that reads past the packed codes plus metadata faults, and
// one that lets the padding nibble in misses the double reference.
TEST(SQ4SIMD, SQ4ExactBuffersBeforeGuard) {
    const auto tiers = sq4SQ4Tiers();
    if (tiers.empty()) {
        GTEST_SKIP() << "no SIMD SQ4-SQ4 tier on this machine";
    }
    std::vector<size_t> dims;
    for (size_t dim = 0; dim < 128; ++dim) {
        dims.push_back(dim);
    }
    dims.insert(dims.end(), {127u, 128u, 129u, 767u, 768u, 769u, 1536u, 65536u});
    for (auto form : {SQ4Form::IP, SQ4Form::L2}) {
        SCOPED_TRACE(int(form));
        const VecSimMetric metric = metricOf(form);
        for (size_t dim : dims) {
            SCOPED_TRACE(dim);
            std::vector<uint8_t> x_codes(dim), y_codes(dim);
            for (size_t i = 0; i < dim; ++i) {
                x_codes[i] = (i * 7) % 16;
                y_codes[i] = (i * 5 + 3) % 16;
            }
            OffsetVector x(metric, x_codes, -1.25f, 0.125f, 0);
            OffsetVector y(metric, y_codes, 0.5f, 0.0625f, 0);
            GuardedBuffer guarded_x(x.size()), guarded_y(y.size());
            ASSERT_NE(guarded_x.data(), nullptr);
            ASSERT_NE(guarded_y.data(), nullptr);
            std::memcpy(guarded_x.data(), x.data(), x.size());
            std::memcpy(guarded_y.data(), y.data(), y.size());
            const uint8_t *gx = guarded_x.data(), *gy = guarded_y.data();
            const double expected = sq4Reference(form, gx, gy, dim);
            const double tolerance = sq4Tolerance(form, gx, gy, dim);
            for (const auto &tier : tiers) {
                SCOPED_TRACE(tier.name);
                const auto kernel = tier.choose(form)(dim);
                EXPECT_NEAR(kernel(gx, gy, dim), expected, tolerance);
                EXPECT_NEAR(kernel(gy, gx, dim), expected, tolerance);
            }
        }
    }
}
#endif

#if defined(CPU_FEATURES_ARCH_X86_64) && defined(OPT_AVX2)
TEST(SQ4SIMD, AVX2NibbleProductsAreExact) {
    if (!spaces::getCpuOptimizationFeatures().avx2) {
        GTEST_SKIP() << "AVX2 unavailable";
    }
    for (size_t dim = 64; dim < 192; ++dim) {
        SCOPED_TRACE(dim);
        // All-15 pairs exercise maddubs' largest sum; alternating and asymmetric codes
        // distinguish low and high nibbles and expose leakage from the 16-bit shift.
        for (unsigned pattern = 0; pattern < 3; ++pattern) {
            SCOPED_TRACE(pattern);
            std::vector<uint8_t> xc(dim), yc(dim);
            uint64_t dot = 0;
            for (size_t i = 0; i < dim; ++i) {
                xc[i] = pattern == 0 ? 15 : (i % 2 ? 15 : 0);
                yc[i] = pattern < 2 ? 15 : (i % 2 ? 1 : 14);
                dot += uint64_t(xc[i]) * yc[i];
            }
            PackedVector x(xc, 0.0f, 1.0f), y(yc, 0.0f, 1.0f);
            if (dim % 2) {
                x.data()[dim / 2] |= 0xf0;
                y.data()[dim / 2] |= 0xf0;
            }
            const float expected_ip = 1.0f - static_cast<float>(dot);
            const float expected_l2 =
                referenceDistance(VecSimMetric_L2, x.reconstructed, y.reconstructed);
            EXPECT_EQ(spaces::Choose_SQ4_SQ4_IP_implementation_AVX2(dim)(x.data(), y.data(), dim),
                      expected_ip);
            EXPECT_EQ(spaces::Choose_SQ4_SQ4_L2_implementation_AVX2(dim)(x.data(), y.data(), dim),
                      expected_l2);
        }
    }
}
#endif
