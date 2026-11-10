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

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

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

TEST(SQ4Distances, DispatchIsScalarWithZeroAlignment) {
    for (size_t dim : {7u, 8u, 15u, 16u, 63u, 64u, 768u}) {
        SCOPED_TRACE(dim);
        for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
            SCOPED_TRACE(metric);
            const bool ip = metric == VecSimMetric_IP;
            unsigned char alignment = 255;
            EXPECT_EQ((spaces::GetDistFunc<sq4, float, float>(metric, dim, &alignment)),
                      ip ? SQ4_FP32_InnerProduct : SQ4_FP32_L2Sqr);
            EXPECT_EQ(alignment, 0);
            alignment = 255;
            EXPECT_EQ((spaces::GetDistFunc<sq4, float>(metric, dim, &alignment)),
                      ip ? SQ4_SQ4_InnerProduct : SQ4_SQ4_L2Sqr);
            EXPECT_EQ(alignment, 0);
        }
    }
}
