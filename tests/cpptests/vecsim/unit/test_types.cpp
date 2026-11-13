/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <algorithm>
#include <array>
#include <random>
#include "gtest/gtest.h"
#include "VecSim/types/float16.h"

class FP16TypeCompare : public ::testing::Test {};

TEST_F(FP16TypeCompare, OrderingMatchesFP32) {
    using vecsim_types::float16;
    using vecsim_types::FP32_to_FP16;

    // Mix of negatives, zeros, and positives. Same-magnitude positive vs negative is the
    // historically broken case (raw uint16_t compare would put -1.0 above +1.0 because of the
    // sign bit).
    const std::array<float, 7> values{-2.5f, -1.0f, -0.0f, 0.0f, 0.5f, 1.0f, 2.5f};

    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            const float16 a = FP32_to_FP16(values[i]);
            const float16 b = FP32_to_FP16(values[j]);
            EXPECT_EQ(a < b, values[i] < values[j]) << values[i] << " < " << values[j];
            EXPECT_EQ(a > b, values[i] > values[j]) << values[i] << " > " << values[j];
            EXPECT_EQ(a <= b, values[i] <= values[j]) << values[i] << " <= " << values[j];
            EXPECT_EQ(a >= b, values[i] >= values[j]) << values[i] << " >= " << values[j];
            EXPECT_EQ(a == b, values[i] == values[j]) << values[i] << " == " << values[j];
            EXPECT_EQ(a != b, values[i] != values[j]) << values[i] << " != " << values[j];
        }
    }
}

TEST_F(FP16TypeCompare, MinmaxElementHandlesNegatives) {
    using vecsim_types::float16;
    using vecsim_types::FP16_to_FP32;
    using vecsim_types::FP32_to_FP16;

    // Min and max are both negative, with the min having a larger absolute value. Under the
    // pre-fix uint16_t-based comparison, the negative with the larger magnitude would have
    // compared as the maximum, swapping the result.
    const std::array<float16, 5> data{FP32_to_FP16(-3.5f), FP32_to_FP16(0.5f), FP32_to_FP16(-1.25f),
                                      FP32_to_FP16(2.0f), FP32_to_FP16(-0.75f)};

    auto [min_it, max_it] = std::minmax_element(data.begin(), data.end());
    EXPECT_FLOAT_EQ(FP16_to_FP32(*min_it), -3.5f);
    EXPECT_FLOAT_EQ(FP16_to_FP32(*max_it), 2.0f);
}

#ifdef OPT_AVX512_FP16_VL
class FP16Type : public ::testing::Test {};

TEST_F(FP16Type, Test_Float16VSvecsim) {
    static_assert(sizeof(_Float16) == sizeof(vecsim_types::float16));
    size_t runs = 10;

    std::mt19937 gen{};
    std::uniform_real_distribution<float> dis(-0.999999, 0.999999);

    for (size_t i = 0; i < runs; i++) {
        float val = dis(gen);
        _Float16 nativeCast = static_cast<_Float16>(val);
        vecsim_types::float16 vecsimCast = vecsim_types::FP32_to_FP16(val);

        uint16_t nativeBits, vecsimBits;
        std::memcpy(&nativeBits, &nativeCast, sizeof(_Float16));
        std::memcpy(&vecsimBits, &vecsimCast, sizeof(vecsim_types::float16));

        ASSERT_EQ(nativeBits, vecsimBits) << "failed at iteration " << i << "with value " << val;
    }
}

#endif
