/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "factory/components/disk_components_factory.h"
#include "factory/components/disk_calculator.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/types/sq8.h"

using sq8 = vecsim_types::sq8;

// =============================================================================
// Disk Components Factory Tests
// =============================================================================
//
// These tests verify the DiskComponentsFactory creates correct components
// (calculator and preprocessor) for disk-based indexes.

class DiskComponentsTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;
};

// Parameterized test class for different metrics
class DiskComponentsMetricTest : public DiskComponentsTest, public testing::WithParamInterface<VecSimMetric> {};

// Test DiskDistanceCalculator: creation and all distance modes (Full, QuantizedVsFull, Quantized)
TEST_P(DiskComponentsMetricTest, DiskCalculatorAllModes) {
    VecSimMetric metric = GetParam();
    auto allocator = VecSimAllocator::newVecsimAllocator();

    auto* calculator = DiskComponentsFactory::CreateDiskCalculator<float>(allocator, metric, DIM);
    ASSERT_NE(calculator, nullptr);

    // Verify each distance mode returns the expected function
    auto expectedFuncFull = spaces::GetDistFunc<float, float>(metric, DIM, nullptr);
    EXPECT_EQ(calculator->getDistFunc<DistanceMode::Full>(), expectedFuncFull);

    auto expectedFuncQVF = spaces::GetDistFunc<sq8, float, float>(metric, DIM, nullptr);
    EXPECT_EQ(calculator->getDistFunc<DistanceMode::QuantizedVsFull>(), expectedFuncQVF);

    auto expectedFuncQ = spaces::GetDistFunc<sq8, float>(metric, DIM, nullptr);
    EXPECT_EQ(calculator->getDistFunc<DistanceMode::Quantized>(), expectedFuncQ);

    delete calculator;
}

// Test DiskIndexComponents: creation and conversion to base class
TEST_P(DiskComponentsMetricTest, DiskIndexComponentsAndConversion) {
    VecSimMetric metric = GetParam();
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // --- Component creation ---
    auto components =
        DiskComponentsFactory::CreateDiskIndexComponents<float, float>(allocator, metric, DIM, /*is_normalized=*/true);

    ASSERT_NE(components.diskCalculator, nullptr);
    ASSERT_NE(components.preprocessors, nullptr);

    // --- Implicit conversion to IndexComponents ---
    IndexComponents<float, float> baseComponents = components;
    EXPECT_EQ(baseComponents.indexCalculator, components.diskCalculator);
    EXPECT_EQ(baseComponents.preprocessors, components.preprocessors);

    delete components.preprocessors;
    delete components.diskCalculator;
}

// Instantiate parameterized tests for all metrics
INSTANTIATE_TEST_SUITE_P(MetricTests, DiskComponentsMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric>& info) {
                             return VecSimMetric_ToString(info.param);
                         });
