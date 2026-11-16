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
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <vector>
#include "gtest/gtest.h"
#include "vecsim/index_factories/components/components_factory.h"

namespace {
using vecsim_types::sq8_fp64;

std::vector<double> reconstruct(const void *blob, size_t dim, double &delta) {
    const auto *const bytes = static_cast<const uint8_t *>(blob);
    double minimum;
    std::memcpy(&minimum, bytes + dim, sizeof(double));
    std::memcpy(&delta, bytes + dim + sizeof(double), sizeof(double));
    std::vector<double> values(dim);
    for (size_t i = 0; i < dim; ++i) {
        values[i] = minimum + delta * bytes[i];
    }
    return values;
}

template <VecSimMetric Metric>
double reference(const std::vector<double> &a, const std::vector<double> &b) {
    double result = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        if constexpr (Metric == VecSimMetric_L2) {
            const double residual = a[i] - b[i];
            result += residual * residual;
        } else {
            result += a[i] * b[i];
        }
    }
    return Metric == VecSimMetric_L2 ? result : 1.0 - result;
}

void normalize(std::vector<double> &values) {
    const double norm =
        std::sqrt(std::inner_product(values.begin(), values.end(), values.begin(), 0.0));
    for (auto &value : values) {
        value /= norm;
    }
}

void checkBound(VecSimMetric metric, double actual, double expected, double delta_a, double delta_b,
                const std::vector<double> &a, const std::vector<double> &b) {
    double norm1_a = 0.0;
    double norm1_b = 0.0;
    double scale = 1.0;
    for (size_t i = 0; i < a.size(); ++i) {
        norm1_a += std::abs(a[i]);
        norm1_b += std::abs(b[i]);
        scale = std::max({scale, std::abs(a[i]), std::abs(b[i])});
    }
    const double error_a =
        std::all_of(a.begin(), a.end(), [&](double value) { return value == a.front(); }) ? 0.0
                                                                                          : delta_a;
    const double error_b =
        std::all_of(b.begin(), b.end(), [&](double value) { return value == b.front(); }) ? 0.0
                                                                                          : delta_b;
    const double slack = 64 * std::numeric_limits<double>::epsilon() * (a.size() + 1) * scale;
    if (metric == VecSimMetric_L2) {
        EXPECT_GE(actual, 0.0);
        EXPECT_LE(std::abs(std::sqrt(actual) - std::sqrt(expected)),
                  (error_a + error_b) * std::sqrt(double(a.size())) / 2 + slack);
    } else {
        EXPECT_LE(std::abs(actual - expected), error_a * norm1_b / 2 + error_b * norm1_a / 2 +
                                                   a.size() * error_a * error_b / 4 +
                                                   slack * scale);
    }
}

template <VecSimMetric Metric>
void checkDistances(size_t dim, bool nearly_parallel) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto components = CreateSQ8FP64IndexComponents<Metric>(allocator, dim);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    std::vector<double> a(dim);
    std::vector<double> b(dim);
    for (size_t i = 0; i < dim; ++i) {
        a[i] = 0.31 * (int(i % 7) - 3) + 0.11;
        b[i] = nearly_parallel ? a[i] + 1e-7 * i : 0.23 * (int(i % 5) - 2) - 0.07;
    }
    if constexpr (Metric == VecSimMetric_Cosine) {
        normalize(a);
        normalize(b);
    }
    const size_t input_size = dim * sizeof(double);
    const auto left = preprocessors->preprocessForStorage(a.data(), input_size);
    const auto right = preprocessors->preprocessForStorage(b.data(), input_size);
    const auto query = preprocessors->preprocessQuery(b.data(), input_size);
    const auto combined = preprocessors->preprocess(b.data(), input_size);
    EXPECT_EQ(std::memcmp(right.get(), combined.getStorageBlob(),
                          sq8_fp64::storage_bytes_count<Metric>(dim)),
              0);
    EXPECT_EQ(
        std::memcmp(query.get(), combined.getQueryBlob(), sq8_fp64::query_bytes_count<Metric>(dim)),
        0);
    double delta_a;
    double delta_b;
    const auto ra = reconstruct(left.get(), dim, delta_a);
    const auto rb = reconstruct(right.get(), dim, delta_b);
    const double symmetric = calculator->calcDistance(left.get(), right.get(), dim);
    const double asymmetric = calculator->calcDistanceForQuery(left.get(), query.get(), dim);
    EXPECT_NEAR(symmetric, reference<Metric>(ra, rb), 1e-13 * (dim + 1));
    EXPECT_NEAR(asymmetric, reference<Metric>(ra, b), 1e-13 * (dim + 1));
    checkBound(Metric, symmetric, reference<Metric>(a, b), delta_a, delta_b, a, b);
    checkBound(Metric, asymmetric, reference<Metric>(a, b), delta_a, 0, a, b);
    EXPECT_DOUBLE_EQ(
        calculator->getDistanceDispatch(DistanceMode::StoredToStored)(left.get(), right.get(), dim),
        symmetric);
    EXPECT_DOUBLE_EQ(
        calculator->getDistanceDispatch(DistanceMode::StoredToQuery)(left.get(), query.get(), dim),
        asymmetric);
    const auto self_query = preprocessors->preprocessQuery(a.data(), input_size);
    checkBound(Metric, calculator->calcDistanceForQuery(left.get(), self_query.get(), dim),
               reference<Metric>(a, a), delta_a, 0, a, a);
}

TEST(SQ8FP64Distances, MetricsReconstructionAndAnalyticalBounds) {
    for (size_t dim : {1, 3, 4, 5, 17}) {
        checkDistances<VecSimMetric_L2>(dim, false);
        checkDistances<VecSimMetric_IP>(dim, false);
        checkDistances<VecSimMetric_Cosine>(dim, false);
        checkDistances<VecSimMetric_Cosine>(dim, true);
    }
}

TEST(SQ8FP64Distances, OriginalDoubleTopTwoRanking) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto components = CreateSQ8FP64IndexComponents<VecSimMetric_L2>(allocator, 3);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    const std::vector<double> query(3, 1e8 + 3);
    const auto processed = preprocessors->preprocessQuery(query.data(), 3 * sizeof(double));
    const std::array<double, 4> offsets = {0, 2, 5, 9};
    const std::array<double, 4> expected = {27, 3, 12, 108};
    std::array<size_t, 4> original_order = {0, 1, 2, 3};
    auto quantized_order = original_order;
    std::array<double, 4> scores;
    for (size_t i = 0; i < offsets.size(); ++i) {
        const std::vector<double> input(3, 1e8 + offsets[i]);
        const auto stored = preprocessors->preprocessForStorage(input.data(), 3 * sizeof(double));
        scores[i] = calculator->calcDistanceForQuery(stored.get(), processed.get(), 3);
        EXPECT_DOUBLE_EQ(reference<VecSimMetric_L2>(input, query), expected[i]);
        EXPECT_DOUBLE_EQ(scores[i], expected[i]);
    }
    std::sort(original_order.begin(), original_order.end(),
              [&](size_t a, size_t b) { return expected[a] < expected[b]; });
    std::sort(quantized_order.begin(), quantized_order.end(),
              [&](size_t a, size_t b) { return scores[a] < scores[b]; });
    EXPECT_EQ(original_order, quantized_order);
    EXPECT_EQ(quantized_order[0], 1);
    EXPECT_EQ(quantized_order[1], 2);
}

TEST(SQ8FP64Distances, DispatchAlignmentUnalignedBlobsAndInvalidMetric) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8FP64Preprocessor<VecSimMetric_L2> preprocessor(allocator, 3);
    const double input[] = {1e8, 1e8 + 1, 1e8 + 2};
    void *storage = nullptr;
    size_t size = sizeof(input);
    preprocessor.preprocessForStorage(input, storage, size, 0);
    std::vector<uint8_t> unaligned(size + 1);
    std::memcpy(unaligned.data() + 1, storage, size);
    unsigned char alignment = 99;
    const auto symmetric = spaces::GetDistFunc<sq8_fp64, double>(VecSimMetric_L2, 3, &alignment);
    EXPECT_EQ(alignment, 0);
    EXPECT_DOUBLE_EQ(symmetric(unaligned.data() + 1, storage, 3), 0.0);
    const auto asymmetric =
        spaces::GetDistFunc<sq8_fp64, double, double>(VecSimMetric_L2, 3, nullptr);
    std::vector<uint8_t> unaligned_query(sizeof(input) + 1);
    std::memcpy(unaligned_query.data() + 1, input, sizeof(input));
    EXPECT_NEAR(asymmetric(unaligned.data() + 1, unaligned_query.data() + 1, 3),
                std::pow(1.0 / 255, 2), 1e-15);
    EXPECT_THROW((spaces::GetDistFunc<sq8_fp64, double>(VecSimMetric(99), 3, nullptr)),
                 std::invalid_argument);
    EXPECT_THROW((spaces::GetDistFunc<sq8_fp64, double, double>(VecSimMetric(99), 3, nullptr)),
                 std::invalid_argument);
    allocator->free_allocation(storage);
}
} // namespace
