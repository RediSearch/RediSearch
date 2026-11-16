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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include "gtest/gtest.h"
#include "vecsim/index_factories/components/components_factory.h"
#include "vecsim/spaces/sq8_fp64.h"

namespace {
using vecsim_types::sq8_fp64;

static_assert(QuantPreprocessorInput<float, vecsim_types::sq8, false>);
static_assert(QuantPreprocessorInput<float, vecsim_types::sq8, true>);
static_assert(QuantPreprocessorInput<vecsim_types::float16, vecsim_types::sq8, false>);
static_assert(QuantPreprocessorInput<vecsim_types::float16, vecsim_types::sq8, true>);
static_assert(QuantPreprocessorInput<float, vecsim_types::sq4, false>);
static_assert(QuantPreprocessorInput<float, vecsim_types::sq4, true>);
static_assert(QuantPreprocessorInput<vecsim_types::float16, vecsim_types::sq4, false>);
static_assert(QuantPreprocessorInput<vecsim_types::float16, vecsim_types::sq4, true>);
static_assert(QuantPreprocessorInput<double, sq8_fp64, false>);

static_assert(!QuantPreprocessorInput<double, vecsim_types::sq8, false>);
static_assert(!QuantPreprocessorInput<double, vecsim_types::sq8, true>);
static_assert(!QuantPreprocessorInput<double, vecsim_types::sq4, false>);
static_assert(!QuantPreprocessorInput<double, vecsim_types::sq4, true>);
static_assert(!QuantPreprocessorInput<float, sq8_fp64, false>);
static_assert(!QuantPreprocessorInput<float, sq8_fp64, true>);
static_assert(!QuantPreprocessorInput<vecsim_types::float16, sq8_fp64, false>);
static_assert(!QuantPreprocessorInput<vecsim_types::float16, sq8_fp64, true>);
static_assert(!QuantPreprocessorInput<double, sq8_fp64, true>);

static_assert(!QuantPreprocessorInput<int, vecsim_types::sq8, false>);
static_assert(!QuantPreprocessorInput<int, vecsim_types::sq4, false>);
static_assert(!QuantPreprocessorInput<int, sq8_fp64, false>);

double readMetadata(const void *blob, size_t offset, size_t index) {
    double result;
    std::memcpy(&result, static_cast<const uint8_t *>(blob) + offset + index * sizeof(double),
                sizeof(result));
    return result;
}

template <VecSimMetric Metric>
void checkLayout(size_t dim, double constant) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8FP64Preprocessor<Metric> preprocessor(allocator, dim);
    const std::vector<double> input(dim, constant);
    const auto original = input;
    void *storage = nullptr;
    void *query = nullptr;
    size_t storage_size = input.size() * sizeof(double);
    size_t query_size = storage_size;
    preprocessor.preprocess(input.data(), storage, query, storage_size, query_size, 32, 64);
    EXPECT_EQ(storage_size, sq8_fp64::storage_bytes_count<Metric>(dim));
    EXPECT_EQ(query_size, sq8_fp64::query_bytes_count<Metric>(dim));
    EXPECT_EQ(reinterpret_cast<uintptr_t>(storage) % 32, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(query) % 64, 0);
    EXPECT_EQ(std::memcmp(input.data(), query, dim * sizeof(double)), 0);
    EXPECT_EQ(input, original);
    EXPECT_DOUBLE_EQ(readMetadata(storage, dim, sq8_fp64::MIN_VAL), constant);
    EXPECT_DOUBLE_EQ(readMetadata(storage, dim, sq8_fp64::DELTA), 1.0);
    EXPECT_EQ(storage_size, dim + 2 * sizeof(double));
    EXPECT_EQ(query_size, dim * sizeof(double));
    EXPECT_NE(query, input.data());
    EXPECT_NE(query, storage);
    EXPECT_EQ(sq8_fp64::storage_metadata_count<Metric>(), 2);
    EXPECT_EQ(sq8_fp64::query_metadata_count<Metric>(), 0);
    for (size_t i = 0; i < dim; ++i) {
        EXPECT_EQ(static_cast<const uint8_t *>(storage)[i], 0);
    }
    std::vector<double> inplace((std::max(storage_size, dim * sizeof(double)) + 7) / 8);
    std::memcpy(inplace.data(), input.data(), dim * sizeof(double));
    preprocessor.preprocessStorageInPlace(inplace.data(), inplace.size() * sizeof(double));
    EXPECT_EQ(std::memcmp(inplace.data(), storage, storage_size), 0);
    allocator->free_allocation(storage);
    allocator->free_allocation(query);
}

TEST(SQ8FP64Components, LayoutAlignmentConstantsAndInPlace) {
    for (size_t dim : {1, 3, 4, 5, 17}) {
        checkLayout<VecSimMetric_L2>(dim, 0.0);
        checkLayout<VecSimMetric_L2>(dim, 1.25);
        checkLayout<VecSimMetric_IP>(dim, -2.0);
        checkLayout<VecSimMetric_Cosine>(dim, 1.0 / std::sqrt(double(dim)));
    }
}

TEST(SQ8FP64Components, DoublePrecisionRangeAndMetadata) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8FP64Preprocessor<VecSimMetric_L2> preprocessor(allocator, 3);
    for (double scale : {1.0, 1e100, 1e-300}) {
        const double input[] = {scale, 1.25 * scale, 2.0 * scale};
        void *storage = nullptr;
        size_t size = sizeof(input);
        preprocessor.preprocessForStorage(input, storage, size, 0);
        std::vector<double> inplace((std::max(size, sizeof(input)) + sizeof(double) - 1) /
                                    sizeof(double));
        std::memcpy(inplace.data(), input, sizeof(input));
        preprocessor.preprocessStorageInPlace(inplace.data(), inplace.size() * sizeof(double));
        EXPECT_EQ(std::memcmp(inplace.data(), storage, size), 0);
        const auto *const codes = static_cast<const uint8_t *>(storage);
        EXPECT_EQ(codes[0], 0);
        EXPECT_EQ(codes[1], 64);
        EXPECT_EQ(codes[2], 255);
        const double delta = readMetadata(storage, 3, sq8_fp64::DELTA);
        EXPECT_GT(delta, 0.0);
        EXPECT_TRUE(std::isfinite(delta));
        EXPECT_DOUBLE_EQ(readMetadata(storage, 3, sq8_fp64::MIN_VAL), scale);
        EXPECT_DOUBLE_EQ(delta, scale / 255);
        allocator->free_allocation(storage);
    }
}

TEST(SQ8FP64Components, DefinedConversionOutsideRepresentableDeltaDomain) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8FP64Preprocessor<VecSimMetric_IP> preprocessor(allocator, 2);
    const double tiny = std::numeric_limits<double>::denorm_min();
    for (const auto &input : {std::vector<double>{-1e308, 1e308}, std::vector<double>{0.0, tiny}}) {
        void *storage = nullptr;
        size_t size = input.size() * sizeof(double);
        preprocessor.preprocessForStorage(input.data(), storage, size, 0);
        const auto *const codes = static_cast<const uint8_t *>(storage);
        EXPECT_EQ(codes[0], 0);
        EXPECT_EQ(codes[1], input[0] == 0.0 ? 255 : 0);
        allocator->free_allocation(storage);
    }
}

template <VecSimMetric Metric>
void checkZeroDistance(double expected) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto components = CreateSQ8FP64IndexComponents<Metric>(allocator, 3);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    const double input[] = {0.0, 0.0, 0.0};
    const auto blobs = preprocessors->preprocess(input, sizeof(input));
    EXPECT_DOUBLE_EQ(calculator->calcDistance(blobs.getStorageBlob(), blobs.getStorageBlob(), 3),
                     expected);
    EXPECT_DOUBLE_EQ(
        calculator->calcDistanceForQuery(blobs.getStorageBlob(), blobs.getQueryBlob(), 3),
        expected);
}

TEST(SQ8FP64Components, ZeroDistances) {
    checkZeroDistance<VecSimMetric_L2>(0.0);
    checkZeroDistance<VecSimMetric_IP>(1.0);
}

TEST(SQ8FP64Components, NearbyLargeValuesKeepDoublePrecision) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto components = CreateSQ8FP64IndexComponents<VecSimMetric_L2>(allocator, 2);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    const double input[] = {1e8, 1e8 + 1.0};
    const double query[] = {1e8, 1e8};
    const auto stored = preprocessors->preprocessForStorage(input, sizeof(input));
    const auto processed = preprocessors->preprocessQuery(query, sizeof(query));
    EXPECT_EQ(static_cast<const uint8_t *>(stored.get())[0], 0);
    EXPECT_EQ(static_cast<const uint8_t *>(stored.get())[1], 255);
    EXPECT_NEAR(calculator->calcDistanceForQuery(stored.get(), processed.get(), 2), 1.0, 1e-14);
    const double close[] = {1.0, 1.0 + std::ldexp(1.0, -40)};
    const auto precise = preprocessors->preprocessForStorage(close, sizeof(close));
    EXPECT_DOUBLE_EQ(readMetadata(precise.get(), 2, sq8_fp64::DELTA), std::ldexp(1.0, -40) / 255);
    EXPECT_NEAR(calculator->calcDistance(precise.get(), precise.get(), 2), 0.0, 1e-40);
}
} // namespace

TEST(SQ8FP64Components, AsymmetricL2UnalignedQuery) {
    constexpr size_t dim = 3;
    std::vector<uint8_t> stored(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    stored[0] = 2;
    stored[1] = 5;
    stored[2] = 9;
    const double minimum = 100000000.0;
    const double delta = 0.5;
    std::memcpy(stored.data() + dim, &minimum, sizeof(minimum));
    std::memcpy(stored.data() + dim + sizeof(double), &delta, sizeof(delta));
    const double values[] = {100000000.5, 100000003.5, 100000002.0};
    alignas(double) uint8_t query[sizeof(values) + 1];
    std::memcpy(query + 1, values, sizeof(values));
    EXPECT_DOUBLE_EQ(spaces::L2_SQ8FP64_FP64(stored.data(), query + 1, dim), 7.5);
}

TEST(SQ8FP64Components, AsymmetricL2PreservesSmallResiduals) {
    constexpr size_t dim = 3;
    std::vector<uint8_t> stored(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    stored[0] = 1;
    stored[1] = 2;
    stored[2] = 3;
    const double minimum = 1e15;
    const double delta = 0x1p-10;
    std::memcpy(stored.data() + dim, &minimum, sizeof(minimum));
    std::memcpy(stored.data() + dim + sizeof(double), &delta, sizeof(delta));
    const double query[] = {minimum + 0.125, minimum + 0.25, minimum + 0.375};
    EXPECT_EQ(spaces::L2_SQ8FP64_FP64(stored.data(), query, dim), 112903.0 / 524288.0);
}

TEST(SQ8FP64Components, SymmetricL2PreservesDistinctSmallResiduals) {
    constexpr size_t dim = 3;
    std::vector<uint8_t> left(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    std::vector<uint8_t> right(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    const uint8_t left_codes[] = {1, 2, 3};
    const uint8_t right_codes[] = {3, 2, 1};
    const double left_metadata[] = {1e15, 0x1p-10};
    const double right_metadata[] = {1e15, 0x1p-9};
    std::memcpy(left.data(), left_codes, sizeof(left_codes));
    std::memcpy(right.data(), right_codes, sizeof(right_codes));
    std::memcpy(left.data() + dim, left_metadata, sizeof(left_metadata));
    std::memcpy(right.data() + dim, right_metadata, sizeof(right_metadata));
    EXPECT_EQ(spaces::L2_SQ8FP64(left.data(), right.data(), dim), 30.0 / 1048576.0);
    EXPECT_EQ(spaces::L2_SQ8FP64(right.data(), left.data(), dim), 30.0 / 1048576.0);
}

TEST(SQ8FP64Components, SymmetricL2PreservesTinyRangeAgainstConstant) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto components = CreateSQ8FP64IndexComponents<VecSimMetric_L2>(allocator, 2);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    for (double origin : {0.0, 1.0}) {
        const double variable[] = {origin, origin + 0x1p-47};
        const double constant[] = {origin, origin};
        const auto left = preprocessors->preprocessForStorage(variable, sizeof(variable));
        const auto right = preprocessors->preprocessForStorage(constant, sizeof(constant));
        EXPECT_EQ(calculator->calcDistance(left.get(), right.get(), 2), 0x1p-94);
        EXPECT_EQ(calculator->calcDistance(right.get(), left.get(), 2), 0x1p-94);
        EXPECT_EQ(calculator->calcDistance(left.get(), left.get(), 2), 0.0);
        EXPECT_EQ(calculator->calcDistance(right.get(), right.get(), 2), 0.0);
    }
}

TEST(SQ8FP64Components, SymmetricL2PreservesCloseLargeDeltas) {
    constexpr size_t dim = 3;
    std::vector<uint8_t> left(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    std::vector<uint8_t> right(sq8_fp64::storage_bytes_count<VecSimMetric_L2>(dim));
    const uint8_t codes[] = {0, 3, 255};
    const double left_metadata[] = {0.0, 1e15 + 0.125};
    const double right_metadata[] = {0.0, 1e15 + 0.25};
    std::memcpy(left.data(), codes, sizeof(codes));
    std::memcpy(right.data(), codes, sizeof(codes));
    std::memcpy(left.data() + dim, left_metadata, sizeof(left_metadata));
    std::memcpy(right.data() + dim, right_metadata, sizeof(right_metadata));
    EXPECT_EQ(spaces::L2_SQ8FP64(left.data(), right.data(), dim), 32517.0 / 32.0);
    EXPECT_EQ(spaces::L2_SQ8FP64(right.data(), left.data(), dim), 32517.0 / 32.0);
}

TEST(SQ8FP64Components, TinyDeltaUsesDirectDivision) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8FP64Preprocessor<VecSimMetric_L2> preprocessor(allocator, 3);
    const double input[] = {0.0, 6.4e-309, 2.55e-308};
    const auto original = std::to_array(input);
    void *storage = nullptr;
    size_t size = sizeof(input);
    preprocessor.preprocessForStorage(input, storage, size, 0);
    const auto *const codes = static_cast<const uint8_t *>(storage);
    EXPECT_EQ(size, 3 + 2 * sizeof(double));
    EXPECT_EQ(codes[0], 0);
    EXPECT_EQ(codes[1], 64);
    EXPECT_EQ(codes[2], 255);
    EXPECT_EQ(std::memcmp(input, original.data(), sizeof(input)), 0);
    allocator->free_allocation(storage);
}

TEST(SQ8FP64Components, SharedFactoryRejectsMeanBeforeAllocation) {
    const auto allocator = VecSimAllocator::newVecsimAllocator();
    const float mean[] = {0.0f, 0.0f, 0.0f};
    const size_t before = allocator->getAllocationSize();
    EXPECT_THROW(
        (CreateQuantizedIndexComponents<double, VecSimMetric_L2, sq8_fp64>(allocator, 3, mean)),
        std::invalid_argument);
    EXPECT_EQ(allocator->getAllocationSize(), before);
    EXPECT_THROW((CreateQuantizedIndexComponents<double, VecSimMetric_L2, false, sq8_fp64>(
                     allocator, 3, mean)),
                 std::invalid_argument);
    EXPECT_EQ(allocator->getAllocationSize(), before);
    const auto components =
        CreateQuantizedIndexComponents<double, VecSimMetric_L2, sq8_fp64>(allocator, 3, nullptr);
    const std::unique_ptr<IndexCalculatorInterface<double>> calculator(components.indexCalculator);
    const std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    const double input[] = {1.0, 2.0, 3.0};
    const auto blobs = preprocessors->preprocess(input, sizeof(input));
    EXPECT_EQ(calculator->calcDistance(blobs.getStorageBlob(), blobs.getStorageBlob(), 3), 0.0);
}
