/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "VecSim/index_factories/components/components_factory.h"
#include "VecSim/spaces/L2_space.h"
#include "VecSim/types/sq4.h"

namespace {

using vecsim_types::sq4;

float metadata(const void *blob, size_t body_bytes, size_t index) {
    return load_unaligned<float>(static_cast<const uint8_t *>(blob) + body_bytes +
                                 index * sizeof(float));
}

std::vector<double> reconstruct(const void *blob, size_t dim, const std::vector<float> &mean) {
    const auto *codes = static_cast<const uint8_t *>(blob);
    const double min = metadata(blob, sq4::data_bytes_count(dim), sq4::MIN_VAL);
    const double delta = metadata(blob, sq4::data_bytes_count(dim), sq4::DELTA);
    std::vector<double> result(dim);
    for (size_t i = 0; i < dim; ++i) {
        // Decode directly so the oracle also checks the production nibble order.
        const unsigned code = (codes[i / 2] >> (4 * (i % 2))) & 15;
        result[i] = min + delta * code + (mean.empty() ? 0.0 : mean[i]);
    }
    return result;
}

template <VecSimMetric Metric, typename Left, typename Right>
double referenceDistance(const Left &left, const Right &right) {
    double result = Metric == VecSimMetric_IP ? 1.0 : 0.0;
    for (size_t i = 0; i < left.size(); ++i) {
        if constexpr (Metric == VecSimMetric_IP) {
            result -= double(left[i]) * double(right[i]);
        } else {
            const double difference = double(left[i]) - double(right[i]);
            result += difference * difference;
        }
    }
    return result;
}

template <VecSimMetric Metric, bool WithMean>
void checkComponents(size_t dim, bool constant) {
    SCOPED_TRACE(::testing::Message() << "metric=" << Metric << " mean=" << WithMean
                                      << " dim=" << dim << " constant=" << constant);
    auto allocator = VecSimAllocator::newVecsimAllocator();
    std::vector<float> x(dim), y(dim), mean(WithMean ? dim : 0);
    for (size_t i = 0; i < dim; ++i) {
        if constexpr (WithMean) {
            mean[i] = 0.7f * (int(i % 5) - 2);
        }
        const float offset = WithMean ? mean[i] : 0.0f;
        x[i] = offset + (constant ? 1.25f : 0.31f * (int((i * 7) % 19) - 9));
        y[i] = offset + (constant ? -0.75f : 0.23f * (int((i * 11) % 17) - 8));
    }
    auto components = CreateQuantizedIndexComponents<float, Metric, vecsim_types::sq4>(
        allocator, dim, WithMean ? mean.data() : nullptr);
    std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
    std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    const size_t input_size = dim * sizeof(float);
    const size_t storage_size = sq4::storage_bytes_count<Metric, WithMean>(dim);
    auto *container = dynamic_cast<MultiPreprocessorsContainer<float, 1> *>(preprocessors.get());
    ASSERT_NE(container, nullptr);
    auto *quantizer = dynamic_cast<QuantPreprocessor<float, Metric, WithMean, sq4> *>(
        container->getPreprocessors()[0]);
    ASSERT_NE(quantizer, nullptr);
    auto x_blob = preprocessors->preprocessForStorage(x.data(), input_size);
    auto y_blob = preprocessors->preprocessForStorage(y.data(), input_size);
    auto query = preprocessors->preprocessQuery(y.data(), input_size);
    auto combined = preprocessors->preprocess(y.data(), input_size);
    const size_t query_size =
        input_size + sq4::query_metadata_count<Metric, WithMean>() * sizeof(float);
    EXPECT_EQ(std::memcmp(combined.getStorageBlob(), y_blob.get(), storage_size), 0);
    EXPECT_EQ(std::memcmp(combined.getQueryBlob(), query.get(), query_size), 0);
    if constexpr (Metric == VecSimMetric_L2) {
        // Direct residual kernels need no query sums, but SIMD odd tails may read padding.
        EXPECT_FLOAT_EQ(metadata(query.get(), input_size, sq4::SUM_QUERY), 0.0f);
        EXPECT_FLOAT_EQ(metadata(query.get(), input_size, sq4::SUM_SQUARES_QUERY), 0.0f);
        const auto *query_values = static_cast<const float *>(query.get());
        for (size_t i = 0; i < dim; ++i) {
            const float expected = WithMean ? y[i] - mean[i] : y[i];
            EXPECT_FLOAT_EQ(query_values[i], expected);
        }
    }
    const auto rx = reconstruct(x_blob.get(), dim, mean);
    const auto ry = reconstruct(y_blob.get(), dim, mean);
    double symmetric = referenceDistance<Metric>(rx, ry);
    if constexpr (WithMean && Metric == VecSimMetric_IP) {
        // The kernel uses reconstructed centered values, but the mean correction uses the
        // original inputs to avoid multiplying quantization error by the mean.
        symmetric = 1.0;
        for (size_t i = 0; i < dim; ++i) {
            symmetric -= (rx[i] - mean[i]) * (ry[i] - mean[i]) + double(x[i]) * mean[i] +
                         double(y[i]) * mean[i] - double(mean[i]) * mean[i];
        }
    }
    const double asymmetric = referenceDistance<Metric>(rx, y);
    const double sym_tolerance = 2e-5 * (1.0 + std::abs(symmetric));
    const double asym_tolerance = 2e-5 * (1.0 + std::abs(asymmetric));
    EXPECT_NEAR(calculator->calcDistance(x_blob.get(), y_blob.get(), dim), symmetric,
                sym_tolerance);
    EXPECT_NEAR(calculator->calcDistanceForQuery(x_blob.get(), query.get(), dim), asymmetric,
                asym_tolerance);
    auto stored_dispatch = calculator->getDistanceDispatch(DistanceMode::StoredToStored);
    auto query_dispatch = calculator->getDistanceDispatch(DistanceMode::StoredToQuery);
    ASSERT_TRUE(stored_dispatch.isValid());
    ASSERT_TRUE(query_dispatch.isValid());
    EXPECT_NEAR(stored_dispatch(x_blob.get(), y_blob.get(), dim), symmetric, sym_tolerance);
    EXPECT_NEAR(query_dispatch(x_blob.get(), query.get(), dim), asymmetric, asym_tolerance);

    if (dim % 2) {
        EXPECT_EQ(static_cast<const uint8_t *>(x_blob.get())[dim / 2] & 0xf0, 0);
        EXPECT_EQ(static_cast<const uint8_t *>(y_blob.get())[dim / 2] & 0xf0, 0);
    }
    // The in-place allocation must hold both the original input and the processed blob;
    // metadata makes the processed blob larger for tiny dimensions.
    const size_t buffer_size = std::max(input_size, storage_size);
    std::vector<float> in_place((buffer_size + sizeof(float) - 1) / sizeof(float) + 4);
    std::memset(in_place.data(), 0xa5, in_place.size() * sizeof(float));
    std::memcpy(in_place.data(), x.data(), input_size);
    preprocessors->preprocessStorageInPlace(in_place.data(), buffer_size);
    EXPECT_EQ(std::memcmp(in_place.data(), x_blob.get(), storage_size), 0);
    const auto *bytes = reinterpret_cast<const uint8_t *>(in_place.data());
    for (size_t i = buffer_size; i < in_place.size() * sizeof(float); ++i) {
        EXPECT_EQ(bytes[i], 0xa5);
    }

    if constexpr (WithMean && Metric == VecSimMetric_IP) {
        float correction = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            correction += x[i] * mean[i];
        }
        EXPECT_NEAR(
            metadata(x_blob.get(), sq4::data_bytes_count(dim), sq4::mean_ip_index<Metric>()),
            correction, 2e-6 * (1.0 + std::abs(correction)));
    }
}

template <VecSimMetric Metric>
void checkDimensions() {
    for (size_t dim : {1u, 2u, 3u, 4u, 5u, 7u, 16u, 37u, 128u}) {
        for (bool constant : {false, true}) {
            checkComponents<Metric, false>(dim, constant);
            checkComponents<Metric, true>(dim, constant);
        }
    }
}

} // namespace

TEST(SQ4Components, L2MatchesReconstruction) { checkDimensions<VecSimMetric_L2>(); }

TEST(SQ4Components, IPMatchesQuantizedKernelAndOriginalMeanCorrection) {
    checkDimensions<VecSimMetric_IP>();
}

TEST(SQ4Components, CenteredIPCorrectionAvoidsMeanAmplifiedQuantizationError) {
    constexpr size_t dim = 3;
    const std::vector<float> mean(dim, 10.0f);
    const std::vector<float> x = {10.0f, 13.0f, 12.1f};
    const std::vector<float> y = {10.0f, 13.0f, 11.0f};
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto components =
        CreateQuantizedIndexComponents<float, VecSimMetric_IP, sq4>(allocator, dim, mean.data());
    std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
    std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    auto xb = preprocessors->preprocessForStorage(x.data(), dim * sizeof(float));
    auto yb = preprocessors->preprocessForStorage(y.data(), dim * sizeof(float));
    const auto rx = reconstruct(xb.get(), dim, mean);
    const auto ry = reconstruct(yb.get(), dim, mean);
    const double truth = referenceDistance<VecSimMetric_IP>(x, y);
    const double decoded = referenceDistance<VecSimMetric_IP>(rx, ry);
    const float actual = calculator->calcDistance(xb.get(), yb.get(), dim);
    EXPECT_FLOAT_EQ(
        metadata(xb.get(), sq4::data_bytes_count(dim), sq4::mean_ip_index<VecSimMetric_IP>()),
        351.0f);
    EXPECT_LT(std::abs(actual - truth), 0.11);
    EXPECT_GT(std::abs(decoded - truth), 1.0);
}

TEST(SQ4Components, PackedCodesAndMetadataSizes) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    const float input[] = {0, 4, 8, 11, 15};
    QuantPreprocessor<float, VecSimMetric_L2, false, sq4> preprocessor(allocator, 5);
    void *storage = nullptr;
    size_t size = sizeof(input);
    preprocessor.preprocessForStorage(input, storage, size, 0);
    ASSERT_EQ(size, 3 + 4 * sizeof(float));
    const auto *bytes = static_cast<const uint8_t *>(storage);
    EXPECT_EQ(bytes[0], 0x40);
    EXPECT_EQ(bytes[1], 0xb8);
    EXPECT_EQ(bytes[2], 0x0f);
    EXPECT_FLOAT_EQ(metadata(storage, 3, sq4::MIN_VAL), 0);
    EXPECT_FLOAT_EQ(metadata(storage, 3, sq4::DELTA), 1.0f);
    allocator->free_allocation(storage);
}

TEST(SQ4Components, OddDimTailNibbleIsZero) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (size_t dim : {1u, 15u, 17u, 767u}) {
        SCOPED_TRACE(::testing::Message() << "dim=" << dim);
        std::vector<float> input(dim);
        for (size_t i = 0; i < dim; ++i) {
            input[i] = 0.37f * (int((i * 13) % 23) - 11);
        }
        // The last coordinate holds the maximum, so its code is 15 (for dim > 1) and a misplaced
        // nibble shows.
        input[dim - 1] = 100.0f;
        auto check = [&](auto metric_tag) {
            constexpr VecSimMetric Metric = decltype(metric_tag)::value;
            auto components = CreateQuantizedIndexComponents<float, Metric, vecsim_types::sq4>(
                allocator, dim, nullptr);
            std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
            std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
            auto blob = preprocessors->preprocessForStorage(input.data(), dim * sizeof(float));
            const auto *bytes = static_cast<const uint8_t *>(blob.get());
            const size_t metadata_count = Metric == VecSimMetric_L2 ? 4 : 3;
            EXPECT_EQ((sq4::storage_bytes_count<Metric>(dim)),
                      dim / 2 + dim % 2 + metadata_count * sizeof(float));
            // A single coordinate is a constant range, which encodes as code 0.
            EXPECT_EQ(sq4::get_code(bytes, dim - 1), dim == 1 ? 0 : 15);
            if (dim % 2) {
                EXPECT_EQ(bytes[dim / 2] & 0xf0, 0);
            }
        };
        check(std::integral_constant<VecSimMetric, VecSimMetric_L2>{});
        check(std::integral_constant<VecSimMetric, VecSimMetric_IP>{});
    }
}

TEST(SQ4Components, NonFiniteInputEncodesInBounds) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 17;
    const float inf = std::numeric_limits<float>::infinity();
    std::vector<float> input(dim);
    for (size_t i = 0; i < dim; ++i) {
        input[i] = 0.5f * (int(i % 7) - 3);
    }
    input[2] = std::numeric_limits<float>::quiet_NaN();
    input[5] = inf;
    input[9] = -inf;
    input[13] = 1e30f;
    std::vector<float> mean(dim);
    for (size_t i = 0; i < dim; ++i) {
        mean[i] = 0.25f * (int(i % 3) - 1);
    }

    auto check = [&](auto metric_tag, auto with_mean_tag) {
        constexpr VecSimMetric Metric = decltype(metric_tag)::value;
        constexpr bool WithMean = decltype(with_mean_tag)::value;
        SCOPED_TRACE(::testing::Message() << "metric=" << Metric << " mean=" << WithMean);
        auto components = CreateQuantizedIndexComponents<float, Metric, vecsim_types::sq4>(
            allocator, dim, WithMean ? mean.data() : nullptr);
        std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
        std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
        const size_t storage_size = sq4::storage_bytes_count<Metric, WithMean>(dim);

        auto blob = preprocessors->preprocessForStorage(input.data(), dim * sizeof(float));
        for (size_t i = 0; i < dim; ++i) {
            EXPECT_LE(sq4::get_code(static_cast<const uint8_t *>(blob.get()), i), 15);
        }

        // In place, with a sentinel tail: nothing past the stored blob may be written. The input
        // is larger than the stored blob here, so the bytes between them keep the input.
        const size_t input_size = dim * sizeof(float);
        ASSERT_GT(input_size, storage_size);
        std::vector<uint8_t> buffer(input_size + 16, 0xa5);
        std::memcpy(buffer.data(), input.data(), input_size);
        preprocessors->preprocessStorageInPlace(buffer.data(), input_size);
        EXPECT_EQ(std::memcmp(buffer.data(), blob.get(), storage_size), 0);
        EXPECT_EQ(std::memcmp(buffer.data() + storage_size,
                              reinterpret_cast<const uint8_t *>(input.data()) + storage_size,
                              input_size - storage_size),
                  0);
        for (size_t i = input_size; i < buffer.size(); ++i) {
            EXPECT_EQ(buffer[i], 0xa5);
        }
        // Odd dim: the unused high nibble stays zero even for non-finite input.
        EXPECT_EQ(buffer[dim / 2] & 0xf0, 0);
    };
    check(std::integral_constant<VecSimMetric, VecSimMetric_L2>{}, std::false_type{});
    check(std::integral_constant<VecSimMetric, VecSimMetric_IP>{}, std::false_type{});
    check(std::integral_constant<VecSimMetric, VecSimMetric_L2>{}, std::true_type{});
    check(std::integral_constant<VecSimMetric, VecSimMetric_IP>{}, std::true_type{});
}

TEST(SQ4Components, CenteredIPBlobSize768) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 768;
    static_assert(sq4::storage_bytes_count<VecSimMetric_IP, true>(dim) == 400);
    std::vector<float> input(dim), mean(dim);
    for (size_t i = 0; i < dim; ++i) {
        input[i] = 0.01f * (int(i % 41) - 20);
        mean[i] = 0.002f * (int(i % 9) - 4);
    }
    auto components = CreateQuantizedIndexComponents<float, VecSimMetric_IP, vecsim_types::sq4>(
        allocator, dim, mean.data());
    std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
    std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    auto *container = dynamic_cast<MultiPreprocessorsContainer<float, 1> *>(preprocessors.get());
    ASSERT_NE(container, nullptr);
    auto *quantizer = container->getPreprocessors()[0];
    ASSERT_NE((dynamic_cast<QuantPreprocessor<float, VecSimMetric_IP, true, sq4> *>(quantizer)),
              nullptr);
    void *storage = nullptr;
    size_t size = dim * sizeof(float);
    quantizer->preprocessForStorage(input.data(), storage, size, 0);
    EXPECT_EQ(size, 400u);
    allocator->free_allocation(storage);
}

// SQ4 components are distinct types from the SQ8 ones that applySQ8Mean and
// HNSWIndex::setQuantizationMean dynamic_cast to, so the SQ8 mean path cannot apply to SQ4.
TEST(SQ4Components, SQ4ComponentsAreNotSQ8Types) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    std::vector<float> mean(dim, 0.5f);
    auto components = CreateQuantizedIndexComponents<float, VecSimMetric_IP, vecsim_types::sq4>(
        allocator, dim, mean.data());
    std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
    std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
    auto *container = dynamic_cast<MultiPreprocessorsContainer<float, 1> *>(preprocessors.get());
    ASSERT_NE(container, nullptr);
    EXPECT_EQ((dynamic_cast<QuantPreprocessor<float, VecSimMetric_IP, true> *>(
                  container->getPreprocessors()[0])),
              nullptr);
    EXPECT_EQ((dynamic_cast<DistanceCalculatorWithNorm<float, float, VecSimMetric_IP> *>(
                  calculator.get())),
              nullptr);
    EXPECT_NE((dynamic_cast<DistanceCalculatorWithNorm<float, float, VecSimMetric_IP, sq4> *>(
                  calculator.get())),
              nullptr);
}

// Centered and plain SQ4 L2 indexes compare stored vectors with the same kernel, as SQ8 does.
TEST(SQ4Components, StoredL2UsesOneKernel) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 64;
    std::vector<float> zero_mean(dim, 0.0f);
    for (bool centered : {true, false}) {
        SCOPED_TRACE(centered);
        auto components = CreateQuantizedIndexComponents<float, VecSimMetric_L2, vecsim_types::sq4>(
            allocator, dim, centered ? zero_mean.data() : nullptr);
        std::unique_ptr<IndexCalculatorInterface<float>> calculator(components.indexCalculator);
        std::unique_ptr<PreprocessorsContainerAbstract> preprocessors(components.preprocessors);
        EXPECT_EQ(calculator->getDistanceDispatch(DistanceMode::StoredToStored).stateless_func,
                  spaces::L2_SQ4_SQ4_GetDistFunc(dim));
    }
}
