/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"
#include "vecsim/algorithms/hnsw/hnsw_single.h"
#include "vecsim/algorithms/hnsw/hnsw_tiered.h"
#include "vecsim/index_factories/hnsw_factory.h"
#include "vecsim/index_factories/tiered_factory.h"
#include "vecsim/quantization/turboquant_components.h"
#include "vecsim/spaces/functions/AVX2_FMA.h"
#include "vecsim/spaces/functions/AVX512F.h"
#include "vecsim/vec_sim.h"
#include "mock_thread_pool.h"
#include "unit_test_utils.h"
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace vecsim::quantization;

// Friend of HNSWIndex and TieredHNSWIndex, for typed inspection in the tests below.
class TurboQuantTest : public ::testing::TestWithParam<unsigned> {
protected:
    static const TurboQuant &codecOf(const HNSWIndex<float, float> *index) {
        const auto *codec = GetTurboQuantCodec(index->getIndexCalculator());
        if (!codec)
            throw std::runtime_error("Expected a TurboQuant index");
        return *codec;
    }
    static HNSWIndex<float, float> *backendOf(VecSimIndex *index) {
        return dynamic_cast<TieredHNSWIndex<float, float> *>(index)->getHNSWIndex();
    }
};

namespace {
using IndexOwner = std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)>;

HNSWParams parameters(unsigned bits, size_t dim, const float *mean = nullptr) {
    return {.type = VecSimType_FLOAT32,
            .dim = dim,
            .metric = VecSimMetric_IP,
            .multi = false,
            .blockSize = 8,
            .M = 8,
            .efConstruction = 64,
            .efRuntime = 128,
            .quantType = bits == 2 ? VecSimQuant_TQ2 : VecSimQuant_TQ3,
            .quantParams = mean};
}
float unalignedFloat(const void *data, size_t offset) {
    float value;
    std::memcpy(&value, static_cast<const char *>(data) + offset, sizeof(value));
    return value;
}
std::vector<float> gaussianVector(size_t dim, uint32_t seed, float scale = 1.0f) {
    std::mt19937 rng(seed); // NOSONAR(S2245) test data
    std::normal_distribution<float> normal(0.0f, scale);
    std::vector<float> values(dim);
    for (float &value : values)
        value = normal(rng);
    return values;
}
std::vector<float> applyRotation(const TurboQuantRotation &rotation, const std::vector<float> &x) {
    std::vector<float> out(x.size()), scratch(x.size());
    rotation.apply(x.data(), out.data(), scratch.data());
    return out;
}
// An independent bit reader and decoded-score references, including the full mean error.
std::vector<double> decoded(const TurboQuant &quantizer, const void *blob) {
    const auto *bytes = static_cast<const unsigned char *>(blob);
    std::vector<double> values(quantizer.dim());
    const float scale = unalignedFloat(blob, (quantizer.dim() * quantizer.bits() + 7) / 8);
    for (size_t i = 0; i < values.size(); ++i) {
        unsigned code = 0;
        for (unsigned b = 0; b < quantizer.bits(); ++b) {
            const size_t bit = i * quantizer.bits() + b;
            code |= ((bytes[bit / 8] >> (bit % 8)) & 1) << b;
        }
        values[i] = double(scale) * quantizer.centroids()[code];
    }
    return values;
}
double queryReference(const TurboQuant &quantizer, const void *blob,
                      const std::vector<float> &query) {
    const auto values = decoded(quantizer, blob);
    const auto rotated = applyRotation(quantizer.rotation(), query);
    double score = 1 + unalignedFloat(blob, quantizer.storageBytes() - sizeof(float));
    for (size_t i = 0; i < quantizer.dim(); ++i)
        score -= values[i] * rotated[i] + double(quantizer.biasMean()[i]) * query[i];
    return score;
}
double graphReference(const TurboQuant &quantizer, const void *a, const void *b) {
    const auto x = decoded(quantizer, a), y = decoded(quantizer, b);
    double inner = 0, mean_squared = 0;
    for (size_t i = 0; i < quantizer.dim(); ++i) {
        inner += x[i] * y[i] + (x[i] + y[i]) * quantizer.biasRotatedMean()[i];
        mean_squared += double(quantizer.biasMean()[i]) * quantizer.biasMean()[i];
    }
    return 1 - (inner + mean_squared) +
           unalignedFloat(a, quantizer.storageBytes() - sizeof(float)) +
           unalignedFloat(b, quantizer.storageBytes() - sizeof(float));
}
// Independent double-precision reference of the documented transform: explicit Sylvester
// Hadamard entries (-1)^popcount(i & j) / sqrt(block), no butterflies.
std::vector<double> referenceRotation(const TurboQuantRotation &rotation,
                                      const std::vector<float> &x) {
    const size_t dim = rotation.dim(), block = rotation.block();
    std::vector<double> current(x.begin(), x.end()), next(dim);
    for (unsigned round = 0; round < TurboQuantRotation::kRounds; ++round) {
        if (round > 0) {
            const auto *permutation = rotation.permutations().data() + (round - 1) * dim;
            for (size_t i = 0; i < dim; ++i)
                next[i] = current[permutation[i]];
            current.swap(next);
        }
        const auto *signs = rotation.scaledSigns().data() + round * dim;
        for (size_t i = 0; i < dim; ++i)
            current[i] *= signs[i] < 0 ? -1.0 : 1.0;
        for (size_t start = 0; start < dim; start += block) {
            for (size_t i = 0; i < block; ++i) {
                double sum = 0;
                for (size_t j = 0; j < block; ++j)
                    sum += (std::popcount(uint64_t(i & j)) % 2 ? -1.0 : 1.0) * current[start + j];
                next[start + i] = sum / std::sqrt(double(block));
            }
        }
        current.swap(next);
    }
    return current;
}
std::shared_ptr<TurboQuant> newCodec(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                                     unsigned bits = 2, uint64_t seed = kTurboQuantSeed) {
    std::vector<float> mean(dim, 0.0f);
    return std::allocate_shared<TurboQuant>(VecsimSTLAllocator<TurboQuant>(allocator), allocator,
                                            dim, bits, seed, std::span<const float>(mean));
}
struct WriteModeGuard {
    VecSimWriteMode previous = VecSimIndexInterface::asyncWriteMode;
    ~WriteModeGuard() { VecSim_SetWriteMode(previous); }
};
struct TestPool : tieredIndexMock {
    TestPool() : tieredIndexMock(0) {}
    ~TestPool() {
        while (!jobQ.empty())
            thread_iteration();
        reset_ctx(); // The mock destructor otherwise dereferences an empty index context.
    }
};
VecSimIndex *newTiered(TestPool &pool, VecSimParams &primary, size_t training_size) {
    TieredIndexParams params{
        .jobQueue = &pool.jobQ,
        .jobQueueCtx = pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .flatBufferLimit = 1,
        .primaryIndexParams = &primary,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = training_size}}};
    auto *index = TieredFactory::NewIndex(&params);
    pool.ctx->index_strong_ref = std::shared_ptr<VecSimIndex>(index, VecSimIndex_Free);
    return index;
}
} // namespace

TEST_P(TurboQuantTest, EncodingAndScoresMatchIndependentReferences) {
    const unsigned bits = GetParam();
    for (size_t dim : {64, 768}) {
        auto allocator = VecSimAllocator::newVecsimAllocator();
        const auto mean = gaussianVector(dim, 11, 0.02f);
        auto parts = CreateTurboQuantIndexComponents(allocator, dim, bits, mean.data());
        std::unique_ptr<PreprocessorsContainerAbstract> pp(parts.preprocessors);
        std::unique_ptr<IndexCalculatorInterface<float>> calculator(parts.indexCalculator);
        const TurboQuant &native = *GetTurboQuantCodec(calculator.get());
        const size_t code_bytes = (dim * bits + 7) / 8;
        EXPECT_EQ(native.storageBytes(), code_bytes + 8);
        EXPECT_EQ(native.queryBytes(), 4 * dim + 4);
        // The rotated mean is exactly R * mean.
        EXPECT_TRUE(
            std::ranges::equal(native.biasRotatedMean(), applyRotation(native.rotation(), mean)));
        const auto x = gaussianVector(dim, 100, 0.04f), y = gaussianVector(dim, 101, 0.04f);
        const auto query = gaussianVector(dim, 200, 0.04f);
        auto a = pp->preprocessForStorage(x.data(), dim * sizeof(float));
        auto b = pp->preprocessForStorage(y.data(), dim * sizeof(float));
        auto q = pp->preprocessQuery(query.data(), dim * sizeof(float));
        // Query preparation writes exactly [R * q][q.mean].
        const auto rotated_query = applyRotation(native.rotation(), query);
        for (size_t i = 0; i < dim; ++i)
            ASSERT_EQ(unalignedFloat(q.get(), i * sizeof(float)), rotated_query[i]) << i;
        // Independently recover the codes, scale and mean-error delta of
        // R * (x - mean) / ||x - mean||.
        std::vector<float> unit(dim);
        float norm = 0;
        for (size_t i = 0; i < dim; ++i)
            norm = std::hypot(norm, x[i] - mean[i]);
        for (size_t i = 0; i < dim; ++i)
            unit[i] = (x[i] - mean[i]) / norm;
        const auto rotated_unit = applyRotation(native.rotation(), unit);
        double projection = 0, code_mean = 0, residual_mean = 0;
        std::vector<unsigned> codes(dim);
        for (size_t i = 0; i < dim; ++i) {
            for (size_t c = 1; c < native.centroids().size(); ++c)
                if (rotated_unit[i] > (native.centroids()[c - 1] + native.centroids()[c]) * 0.5f)
                    codes[i] = c;
            projection += double(rotated_unit[i]) * native.centroids()[codes[i]];
            code_mean += double(native.centroids()[codes[i]]) * native.biasRotatedMean()[i];
            residual_mean += double(x[i] - mean[i]) * mean[i];
        }
        const float scale = float(double(norm) / projection);
        EXPECT_FLOAT_EQ(unalignedFloat(a.get(), code_bytes), scale);
        EXPECT_FLOAT_EQ(unalignedFloat(a.get(), code_bytes + 4),
                        float(double(scale) * code_mean - residual_mean));
        const auto values = decoded(native, a.get());
        for (size_t i = 0; i < dim; ++i)
            ASSERT_DOUBLE_EQ(values[i], double(scale) * native.centroids()[codes[i]]) << i;
        // A one-byte offset exercises the real unaligned record layout.
        std::vector<unsigned char> unaligned(native.storageBytes() + 1);
        std::memcpy(unaligned.data() + 1, a.get(), native.storageBytes());
        EXPECT_NEAR(calculator->calcDistanceForQuery(unaligned.data() + 1, q.get(), dim),
                    queryReference(native, a.get(), query), 3e-6);
        EXPECT_NEAR(calculator->calcDistance(a.get(), b.get(), dim),
                    graphReference(native, a.get(), b.get()), 3e-6);
        // Cached dispatches use the same scoring as the virtual calls.
        const auto cached = calculator->getDistanceDispatch(DistanceMode::StoredToQuery);
        EXPECT_EQ(cached(a.get(), q.get(), dim),
                  calculator->calcDistanceForQuery(a.get(), q.get(), dim));
        // In-place encoding reads the whole input before writing, so it matches exactly.
        std::vector<float> in_place(x);
        pp->preprocessStorageInPlace(in_place.data(), dim * sizeof(float));
        EXPECT_EQ(std::memcmp(in_place.data(), a.get(), native.storageBytes()), 0);
        auto zero_residual = pp->preprocessForStorage(mean.data(), dim * sizeof(float));
        EXPECT_EQ(unalignedFloat(zero_residual.get(), code_bytes), 0);
        EXPECT_NEAR(calculator->calcDistanceForQuery(zero_residual.get(), q.get(), dim),
                    queryReference(native, zero_residual.get(), query), 3e-6);
    }
}

TEST_P(TurboQuantTest, PublicFactoryMetadataQueriesAndMutation) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 64;
    std::vector<float> mean(dim, 0.01f), query(dim, 0.05f);
    auto params = CreateParams(parameters(bits, dim, mean.data()));
    IndexOwner index(VecSimIndex_New(&params), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);
    const TurboQuant &codec = codecOf(hnsw);
    EXPECT_TRUE(std::ranges::equal(codec.biasMean(), mean));
    EXPECT_EQ(hnsw->getStoredDataSize(), (dim * bits + 7) / 8 + 8);
    EXPECT_EQ(index->debugInfo().commonInfo.memory, hnsw->getAllocator()->getAllocationSize());
    EXPECT_GE(VecSimIndex_EstimateInitialSize(&params), index->getAllocationSize());
    EXPECT_GE(VecSimIndex_EstimateElementSize(&params), hnsw->getStoredDataSize());
    std::array<std::array<float, dim>, 12> rows{};
    for (size_t i = 0; i < rows.size(); ++i) {
        for (size_t j = 0; j < dim; ++j)
            rows[i][j] = 0.2f * std::sin(float(i * dim + j + 1));
        EXPECT_EQ(VecSimIndex_AddVector(index.get(), rows[i].data(), i), 1);
    }
    const double expected = queryReference(codec, hnsw->getDataByInternalId(0), query);
    EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), 0, query.data()), expected, 3e-6);
    for (double radius : {expected - 1e-3, expected + 1e-3}) {
        auto *reply = VecSimIndex_RangeQuery(index.get(), query.data(), radius, nullptr, BY_SCORE);
        auto *it = VecSimQueryReply_GetIterator(reply);
        bool found = false;
        while (VecSimQueryReply_IteratorHasNext(it)) {
            const auto *item = VecSimQueryReply_IteratorNext(it);
            if (VecSimQueryResult_GetId(item) == 0)
                found = true;
            EXPECT_LE(VecSimQueryResult_GetScore(item), radius + 1e-6);
        }
        EXPECT_EQ(found, radius > expected);
        VecSimQueryReply_IteratorFree(it);
        VecSimQueryReply_Free(reply);
    }
    EXPECT_EQ(VecSimIndex_AddVector(index.get(), rows[0].data(), 99), 1);
    EXPECT_EQ(index->relabelVector(99, 100), VecSimRelabel_OK);
    EXPECT_EQ(VecSimIndex_DeleteVector(index.get(), 100), 1);
    EXPECT_TRUE(hnsw->checkIntegrity().valid_state);
    // TQ state is not serialized: saving is refused before the destination is opened.
    const auto filename = ::testing::TempDir() + "vecsim_tq" + std::to_string(bits) + ".hnsw";
    std::ofstream(filename) << "existing";
    EXPECT_THROW(hnsw->saveIndex(filename), std::runtime_error);
    std::ifstream kept(filename);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(kept), std::istreambuf_iterator<char>()),
              "existing");
    std::remove(filename.c_str());
}

TEST_P(TurboQuantTest, CosineRequiresNormalizedInputOptIn) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 64;
    auto hp = parameters(bits, dim);
    hp.metric = VecSimMetric_Cosine;
    auto params = CreateParams(hp);
    EXPECT_EQ(VecSimIndex_New(&params), nullptr);
    // Opting in promises normalized queries and inserts; the backend normalizes neither.
    IndexOwner index(HNSWFactory::NewIndex(&params, true), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *backend = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(backend, nullptr);
    std::vector<float> row(dim), query(dim);
    row[0] = 3, row[1] = 4, query[0] = 4, query[1] = 3;
    VecSim_Normalize(row.data(), dim, VecSimType_FLOAT32);
    VecSim_Normalize(query.data(), dim, VecSimType_FLOAT32);
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), row.data(), 0), 1);
    EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), 0, query.data()),
                queryReference(codecOf(backend), backend->getDataByInternalId(0), query), 3e-6);
}

TEST_P(TurboQuantTest, UnsupportedConfigurationsAreRejected) {
    auto hp = parameters(GetParam(), 64);
    auto expectRejected = [](const HNSWParams &rejected) {
        auto params = CreateParams(rejected);
        EXPECT_EQ(VecSimIndex_New(&params), nullptr);
        EXPECT_EQ(VecSimIndex_EstimateInitialSize(&params), SIZE_MAX);
        EXPECT_EQ(VecSimIndex_EstimateElementSize(&params), SIZE_MAX);
    };
    for (auto type : {VecSimType_FLOAT16, VecSimType_FLOAT64, VecSimType_INT8}) {
        hp.type = type;
        expectRejected(hp);
    }
    hp.type = VecSimType_FLOAT32;
    hp.metric = VecSimMetric_L2;
    expectRejected(hp);
    hp.metric = VecSimMetric_IP;
    // A nonfinite mean is rejected at construction.
    hp.dim = 64;
    std::vector<float> nan_mean(64, 0.0f);
    nan_mean[5] = std::nanf("");
    hp.quantParams = nan_mean.data();
    auto nan_params = CreateParams(hp);
    EXPECT_EQ(VecSimIndex_New(&nan_params), nullptr);
    hp.quantParams = nullptr;
    // The block Hadamard rotation needs dim to be a multiple of 64.
    for (size_t dim : {1, 32, 96, 100}) {
        hp.dim = dim;
        expectRejected(hp);
    }
}

TEST_P(TurboQuantTest, TieredLiveMeanTrainingRetriesAndMigratesInBothWriteModes) {
    const unsigned bits = GetParam();
    WriteModeGuard mode_guard;
    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        for (auto metric : {VecSimMetric_IP, VecSimMetric_Cosine}) {
            VecSim_SetWriteMode(mode);
            TestPool pool;
            constexpr size_t dim = 64;
            auto hp = parameters(bits, dim);
            hp.metric = metric;
            auto primary = CreateParams(hp);
            auto *base = newTiered(pool, primary, 4);
            ASSERT_NE(base, nullptr);
            auto *tiered = dynamic_cast<TieredHNSWIndex<float, float> *>(base);
            auto *backend = backendOf(base);
            std::array<std::array<float, dim>, 5> rows{};
            for (size_t i = 0; i < rows.size(); ++i)
                for (size_t j = 0; j < dim; ++j)
                    rows[i][j] = 0.3f * std::sin(float(i * dim + j + 1));
            VecSimIndex_AddVector(base, rows[0].data(), 10);
            VecSimIndex_AddVector(base, rows[1].data(), 20);
            EXPECT_EQ(VecSimIndex_AddVector(base, rows[2].data(), 10), 0);
            EXPECT_EQ(VecSimIndex_DeleteVector(base, 20), 1);
            EXPECT_EQ(base->relabelVector(10, 11), VecSimRelabel_OK);
            EXPECT_EQ(base->statisticInfo().flatBufferSize, 1);
            EXPECT_EQ(backend->indexSize(), 0);
            EXPECT_TRUE(pool.jobQ.empty());
            VecSimIndex_AddVector(base, rows[1].data(), 20);
            VecSimIndex_AddVector(base, rows[3].data(), 30);
            tiered->setBeforeQuantizationFinalizationHook([] { throw std::bad_alloc(); });
            EXPECT_THROW(VecSimIndex_AddVector(base, rows[4].data(), 40), std::bad_alloc);
            EXPECT_EQ(base->statisticInfo().flatBufferSize, 4);
            EXPECT_EQ(backend->indexSize(), 0);
            for (float value : codecOf(backend).biasMean())
                EXPECT_EQ(value, 0);
            // A failed finalization attempt is retried by the next writer.
            tiered->setBeforeQuantizationFinalizationHook({});
            EXPECT_EQ(VecSimIndex_AddVector(base, rows[4].data(), 40), 0);
            if (mode == VecSim_WriteAsync) {
                EXPECT_EQ(pool.jobQ.size(), 4);
                while (!pool.jobQ.empty())
                    pool.thread_iteration();
            } else {
                EXPECT_TRUE(pool.jobQ.empty());
            }
            ASSERT_EQ(backend->indexSize(), 4);
            EXPECT_EQ(base->statisticInfo().flatBufferSize, 0);
            std::vector<double> expected_mean(dim);
            for (size_t i : {size_t(1), size_t(2), size_t(3), size_t(4)}) {
                auto row = rows[i];
                if (metric == VecSimMetric_Cosine)
                    VecSim_Normalize(row.data(), dim, VecSimType_FLOAT32);
                for (size_t j = 0; j < dim; ++j)
                    expected_mean[j] += double(row[j]) / 4;
            }
            for (size_t j = 0; j < dim; ++j)
                EXPECT_NEAR(codecOf(backend).biasMean()[j], expected_mean[j], 3e-8);
            EXPECT_TRUE(backend->checkIntegrity().valid_state);
            const auto saved = codecOf(backend).biasMean();
            const std::vector<float> saved_mean(saved.begin(), saved.end());
            // Removing every row and adding again never restarts a fitted backend.
            for (size_t label : {11, 20, 30, 40})
                VecSimIndex_DeleteVector(base, label);
            while (!pool.jobQ.empty())
                pool.thread_iteration();
            VecSimIndex_AddVector(base, rows[0].data(), 50);
            while (!pool.jobQ.empty())
                pool.thread_iteration();
            EXPECT_TRUE(std::equal(saved_mean.begin(), saved_mean.end(),
                                   codecOf(backend).biasMean().begin()));
        }
    }
}

TEST_P(TurboQuantTest, MultiValueTrainingAndSubsequentUpdatesKeepOneFittedMean) {
    WriteModeGuard guard;
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    TestPool pool;
    constexpr size_t dim = 64;
    auto hp = parameters(GetParam(), dim);
    hp.multi = true;
    auto primary = CreateParams(hp);
    auto *base = newTiered(pool, primary, 3);
    ASSERT_NE(base, nullptr);
    auto *backend = backendOf(base);
    std::array<float, dim> a{}, b{}, c{};
    for (size_t i = 0; i < dim; ++i) {
        a[i] = 0.01f * i;
        b[i] = -0.01f * i;
        c[i] = 0.003f * i;
    }
    VecSimIndex_AddVector(base, a.data(), 10);
    VecSimIndex_AddVector(base, b.data(), 10);
    EXPECT_EQ(VecSimIndex_DeleteVector(base, 10), 2);
    EXPECT_EQ(base->statisticInfo().flatBufferSize, 0);
    VecSimIndex_AddVector(base, a.data(), 11);
    std::array<float, 2 * dim> replacement{};
    std::copy(b.begin(), b.end(), replacement.begin());
    std::copy(c.begin(), c.end(), replacement.begin() + dim);
    EXPECT_EQ(VecSimIndex_UpdateVectors(base, 11, replacement.data(), 2), VecSimUpdate_OK);
    EXPECT_EQ(base->statisticInfo().flatBufferSize, 2);
    EXPECT_EQ(backend->indexSize(), 0);
    VecSimIndex_AddVector(base, a.data(), 12);
    EXPECT_EQ(base->statisticInfo().flatBufferSize, 0);
    EXPECT_EQ(backend->indexSize(), 3);
    for (size_t i = 0; i < dim; ++i)
        EXPECT_NEAR(codecOf(backend).biasMean()[i], (double(a[i]) + b[i] + c[i]) / 3, 2e-8);
    EXPECT_EQ(VecSimIndex_UpdateVectors(base, 11, a.data(), 1), VecSimUpdate_OK);
    EXPECT_EQ(base->indexSize(), 2);
    EXPECT_TRUE(backend->checkIntegrity().valid_state);
}

TEST(TurboQuantRotation, BlockSizeRule) {
    const std::pair<size_t, size_t> cases[] = {{768, 256},   {384, 128}, {1536, 512}, {1024, 1024},
                                               {2048, 1024}, {960, 64},  {64, 64},    {96, 0},
                                               {100, 0},     {32, 0},    {0, 0}};
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (auto [dim, block] : cases) {
        EXPECT_EQ(TurboQuantRotation::hadamardBlock(dim), block) << dim;
        EXPECT_EQ(TurboQuant::dimSupported(dim), block != 0) << dim;
        if (!block)
            continue;
        const auto codec = newCodec(allocator, dim);
        EXPECT_EQ(codec->rotation().block(), block) << dim;
        EXPECT_EQ(codec->rotation().sharedBytes(), TurboQuantRotation::sharedBytesFor(dim)) << dim;
    }
    // About 15 KB instead of the 2.36 MB dense matrix at dim 768.
    EXPECT_EQ(TurboQuantRotation::sharedBytesFor(768), 3 * 768 * 4 + 2 * 768 * 4);
}

TEST(TurboQuantRotation, MatchesReferenceAndPreservesNormsAndInnerProducts) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (size_t dim : {64, 384, 768, 960, 1024, 1536}) {
        const auto owner = newCodec(allocator, dim);
        const auto &rotation = owner->rotation();
        for (uint32_t trial = 0; trial < 4; ++trial) {
            const auto x = gaussianVector(dim, 1000 * dim + 2 * trial);
            const auto y = gaussianVector(dim, 1000 * dim + 2 * trial + 1);
            const auto rx = applyRotation(rotation, x), ry = applyRotation(rotation, y);
            double nx = 0, nrx = 0, xy = 0, rxy = 0;
            for (size_t i = 0; i < dim; ++i) {
                nx += double(x[i]) * x[i];
                nrx += double(rx[i]) * rx[i];
                xy += double(x[i]) * y[i];
                rxy += double(rx[i]) * ry[i];
            }
            EXPECT_NEAR(std::sqrt(nrx), std::sqrt(nx), 1e-5 * std::sqrt(nx)) << dim;
            EXPECT_NEAR(rxy, xy, 1e-5 * nx) << dim; // |x| ~ |y|
            if (trial == 0 && dim <= 1024) {
                const auto reference = referenceRotation(rotation, x);
                double max_error = 0;
                for (size_t i = 0; i < dim; ++i)
                    max_error = std::max(max_error, std::abs(reference[i] - rx[i]));
                EXPECT_LT(max_error, 1e-5) << dim;
            }
        }
    }
}

TEST(TurboQuantRotation, DeterministicPerSeedAndDistinctAcrossSeeds) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto a = newCodec(allocator, 768, 2, 42), b = newCodec(allocator, 768, 3, 42);
    auto c = newCodec(allocator, 768, 2, 43);
    const auto &ra = a->rotation(), &rb = b->rotation(), &rc = c->rotation();
    EXPECT_TRUE(std::ranges::equal(ra.scaledSigns(), rb.scaledSigns()));
    EXPECT_TRUE(std::ranges::equal(ra.permutations(), rb.permutations()));
    EXPECT_FALSE(std::ranges::equal(ra.scaledSigns(), rc.scaledSigns()));
    EXPECT_FALSE(std::ranges::equal(ra.permutations(), rc.permutations()));
    // Every round holds a valid sign vector and permutation.
    const float magnitude = 1.0f / 16.0f;
    for (float sign : ra.scaledSigns())
        EXPECT_TRUE(sign == magnitude || sign == -magnitude);
    for (size_t round = 0; round + 1 < TurboQuantRotation::kRounds; ++round) {
        std::vector<uint32_t> sorted(ra.permutations().begin() + round * 768,
                                     ra.permutations().begin() + (round + 1) * 768);
        std::sort(sorted.begin(), sorted.end());
        for (uint32_t i = 0; i < 768; ++i)
            EXPECT_EQ(sorted[i], i);
    }
}

// Every SIMD kernel this CPU runs matches the scalar kernel on random codes, for both widths.
TEST(TurboQuantKernels, SimdMatchesScalar) {
    struct Kernels {
        const char *name;
        spaces::tq_packed_dot_t dot;
        spaces::tq_packed_pair_dot_t pair;
    };
    for (unsigned bits : {2U, 3U}) {
        std::vector<Kernels> kernels;
#ifdef CPU_FEATURES_ARCH_X86_64
        [[maybe_unused]] const auto features = cpu_features::GetX86Info().features;
#ifdef OPT_AVX2_FMA
        if (features.avx2 && features.fma3)
            kernels.push_back({"AVX2_FMA", spaces::Choose_TQ_FP32_IP_implementation_AVX2_FMA(bits),
                               spaces::Choose_TQ_TQ_IP_implementation_AVX2_FMA(bits)});
#endif
#ifdef OPT_AVX512F
        if (features.avx512f)
            kernels.push_back({"AVX512F", spaces::Choose_TQ_FP32_IP_implementation_AVX512F(bits),
                               spaces::Choose_TQ_TQ_IP_implementation_AVX512F(bits)});
#endif
#endif
#if defined(CPU_FEATURES_ARCH_AARCH64) && defined(__ARM_NEON)
        kernels.push_back(
            {"NEON",
             bits == 2 ? turboquant_detail::packedDotNEON<2> : turboquant_detail::packedDotNEON<3>,
             bits == 2 ? turboquant_detail::packedPairDotNEON<2>
                       : turboquant_detail::packedPairDotNEON<3>});
#endif
        const auto scalar_dot =
            bits == 2 ? turboquant_detail::packedDot<2> : turboquant_detail::packedDot<3>;
        const auto scalar_pair =
            bits == 2 ? turboquant_detail::packedPairDot<2> : turboquant_detail::packedPairDot<3>;
        std::mt19937 rng(bits); // NOSONAR(S2245) test data
        for (size_t dim : {64, 768, 1024}) {
            std::vector<unsigned char> a(dim * bits / 8), b(dim * bits / 8);
            for (auto &byte : a)
                byte = static_cast<unsigned char>(rng());
            for (auto &byte : b)
                byte = static_cast<unsigned char>(rng());
            const auto query = gaussianVector(dim, 7 + bits, 0.05f);
            const auto rotated_mean = gaussianVector(dim, 9 + bits, 0.02f);
            const auto centroids = gaussianVector(size_t(1) << bits, 11 + bits, 0.1f);
            const auto *q = reinterpret_cast<const unsigned char *>(query.data());
            const float dot = scalar_dot(a.data(), q, centroids.data(), dim);
            const float pair = scalar_pair(a.data(), b.data(), centroids.data(), dim, 1.3f, 0.7f,
                                           rotated_mean.data());
            for (const auto &kernel : kernels) {
                SCOPED_TRACE(::testing::Message()
                             << kernel.name << " bits " << bits << " dim " << dim);
                // Only the float summation order differs from the scalar loop.
                EXPECT_NEAR(kernel.dot(a.data(), q, centroids.data(), dim), dot, 1e-5);
                EXPECT_NEAR(kernel.pair(a.data(), b.data(), centroids.data(), dim, 1.3f, 0.7f,
                                        rotated_mean.data()),
                            pair, 1e-5);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(TQ2AndTQ3, TurboQuantTest, ::testing::Values(2U, 3U));
