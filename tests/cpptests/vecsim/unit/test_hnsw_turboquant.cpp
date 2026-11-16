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
#include <limits>
#include <memory>
#include <random>
#include <sstream>
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

HNSWParams parameters(unsigned bits, size_t dim, const VecSimTurboQuantParams *config = nullptr) {
    return {.type = VecSimType_FLOAT32, .dim = dim, .metric = VecSimMetric_IP, .multi = false,
            .blockSize = 8, .M = 8, .efConstruction = 64, .efRuntime = 128,
            .quantType = bits == 2 ? VecSimQuant_TQ2 : VecSimQuant_TQ3, .quantParams = config};
}
float unalignedFloat(const void *data, size_t offset) {
    float value;
    std::memcpy(&value, static_cast<const char *>(data) + offset, sizeof(value));
    return value;
}
// An independent bit reader and decoded-score reference, including the full mean error.
// The references read the dense matrix, so they only apply where the rotation is dense.
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
double queryReference(const TurboQuant &quantizer, const void *blob, const float *query) {
    const auto values = decoded(quantizer, blob);
    double score = 1 + unalignedFloat(blob, quantizer.storageBytes() - sizeof(float));
    for (size_t i = 0; i < quantizer.dim(); ++i) {
        double rotated = 0;
        for (size_t j = 0; j < quantizer.dim(); ++j)
            rotated += double(quantizer.rotation()[i * quantizer.dim() + j]) * query[j];
        score -= values[i] * rotated;
        score -= double(quantizer.biasMean()[i]) * query[i];
    }
    return score;
}
double graphReference(const TurboQuant &quantizer, const void *a, const void *b) {
    const auto x = decoded(quantizer, a), y = decoded(quantizer, b);
    double inner = 0, mean_squared = 0;
    for (size_t i = 0; i < quantizer.dim(); ++i) {
        double rotated_mean = 0;
        for (size_t j = 0; j < quantizer.dim(); ++j)
            rotated_mean +=
                double(quantizer.rotation()[i * quantizer.dim() + j]) * quantizer.biasMean()[j];
        inner += x[i] * y[i] + (x[i] + y[i]) * rotated_mean;
        mean_squared += double(quantizer.biasMean()[i]) * quantizer.biasMean()[i];
    }
    return 1 - (inner + mean_squared) +
           unalignedFloat(a, quantizer.storageBytes() - sizeof(float)) +
           unalignedFloat(b, quantizer.storageBytes() - sizeof(float));
}
std::string path(unsigned bits, const char *suffix) {
    return ::testing::TempDir() + "vecsim_tq" + std::to_string(bits) + "_" + suffix + ".hnsw";
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
    TieredIndexParams params{.jobQueue = &pool.jobQ, .jobQueueCtx = pool.ctx,
                             .submitCb = tieredIndexMock::submit_callback, .flatBufferLimit = 1,
                             .primaryIndexParams = &primary,
                             .specificParams = {TieredHNSWParams{
                                 .QuantNormalizationSetSize = training_size}}};
    auto *index = TieredFactory::NewIndex(&params);
    pool.ctx->index_strong_ref = std::shared_ptr<VecSimIndex>(index, VecSimIndex_Free);
    return index;
}
} // namespace

TEST_P(TurboQuantTest, IndependentEncodingAndScoreReferencesWithUnalignedTails) {
    const unsigned bits = GetParam();
    for (size_t dim : {2, 7, 8, 9, 17}) {
        auto allocator = VecSimAllocator::newVecsimAllocator();
        std::vector<float> mean(dim), x(dim), y(dim), query(dim), residual(dim);
        for (size_t i = 0; i < dim; ++i) {
            mean[i] = 0.12f * std::sin(float(i + 1));
            x[i] = 0.3f * std::cos(float(i + 2));
            y[i] = 0.2f * std::sin(float(i + 3));
            query[i] = 0.1f * std::cos(float(i + 4));
            residual[i] = x[i] - mean[i];
        }
        const VecSimTurboQuantParams config{.mean = mean.data(), .seed = 42};
        auto parts = CreateTurboQuantIndexComponents(allocator, dim, bits, &config);
        std::unique_ptr<PreprocessorsContainerAbstract> pp(parts.preprocessors);
        std::unique_ptr<IndexCalculatorInterface<float>> calculator(parts.indexCalculator);
        const TurboQuant &native = *GetTurboQuantCodec(calculator.get());
        auto a = pp->preprocessForStorage(x.data(), dim * sizeof(float));
        auto b = pp->preprocessForStorage(y.data(), dim * sizeof(float));
        auto q = pp->preprocessQuery(query.data(), dim * sizeof(float));
        EXPECT_EQ(native.storageBytes(), (dim * bits + 7) / 8 + 8);
        EXPECT_EQ(native.queryBytes(), 4 * dim + 4);
        // Independently recover centroid choices and projection-match scale from raw residual.
        float norm = 0;
        for (float value : residual)
            norm = std::hypot(norm, value);
        double projection = 0, code_mean = 0, residual_mean = 0;
        std::vector<unsigned> expected_codes(dim);
        for (size_t i = 0; i < dim; ++i) {
            float rotated = 0;
            for (size_t j = 0; j < dim; ++j)
                rotated += native.rotation()[i * dim + j] * (residual[j] / norm);
            unsigned expected_code = 0;
            for (size_t c = 1; c < native.centroids().size(); ++c)
                if (rotated > (native.centroids()[c - 1] + native.centroids()[c]) * 0.5f)
                    expected_code = c;
            expected_codes[i] = expected_code;
            projection += double(rotated) * native.centroids()[expected_code];
            code_mean += double(native.centroids()[expected_code]) * native.biasRotatedMean()[i];
            residual_mean += double(residual[i]) * mean[i];
        }
        const size_t code_bytes = (dim * bits + 7) / 8;
        const float expected_scale = float(double(norm) / projection);
        EXPECT_FLOAT_EQ(unalignedFloat(a.get(), code_bytes), expected_scale);
        EXPECT_FLOAT_EQ(unalignedFloat(a.get(), code_bytes + 4),
                        float(double(expected_scale) * code_mean - residual_mean));
        const auto decoded_a = decoded(native, a.get());
        for (size_t i = 0; i < dim; ++i)
            EXPECT_DOUBLE_EQ(decoded_a[i],
                             double(expected_scale) * native.centroids()[expected_codes[i]]);
        // A one-byte offset exercises the real unaligned record layout and full stride.
        std::vector<unsigned char> unaligned(native.storageBytes() + 1);
        std::memcpy(unaligned.data() + 1, a.get(), native.storageBytes());
        EXPECT_NEAR(calculator->calcDistanceForQuery(unaligned.data() + 1, q.get(), dim),
                    queryReference(native, a.get(), query.data()), 3e-6);
        EXPECT_NEAR(calculator->calcDistance(a.get(), b.get(), dim),
                    graphReference(native, a.get(), b.get()), 3e-6);
        // Cached dispatches use the same scoring as the virtual calls.
        const auto cached = calculator->getDistanceDispatch(DistanceMode::StoredToQuery);
        EXPECT_EQ(cached(a.get(), q.get(), dim),
                  calculator->calcDistanceForQuery(a.get(), q.get(), dim));
        auto zero_residual = pp->preprocessForStorage(mean.data(), dim * sizeof(float));
        EXPECT_EQ(unalignedFloat(zero_residual.get(), code_bytes), 0);
        EXPECT_NEAR(calculator->calcDistanceForQuery(zero_residual.get(), q.get(), dim),
                    queryReference(native, zero_residual.get(), query.data()), 3e-6);
    }
}

TEST_P(TurboQuantTest, PublicFactoryMetadataRangePersistenceAndMutation) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 9;
    std::vector<float> mean(dim, 0.1f), query(dim, 0.05f);
    VecSimTurboQuantParams config{.mean = mean.data(), .seed = 0};
    auto params = CreateParams(parameters(bits, dim, &config));
    IndexOwner index(VecSimIndex_New(&params), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);
    const TurboQuant &codec = codecOf(hnsw);
    EXPECT_EQ(codec.seed(), 0);
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
    const double expected = queryReference(codec, hnsw->getDataByInternalId(0), query.data());
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
    const auto filename = path(bits, "roundtrip");
    hnsw->saveIndex(filename);
    {
        std::ifstream in(filename, std::ios::binary);
        EXPECT_EQ(HNSWSerializer::ReadVersion(in), HNSWSerializer::EncodingVersion::V5);
    }
    IndexOwner restored(HNSWFactory::NewIndex(filename), VecSimIndex_Free);
    ASSERT_NE(restored, nullptr);
    auto *loaded = dynamic_cast<HNSWIndex<float, float> *>(restored.get());
    ASSERT_NE(loaded, nullptr);
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(std::memcmp(hnsw->getDataByInternalId(i), loaded->getDataByInternalId(i),
                              hnsw->getStoredDataSize()),
                  0);
        EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), i, query.data()),
                        VecSimIndex_GetDistanceFrom_Unsafe(restored.get(), i, query.data()));
    }
    // The fitted state, including the full rotation, round-trips exactly.
    for (auto getter : {&TurboQuant::rotation, &TurboQuant::centroids, &TurboQuant::biasMean,
                        &TurboQuant::biasRotatedMean}) {
        const auto lhs = (codec.*getter)();
        const auto rhs = (codecOf(loaded).*getter)();
        ASSERT_EQ(lhs.size(), rhs.size());
        EXPECT_TRUE(std::equal(lhs.begin(), lhs.end(), rhs.begin()));
    }
    EXPECT_EQ(VecSimIndex_AddVector(restored.get(), rows[0].data(), 99), 1);
    EXPECT_EQ(restored->relabelVector(99, 100), VecSimRelabel_OK);
    EXPECT_EQ(VecSimIndex_DeleteVector(restored.get(), 100), 1);
    EXPECT_TRUE(loaded->checkIntegrity().valid_state);
    std::remove(filename.c_str());
}

TEST_P(TurboQuantTest, CosinePersistenceRequiresNormalizedInputOptIn) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 9;
    auto hp = parameters(bits, dim);
    hp.metric = VecSimMetric_Cosine;
    auto params = CreateParams(hp);
    EXPECT_EQ(VecSimIndex_New(&params), nullptr);
    IndexOwner index(HNSWFactory::NewIndex(&params, true), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *backend = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(backend, nullptr);
    std::array<float, dim> row{3, 4}, query{4, 3};
    VecSim_Normalize(row.data(), dim, VecSimType_FLOAT32);
    VecSim_Normalize(query.data(), dim, VecSimType_FLOAT32);
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), row.data(), 0), 1);
    const auto filename = path(bits, "cosine");
    backend->saveIndex(filename);
    IndexOwner raw_loaded(HNSWFactory::NewIndex(filename, false), VecSimIndex_Free);
    EXPECT_EQ(raw_loaded, nullptr);
    IndexOwner restored(HNSWFactory::NewIndex(filename, true), VecSimIndex_Free);
    ASSERT_NE(restored, nullptr);
    auto *loaded = dynamic_cast<HNSWIndex<float, float> *>(restored.get());
    ASSERT_NE(loaded, nullptr);
    // Opting in promises normalized queries and inserts; the backend normalizes neither.
    row = {4, -3};
    VecSim_Normalize(row.data(), dim, VecSimType_FLOAT32);
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), row.data(), 1), 1);
    ASSERT_EQ(VecSimIndex_AddVector(restored.get(), row.data(), 1), 1);
    for (size_t label : {0U, 1U}) {
        EXPECT_EQ(std::memcmp(backend->getDataByInternalId(label),
                              loaded->getDataByInternalId(label), backend->getStoredDataSize()),
                  0);
        EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), label, query.data()),
                        VecSimIndex_GetDistanceFrom_Unsafe(restored.get(), label, query.data()));
    }
    const auto *stored = loaded->getDataByInternalId(0);
    EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(restored.get(), 0, query.data()),
                queryReference(codecOf(loaded), stored, query.data()), 3e-6);
    EXPECT_TRUE(loaded->checkIntegrity().valid_state);
    std::remove(filename.c_str());
}

TEST_P(TurboQuantTest, UnsupportedConfigurationsAreRejected) {
    auto hp = parameters(GetParam(), 9);
    for (auto type : {VecSimType_FLOAT16, VecSimType_FLOAT64, VecSimType_INT8}) {
        hp.type = type;
        auto params = CreateParams(hp);
        EXPECT_EQ(VecSimIndex_New(&params), nullptr);
        EXPECT_EQ(VecSimIndex_EstimateInitialSize(&params), SIZE_MAX);
        EXPECT_EQ(VecSimIndex_EstimateElementSize(&params), SIZE_MAX);
    }
    hp.type = VecSimType_FLOAT32;
    hp.metric = VecSimMetric_L2;
    auto l2 = CreateParams(hp);
    EXPECT_EQ(VecSimIndex_New(&l2), nullptr);
    EXPECT_EQ(VecSimIndex_EstimateInitialSize(&l2), SIZE_MAX);
    EXPECT_EQ(VecSimIndex_EstimateElementSize(&l2), SIZE_MAX);
    hp.metric = VecSimMetric_IP;
    hp.dim = 1;
    auto too_small = CreateParams(hp);
    EXPECT_EQ(VecSimIndex_New(&too_small), nullptr);
    EXPECT_EQ(VecSimIndex_EstimateInitialSize(&too_small), SIZE_MAX);
    EXPECT_EQ(VecSimIndex_EstimateElementSize(&too_small), SIZE_MAX);
}

TEST_P(TurboQuantTest, TieredLiveMeanTrainingRetriesAndMigratesInBothWriteModes) {
    const unsigned bits = GetParam();
    WriteModeGuard mode_guard;
    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        for (auto metric : {VecSimMetric_IP, VecSimMetric_Cosine}) {
            VecSim_SetWriteMode(mode);
            TestPool pool;
            constexpr size_t dim = 9;
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
    constexpr size_t dim = 7;
    auto hp = parameters(GetParam(), dim);
    hp.multi = true;
    auto primary = CreateParams(hp);
    auto *base = newTiered(pool, primary, 3);
    ASSERT_NE(base, nullptr);
    auto *backend = backendOf(base);
    std::array<float, dim> a{}, b{}, c{};
    for (size_t i = 0; i < dim; ++i) {
        a[i] = 0.1f * i;
        b[i] = -0.1f * i;
        c[i] = 0.03f * i;
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

TEST_P(TurboQuantTest, RejectsTruncatedOrCorruptFittedState) {
    const unsigned bits = GetParam();
    auto params = CreateParams(parameters(bits, 9));
    IndexOwner index(VecSimIndex_New(&params), VecSimIndex_Free);
    auto *backend = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(backend, nullptr);
    std::array<float, 9> row{};
    row[0] = 0.5f;
    VecSimIndex_AddVector(index.get(), row.data(), 1);
    const auto filename = path(bits, "corrupt");
    backend->saveIndex(filename);
    std::string bytes;
    {
        std::ifstream source(filename, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(source), std::istreambuf_iterator<char>());
    }
    // V5 writes the fitted state right after quantType.
    const size_t fitted = sizeof(HNSWSerializer::EncodingVersion) + sizeof(VecSimAlgo) +
                          sizeof(size_t) + sizeof(VecSimType) + sizeof(VecSimMetric) +
                          sizeof(size_t) + sizeof(bool) + sizeof(size_t) + sizeof(VecSimQuantType);
    auto reject = [&](const std::string &corrupt) {
        {
            std::ofstream out(filename, std::ios::binary | std::ios::trunc);
            out.write(corrupt.data(), corrupt.size());
        }
        EXPECT_THROW({ IndexOwner invalid(HNSWFactory::NewIndex(filename), VecSimIndex_Free); },
                     std::runtime_error);
    };
    for (size_t length : {size_t(0), fitted, fitted + 16, fitted + 200})
        reject(bytes.substr(0, length));
    auto unknown = bytes;
    const uint32_t bad_policy = 999;
    std::memcpy(unknown.data() + fitted + 3 * sizeof(uint32_t), &bad_policy, sizeof(bad_policy));
    reject(unknown);
    auto bad_length = bytes;
    const uint64_t huge = UINT64_MAX;
    std::memcpy(bad_length.data() + fitted + 5 * sizeof(uint32_t) + sizeof(uint64_t), &huge,
                sizeof(huge));
    reject(bad_length);
    auto nonfinite = bytes;
    const float nan = std::numeric_limits<float>::quiet_NaN();
    std::memcpy(nonfinite.data() + fitted + 5 * sizeof(uint32_t) + 2 * sizeof(uint64_t), &nan,
                sizeof(nan));
    reject(nonfinite);
    std::remove(filename.c_str());
}

TEST_P(TurboQuantTest, ExistingWriterVersionsRemainUnchanged) {
    for (auto type : {VecSimQuant_NONE, VecSimQuant_SQ8, VecSimQuant_SQ4}) {
        auto hp = parameters(GetParam(), 9);
        hp.quantType = type;
        auto params = CreateParams(hp);
        IndexOwner index(VecSimIndex_New(&params), VecSimIndex_Free);
        ASSERT_NE(index, nullptr);
        auto *backend = dynamic_cast<HNSWIndex<float, float> *>(index.get());
        ASSERT_NE(backend, nullptr);
        EXPECT_THROW(codecOf(backend), std::runtime_error);
        const auto filename = path(unsigned(type), "legacy");
        backend->saveIndex(filename);
        {
            std::ifstream in(filename, std::ios::binary);
            EXPECT_EQ(HNSWSerializer::ReadVersion(in), type == VecSimQuant_NONE
                                                           ? HNSWSerializer::EncodingVersion::V4
                                                           : HNSWSerializer::EncodingVersion::V5);
        }
        IndexOwner restored(HNSWFactory::NewIndex(filename), VecSimIndex_Free);
        EXPECT_NE(restored, nullptr);
        std::remove(filename.c_str());
    }
}

// Block Hadamard rotation.
namespace {
std::vector<float> gaussianVector(size_t dim, uint32_t seed, float scale = 1.0f) {
    std::mt19937 rng(seed);
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
std::shared_ptr<TurboQuant> newCodec(const std::shared_ptr<VecSimAllocator> &allocator,
                                     size_t dim, unsigned bits = 2, uint64_t seed = 42,
                                     std::vector<float> mean = {}) {
    mean.resize(dim, 0.0f);
    return std::allocate_shared<TurboQuant>(VecsimSTLAllocator<TurboQuant>(allocator), allocator,
                                            dim, bits, seed, std::span<const float>(mean));
}
std::string savedFitted(const TurboQuant &quantizer) {
    std::stringstream out;
    quantizer.saveFitted(out);
    return out.str();
}
std::shared_ptr<TurboQuant> loadFittedString(const std::string &bytes, size_t dim, unsigned bits) {
    std::stringstream in(bytes);
    return TurboQuant::loadFitted(in, VecSimAllocator::newVecsimAllocator(), dim, bits);
}
} // namespace

TEST(TurboQuantRotation, BlockSizeRuleAndDenseFallback) {
    const std::pair<size_t, size_t> cases[] = {{768, 256}, {384, 128}, {1536, 512},
                                               {1024, 1024}, {2048, 1024}, {960, 64},
                                               {64, 64}, {96, 0}, {100, 0}, {9, 0}, {2, 0}};
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (auto [dim, block] : cases) {
        EXPECT_EQ(TurboQuantRotation::hadamardBlock(dim), block) << dim;
        auto codec = newCodec(allocator, dim);
        const auto &rotation = codec->rotationOperator();
        EXPECT_EQ(rotation.kind(), block ? TurboQuantRotationKind::BlockHadamard
                                         : TurboQuantRotationKind::Dense)
            << dim;
        EXPECT_EQ(rotation.block(), block) << dim;
        EXPECT_EQ(codec->rotation().size(), block ? 0 : dim * dim) << dim;
        EXPECT_EQ(rotation.sharedBytes(), TurboQuantRotation::sharedBytesFor(dim)) << dim;
    }
    // About 15 KB instead of the 2.36 MB dense matrix at dim 768.
    EXPECT_EQ(TurboQuantRotation::sharedBytesFor(768), 3 * 768 * 4 + 2 * 768 * 4);
}

TEST(TurboQuantRotation, MatchesReferenceAndPreservesNormsAndInnerProducts) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (size_t dim : {64, 384, 768, 960, 1024, 1536}) {
        const auto owner = newCodec(allocator, dim);
        const auto &rotation = owner->rotationOperator();
        ASSERT_TRUE(rotation.isBlockHadamard());
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

TEST(TurboQuantRotation, IsOrthonormal) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (size_t dim : {256, 384}) {
        const auto owner = newCodec(allocator, dim);
        std::vector<std::vector<float>> columns;
        for (size_t i = 0; i < dim; ++i) {
            std::vector<float> basis(dim, 0.0f);
            basis[i] = 1.0f;
            columns.push_back(applyRotation(owner->rotationOperator(), basis));
        }
        double max_error = 0;
        for (size_t i = 0; i < dim; ++i)
            for (size_t j = i; j < dim; ++j) {
                double dot = 0;
                for (size_t k = 0; k < dim; ++k)
                    dot += double(columns[i][k]) * columns[j][k];
                max_error = std::max(max_error, std::abs(dot - (i == j ? 1.0 : 0.0)));
            }
        EXPECT_LT(max_error, 1e-5) << dim;
    }
}

TEST(TurboQuantRotation, DeterministicPerSeedAndDistinctAcrossSeeds) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto a = newCodec(allocator, 768, 2, 42), b = newCodec(allocator, 768, 3, 42);
    auto c = newCodec(allocator, 768, 2, 43);
    const auto &ra = a->rotationOperator(), &rb = b->rotationOperator();
    const auto &rc = c->rotationOperator();
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

TEST_P(TurboQuantTest, BlockHadamardEncodeAndQueryShareTheRotation) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 768;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    const auto mean = gaussianVector(dim, 11, 0.02f);
    const VecSimTurboQuantParams config{.mean = mean.data(), .seed = 42};
    auto parts = CreateTurboQuantIndexComponents(allocator, dim, bits, &config);
    std::unique_ptr<PreprocessorsContainerAbstract> pp(parts.preprocessors);
    std::unique_ptr<IndexCalculatorInterface<float>> calculator(parts.indexCalculator);
    const TurboQuant &native = *GetTurboQuantCodec(calculator.get());
    const auto &rotation = native.rotationOperator();
    ASSERT_TRUE(rotation.isBlockHadamard());
    // The rotated mean is exactly R * mean.
    EXPECT_TRUE(std::ranges::equal(native.biasRotatedMean(), applyRotation(rotation, mean)));
    for (uint32_t trial = 0; trial < 3; ++trial) {
        const auto x = gaussianVector(dim, 100 + trial, 0.04f);
        const auto q = gaussianVector(dim, 200 + trial, 0.04f);
        auto stored = pp->preprocessForStorage(x.data(), dim * sizeof(float));
        auto prepared = pp->preprocessQuery(q.data(), dim * sizeof(float));
        // Query preparation writes exactly R * q.
        const auto rotated_query = applyRotation(rotation, q);
        for (size_t i = 0; i < dim; ++i)
            ASSERT_EQ(unalignedFloat(prepared.get(), i * sizeof(float)), rotated_query[i]) << i;
        // Encoding quantizes exactly R * (x - mean) / ||x - mean||.
        std::vector<float> unit(dim);
        float norm = 0;
        for (size_t i = 0; i < dim; ++i)
            norm = std::hypot(norm, x[i] - mean[i]);
        for (size_t i = 0; i < dim; ++i)
            unit[i] = (x[i] - mean[i]) / norm;
        const auto rotated_unit = applyRotation(rotation, unit);
        const auto values = decoded(native, stored.get());
        const float scale = unalignedFloat(stored.get(), (dim * bits + 7) / 8);
        for (size_t i = 0; i < dim; ++i) {
            unsigned code = 0;
            for (size_t c = 1; c < native.centroids().size(); ++c)
                if (rotated_unit[i] > (native.centroids()[c - 1] + native.centroids()[c]) * 0.5f)
                    code = c;
            ASSERT_DOUBLE_EQ(values[i], double(scale) * native.centroids()[code]) << i;
        }
        // The score matches the decoded double reference built from the same rotation.
        double reference = 1 + unalignedFloat(stored.get(), native.storageBytes() - sizeof(float));
        for (size_t i = 0; i < dim; ++i)
            reference -= values[i] * rotated_query[i] + double(mean[i]) * q[i];
        EXPECT_NEAR(calculator->calcDistanceForQuery(stored.get(), prepared.get(), dim),
                    reference, 3e-6);
    }
}

TEST_P(TurboQuantTest, BlockHadamardIndexSaveLoadRoundTrip) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 768;
    const auto mean = gaussianVector(dim, 21, 0.02f);
    VecSimTurboQuantParams config{.mean = mean.data(), .seed = 7};
    auto params = CreateParams(parameters(bits, dim, &config));
    IndexOwner index(VecSimIndex_New(&params), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);
    EXPECT_TRUE(codecOf(hnsw).rotationOperator().isBlockHadamard());
    EXPECT_GE(VecSimIndex_EstimateInitialSize(&params), index->getAllocationSize());
    constexpr size_t rows = 24;
    for (size_t i = 0; i < rows; ++i) {
        const auto row = gaussianVector(dim, 300 + i, 0.04f);
        ASSERT_EQ(VecSimIndex_AddVector(index.get(), row.data(), i), 1);
    }
    const auto query = gaussianVector(dim, 999, 0.04f);
    const auto filename = path(bits, "hadamard_roundtrip");
    hnsw->saveIndex(filename);
    IndexOwner restored(HNSWFactory::NewIndex(filename), VecSimIndex_Free);
    std::remove(filename.c_str());
    ASSERT_NE(restored, nullptr);
    auto *loaded = dynamic_cast<HNSWIndex<float, float> *>(restored.get());
    ASSERT_NE(loaded, nullptr);
    const auto &a = codecOf(hnsw).rotationOperator();
    const auto &b = codecOf(loaded).rotationOperator();
    EXPECT_TRUE(b.isBlockHadamard());
    EXPECT_EQ(b.block(), a.block());
    EXPECT_TRUE(std::ranges::equal(a.scaledSigns(), b.scaledSigns()));
    EXPECT_TRUE(std::ranges::equal(a.permutations(), b.permutations()));
    EXPECT_TRUE(
        std::ranges::equal(codecOf(hnsw).biasRotatedMean(), codecOf(loaded).biasRotatedMean()));
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(std::memcmp(hnsw->getDataByInternalId(i), loaded->getDataByInternalId(i),
                              hnsw->getStoredDataSize()),
                  0);
        EXPECT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), i, query.data()),
                  VecSimIndex_GetDistanceFrom_Unsafe(restored.get(), i, query.data()));
    }
    // A vector added after reload encodes identically to one added to the original.
    const auto extra = gaussianVector(dim, 4242, 0.04f);
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), extra.data(), 500), 1);
    ASSERT_EQ(VecSimIndex_AddVector(restored.get(), extra.data(), 500), 1);
    EXPECT_EQ(std::memcmp(hnsw->getDataByInternalId(rows), loaded->getDataByInternalId(rows),
                          hnsw->getStoredDataSize()),
              0);
    EXPECT_TRUE(loaded->checkIntegrity().valid_state);
}

TEST_P(TurboQuantTest, LegacyDenseSchemaStillLoadsAtHadamardDimensions) {
    // A schema-1 (dense) fitted blob at dim 768, as written before block Hadamard existed. Its
    // matrix is the materialized block Hadamard rotation, so the dense codec must reproduce the
    // structured codec's prepared queries to float tolerance.
    const unsigned bits = GetParam();
    constexpr size_t dim = 768;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto structured = newCodec(allocator, dim, bits, 42, gaussianVector(dim, 31, 0.02f));
    std::vector<float> matrix(dim * dim);
    for (size_t col = 0; col < dim; ++col) {
        std::vector<float> basis(dim, 0.0f);
        basis[col] = 1.0f;
        const auto column = applyRotation(structured->rotationOperator(), basis);
        for (size_t row = 0; row < dim; ++row)
            matrix[row * dim + col] = column[row];
    }
    const auto mean = structured->biasMean();
    std::vector<float> rotated_mean(dim);
    double norm = 0;
    for (size_t row = 0; row < dim; ++row) {
        double dot = 0;
        for (size_t col = 0; col < dim; ++col)
            dot += double(matrix[row * dim + col]) * mean[col];
        rotated_mean[row] = float(dot);
        norm += double(mean[row]) * mean[row];
    }
    std::string blob;
    auto append = [&](const void *data, size_t bytes) {
        blob.append(static_cast<const char *>(data), bytes);
    };
    auto appendFloats = [&](std::span<const float> values) {
        const uint64_t count = values.size();
        append(&count, sizeof(count));
        append(values.data(), values.size_bytes());
    };
    const uint32_t header[] = {1, bits, 1, 3, 1};
    const uint64_t seed = 42;
    const float norm_squared = float(norm);
    append(header, sizeof(header));
    append(&seed, sizeof(seed));
    appendFloats(matrix);
    appendFloats(structured->centroids());
    appendFloats(mean);
    appendFloats(rotated_mean);
    append(&norm_squared, sizeof(norm_squared));

    auto legacy = loadFittedString(blob, dim, bits);
    EXPECT_EQ(legacy->rotationOperator().kind(), TurboQuantRotationKind::Dense);
    EXPECT_EQ(legacy->rotation().size(), dim * dim);
    // Re-saving a dense codec is byte-for-byte the schema-1 layout.
    EXPECT_EQ(savedFitted(*legacy), blob);
    auto dense_parts = CreateTurboQuantIndexComponents(allocator, legacy);
    auto structured_parts = CreateTurboQuantIndexComponents(allocator, structured);
    std::unique_ptr<PreprocessorsContainerAbstract> dense_pp(dense_parts.preprocessors);
    std::unique_ptr<PreprocessorsContainerAbstract> structured_pp(structured_parts.preprocessors);
    std::unique_ptr<IndexCalculatorInterface<float>> dense_calc(dense_parts.indexCalculator);
    std::unique_ptr<IndexCalculatorInterface<float>> structured_calc(
        structured_parts.indexCalculator);
    const auto q = gaussianVector(dim, 77, 0.04f);
    auto a = dense_pp->preprocessQuery(q.data(), dim * sizeof(float));
    auto b = structured_pp->preprocessQuery(q.data(), dim * sizeof(float));
    for (size_t i = 0; i < dim; ++i)
        EXPECT_NEAR(unalignedFloat(a.get(), 4 * i), unalignedFloat(b.get(), 4 * i), 1e-6) << i;
}

TEST_P(TurboQuantTest, RejectsCorruptBlockHadamardFittedState) {
    const unsigned bits = GetParam();
    constexpr size_t dim = 768;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto structured = newCodec(allocator, dim, bits, 42, gaussianVector(dim, 41, 0.02f));
    const auto bytes = savedFitted(*structured);
    ASSERT_NO_THROW(loadFittedString(bytes, dim, bits));
    // Layout: header (5 x u32), seed, block, rounds, sign count, signs, perm count, perms, ...
    const size_t block_offset = 5 * sizeof(uint32_t) + sizeof(uint64_t);
    const size_t signs_offset = block_offset + 3 * sizeof(uint64_t);
    const size_t perms_offset = signs_offset + 3 * dim * sizeof(float) + sizeof(uint64_t);
    auto expectReject = [&](const std::string &corrupt) {
        EXPECT_THROW(loadFittedString(corrupt, dim, bits), std::runtime_error);
    };
    for (size_t length :
         {size_t(0), block_offset, signs_offset + 7, perms_offset + 5, bytes.size() - 1})
        expectReject(bytes.substr(0, length));
    auto patch = [&](size_t offset, const auto &value) {
        auto corrupt = bytes;
        std::memcpy(corrupt.data() + offset, &value, sizeof(value));
        return corrupt;
    };
    expectReject(patch(block_offset, uint64_t(128)));   // wrong block for dim 768
    expectReject(patch(block_offset + 8, uint64_t(2))); // wrong round count
    expectReject(patch(signs_offset, 0.5f));            // not +-1/sqrt(256)
    expectReject(patch(perms_offset, uint32_t(dim)));   // out of range
    uint32_t second;
    std::memcpy(&second, bytes.data() + perms_offset + 4, sizeof(second));
    expectReject(patch(perms_offset, second)); // duplicate entry
    expectReject(patch(0, uint32_t(3)));       // unknown schema
    // Wrong dimension for the payload, and schema 2 where only dense is valid.
    EXPECT_THROW(loadFittedString(bytes, 384, bits), std::runtime_error);
    auto small = newCodec(allocator, 96, bits, 42, gaussianVector(96, 43, 0.02f));
    ASSERT_EQ(small->rotationOperator().kind(), TurboQuantRotationKind::Dense);
    auto small_bytes = savedFitted(*small);
    const uint32_t hadamard_schema = 2;
    std::memcpy(small_bytes.data(), &hadamard_schema, sizeof(hadamard_schema));
    EXPECT_THROW(loadFittedString(small_bytes, 96, bits), std::runtime_error);
}

TEST(TurboQuantRotation, DirectLoadRejectsTruncationAndLeavesStateUntouched) {
    // The codec-level size check hides these branches, so exercise the rotation loader directly.
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto owner = newCodec(allocator, 768);
    std::stringstream saved;
    owner->rotationOperator().saveBlockHadamard(saved);
    const std::string bytes = saved.str();
    TurboQuantRotation target(allocator);
    {
        std::stringstream full(bytes);
        target.loadBlockHadamard(full, 768);
    }
    const std::vector<float> signs(target.scaledSigns().begin(), target.scaledSigns().end());
    ASSERT_TRUE(std::ranges::equal(signs, owner->rotationOperator().scaledSigns()));
    const auto x = gaussianVector(768, 5);
    for (size_t length : {size_t(10), size_t(24), size_t(24 + 100), size_t(24 + 3 * 768 * 4 + 4),
                          bytes.size() - 1}) {
        std::stringstream truncated(bytes.substr(0, length));
        EXPECT_THROW(target.loadBlockHadamard(truncated, 768), std::runtime_error) << length;
        // A failed load keeps the previous, still usable rotation.
        EXPECT_TRUE(target.isBlockHadamard());
        EXPECT_TRUE(std::ranges::equal(target.scaledSigns(), signs));
        EXPECT_EQ(applyRotation(target, x), applyRotation(owner->rotationOperator(), x));
    }
}

INSTANTIATE_TEST_SUITE_P(TQ2AndTQ3, TurboQuantTest, ::testing::Values(2U, 3U));
