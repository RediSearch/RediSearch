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
#include <array>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
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

INSTANTIATE_TEST_SUITE_P(TQ2AndTQ3, TurboQuantTest, ::testing::Values(2U, 3U));
