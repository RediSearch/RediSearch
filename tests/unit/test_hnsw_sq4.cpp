/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "VecSim/index_factories/tiered_factory.h"
#include "VecSim/types/sq4.h"
#include "VecSim/vec_sim.h"
#include "unit_test_utils.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
using IndexPtr = std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)>;
using SQ4Case = std::tuple<VecSimMetric, bool, bool, size_t>;
using Results = std::vector<std::pair<size_t, double>>;
class HNSWSQ4Test : public ::testing::TestWithParam<SQ4Case> {};

std::vector<float> reconstruct(const char *blob, size_t dim, const std::vector<float> &mean) {
    float metadata[2];
    std::memcpy(metadata, blob + (dim + 1) / 2, sizeof(metadata));
    std::vector<float> result(dim);
    for (size_t d = 0; d < dim; ++d) {
        auto code = (static_cast<unsigned char>(blob[d / 2]) >> (4 * (d % 2))) & 15;
        result[d] = metadata[0] + metadata[1] * code + (mean.empty() ? 0 : mean[d]);
    }
    return result;
}

double distance(const std::vector<float> &a, const std::vector<float> &b, VecSimMetric metric) {
    double sum = 0;
    for (size_t d = 0; d < a.size(); ++d) {
        double x = a[d], y = b[d];
        sum += metric == VecSimMetric_L2 ? (x - y) * (x - y) : x * y;
    }
    return metric == VecSimMetric_L2 ? sum : 1 - sum;
}

std::vector<float> make_mean(bool centered, size_t dim) {
    std::vector<float> mean;
    if (centered) {
        mean.resize(dim);
        for (size_t d = 0; d < dim; ++d)
            mean[d] = 0.3f * std::sin(float(d + 1));
    }
    return mean;
}

std::vector<std::vector<float>> random_vectors(size_t count, size_t dim, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> random(-1.0f, 1.0f);
    std::vector<std::vector<float>> vectors(count, std::vector<float>(dim));
    for (auto &vector : vectors)
        for (auto &value : vector)
            value = random(rng);
    return vectors;
}

HNSWParams sq4_params(VecSimMetric metric, bool multi, size_t dim, const std::vector<float> &mean) {
    return {.type = VecSimType_FLOAT32,
            .dim = dim,
            .metric = metric,
            .multi = multi,
            .blockSize = 32,
            .M = 16,
            .efConstruction = 100,
            .efRuntime = 128,
            .quantType = VecSimQuant_SQ4,
            .quantParams = mean.empty() ? nullptr : mean.data()};
}

// An ef far above the index size makes the HNSW search exhaustive over the connected graph, so
// indexes whose graphs differ (the level generator is not serialized) still agree on the top k.
Results top_k(VecSimIndex *index, const float *query, size_t k) {
    VecSimQueryParams query_params{};
    query_params.hnswRuntimeParams.efRuntime = 1024;
    Results results;
    auto *reply = VecSimIndex_TopKQuery(index, query, k, &query_params, BY_SCORE);
    EXPECT_EQ(VecSimQueryReply_GetCode(reply), VecSim_QueryReply_OK);
    auto *iterator = VecSimQueryReply_GetIterator(reply);
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        const auto *item = VecSimQueryReply_IteratorNext(iterator);
        results.emplace_back(VecSimQueryResult_GetId(item), VecSimQueryResult_GetScore(item));
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(reply);
    return results;
}

std::string temp_index_path(const std::string &prefix) {
    std::string case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::replace(case_name.begin(), case_name.end(), '/', '_');
    return ::testing::TempDir() + prefix + case_name + ".hnsw";
}

TEST_P(HNSWSQ4Test, InsertQueryMutateAndSerialize) {
    const auto [metric, centered, multi, dim] = GetParam();
    std::vector<float> mean = make_mean(centered, dim);
    HNSWParams hp = sq4_params(metric, multi, dim, mean);
    auto params = CreateParams(hp);
    IndexPtr index(VecSimIndex_New(&params), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);
    EXPECT_EQ(EstimateInitialSize(hp), index->getAllocationSize());
    EXPECT_TRUE(hnsw->usesQuantizedStorage());
    EXPECT_EQ(hnsw->getStoredDataSize(),
              (dim + 1) / 2 + sizeof(float) * (metric == VecSimMetric_L2 ? 4 : (centered ? 4 : 3)));

    constexpr size_t count = 64;
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> random(-1.0f, 1.0f);
    std::vector<std::vector<float>> inputs(count, std::vector<float>(dim));
    for (size_t i = 0; i < count; ++i) {
        for (auto &value : inputs[i])
            value = random(rng);
        ASSERT_EQ(VecSimIndex_AddVector(index.get(), inputs[i].data(), i), 1);
    }
    ASSERT_TRUE(hnsw->checkIntegrity().valid_state);
    std::vector<float> query(dim);
    for (auto &value : query)
        value = random(rng);
    std::vector<std::pair<double, size_t>> expected;
    for (size_t i = 0; i < count; ++i) {
        auto reconstructed = reconstruct(hnsw->getDataByInternalId(i), dim, mean);
        double score = distance(reconstructed, query, metric);
        expected.emplace_back(score, i);
        EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), i, query.data()), score,
                    2e-5 * (1.0 + std::abs(score)));
        auto other = reconstruct(hnsw->getDataByInternalId((i + 1) % count), dim, mean);
        double expected_pair = distance(reconstructed, other, metric);
        if (centered && metric == VecSimMetric_IP) {
            expected_pair = 1.0;
            for (size_t j = 0; j < dim; ++j) {
                expected_pair -=
                    (double(reconstructed[j]) - mean[j]) * (double(other[j]) - mean[j]) +
                    double(inputs[i][j]) * mean[j] + double(inputs[(i + 1) % count][j]) * mean[j] -
                    double(mean[j]) * mean[j];
            }
        }
        EXPECT_NEAR(hnsw->calcDistance(hnsw->getDataByInternalId(i),
                                       hnsw->getDataByInternalId((i + 1) % count)),
                    expected_pair, 3e-5 * (1.0 + std::abs(expected_pair)));
    }
    std::sort(expected.begin(), expected.end());
    auto check_results = [&](VecSimIndex *target) {
        runTopKSearchTest(target, query.data(), 10, [&](size_t id, double score, size_t pos) {
            EXPECT_EQ(id, expected[pos].second);
            EXPECT_NEAR(score, expected[pos].first, 2e-5 * (1.0 + std::abs(expected[pos].first)));
        });
    };
    check_results(index.get());
    const double radius = std::max(0.0, (expected[19].first + expected[20].first) / 2);
    const size_t range_count =
        std::count_if(expected.begin(), expected.end(),
                      [radius](const auto &result) { return result.first <= radius; });
    // A bounded HNSW range search is approximate. Validate every returned score and membership.
    auto *range = VecSimIndex_RangeQuery(index.get(), query.data(), radius, nullptr, BY_SCORE);
    EXPECT_EQ(VecSimQueryReply_GetCode(range), VecSim_QueryReply_OK);
    EXPECT_GT(VecSimQueryReply_Len(range), 0);
    EXPECT_LE(VecSimQueryReply_Len(range), range_count);
    std::unordered_set<size_t> range_ids;
    auto *iterator = VecSimQueryReply_GetIterator(range);
    while (VecSimQueryReply_IteratorHasNext(iterator)) {
        const auto *item = VecSimQueryReply_IteratorNext(iterator);
        const size_t id = VecSimQueryResult_GetId(item);
        EXPECT_TRUE(range_ids.insert(id).second);
        const double score = VecSimQueryResult_GetScore(item);
        const auto match = std::find_if(expected.begin(), expected.end(),
                                        [id](const auto &result) { return result.second == id; });
        EXPECT_NE(match, expected.end());
        if (match != expected.end())
            EXPECT_NEAR(score, match->first, 2e-5 * (1.0 + std::abs(match->first)));
        EXPECT_LE(score, radius);
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(range);
    // A radius containing the entire connected graph verifies full traversal and scoring.
    runRangeQueryTest(
        index.get(), query.data(), std::max(0.0, expected.back().first) + 1.0,
        [&](size_t id, double score, size_t pos) {
            EXPECT_EQ(id, expected[pos].second);
            EXPECT_NEAR(score, expected[pos].first, 2e-5 * (1.0 + std::abs(expected[pos].first)));
        },
        count, BY_SCORE);
    auto *batch = VecSimBatchIterator_New(index.get(), query.data(), nullptr);
    runBatchIteratorSearchTest(batch, 10, [&](size_t id, double score, size_t pos) {
        EXPECT_EQ(id, expected[pos].second);
        EXPECT_NEAR(score, expected[pos].first, 2e-5 * (1.0 + std::abs(expected[pos].first)));
    });
    VecSimBatchIterator_Free(batch);
    const auto path = temp_index_path("sq4_roundtrip_");
    hnsw->saveIndex(path);
    IndexPtr loaded(HNSWFactory::NewIndex(path), VecSimIndex_Free);
    std::remove(path.c_str());
    ASSERT_NE(loaded, nullptr);
    auto *restored = dynamic_cast<HNSWIndex<float, float> *>(loaded.get());
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(restored->quantType, VecSimQuant_SQ4);
    EXPECT_EQ(restored->serializedMeanVector, mean);
    ASSERT_TRUE(restored->checkIntegrity().valid_state);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(std::memcmp(hnsw->getDataByInternalId(i), restored->getDataByInternalId(i),
                              hnsw->getStoredDataSize()),
                  0);
    }
    check_results(loaded.get());
    // Exercise duplicate-label insertion and deletion through the actual index API.
    EXPECT_EQ(VecSimIndex_AddVector(loaded.get(), query.data(), 0), multi ? 1 : 0);
    EXPECT_EQ(VecSimIndex_IndexSize(loaded.get()), count + (multi ? 1 : 0));
    EXPECT_EQ(VecSimIndex_DeleteVector(loaded.get(), 0), multi ? 2 : 1);
    EXPECT_EQ(VecSimIndex_IndexSize(loaded.get()), count - 1);
    EXPECT_TRUE(restored->checkIntegrity().valid_state);
}

// A loaded index must keep working as a writable index: new vectors are quantized with the
// restored mean and the index answers like one that never went through serialization.
TEST_P(HNSWSQ4Test, SaveLoadThenMutate) {
    const auto [metric, centered, multi, dim] = GetParam();
    constexpr size_t initial = 200, added = 50, deleted = 20, queries = 10, k = 10;
    const std::vector<float> mean = make_mean(centered, dim);
    const HNSWParams hp = sq4_params(metric, multi, dim, mean);
    const auto vectors = random_vectors(initial + added, dim, 7);
    const auto query_vectors = random_vectors(queries, dim, 8);

    IndexPtr original(HNSWFactory::NewIndex(&hp), VecSimIndex_Free);
    ASSERT_NE(original, nullptr);
    auto *original_hnsw = dynamic_cast<HNSWIndex<float, float> *>(original.get());
    ASSERT_NE(original_hnsw, nullptr);
    for (size_t i = 0; i < initial; ++i) {
        ASSERT_EQ(VecSimIndex_AddVector(original.get(), vectors[i].data(), i), 1);
    }
    std::vector<Results> before;
    for (const auto &query : query_vectors) {
        before.push_back(top_k(original.get(), query.data(), k));
        ASSERT_EQ(before.back().size(), k);
    }

    const auto path = temp_index_path("sq4_save_load_mutate_");
    original_hnsw->saveIndex(path);
    IndexPtr loaded(HNSWFactory::NewIndex(path), VecSimIndex_Free);
    std::remove(path.c_str());
    ASSERT_NE(loaded, nullptr);
    auto *loaded_hnsw = dynamic_cast<HNSWIndex<float, float> *>(loaded.get());
    ASSERT_NE(loaded_hnsw, nullptr);
    EXPECT_EQ(loaded_hnsw->quantType, VecSimQuant_SQ4);
    EXPECT_EQ(loaded_hnsw->serializedMeanVector, mean);
    ASSERT_EQ(loaded_hnsw->getStoredDataSize(), original_hnsw->getStoredDataSize());
    for (size_t q = 0; q < queries; ++q) {
        EXPECT_EQ(top_k(loaded.get(), query_vectors[q].data(), k), before[q]) << "query " << q;
    }

    // Each label holds one vector, so before any deletion label i has internal id i.
    for (size_t i = initial; i < initial + added; ++i) {
        ASSERT_EQ(VecSimIndex_AddVector(loaded.get(), vectors[i].data(), i), 1);
        const auto expected_blob = original_hnsw->preprocessForStorage(vectors[i].data());
        EXPECT_EQ(std::memcmp(loaded_hnsw->getDataByInternalId(i), expected_blob.get(),
                              loaded_hnsw->getStoredDataSize()),
                  0)
            << "label " << i;
    }
    for (size_t i = 0; i < deleted; ++i) {
        ASSERT_EQ(VecSimIndex_DeleteVector(loaded.get(), i), 1);
    }
    EXPECT_EQ(VecSimIndex_IndexSize(loaded.get()), initial + added - deleted);
    EXPECT_TRUE(loaded_hnsw->checkIntegrity().valid_state);

    IndexPtr fresh(HNSWFactory::NewIndex(&hp), VecSimIndex_Free);
    ASSERT_NE(fresh, nullptr);
    for (size_t i = 0; i < initial + added; ++i) {
        ASSERT_EQ(VecSimIndex_AddVector(fresh.get(), vectors[i].data(), i), 1);
    }
    for (size_t i = 0; i < deleted; ++i) {
        ASSERT_EQ(VecSimIndex_DeleteVector(fresh.get(), i), 1);
    }
    for (size_t q = 0; q < queries; ++q) {
        const Results expected = top_k(fresh.get(), query_vectors[q].data(), k);
        ASSERT_EQ(expected.size(), k);
        EXPECT_EQ(top_k(loaded.get(), query_vectors[q].data(), k), expected) << "query " << q;
    }
}

// Multi-value only: gtest cannot mix TEST and TEST_P in one suite, so single-value cases skip.
TEST_P(HNSWSQ4Test, DuplicateLabelsMulti) {
    const auto [metric, centered, multi, dim] = GetParam();
    if (!multi) {
        GTEST_SKIP() << "multi-value only";
    }
    const std::vector<float> mean = make_mean(centered, dim);
    const HNSWParams hp = sq4_params(metric, multi, dim, mean);
    IndexPtr index(HNSWFactory::NewIndex(&hp), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);

    constexpr size_t label = 5, other_label = 9, copies = 3;
    const auto vectors = random_vectors(copies + 1, dim, 11);
    for (size_t i = 0; i < copies; ++i) {
        ASSERT_EQ(VecSimIndex_AddVector(index.get(), vectors[i].data(), label), 1);
    }
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), vectors[copies].data(), other_label), 1);
    EXPECT_EQ(VecSimIndex_IndexSize(index.get()), copies + 1);
    EXPECT_EQ(index->indexLabelCount(), 2u);

    // KNN reports the duplicated label once, scored by its closest vector (which is also what
    // the multi-value getDistanceFrom reports).
    const Results results = top_k(index.get(), vectors[0].data(), 10);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_NE(results[0].first, results[1].first);
    const auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);
    const auto query_blob = hnsw->preprocessQuery(vectors[0].data());
    double best = std::numeric_limits<double>::max();
    for (size_t id = 0; id < copies; ++id) {
        best = std::min<double>(
            best, hnsw->calcDistanceForQuery(hnsw->getDataByInternalId(id), query_blob.get()));
    }
    EXPECT_DOUBLE_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), label, vectors[0].data()),
                     best);
    size_t label_hits = 0;
    for (const auto &[id, score] : results) {
        if (id == label) {
            ++label_hits;
            EXPECT_DOUBLE_EQ(score, best);
        }
    }
    EXPECT_EQ(label_hits, 1u);

    EXPECT_EQ(VecSimIndex_DeleteVector(index.get(), label), static_cast<int>(copies));
    EXPECT_EQ(VecSimIndex_IndexSize(index.get()), 1u);
    EXPECT_EQ(index->indexLabelCount(), 1u);
    const Results remaining = top_k(index.get(), vectors[0].data(), 10);
    ASSERT_EQ(remaining.size(), 1u);
    EXPECT_EQ(remaining[0].first, other_label);
}

INSTANTIATE_TEST_SUITE_P(FP32, HNSWSQ4Test,
                         ::testing::Combine(::testing::Values(VecSimMetric_IP, VecSimMetric_L2),
                                            ::testing::Bool(), ::testing::Bool(),
                                            ::testing::Values(1u, 17u, 64u)));

TEST(HNSWSQ4Factory, PreNormalizedCosineUsesIP) {
    HNSWParams hp = {.type = VecSimType_FLOAT32,
                     .dim = 3,
                     .metric = VecSimMetric_Cosine,
                     .quantType = VecSimQuant_SQ4};
    VecSimParams params = CreateParams(hp);
    IndexPtr index(HNSWFactory::NewIndex(&params, true), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    const float x[] = {1, 0, 0};
    const float y[] = {0, 1, 0};
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), x, 0), 1);
    EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), 0, x), 0);
    EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(index.get(), 0, y), 1);
}

TEST(HNSWSQ4Factory, LoadRejectsUnsupportedSQ4Snapshot) {
    HNSWParams hp = {.type = VecSimType_FLOAT32,
                     .dim = 4,
                     .metric = VecSimMetric_L2,
                     .M = 8,
                     .efConstruction = 40,
                     .quantType = VecSimQuant_SQ4};
    IndexPtr index(HNSWFactory::NewIndex(&hp), VecSimIndex_Free);
    ASSERT_NE(index, nullptr);
    const float a[] = {0.25f, 0.3f, 0.35f, 0.4f};
    const float b[] = {0.75f, 0.8f, 0.85f, 0.9f};
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), a, 0), 1);
    ASSERT_EQ(VecSimIndex_AddVector(index.get(), b, 1), 1);
    auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
    ASSERT_NE(hnsw, nullptr);

    const auto path = temp_index_path("sq4_reject_");
    // Field offsets follow HNSWSerializer::saveIndex and HNSWIndex::saveIndexFields.
    constexpr std::streamoff type_offset =
        sizeof(HNSWSerializer::EncodingVersion) + sizeof(VecSimAlgo) + sizeof(size_t);
    constexpr std::streamoff metric_offset = type_offset + sizeof(VecSimType);
    auto save_and_patch = [&](std::streamoff offset, auto value) {
        hnsw->saveIndex(path);
        std::fstream stream(path, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(stream.is_open());
        // Check the offset points at the expected field before overwriting it.
        decltype(value) original{};
        stream.seekg(offset);
        Serializer::readBinaryPOD(stream, original);
        if constexpr (std::is_same_v<decltype(value), VecSimType>) {
            ASSERT_EQ(original, VecSimType_FLOAT32);
        } else {
            ASSERT_EQ(original, VecSimMetric_L2);
        }
        stream.seekp(offset);
        Serializer::writeBinaryPOD(stream, value);
    };
    auto expect_rejected = [&]() {
        IndexPtr loaded(HNSWFactory::NewIndex(path), VecSimIndex_Free);
        EXPECT_EQ(loaded, nullptr);
    };

    // The unmodified snapshot loads.
    hnsw->saveIndex(path);
    {
        IndexPtr loaded(HNSWFactory::NewIndex(path), VecSimIndex_Free);
        ASSERT_NE(loaded, nullptr);
    }
    // SQ4 accepts FP32 only, unlike SQ8.
    save_and_patch(type_offset, VecSimType_FLOAT16);
    expect_rejected();
    // A raw Cosine metric has no scalar quantization kernel.
    save_and_patch(metric_offset, VecSimMetric_Cosine);
    expect_rejected();
    std::remove(path.c_str());
}

// Mirrors HNSWSQ8Test.SizeEstimation, including its 1% tolerance on block growth.
TEST(HNSWSQ4Factory, EstimatesMatchAllocation) {
    constexpr size_t dim = 768, block_size = 256;
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
        for (bool centered : {false, true}) {
            SCOPED_TRACE(std::string(VecSimMetric_ToString(metric)) +
                         (centered ? " centered" : " plain"));
            const std::vector<float> mean = make_mean(centered, dim);
            HNSWParams hp = sq4_params(metric, false, dim, mean);
            hp.blockSize = block_size;
            hp.M = 64;
            VecSimParams params = CreateParams(hp);
            IndexPtr index(VecSimIndex_New(&params), VecSimIndex_Free);
            ASSERT_NE(index, nullptr);
            EXPECT_EQ(VecSimIndex_EstimateInitialSize(&params),
                      VecSimIndex_StatsInfo(index.get()).memory);

            const auto vectors = random_vectors(block_size + 1, dim, 3);
            for (size_t i = 0; i < block_size; ++i) {
                ASSERT_EQ(VecSimIndex_AddVector(index.get(), vectors[i].data(), i), 1);
            }

            // Measure the allocation caused by growing the index by one block.
            const size_t estimation = VecSimIndex_EstimateElementSize(&params) * block_size;
            const size_t before = VecSimIndex_StatsInfo(index.get()).memory;
            ASSERT_EQ(VecSimIndex_AddVector(index.get(), vectors[block_size].data(), block_size),
                      1);
            const size_t actual = VecSimIndex_StatsInfo(index.get()).memory - before;

            EXPECT_GE(estimation, actual * 0.99);
            EXPECT_LE(estimation, actual * 1.01);
        }
    }
}

// Tiered SQ4 needs mean training, which is out of scope: creation and estimation both reject it,
// and the estimator's exception is translated to SIZE_MAX at the C API.
TEST(HNSWSQ4Factory, RejectsTieredSQ4) {
    constexpr size_t dim = 32;
    const std::vector<float> mean = make_mean(true, dim);
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
        for (bool centered : {false, true}) {
            for (size_t training_set_size : {size_t{0}, size_t{10}}) {
                SCOPED_TRACE(std::string(VecSimMetric_ToString(metric)) +
                             (centered ? " centered" : " plain") + " training " +
                             std::to_string(training_set_size));
                HNSWParams hp = {.type = VecSimType_FLOAT32,
                                 .dim = dim,
                                 .metric = metric,
                                 .quantType = VecSimQuant_SQ4,
                                 .quantParams = centered ? mean.data() : nullptr};
                VecSimParams primary = CreateParams(hp);
                TieredIndexParams tiered = {.primaryIndexParams = &primary,
                                            .specificParams = {TieredHNSWParams{
                                                .QuantNormalizationSetSize = training_set_size}}};
                VecSimParams params = CreateParams(tiered);

                VecSimIndex *direct = TieredFactory::NewIndex(&tiered);
                EXPECT_EQ(direct, nullptr);
                if (direct) {
                    VecSimIndex_Free(direct);
                }
                VecSimIndex *created = VecSimIndex_New(&params);
                EXPECT_EQ(created, nullptr);
                if (created) {
                    VecSimIndex_Free(created);
                }
                EXPECT_THROW(TieredFactory::EstimateInitialSize(&tiered), std::invalid_argument);
                size_t estimate = 0;
                EXPECT_NO_THROW(estimate = VecSimIndex_EstimateInitialSize(&params));
                EXPECT_EQ(estimate, SIZE_MAX);
            }
        }
    }
}

// Every rejection must happen in parameter validation, before components are built, so no
// exception reaches the C API and the factory returns NULL rather than throwing.
TEST(HNSWSQ4Factory, RejectsInvalidParams) {
    auto expect_rejected = [](const HNSWParams &hp, bool is_normalized) {
        VecSimIndex *direct = nullptr;
        EXPECT_NO_THROW(direct = HNSWFactory::NewIndex(&hp, is_normalized));
        EXPECT_EQ(direct, nullptr);
        if (direct) {
            VecSimIndex_Free(direct);
        }
        EXPECT_THROW(HNSWFactory::EstimateInitialSize(&hp, is_normalized), std::invalid_argument);
        if (is_normalized) {
            // The C API never declares input normalized; that path is covered above.
            return;
        }
        VecSimParams params = CreateParams(hp);
        VecSimIndex *created = nullptr;
        EXPECT_NO_THROW(created = VecSimIndex_New(&params));
        EXPECT_EQ(created, nullptr);
        if (created) {
            VecSimIndex_Free(created);
        }
        size_t estimate = 0;
        EXPECT_NO_THROW(estimate = VecSimIndex_EstimateInitialSize(&params));
        EXPECT_EQ(estimate, SIZE_MAX);
    };

    constexpr size_t dim = 32;
    const std::vector<float> mean = make_mean(true, dim);
    for (const float *quant_params : {static_cast<const float *>(nullptr), mean.data()}) {
        SCOPED_TRACE(quant_params ? "centered" : "plain");
        for (auto type : {VecSimType_FLOAT16, VecSimType_BFLOAT16, VecSimType_FLOAT64,
                          VecSimType_INT8, VecSimType_UINT8}) {
            SCOPED_TRACE(VecSimType_ToString(type));
            for (auto metric : {VecSimMetric_IP, VecSimMetric_L2}) {
                expect_rejected({.type = type,
                                 .dim = dim,
                                 .metric = metric,
                                 .quantType = VecSimQuant_SQ4,
                                 .quantParams = quant_params},
                                false);
            }
        }

        // Raw Cosine has no SQ4 kernel; only pre-normalized Cosine (resolved to IP) is accepted.
        expect_rejected({.type = VecSimType_FLOAT32,
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .quantType = VecSimQuant_SQ4,
                         .quantParams = quant_params},
                        false);

        // Value 3 is not a VecSimQuantType enumerator.
        for (bool is_normalized : {false, true}) {
            expect_rejected({.type = VecSimType_FLOAT32,
                             .dim = dim,
                             .metric = VecSimMetric_IP,
                             .quantType = static_cast<VecSimQuantType>(3),
                             .quantParams = quant_params},
                            is_normalized);
        }
    }
}
} // namespace
