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
#include "VecSim/algorithms/hnsw/hnsw_multi.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/vec_sim.h"
#include "mock_thread_pool.h"
#include "unit_test_utils.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <cmath>
#include <cstdio>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iterator>
#include <mutex>
#include <memory>
#include <random>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

template <VecSimType type, typename DataType, bool WithQuantParams>
struct HNSWSQ8IndexType : IndexType<type, DataType, float> {
    static constexpr bool with_quant_params = WithQuantParams;
};

using HNSWSQ8DataTypeSet =
    ::testing::Types<HNSWSQ8IndexType<VecSimType_FLOAT32, float, false>,
                     HNSWSQ8IndexType<VecSimType_FLOAT32, float, true>,
                     HNSWSQ8IndexType<VecSimType_FLOAT16, vecsim_types::float16, false>,
                     HNSWSQ8IndexType<VecSimType_FLOAT16, vecsim_types::float16, true>>;

template <typename index_type_t>
class HNSWSQ8Test : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;

protected:
    static constexpr float quantization_mean_value = 1.0f;

    static data_t ToDataType(float value) {
        if constexpr (std::is_same_v<data_t, vecsim_types::float16>) {
            return vecsim_types::FP32_to_FP16(value);
        } else {
            return value;
        }
    }

    virtual void SetUp(HNSWParams &params) {
        params.type = index_type_t::get_index_type();
        params.quantType = VecSimQuant_SQ8;
        if constexpr (index_type_t::with_quant_params) {
            quantization_mean.assign(params.dim, quantization_mean_value);
            params.quantParams = quantization_mean.data();
        }
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        ASSERT_NE(index, nullptr);
        dim = params.dim;
    }

    void TearDown() override {
        if (index) {
            VecSimIndex_Free(index);
        }
    }

    virtual HNSWIndex<data_t, float> *CastToHNSW() {
        return dynamic_cast<HNSWIndex<data_t, float> *>(index);
    }

    void GenerateVector(data_t *out_vec, float initial_value = 0.25f, float step = 0.0f) {
        for (size_t i = 0; i < dim; i++) {
            out_vec[i] = ToDataType(initial_value + step * static_cast<float>(i));
        }
    }

    int GenerateAndAddVector(size_t label, float initial_value = 0.25f, float step = 0.0f) {
        std::vector<data_t> vector(dim);
        GenerateVector(vector.data(), initial_value, step);
        return VecSimIndex_AddVector(index, vector.data(), label);
    }

    void create_index_test();
    void search_by_id_test();
    void search_by_score_test();
    void search_empty_index_test();
    void test_override();
    void test_range_query();
    void test_get_distance(VecSimMetric metric, bool multi);
    void test_batch_iterator_basic();
    void test_multi_readback(VecSimMetric metric, size_t dimensions);
    void test_functional_queries(VecSimMetric metric, bool multi);
    void test_graph_routing(VecSimMetric metric, bool multi);

    VecSimIndex *index = nullptr;
    size_t dim = 0;
    std::vector<float> quantization_mean;
};

TYPED_TEST_SUITE(HNSWSQ8Test, HNSWSQ8DataTypeSet);

// Removes a saved snapshot when a test scope ends.
struct TemporaryFile {
    std::string path;
    ~TemporaryFile() { std::remove(path.c_str()); }
};

// Index creation

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::create_index_test() {
    HNSWParams params = {.dim = 40, .M = 16, .efConstruction = 200};
    SetUp(params);

    constexpr float initial_value = 0.5f;
    constexpr float step = 1.0f;
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0u);
    ASSERT_EQ(GenerateAndAddVector(0, initial_value, step), 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1u);

    auto *hnsw_index = CastToHNSW();
    ASSERT_NE(hnsw_index, nullptr);
    const auto *stored = reinterpret_cast<const uint8_t *>(hnsw_index->getDataByInternalId(0));
    EXPECT_EQ(stored[0], 0);
    EXPECT_EQ(stored[dim - 1], 255);

    // Stored layout: quantized bytes followed by the minimum value and quantization delta.
    float stored_min;
    float stored_delta;
    std::memcpy(&stored_min, stored + dim + sq8::MIN_VAL * sizeof(float), sizeof(float));
    std::memcpy(&stored_delta, stored + dim + sq8::DELTA * sizeof(float), sizeof(float));
    const float expected_min =
        initial_value - (index_type_t::with_quant_params ? quantization_mean_value : 0.0f);
    EXPECT_FLOAT_EQ(stored_min, expected_min);
    EXPECT_FLOAT_EQ(stored_delta, step * static_cast<float>(dim - 1) / 255.0f);

    EXPECT_EQ(index->basicInfo().type, index_type_t::get_index_type());
    EXPECT_EQ(index->basicInfo().algo, VecSimAlgo_HNSWLIB);
}

// A quantized element is not the elements, so reading it as values would hand back compression
// and FP32 metadata reinterpreted as floats. `getDataByLabel` reports nothing instead, which
// callers read as "cannot tell".
//
// dim 4 on purpose. SQ8 stores one byte per dimension plus metadata -- 4 slots for L2 -- so the
// element is 20 bytes here against 16 for the raw elements: *larger*. The first version of this
// guard compared sizes and so concluded "not quantized" for exactly these dimensions, and the
// first version of this test used dim 40, where the comparison happens to hold. Both dimensions
// are covered below for that reason.
TYPED_TEST(HNSWSQ8Test, getDataByLabelReportsNothingForQuantizedStorage) {
    // Small dim: the quantized element is *larger* than the raw elements, which is what defeats a
    // size-based test.
    HNSWParams small = {.dim = 4, .M = 16, .efConstruction = 200};
    this->SetUp(small);
    ASSERT_EQ(this->GenerateAndAddVector(0, 0.5f, 1.0f), 1);
    auto *hnsw_index = this->CastToHNSW();
    ASSERT_NE(hnsw_index, nullptr);
    ASSERT_GT(hnsw_index->getStoredDataSize(), this->dim * sizeof(TEST_DATA_T))
        << "premise: at dim 4 the quantized element is larger than the raw elements, so a size "
           "comparison cannot identify it";

    std::vector<std::vector<TEST_DATA_T>> stored;
    hnsw_index->getDataByLabel(0, stored);
    EXPECT_TRUE(stored.empty());
}

TYPED_TEST(HNSWSQ8Test, getDataByLabelReportsNothingForQuantizedStorageLargeDim) {
    // Large dim: here the quantized element *is* smaller, the case the old size test handled.
    HNSWParams large = {.dim = 40, .M = 16, .efConstruction = 200};
    this->SetUp(large);
    ASSERT_EQ(this->GenerateAndAddVector(0, 0.5f, 1.0f), 1);
    auto *hnsw_index = this->CastToHNSW();
    ASSERT_NE(hnsw_index, nullptr);
    ASSERT_LT(hnsw_index->getStoredDataSize(), this->dim * sizeof(TEST_DATA_T));

    std::vector<std::vector<TEST_DATA_T>> stored;
    hnsw_index->getDataByLabel(0, stored);
    EXPECT_TRUE(stored.empty());
}

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_multi_readback(VecSimMetric metric, size_t dimensions) {
    HNSWParams params = {.dim = dimensions, .metric = metric, .multi = true};
    SetUp(params);
    ASSERT_EQ(GenerateAndAddVector(7, 0.5f, 1.0f), 1);
    ASSERT_EQ(GenerateAndAddVector(7, -0.5f, 0.25f), 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2u);

    // Call the backend directly: the tiered wrapper has its own quantized-storage guard.
    auto *hnsw = dynamic_cast<HNSWIndex_Multi<data_t, float> *>(index);
    ASSERT_NE(hnsw, nullptr);
    if (dimensions == 4) {
        ASSERT_GE(hnsw->getStoredDataSize(), dim * sizeof(data_t));
    } else {
        ASSERT_LT(hnsw->getStoredDataSize(), dim * sizeof(data_t));
    }
    std::vector<std::vector<data_t>> stored;
    hnsw->getDataByLabel(7, stored);
    EXPECT_TRUE(stored.empty());
}

TYPED_TEST(HNSWSQ8Test, MultiReadbackL2SmallDim) { this->test_multi_readback(VecSimMetric_L2, 4); }
TYPED_TEST(HNSWSQ8Test, MultiReadbackL2LargeDim) { this->test_multi_readback(VecSimMetric_L2, 40); }
TYPED_TEST(HNSWSQ8Test, MultiReadbackIPSmallDim) { this->test_multi_readback(VecSimMetric_IP, 4); }
TYPED_TEST(HNSWSQ8Test, MultiReadbackIPLargeDim) { this->test_multi_readback(VecSimMetric_IP, 40); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_functional_queries(VecSimMetric metric, bool multi) {
    constexpr size_t labels = 12;
    HNSWParams params = {.dim = 29,
                         .metric = metric,
                         .multi = multi,
                         .M = 16,
                         .efConstruction = 100,
                         .efRuntime = 64};
    SetUp(params);

    // Binary coordinates survive SQ8 exactly, including mean subtraction. Binary fractions in
    // the query are exact in FP16 too, so the oracle needs no quantization-error allowance.
    auto one_hot = [&](size_t coordinate) {
        std::vector<data_t> vector(dim, ToDataType(0.0f));
        vector[coordinate] = ToDataType(1.0f);
        return vector;
    };
    std::vector<data_t> query(dim);
    for (size_t d = 0; d < dim; ++d) {
        query[d] = ToDataType(static_cast<float>(d + 1) / 32.0f);
    }
    std::vector<std::vector<std::vector<data_t>>> vectors(labels);
    for (size_t label = 0; label < labels; ++label) {
        // Alternate which vector wins for a multi-value label.
        if (label % 2) {
            std::swap(query[2 * label], query[2 * label + 1]);
        }
        for (size_t v = 0; v < (multi ? 2u : 1u); ++v) {
            vectors[label].push_back(one_hot(2 * label + v));
            ASSERT_EQ(VecSimIndex_AddVector(index, vectors[label].back().data(), label), 1);
        }
    }

    auto check_queries = [&]() {
        std::vector<std::pair<double, size_t>> expected;
        size_t vector_count = 0;
        for (size_t label = 0; label < labels; ++label) {
            double best = std::numeric_limits<double>::infinity();
            for (const auto &vector : vectors[label]) {
                ++vector_count;
                double score = metric == VecSimMetric_L2 ? 0.0 : 1.0;
                for (size_t d = 0; d < dim; ++d) {
                    const double q = to_fp32(query[d]);
                    const double x = to_fp32(vector[d]);
                    score += metric == VecSimMetric_L2 ? (q - x) * (q - x) : -q * x;
                }
                best = std::min(best, score);
            }
            expected.emplace_back(best, label);
        }
        std::sort(expected.begin(), expected.end());
        ASSERT_EQ(VecSimIndex_IndexSize(index), vector_count);
        ASSERT_EQ(index->indexLabelCount(), labels);
        for (size_t i = 1; i < labels; ++i) {
            ASSERT_GT(expected[i].first - expected[i - 1].first, 0.01);
        }
        auto check_result = [&](size_t id, double score, size_t position) {
            ASSERT_LT(position, expected.size());
            EXPECT_EQ(id, expected[position].second);
            EXPECT_NEAR(score, expected[position].first, 1e-4);
        };

        runTopKSearchTest(index, query.data(), 5, check_result);
        const double radius = (expected[4].first + expected[5].first) / 2.0;
        runRangeQueryTest(index, query.data(), radius, check_result, 5, BY_SCORE);

        std::unique_ptr<VecSimBatchIterator, decltype(&VecSimBatchIterator_Free)> batch(
            VecSimBatchIterator_New(index, query.data(), nullptr), VecSimBatchIterator_Free);
        ASSERT_NE(batch, nullptr);
        for (size_t offset = 0; offset < labels; offset += 5) {
            ASSERT_TRUE(VecSimBatchIterator_HasNext(batch.get()));
            runBatchIteratorSearchTest(
                batch.get(), 5,
                [&](size_t id, double score, size_t position) {
                    check_result(id, score, offset + position);
                },
                BY_SCORE, std::min(size_t(5), labels - offset));
        }
        EXPECT_FALSE(VecSimBatchIterator_HasNext(batch.get()));
        runBatchIteratorSearchTest(batch.get(), 5, check_result, BY_SCORE, 0);
        VecSimBatchIterator_Reset(batch.get());
        EXPECT_TRUE(VecSimBatchIterator_HasNext(batch.get()));
        runBatchIteratorSearchTest(batch.get(), 5, check_result);
    };

    check_queries();
    // Move the worst label to the best position, replacing all its previous vectors.
    vectors[0] = {one_hot(24)};
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, vectors[0][0].data(), 1), VecSimUpdate_OK);
    check_queries();

    auto replacement = one_hot(25);
    ASSERT_EQ(VecSimIndex_AddVector(index, replacement.data(), 0), multi ? 1 : 0);
    if (multi) {
        vectors[0].push_back(replacement);
    } else {
        vectors[0] = {replacement};
    }
    check_queries();

    if (multi) {
        // Replacing two vectors by three exercises changing the label's cardinality in both
        // directions, after the earlier two-to-one replacement.
        vectors[0] = {one_hot(24), one_hot(26), one_hot(27)};
        std::vector<data_t> flat;
        for (const auto &vector : vectors[0]) {
            flat.insert(flat.end(), vector.begin(), vector.end());
        }
        ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, flat.data(), vectors[0].size()),
                  VecSimUpdate_OK);
        check_queries();
    }
}

TYPED_TEST(HNSWSQ8Test, FunctionalQueriesL2Single) {
    this->test_functional_queries(VecSimMetric_L2, false);
}
TYPED_TEST(HNSWSQ8Test, FunctionalQueriesL2Multi) {
    this->test_functional_queries(VecSimMetric_L2, true);
}
TYPED_TEST(HNSWSQ8Test, FunctionalQueriesIPSingle) {
    this->test_functional_queries(VecSimMetric_IP, false);
}
TYPED_TEST(HNSWSQ8Test, FunctionalQueriesIPMulti) {
    this->test_functional_queries(VecSimMetric_IP, true);
}

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_graph_routing(VecSimMetric metric, bool multi) {
    constexpr size_t labels = 256;
    HNSWParams params = {
        .dim = 19, .metric = metric, .multi = multi, .M = 8, .efConstruction = 80, .efRuntime = 8};
    SetUp(params);

    auto make_vector = [&](size_t label, size_t value) {
        std::vector<data_t> vector(dim);
        // Fixed extrema make SQ8 exact. Every label has a different binary payload and equal
        // norm, making its own vector the unique nearest result for both L2 and IP.
        vector[0] = ToDataType(-1.0f);
        vector[1] = ToDataType(1.0f);
        for (size_t bit = 0; bit < 8; ++bit) {
            vector[2 + 2 * bit] = vector[3 + 2 * bit] =
                ToDataType((label & (size_t(1) << bit)) ? 1.0f : -1.0f);
        }
        vector[18] = ToDataType(value ? 1.0f : -1.0f);
        return vector;
    };

    // Interleave distant cube vertices rather than building in geometric order.
    for (size_t i = 0; i < labels; ++i) {
        const size_t label = (73 * i) % labels;
        for (size_t value = 0; value < (multi ? 2u : 1u); ++value) {
            const auto vector = make_vector(label, value);
            ASSERT_EQ(VecSimIndex_AddVector(index, vector.data(), label), 1);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), labels * (multi ? 2 : 1));
    ASSERT_LT(params.efRuntime, labels / 16);
    for (size_t label = 0; label < labels; ++label) {
        SCOPED_TRACE(label);
        const auto query = make_vector(label, label % (multi ? 2 : 1));
        runTopKSearchTest(index, query.data(), 1, [&](size_t id, double score, size_t) {
            EXPECT_EQ(id, label);
            // All components have magnitude one. A different vector is at least 4 away in
            // squared L2, or 2 away in IP distance, well above the rounding tolerance.
            EXPECT_NEAR(score, metric == VecSimMetric_L2 ? 0.0 : 1.0 - double(dim), 1e-4);
        });
    }
}

TYPED_TEST(HNSWSQ8Test, GraphRoutingL2Single) { this->test_graph_routing(VecSimMetric_L2, false); }
TYPED_TEST(HNSWSQ8Test, GraphRoutingL2Multi) { this->test_graph_routing(VecSimMetric_L2, true); }
TYPED_TEST(HNSWSQ8Test, GraphRoutingIPSingle) { this->test_graph_routing(VecSimMetric_IP, false); }
TYPED_TEST(HNSWSQ8Test, GraphRoutingIPMulti) { this->test_graph_routing(VecSimMetric_IP, true); }

TYPED_TEST(HNSWSQ8Test, CreateIndex) { this->create_index_test(); }

TYPED_TEST(HNSWSQ8Test, SerializationRoundTripV5) {
    using data_t = typename TestFixture::data_t;
    constexpr size_t dim = 8;
    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .M = 8, .efConstruction = 40};
    this->SetUp(params);

    for (size_t i = 0; i < 3; ++i) {
        ASSERT_EQ(this->GenerateAndAddVector(10 + i, 0.5f + static_cast<float>(i), 0.1f), 1);
    }
    auto *original = this->CastToHNSW();
    ASSERT_NE(original, nullptr);

    TemporaryFile file{::testing::TempDir() + "hnsw_sq8_" +
                       std::string(VecSimType_ToString(TypeParam::get_index_type())) +
                       (TypeParam::with_quant_params ? "_mean" : "_no_mean") + ".hnsw"};

    original->saveIndex(file.path);
    std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)> loaded(
        HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
    ASSERT_NE(loaded, nullptr);
    auto *restored = dynamic_cast<HNSWIndex<data_t, float> *>(loaded.get());
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(restored->getVersion(), HNSWSerializer::EncodingVersion::V5);
    EXPECT_TRUE(restored->checkIntegrity().valid_state);
    EXPECT_EQ(restored->quantType, VecSimQuant_SQ8);
    EXPECT_TRUE(restored->usesQuantizedStorage());
    EXPECT_EQ(restored->serializedMeanVector, this->quantization_mean);
    EXPECT_EQ(restored->getStoredDataSize(), original->getStoredDataSize());
    ASSERT_EQ(VecSimIndex_IndexSize(loaded.get()), VecSimIndex_IndexSize(this->index));

    std::vector<data_t> query(dim);
    this->GenerateVector(query.data(), 1.5f, 0.1f);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(std::memcmp(original->getDataByInternalId(i), restored->getDataByInternalId(i),
                              original->getStoredDataSize()),
                  0);
        EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(loaded.get(), 10 + i, query.data()),
                        VecSimIndex_GetDistanceFrom_Unsafe(this->index, 10 + i, query.data()));
    }
    auto verify = [&](size_t id, double score, size_t) {
        EXPECT_EQ(id, 11u);
        EXPECT_FLOAT_EQ(score, VecSimIndex_GetDistanceFrom_Unsafe(this->index, id, query.data()));
    };
    runTopKSearchTest(loaded.get(), query.data(), 1, verify);
}

// Covers single and multi-value SQ8 snapshots that span several blocks, then keeps using and
// re-saving the loaded index to check that restored state is complete.
TYPED_TEST(HNSWSQ8Test, SerializationRoundTripV5MultiBlockAndReload) {
    using data_t = typename TestFixture::data_t;
    using index_ptr = std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)>;
    constexpr size_t dim = 8;
    constexpr size_t n = 50;
    constexpr size_t block_size = 16;
    constexpr size_t M = 8;
    constexpr size_t ef_construction = 40;
    constexpr size_t ef_runtime = 20;
    constexpr double epsilon = 0.02;

    std::mt19937 rng(47);
    std::uniform_real_distribution<float> distrib(-1.0f, 1.0f);
    auto random_vector = [&]() {
        std::vector<data_t> vector(dim);
        for (auto &value : vector) {
            value = TestFixture::ToDataType(distrib(rng));
        }
        return vector;
    };

    for (VecSimMetric metric : {VecSimMetric_L2, VecSimMetric_IP}) {
        for (bool multi : {false, true}) {
            SCOPED_TRACE(std::string(VecSimMetric_ToString(metric)) +
                         (multi ? " multi" : " single"));
            const size_t n_labels = multi ? n / 2 : n;
            if (this->index) {
                VecSimIndex_Free(this->index);
                this->index = nullptr;
            }
            HNSWParams params = {.dim = dim,
                                 .metric = metric,
                                 .multi = multi,
                                 .blockSize = block_size,
                                 .M = M,
                                 .efConstruction = ef_construction,
                                 .efRuntime = ef_runtime,
                                 .epsilon = epsilon};
            this->SetUp(params);
            for (size_t i = 0; i < n; ++i) {
                ASSERT_EQ(VecSimIndex_AddVector(this->index, random_vector().data(), i % n_labels),
                          1);
            }
            const std::vector<data_t> query = random_vector();

            // Compares everything a snapshot must preserve, including graph storage layout.
            auto expect_same_index = [&](VecSimIndex *expected, VecSimIndex *actual) {
                auto *expected_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(expected);
                auto *actual_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(actual);
                ASSERT_NE(expected_hnsw, nullptr);
                ASSERT_NE(actual_hnsw, nullptr);
                EXPECT_EQ(actual_hnsw->getVersion(), HNSWSerializer::EncodingVersion::V5);
                EXPECT_TRUE(actual_hnsw->checkIntegrity().valid_state);
                EXPECT_EQ(actual_hnsw->quantType, VecSimQuant_SQ8);
                EXPECT_TRUE(actual_hnsw->usesQuantizedStorage());
                EXPECT_EQ(actual_hnsw->serializedMeanVector, this->quantization_mean);

                VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(actual);
                EXPECT_EQ(info.commonInfo.basicInfo.type, TypeParam::get_index_type());
                EXPECT_EQ(info.commonInfo.basicInfo.dim, dim);
                EXPECT_EQ(info.commonInfo.basicInfo.metric, metric);
                EXPECT_EQ(info.commonInfo.basicInfo.isMulti, multi);
                EXPECT_EQ(info.commonInfo.basicInfo.blockSize, block_size);
                EXPECT_EQ(info.hnswInfo.M, M);
                EXPECT_EQ(info.hnswInfo.efConstruction, ef_construction);
                EXPECT_EQ(info.hnswInfo.efRuntime, ef_runtime);
                EXPECT_EQ(info.hnswInfo.epsilon, epsilon);
                EXPECT_EQ(info.commonInfo.indexSize, VecSimIndex_IndexSize(expected));
                EXPECT_EQ(info.commonInfo.indexLabelCount,
                          VecSimIndex_DebugInfo(expected).commonInfo.indexLabelCount);

                const size_t size = VecSimIndex_IndexSize(expected);
                ASSERT_EQ(VecSimIndex_IndexSize(actual), size);
                for (idType id = 0; id < size; ++id) {
                    EXPECT_EQ(actual_hnsw->getExternalLabel(id),
                              expected_hnsw->getExternalLabel(id));
                    EXPECT_EQ(std::memcmp(actual_hnsw->getDataByInternalId(id),
                                          expected_hnsw->getDataByInternalId(id),
                                          expected_hnsw->getStoredDataSize()),
                              0);
                }

                constexpr size_t k = 10;
                VecSimQueryReply *expected_res =
                    VecSimIndex_TopKQuery(expected, query.data(), k, nullptr, BY_SCORE);
                VecSimQueryReply *actual_res =
                    VecSimIndex_TopKQuery(actual, query.data(), k, nullptr, BY_SCORE);
                ASSERT_EQ(VecSimQueryReply_Len(actual_res), VecSimQueryReply_Len(expected_res));
                auto *expected_it = VecSimQueryReply_GetIterator(expected_res);
                auto *actual_it = VecSimQueryReply_GetIterator(actual_res);
                while (VecSimQueryReply_IteratorHasNext(expected_it)) {
                    auto *e = VecSimQueryReply_IteratorNext(expected_it);
                    auto *a = VecSimQueryReply_IteratorNext(actual_it);
                    EXPECT_EQ(VecSimQueryResult_GetId(a), VecSimQueryResult_GetId(e));
                    EXPECT_EQ(VecSimQueryResult_GetScore(a), VecSimQueryResult_GetScore(e));
                }
                VecSimQueryReply_IteratorFree(expected_it);
                VecSimQueryReply_IteratorFree(actual_it);
                VecSimQueryReply_Free(expected_res);
                VecSimQueryReply_Free(actual_res);
            };

            TemporaryFile file{::testing::TempDir() + "hnsw_sq8_reload_" +
                               std::string(VecSimType_ToString(TypeParam::get_index_type())) +
                               (TypeParam::with_quant_params ? "_mean_" : "_no_mean_") +
                               VecSimMetric_ToString(metric) + (multi ? "_multi" : "_single") +
                               ".hnsw"};
            ASSERT_GT(n, 2 * block_size); // The graph must span several storage blocks.
            this->CastToHNSW()->saveIndex(file.path);
            index_ptr loaded(HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
            ASSERT_NE(loaded, nullptr);
            expect_same_index(this->index, loaded.get());

            // Keep using the loaded index: add a new label, add another vector under an existing
            // label (repeated label for multi, overwrite for single), and delete a label.
            ASSERT_EQ(VecSimIndex_AddVector(loaded.get(), random_vector().data(), n_labels), 1);
            ASSERT_EQ(VecSimIndex_AddVector(loaded.get(), random_vector().data(), 0),
                      multi ? 1 : 0);
            const size_t vectors_per_label = multi ? n / n_labels : 1;
            ASSERT_EQ(VecSimIndex_DeleteVector(loaded.get(), 1), vectors_per_label);
            auto *loaded_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(loaded.get());
            ASSERT_NE(loaded_hnsw, nullptr);
            EXPECT_TRUE(loaded_hnsw->checkIntegrity().valid_state);
            EXPECT_EQ(VecSimIndex_IndexSize(loaded.get()),
                      n + 1 + (multi ? 1 : 0) - vectors_per_label);

            // A second save/load cycle of the modified index must preserve it exactly.
            loaded_hnsw->saveIndex(file.path);
            index_ptr reloaded(HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
            ASSERT_NE(reloaded, nullptr);
            expect_same_index(loaded.get(), reloaded.get());
        }
    }
}

// Saved SQ8 snapshots with an unknown quantizer or an element type SQ8 does not support must be
// rejected instead of being interpreted with the wrong vector layout.
TYPED_TEST(HNSWSQ8Test, LoadRejectsUnsupportedQuantizedSnapshot) {
    HNSWParams params = {.dim = 4, .metric = VecSimMetric_L2, .M = 8, .efConstruction = 40};
    this->SetUp(params);
    ASSERT_EQ(this->GenerateAndAddVector(0, 0.25f, 0.1f), 1);
    ASSERT_EQ(this->GenerateAndAddVector(1, 0.75f, 0.1f), 1);

    TemporaryFile file{::testing::TempDir() + "hnsw_sq8_reject_" +
                       std::string(VecSimType_ToString(TypeParam::get_index_type())) +
                       (TypeParam::with_quant_params ? "_mean" : "_no_mean") + ".hnsw"};

    // Field offsets follow HNSWSerializer::saveIndex and HNSWIndex::saveIndexFields.
    constexpr std::streamoff type_offset =
        sizeof(HNSWSerializer::EncodingVersion) + sizeof(VecSimAlgo) + sizeof(size_t);
    constexpr std::streamoff quant_type_offset = type_offset + sizeof(VecSimType) +
                                                 sizeof(VecSimMetric) + sizeof(size_t) +
                                                 sizeof(bool) + sizeof(size_t);

    using raw_quant_type_t = std::underlying_type_t<VecSimQuantType>;
    auto save_and_patch = [&](std::streamoff offset, auto value) {
        this->CastToHNSW()->saveIndex(file.path);
        std::fstream stream(file.path, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(stream.is_open());
        // Check the offset points at the expected field before overwriting it.
        decltype(value) original{};
        stream.seekg(offset);
        Serializer::readBinaryPOD(stream, original);
        if constexpr (std::is_same_v<decltype(value), raw_quant_type_t>) {
            ASSERT_EQ(original, static_cast<raw_quant_type_t>(VecSimQuant_SQ8));
        } else {
            ASSERT_EQ(original, TypeParam::get_index_type());
        }
        stream.seekp(offset);
        Serializer::writeBinaryPOD(stream, value);
    };
    auto expect_rejected = [&]() {
        std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)> loaded(
            HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
        EXPECT_EQ(loaded, nullptr);
    };

    // The unmodified snapshot loads.
    this->CastToHNSW()->saveIndex(file.path);
    {
        std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)> loaded(
            HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
        ASSERT_NE(loaded, nullptr);
    }

    // Written as raw bytes: an out-of-range value is not a valid VecSimQuantType.
    save_and_patch(quant_type_offset, static_cast<raw_quant_type_t>(VecSimQuant_SQ8 + 1));
    expect_rejected();

    for (VecSimType unsupported_type :
         {VecSimType_FLOAT64, VecSimType_BFLOAT16, VecSimType_INT8, VecSimType_UINT8}) {
        SCOPED_TRACE(VecSimType_ToString(unsupported_type));
        save_and_patch(type_offset, unsupported_type);
        expect_rejected();
    }
}

TYPED_TEST(HNSWSQ8Test, NormalizedCosineSerializationRoundTripV5) {
    using data_t = typename TestFixture::data_t;
    using index_ptr = std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)>;
    constexpr size_t dim = 4;
    constexpr size_t count = 3;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .M = 8,
                         .efConstruction = 40,
                         .quantType = VecSimQuant_SQ8};

    std::vector<float> mean;
    if constexpr (TypeParam::with_quant_params) {
        mean.assign(dim, TestFixture::quantization_mean_value);
        params.quantParams = mean.data();
    }

    // The SQ8 Cosine backend assumes these vectors are already normalized and uses IP internally.
    const std::vector<std::vector<float>> normalized_vectors = {
        {1.0f, 0.0f, 0.0f, 0.0f},
        {0.0f, 1.0f, 0.0f, 0.0f},
        {-1.0f, 0.0f, 0.0f, 0.0f},
    };
    VecSimParams vecsim_params = CreateParams(params);
    index_ptr original(HNSWFactory::NewIndex(&vecsim_params, true), VecSimIndex_Free);
    ASSERT_NE(original, nullptr);
    for (size_t label = 0; label < count; ++label) {
        std::vector<data_t> vector(dim);
        for (size_t d = 0; d < dim; ++d) {
            vector[d] = TestFixture::ToDataType(normalized_vectors[label][d]);
        }
        ASSERT_EQ(VecSimIndex_AddVector(original.get(), vector.data(), label), 1);
    }

    TemporaryFile file{::testing::TempDir() + "hnsw_sq8_normalized_cosine_" +
                       std::string(VecSimType_ToString(TypeParam::get_index_type())) +
                       (TypeParam::with_quant_params ? "_mean" : "_no_mean") + ".hnsw"};

    auto *original_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(original.get());
    ASSERT_NE(original_hnsw, nullptr);
    original_hnsw->saveIndex(file.path);

    // Default loading remains strict; only callers that guarantee normalized input opt in.
    index_ptr default_loaded(HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
    EXPECT_EQ(default_loaded, nullptr);

    index_ptr loaded(HNSWFactory::NewIndex(file.path, true), VecSimIndex_Free);
    ASSERT_NE(loaded, nullptr);
    auto *loaded_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(loaded.get());
    ASSERT_NE(loaded_hnsw, nullptr);
    EXPECT_TRUE(loaded_hnsw->checkIntegrity().valid_state);
    EXPECT_EQ(loaded_hnsw->quantType, VecSimQuant_SQ8);
    EXPECT_EQ(loaded_hnsw->serializedMeanVector, mean);
    ASSERT_EQ(VecSimIndex_IndexSize(loaded.get()), count);

    // Unit queries stay normalized. Cosine distances are 1 - dot(query, vector).
    std::vector<data_t> query(dim, TestFixture::ToDataType(0.0f));
    query[0] = TestFixture::ToDataType(1.0f);
    for (size_t label = 0; label < count; ++label) {
        const double expected_distance = static_cast<double>(label);
        EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(original.get(), label, query.data()),
                    expected_distance, 0.05);
        EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(loaded.get(), label, query.data()),
                    expected_distance, 0.05);
    }
}

TYPED_TEST(HNSWSQ8Test, RejectStandaloneCosine) {
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = 4,
                         .metric = VecSimMetric_Cosine,
                         .quantType = VecSimQuant_SQ8};
    if constexpr (TypeParam::with_quant_params) {
        this->quantization_mean.assign(params.dim, this->quantization_mean_value);
        params.quantParams = this->quantization_mean.data();
    }

    VecSimParams vecsim_params = CreateParams(params);
    this->index = VecSimIndex_New(&vecsim_params);
    EXPECT_EQ(this->index, nullptr);

    EXPECT_EQ(VecSimIndex_EstimateInitialSize(&vecsim_params), SIZE_MAX);
}

// Size estimation

TYPED_TEST(HNSWSQ8Test, SizeEstimation) {
    constexpr size_t block_size = 256;
    HNSWParams params = {.dim = 128, .blockSize = block_size, .M = 64};
    this->SetUp(params);

    // SetUp fills the data type and SQ8 settings used by the estimator.
    EXPECT_EQ(EstimateInitialSize(params), this->index->getAllocationSize());

    size_t label = 0;
    while (this->index->indexSize() < 200 || this->index->indexSize() % block_size != 0) {
        ASSERT_EQ(this->GenerateAndAddVector(label, static_cast<float>(label)), 1);
        label++;
    }

    // Measure the allocation caused by growing the index by one block.
    const size_t estimation = EstimateElementSize(params) * block_size;
    const size_t before = this->index->getAllocationSize();
    ASSERT_EQ(this->GenerateAndAddVector(label, static_cast<float>(label)), 1);
    const size_t actual = this->index->getAllocationSize() - before;

    EXPECT_GE(estimation, actual * 0.99);
    EXPECT_LE(estimation, actual * 1.01);
}

// Search behavior

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::search_by_id_test() {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200, .efRuntime = 100};
    SetUp(params);

    for (size_t i = 0; i < 100; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i)), 1);
    }

    data_t query[4];
    GenerateVector(query, 50.0f);
    // BY_ID returns the 11 nearest labels (45 through 55) in label order.
    static constexpr size_t expected[] = {45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55};
    auto verify = [&](size_t id, double score, size_t result_index) {
        EXPECT_EQ(id, expected[result_index]);
        EXPECT_FLOAT_EQ(score, 4.0f * (50.0f - id) * (50.0f - id));
    };
    runTopKSearchTest(index, query, std::size(expected), verify, nullptr, BY_ID);
}

TYPED_TEST(HNSWSQ8Test, SearchByID) { this->search_by_id_test(); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::search_by_score_test() {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200, .efRuntime = 100};
    SetUp(params);

    for (size_t i = 0; i < 100; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i)), 1);
    }

    data_t query[4];
    GenerateVector(query, 50.0f);
    // BY_SCORE orders labels by their distance from label 50.
    static constexpr size_t expected[] = {50, 49, 51, 48, 52, 47, 53, 46, 54, 45, 55};
    auto verify = [&](size_t id, double score, size_t result_index) {
        EXPECT_EQ(id, expected[result_index]);
        EXPECT_FLOAT_EQ(score, 4.0f * (50.0f - id) * (50.0f - id));
    };
    runTopKSearchTest(index, query, std::size(expected), verify);
}

TYPED_TEST(HNSWSQ8Test, SearchByScore) { this->search_by_score_test(); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::search_empty_index_test() {
    HNSWParams params = {.dim = 4, .initialCapacity = 0};
    SetUp(params);

    data_t query[4];
    GenerateVector(query, 50.0f);

    VecSimQueryReply *reply = VecSimIndex_TopKQuery(index, query, 11, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(reply), 0u);
    VecSimQueryReply_Free(reply);

    reply = VecSimIndex_RangeQuery(index, query, 1.0, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(reply), 0u);
    VecSimQueryReply_Free(reply);

    for (size_t i = 0; i < 100; i++) {
        GenerateAndAddVector(i, static_cast<float>(i));
    }
    for (size_t i = 0; i < 100; i++) {
        VecSimIndex_DeleteVector(index, i);
    }

    reply = VecSimIndex_TopKQuery(index, query, 11, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(reply), 0u);
    VecSimQueryReply_Free(reply);

    reply = VecSimIndex_RangeQuery(index, query, 1.0, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(reply), 0u);
    VecSimQueryReply_Free(reply);
}

TYPED_TEST(HNSWSQ8Test, SearchEmptyIndex) { this->search_empty_index_test(); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_override() {
    constexpr size_t count = 250;
    // Scale the values to keep every distance inside the FP16 range. FP16 tops out at 65504 and
    // L2² = dim × diff², so unscaled labels would need 4 × 250² = 250000. A vector that is still
    // waiting in an FP16 flat buffer is compared by an FP16 kernel, which accumulates in half
    // precision, so an out-of-range distance saturates to infinity there. With scale = 0.1 the
    // largest distance is 4 × (250 × 0.1)² = 10000.
    constexpr float scale = 0.1f;
    HNSWParams params = {
        .dim = 4, .initialCapacity = 100, .M = 8, .efConstruction = 20, .efRuntime = count};
    SetUp(params);

    for (size_t i = 0; i < 100; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i) * scale), 1);
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i) * scale), 0);
    }
    for (size_t i = 100; i < count; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i) * scale), 1);
    }

    data_t query[4];
    GenerateVector(query, static_cast<float>(count) * scale);
    // Distance decreases as the label increases, so results are in descending label order.
    const float query_value = to_fp32<data_t>(ToDataType(static_cast<float>(count) * scale));
    auto verify = [&](size_t id, double score, size_t result_index) {
        EXPECT_EQ(id, count - result_index - 1);
        // Compare against the values as they are stored: FP16 cannot hold 0.1 × i exactly, and
        // half-precision accumulation drops a few more bits, hence the relative tolerance.
        const float diff =
            query_value - to_fp32<data_t>(ToDataType(static_cast<float>(id) * scale));
        const float expected = 4.0f * diff * diff;
        EXPECT_NEAR(score, expected, 1e-4f + expected * 0.002f);
    };
    runTopKSearchTest(index, query, count, verify);
}

TYPED_TEST(HNSWSQ8Test, Override) { this->test_override(); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_range_query() {
    constexpr size_t count = 100;
    constexpr size_t close_count = 20;
    HNSWParams params = {.dim = 4, .initialCapacity = count, .efRuntime = count};
    SetUp(params);

    constexpr float pivot = 1.0f;
    constexpr float value_radius = 1.5f;
    std::mt19937 generator(42);
    std::uniform_real_distribution<float> distribution(pivot - value_radius, pivot + value_radius);
    for (size_t i = 0; i < close_count; i++) {
        GenerateAndAddVector(i, distribution(generator));
    }
    for (size_t i = close_count; i < count; i++) {
        GenerateAndAddVector(i, 5.0f + distribution(generator));
    }

    data_t query[4];
    GenerateVector(query, pivot);
    constexpr double max_distance = 4.0 * value_radius * value_radius;
    auto verify = [&](size_t id, double score, size_t) {
        EXPECT_LT(id, close_count);
        EXPECT_LE(score, max_distance);
    };
    runRangeQueryTest(index, query, max_distance, verify, close_count, BY_SCORE);
}

TYPED_TEST(HNSWSQ8Test, RangeQuery) { this->test_range_query(); }

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_get_distance(VecSimMetric metric, bool multi) {
    HNSWParams params = {.dim = 4, .metric = metric, .initialCapacity = 1};
    params.multi = multi;
    SetUp(params);

    ASSERT_EQ(GenerateAndAddVector(0, 0.25f, 0.25f), 1);
    if (multi) {
        ASSERT_EQ(GenerateAndAddVector(0, -2.0f, 0.25f), 1);
    }
    data_t query[4];
    GenerateVector(query, 0.5f, 0.25f);

    const double expected = metric == VecSimMetric_L2 ? 0.25 : -1.5;
    EXPECT_NEAR(VecSimIndex_GetDistanceFrom_Unsafe(index, 0, query), expected, 1e-5);
    EXPECT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(index, 1, query)));
}

TYPED_TEST(HNSWSQ8Test, GetDistanceL2) { this->test_get_distance(VecSimMetric_L2, false); }
TYPED_TEST(HNSWSQ8Test, GetDistanceIP) { this->test_get_distance(VecSimMetric_IP, false); }
TYPED_TEST(HNSWSQ8Test, GetDistanceMultiL2) { this->test_get_distance(VecSimMetric_L2, true); }
TYPED_TEST(HNSWSQ8Test, GetDistanceMultiIP) { this->test_get_distance(VecSimMetric_IP, true); }

// Batch iteration

template <typename index_type_t>
void HNSWSQ8Test<index_type_t>::test_batch_iterator_basic() {
    constexpr size_t count = 250;
    constexpr size_t batch_size = 5;
    // Scaled for the same reason as test_override: an FP16 kernel saturates a distance above
    // 65504 to infinity, which would leave the farthest labels tied and unordered.
    constexpr float scale = 0.1f;
    HNSWParams params = {
        .dim = 4, .initialCapacity = count, .M = 8, .efConstruction = 20, .efRuntime = count};
    SetUp(params);

    // Store [i, i, i, i] * scale under label i.
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, static_cast<float>(i) * scale), 1);
    }

    data_t query[4];
    GenerateVector(query, static_cast<float>(count) * scale);
    VecSimBatchIterator *iterator = VecSimBatchIterator_New(index, query, nullptr);
    ASSERT_NE(iterator, nullptr);

    // Each batch contains the five largest remaining labels, ordered by distance.
    size_t iteration = 0;
    while (VecSimBatchIterator_HasNext(iterator)) {
        auto verify = [&](size_t id, double, size_t result_index) {
            EXPECT_EQ(id, count - iteration * batch_size - result_index - 1);
        };
        runBatchIteratorSearchTest(iterator, batch_size, verify);
        iteration++;
    }
    EXPECT_EQ(iteration, count / batch_size);
    VecSimBatchIterator_Free(iterator);
}

TYPED_TEST(HNSWSQ8Test, BatchIteratorBasic) { this->test_batch_iterator_basic(); }

// Deferred mean initialization

// Tiered accumulation constructs the SQ8 backend with a zero mean and installs the learned mean
// through `setQuantizationMean()` before the first insertion. The contract under test is that the
// setter alone is enough: an index that received its mean this way must be indistinguishable from
// one constructed with the same mean, in stored bytes, query preprocessing, both distance modes,
// search results, and label distances. Standalone SQ8 rejects cosine, so the tiered cosine path
// is covered through its resolved metric, IP.
TYPED_TEST(HNSWSQ8Test, SetQuantizationMeanMatchesConstructedMean) {
    using data_t = typename TestFixture::data_t;
    using index_ptr = std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)>;
    constexpr size_t dim = 37; // Not a multiple of any SIMD width, so kernel tails run as well.
    constexpr size_t count = 64;
    constexpr size_t k = 10;

    for (auto metric : {VecSimMetric_L2, VecSimMetric_IP}) {
        SCOPED_TRACE(metric == VecSimMetric_L2 ? "L2" : "IP");
        std::vector<float> mean(dim);
        for (size_t d = 0; d < dim; d++) {
            mean[d] = 0.1f * static_cast<float>(d % 7) - 0.3f;
        }
        const std::vector<float> zero_mean(dim, 0.0f);

        HNSWParams params = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = metric,
                             .initialCapacity = count,
                             .M = 16,
                             .efConstruction = 100,
                             .efRuntime = count,
                             .quantType = VecSimQuant_SQ8,
                             .quantParams = mean.data()};
        // The indexes are declared before the preprocessed blobs below: the blobs are released
        // through the index allocators, so they must go out of scope first.
        VecSimParams reference_params = CreateParams(params);
        index_ptr reference(VecSimIndex_New(&reference_params), VecSimIndex_Free);
        ASSERT_NE(reference, nullptr);

        params.quantParams = zero_mean.data();
        VecSimParams deferred_params = CreateParams(params);
        index_ptr deferred(VecSimIndex_New(&deferred_params), VecSimIndex_Free);
        ASSERT_NE(deferred, nullptr);

        auto *reference_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(reference.get());
        auto *deferred_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(deferred.get());
        ASSERT_NE(reference_hnsw, nullptr);
        ASSERT_NE(deferred_hnsw, nullptr);
        ASSERT_TRUE(deferred_hnsw->usesQuantizedStorage());
        ASSERT_EQ(deferred_hnsw->getStoredDataSize(), reference_hnsw->getStoredDataSize());
        const size_t stored_size = reference_hnsw->getStoredDataSize();

        deferred_hnsw->setQuantizationMean(mean);

        // Component level: identical stored blobs and identical distances in both modes.
        std::vector<data_t> x(dim), y(dim);
        for (size_t d = 0; d < dim; d++) {
            x[d] = TestFixture::ToDataType(0.2f + 0.1f * static_cast<float>(d % 5));
            y[d] = TestFixture::ToDataType(-0.3f + 0.07f * static_cast<float>(d % 7));
        }
        auto reference_x = reference_hnsw->preprocessForStorage(x.data());
        auto reference_y = reference_hnsw->preprocessForStorage(y.data());
        auto reference_query = reference_hnsw->preprocessQuery(y.data());
        auto deferred_x = deferred_hnsw->preprocessForStorage(x.data());
        auto deferred_y = deferred_hnsw->preprocessForStorage(y.data());
        auto deferred_query = deferred_hnsw->preprocessQuery(y.data());
        EXPECT_EQ(std::memcmp(reference_x.get(), deferred_x.get(), stored_size), 0);
        EXPECT_EQ(std::memcmp(reference_y.get(), deferred_y.get(), stored_size), 0);
        EXPECT_EQ(reference_hnsw->calcDistance(reference_x.get(), reference_y.get()),
                  deferred_hnsw->calcDistance(deferred_x.get(), deferred_y.get()));
        EXPECT_EQ(reference_hnsw->calcDistanceForQuery(reference_x.get(), reference_query.get()),
                  deferred_hnsw->calcDistanceForQuery(deferred_x.get(), deferred_query.get()));

        // Index level: the same insertion order yields the same internal ids and, with the same
        // random seed, the same graph. Stored elements must then match byte for byte.
        std::vector<data_t> vec(dim);
        for (size_t i = 0; i < count; i++) {
            for (size_t d = 0; d < dim; d++) {
                vec[d] = TestFixture::ToDataType(0.05f * static_cast<float>((i * 7 + d * 3) % 41) -
                                                 1.0f);
            }
            ASSERT_EQ(VecSimIndex_AddVector(reference.get(), vec.data(), i), 1);
            ASSERT_EQ(VecSimIndex_AddVector(deferred.get(), vec.data(), i), 1);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(deferred.get()), count);
        for (size_t id = 0; id < count; id++) {
            EXPECT_EQ(std::memcmp(reference_hnsw->getDataByInternalId(id),
                                  deferred_hnsw->getDataByInternalId(id), stored_size),
                      0)
                << "internal id " << id;
        }

        // Search level: identical top-k ids and scores, and identical label distances.
        auto collect = [](VecSimQueryReply *reply) {
            std::vector<std::pair<int64_t, double>> out;
            auto *iterator = VecSimQueryReply_GetIterator(reply);
            while (VecSimQueryReply_IteratorHasNext(iterator)) {
                auto *item = VecSimQueryReply_IteratorNext(iterator);
                out.emplace_back(VecSimQueryResult_GetId(item), VecSimQueryResult_GetScore(item));
            }
            VecSimQueryReply_IteratorFree(iterator);
            VecSimQueryReply_Free(reply);
            return out;
        };
        std::vector<data_t> query(dim);
        for (size_t q = 0; q < 5; q++) {
            for (size_t d = 0; d < dim; d++) {
                query[d] = TestFixture::ToDataType(
                    0.04f * static_cast<float>((q * 11 + d * 5) % 47) - 0.9f);
            }
            auto expected =
                collect(VecSimIndex_TopKQuery(reference.get(), query.data(), k, nullptr, BY_SCORE));
            auto actual =
                collect(VecSimIndex_TopKQuery(deferred.get(), query.data(), k, nullptr, BY_SCORE));
            ASSERT_EQ(expected.size(), k);
            EXPECT_EQ(actual, expected) << "query " << q;
            for (size_t label = 0; label < count; label += 9) {
                EXPECT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(reference.get(), label, query.data()),
                          VecSimIndex_GetDistanceFrom_Unsafe(deferred.get(), label, query.data()))
                    << "label " << label;
            }
        }

        TemporaryFile file{::testing::TempDir() + "hnsw_sq8_set_mean_" +
                           std::string(VecSimType_ToString(TypeParam::get_index_type())) +
                           (TypeParam::with_quant_params ? "_mean" : "_no_mean") +
                           (metric == VecSimMetric_L2 ? "_l2.hnsw" : "_ip.hnsw")};
        deferred_hnsw->saveIndex(file.path);
        index_ptr restored(HNSWFactory::NewIndex(file.path), VecSimIndex_Free);
        ASSERT_NE(restored, nullptr);
        auto *restored_hnsw = dynamic_cast<HNSWIndex<data_t, float> *>(restored.get());
        ASSERT_NE(restored_hnsw, nullptr);
        EXPECT_EQ(restored_hnsw->serializedMeanVector, mean);
        for (size_t label = 0; label < count; label++) {
            EXPECT_FLOAT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(restored.get(), label, query.data()),
                            VecSimIndex_GetDistanceFrom_Unsafe(deferred.get(), label, query.data()))
                << "serialized label " << label;
        }
    }
}

// SQ8 kernels support only FLOAT32 and FLOAT16 input vectors.
TEST(HNSWSQ8ParamsTest, RejectsUnsupportedDataType) {
    for (auto type : {VecSimType_FLOAT64, VecSimType_BFLOAT16, VecSimType_INT8, VecSimType_UINT8}) {
        HNSWParams hnsw_params = {
            .type = type, .dim = 4, .metric = VecSimMetric_L2, .quantType = VecSimQuant_SQ8};
        VecSimParams params = CreateParams(hnsw_params);

        EXPECT_EQ(VecSimIndex_New(&params), nullptr) << "data type " << type;
        EXPECT_EQ(EstimateInitialSize(hnsw_params), SIZE_MAX) << "data type " << type;
    }
}

// Value 3 has no VecSimMetric enumerator but is within the enum's representable range, so the
// factory must reject it before dispatch.
TEST(HNSWSQ8ParamsTest, RejectsOutOfRangeMetric) {
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = 4,
                              .metric = static_cast<VecSimMetric>(3),
                              .quantType = VecSimQuant_SQ8};
    VecSimParams params = CreateParams(hnsw_params);

    EXPECT_EQ(VecSimIndex_New(&params), nullptr);
    EXPECT_EQ(EstimateInitialSize(hnsw_params), SIZE_MAX);
}

// Exercise both endpoints, a mean not representable in FP16, and odd dimensions that
// also cover SIMD residual handling.
TEST(HNSWSQ8ParamsTest, MeanCenteredFP16L2BoundedValues) {
    using data_t = vecsim_types::float16;
    constexpr size_t count = 32;
    for (size_t dim : {4, 65}) {
        for (float mean_value : {-1.0f, 0.10001f, 1.0f}) {
            for (bool multi : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "dim=" << dim << " mean=" << mean_value << " multi=" << multi);
                std::vector<float> mean(dim, mean_value);
                HNSWParams hnsw_params = {.type = VecSimType_FLOAT16,
                                          .dim = dim,
                                          .metric = VecSimMetric_L2,
                                          .multi = multi,
                                          .efConstruction = count,
                                          .efRuntime = count,
                                          .quantType = VecSimQuant_SQ8,
                                          .quantParams = mean.data()};
                VecSimParams params = CreateParams(hnsw_params);
                std::unique_ptr<VecSimIndex, decltype(&VecSimIndex_Free)> index(
                    VecSimIndex_New(&params), VecSimIndex_Free);
                ASSERT_NE(index, nullptr);
                EXPECT_EQ(EstimateInitialSize(hnsw_params), index->getAllocationSize());

                std::mt19937 generator(42);
                std::uniform_real_distribution<float> distribution(-1.0f, 1.0f);
                std::vector<std::vector<data_t>> vectors(count, std::vector<data_t>(dim));
                for (size_t v = 0; v < count; ++v) {
                    for (size_t d = 0; d < dim; ++d) {
                        const float value =
                            v < 2 ? (v == 0 ? -1.0f : 1.0f) : distribution(generator);
                        vectors[v][d] = vecsim_types::FP32_to_FP16(value);
                    }
                    ASSERT_EQ(
                        VecSimIndex_AddVector(index.get(), vectors[v].data(), multi ? v / 2 : v),
                        1);
                }
                for (size_t v = 0; v < count; ++v) {
                    const auto verify = [&](size_t id, double score, size_t) {
                        EXPECT_EQ(id, multi ? v / 2 : v);
                        EXPECT_TRUE(std::isfinite(score));
                        // SQ8 reconstruction contributes to self-distance.
                        EXPECT_NEAR(score, 0.0, dim * 1e-4);
                    };
                    runTopKSearchTest(index.get(), vectors[v].data(), 1, verify);
                }
            }
        }
    }
}

// Exercise graph construction's stored-to-stored IP kernel with non-degenerate quantization ranges.
TYPED_TEST(HNSWSQ8Test, GraphConstructionIP) {
    constexpr size_t n = 100;
    constexpr size_t dim = 16;
    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_IP, .initialCapacity = n, .M = 16, .efRuntime = n};
    this->SetUp(params);

    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(i, static_cast<float>(i) * 0.5f, 0.25f), 1);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(this->index), n);

    // For inner-product distance (1 - dot product), these positive vectors rank by descending
    // label.
    std::vector<typename TestFixture::data_t> query(dim);
    this->GenerateVector(query.data(), 1.0f, 0.25f);

    auto verify = [&](size_t id, double, size_t result_index) {
        EXPECT_EQ(id, n - 1 - result_index);
    };
    runTopKSearchTest(this->index, query.data(), 10, verify);
}

/* ---------------------------- Tiered HNSW tests ---------------------------- */

using HNSWSQ8TieredDataTypeSet =
    ::testing::Types<HNSWSQ8IndexType<VecSimType_FLOAT32, float, false>,
                     HNSWSQ8IndexType<VecSimType_FLOAT32, float, true>,
                     HNSWSQ8IndexType<VecSimType_FLOAT16, vecsim_types::float16, false>,
                     HNSWSQ8IndexType<VecSimType_FLOAT16, vecsim_types::float16, true>>;

template <typename index_type_t>
class SQ8TieredHNSWTest : public HNSWSQ8Test<index_type_t> {
public:
    using data_t = typename index_type_t::data_t;

    void create_index_test();
    void frontend_size_estimation_test(VecSimMetric metric);
    void destruction_releases_pending_migration_jobs_test();

protected:
    static constexpr size_t normalization_set_size = 10;

    void SetUp(HNSWParams &hnsw_params) override { SetUp(hnsw_params, 0); }

    void SetUp(HNSWParams &hnsw_params, size_t flat_buffer_limit) {
        hnsw_params.type = index_type_t::get_index_type();
        hnsw_params.quantType = VecSimQuant_SQ8;
        VecSimParams vecsim_hnsw_params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = flat_buffer_limit,
            .primaryIndexParams = &vecsim_hnsw_params,
            .specificParams = {TieredHNSWParams{
                .QuantNormalizationSetSize =
                    index_type_t::with_quant_params ? normalization_set_size : 0}}};
        VecSimParams vecsim_params = CreateParams(tiered_params);
        this->index = VecSimIndex_New(&vecsim_params);
        ASSERT_NE(this->index, nullptr);
        this->dim = hnsw_params.dim;
        mock_thread_pool.ctx->index_strong_ref.reset(this->index);
    }

    void TearDown() override {}

    HNSWIndex<data_t, float> *CastToHNSW() override {
        auto *tiered_index = dynamic_cast<TieredHNSWIndex<data_t, float> *>(this->index);
        return tiered_index ? tiered_index->getHNSWIndex() : nullptr;
    }

    tieredIndexMock mock_thread_pool;
};

template <typename index_type_t>
void SQ8TieredHNSWTest<index_type_t>::create_index_test() {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_IP, .M = 16, .efConstruction = 200};
    SetUp(params);

    ASSERT_EQ(VecSimIndex_IndexSize(this->index), 0u);
    for (size_t label = 0; label < 100; label++) {
        ASSERT_EQ(this->GenerateAndAddVector(label, static_cast<float>(label), 1.0f), 1);
        ASSERT_EQ(VecSimIndex_IndexSize(this->index), label + 1);
    }
    EXPECT_EQ(this->index->basicInfo().type, index_type_t::get_index_type());
    EXPECT_TRUE(this->index->basicInfo().isTiered);
}

TYPED_TEST_SUITE(SQ8TieredHNSWTest, HNSWSQ8TieredDataTypeSet);

TYPED_TEST(SQ8TieredHNSWTest, CreateIndex) { this->create_index_test(); }

TYPED_TEST(SQ8TieredHNSWTest, SizeEstimation) {
    constexpr size_t block_size = DEFAULT_BLOCK_SIZE;
    HNSWParams hnsw_params = {
        .dim = 16, .metric = VecSimMetric_IP, .initialCapacity = block_size, .M = 32};
    this->SetUp(hnsw_params);

    VecSimParams vecsim_hnsw_params = CreateParams(hnsw_params);
    TieredIndexParams tiered_params = {
        .jobQueue = &this->mock_thread_pool.jobQ,
        .jobQueueCtx = this->mock_thread_pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .primaryIndexParams = &vecsim_hnsw_params,
        .specificParams = {TieredHNSWParams{
            .QuantNormalizationSetSize =
                TypeParam::with_quant_params ? TestFixture::normalization_set_size : 0}}};
    VecSimParams params = CreateParams(tiered_params);

    EXPECT_EQ(VecSimIndex_EstimateInitialSize(&params), this->index->getAllocationSize());

    for (size_t label = 0; label < block_size; label++) {
        ASSERT_EQ(this->GenerateAndAddVector(label, static_cast<float>(label)), 1);
    }
    while (!this->mock_thread_pool.jobQ.empty()) {
        this->mock_thread_pool.thread_iteration();
    }

    const size_t estimation = VecSimIndex_EstimateElementSize(&params) * block_size;
    const size_t before = this->index->getAllocationSize();
    ASSERT_EQ(this->GenerateAndAddVector(block_size, static_cast<float>(block_size)), 1);
    while (!this->mock_thread_pool.jobQ.empty()) {
        this->mock_thread_pool.thread_iteration();
    }
    const size_t actual = this->index->getAllocationSize() - before;

    EXPECT_EQ(this->index->indexSize(), block_size + 1);
    EXPECT_EQ(this->index->indexCapacity(), 2 * block_size);
    EXPECT_GE(estimation, actual * 0.99);
    EXPECT_LE(estimation, actual * 1.01);
}

template <typename index_type_t>
void SQ8TieredHNSWTest<index_type_t>::frontend_size_estimation_test(VecSimMetric metric) {
    constexpr size_t block_size = 64;
    HNSWParams hnsw_params = {.type = index_type_t::get_index_type(),
                              .dim = 1024,
                              .metric = metric,
                              .blockSize = block_size,
                              .M = 32,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams backend_params = CreateParams(hnsw_params);
    TieredIndexParams tiered_params = {
        .flatBufferLimit = block_size + 1,
        .primaryIndexParams = &backend_params,
        .specificParams = {
            TieredHNSWParams{.QuantNormalizationSetSize =
                                 index_type_t::with_quant_params ? normalization_set_size : 0}}};

    // Wide full-precision vectors make the frontend's block larger than the SQ8 backend's.
    const size_t estimation = EstimateElementSize(tiered_params) * block_size;
    this->SetUp(hnsw_params, tiered_params.flatBufferLimit);
    auto *tiered_index = static_cast<TieredHNSWIndex<data_t, float> *>(this->index);
    auto *frontend = tiered_index->getFlatBufferIndex();

    // Leave insertion jobs queued so the full-precision frontend must grow another block.
    for (size_t label = 0; label < block_size; label++) {
        ASSERT_EQ(this->GenerateAndAddVector(label, 0.25f, 0.001f), 1);
    }
    ASSERT_EQ(frontend->indexSize(), block_size);
    ASSERT_EQ(frontend->indexCapacity(), block_size);
    ASSERT_EQ(this->CastToHNSW()->indexSize(), 0);

    const size_t before = frontend->getAllocationSize();
    ASSERT_EQ(this->GenerateAndAddVector(block_size, 0.25f, 0.001f), 1);
    const size_t actual = frontend->getAllocationSize() - before;

    EXPECT_EQ(frontend->indexCapacity(), 2 * block_size);
    EXPECT_GE(estimation, actual * 0.99);
    EXPECT_LE(estimation, actual * 1.01);

    while (!this->mock_thread_pool.jobQ.empty()) {
        this->mock_thread_pool.thread_iteration();
    }
}

TYPED_TEST(SQ8TieredHNSWTest, FrontendSizeEstimationL2) {
    this->frontend_size_estimation_test(VecSimMetric_L2);
}

TYPED_TEST(SQ8TieredHNSWTest, FrontendSizeEstimationIP) {
    this->frontend_size_estimation_test(VecSimMetric_IP);
}

TYPED_TEST(SQ8TieredHNSWTest, FrontendSizeEstimationCosine) {
    this->frontend_size_estimation_test(VecSimMetric_Cosine);
}

TYPED_TEST(SQ8TieredHNSWTest, SearchByID) { this->search_by_id_test(); }

TYPED_TEST(SQ8TieredHNSWTest, SearchByScore) { this->search_by_score_test(); }

TYPED_TEST(SQ8TieredHNSWTest, SearchEmptyIndex) { this->search_empty_index_test(); }

TYPED_TEST(SQ8TieredHNSWTest, Override) { this->test_override(); }

TYPED_TEST(SQ8TieredHNSWTest, RangeQuery) { this->test_range_query(); }

TYPED_TEST(SQ8TieredHNSWTest, GetDistanceL2) { this->test_get_distance(VecSimMetric_L2, false); }
TYPED_TEST(SQ8TieredHNSWTest, GetDistanceIP) { this->test_get_distance(VecSimMetric_IP, false); }
TYPED_TEST(SQ8TieredHNSWTest, GetDistanceMultiL2) {
    this->test_get_distance(VecSimMetric_L2, true);
}
TYPED_TEST(SQ8TieredHNSWTest, GetDistanceMultiIP) {
    this->test_get_distance(VecSimMetric_IP, true);
}

TYPED_TEST(SQ8TieredHNSWTest, BatchIteratorBasic) { this->test_batch_iterator_basic(); }

template <typename index_type_t>
void SQ8TieredHNSWTest<index_type_t>::destruction_releases_pending_migration_jobs_test() {
    HNSWParams params = {.dim = 64, .metric = VecSimMetric_L2};
    SetUp(params, 1024);
    auto *tiered_index = dynamic_cast<TieredHNSWIndex<data_t, float> *>(this->index);
    ASSERT_NE(tiered_index, nullptr);

    // Keep each allocator alive so its accounting can be checked after destruction.
    auto allocator = this->index->getAllocator();
    auto frontend_allocator = tiered_index->frontendIndex->getAllocator();
    auto backend_allocator = tiered_index->backendIndex->getAllocator();
    const size_t num_vectors = index_type_t::with_quant_params ? normalization_set_size : 2;
    for (size_t label = 0; label < num_vectors; ++label) {
        ASSERT_EQ(this->GenerateAndAddVector(label, static_cast<float>(label), 1.0f), 1);
        if (index_type_t::with_quant_params && label + 1 < normalization_set_size) {
            EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        }
    }
    ASSERT_EQ(mock_thread_pool.jobQ.size(), num_vectors);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), num_vectors);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), num_vectors);

    mock_thread_pool.reset_ctx();
    while (!mock_thread_pool.jobQ.empty()) {
        EXPECT_TRUE(mock_thread_pool.jobQ.front().index_weak_ref.expired());
        // The index already deleted the jobs; only discard their queue wrappers.
        mock_thread_pool.jobQ.pop();
    }
    EXPECT_EQ(allocator->getAllocationSize(), sizeof(VecSimAllocator));
    EXPECT_EQ(frontend_allocator->getAllocationSize(), sizeof(VecSimAllocator));
    EXPECT_EQ(backend_allocator->getAllocationSize(), sizeof(VecSimAllocator));
}

TYPED_TEST(SQ8TieredHNSWTest, DestructionReleasesPendingMigrationJobs) {
    this->destruction_releases_pending_migration_jobs_test();
}

TEST(SQ8TieredHNSWTest, TrainingAccessorsAndRelabelUseEmptyBackend) {
    constexpr size_t dim = 4;
    constexpr size_t normalization_set_size = 4;
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = dim,
                              .metric = VecSimMetric_IP,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams primary_index_params = CreateParams(hnsw_params);
    tieredIndexMock mock_thread_pool;
    TieredIndexParams tiered_params = {
        .jobQueue = &mock_thread_pool.jobQ,
        .jobQueueCtx = mock_thread_pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .primaryIndexParams = &primary_index_params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = normalization_set_size}}};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    auto *tiered_index = dynamic_cast<TieredHNSWIndex<float, float> *>(index);
    ASSERT_NE(tiered_index, nullptr);

    float vector[dim] = {1.0f, 2.0f, 3.0f, 4.0f};
    ASSERT_EQ(VecSimIndex_AddVector(index, vector, 7), 1);

    EXPECT_EQ(tiered_index->indexSize(), 1);
    EXPECT_EQ(tiered_index->getNumMarkedDeleted(), 0);
    EXPECT_GT(tiered_index->getAllocationSize(), 0);
    EXPECT_EQ(tiered_index->indexMetaDataCapacity(),
              tiered_index->getFlatBufferIndex()->indexMetaDataCapacity());
    EXPECT_EQ(tiered_index->preferAdHocSearch(1, 1, true),
              tiered_index->getFlatBufferIndex()->preferAdHocSearch(1, 1, true));
    tiered_index->setLastSearchMode(STANDARD_KNN);
    EXPECT_EQ(tiered_index->debugInfo().commonInfo.lastMode, STANDARD_KNN);
    EXPECT_NO_THROW(tiered_index->fitMemory());
    EXPECT_NO_THROW(tiered_index->runGC());

    std::vector<std::vector<float>> stored;
    tiered_index->getDataByLabel(7, stored);
    EXPECT_TRUE(stored.empty());

    EXPECT_EQ(VecSimIndex_RelabelVector(index, 7, 70), VecSimRelabel_OK);
    stored.clear();
    tiered_index->getDataByLabel(7, stored);
    EXPECT_TRUE(stored.empty());
    tiered_index->getDataByLabel(70, stored);
    EXPECT_TRUE(stored.empty());

    auto allocator = index->getAllocator();
    mock_thread_pool.reset_ctx();
}

namespace {
struct MigrationQuerySubmitContext {
    tieredIndexMock *mock_thread_pool;
    VecSimIndex *index;
    const void *query;
    size_t queued_jobs_after_migration = 0;
    bool query_succeeded = false;
};

int executeOneMigrationThenQuery(void *, void *index_ctx, AsyncJob **jobs, JobCallback *callbacks,
                                 size_t jobs_len) {
    auto *context = static_cast<MigrationQuerySubmitContext *>(index_ctx);
    const int status =
        context->mock_thread_pool->submit_callback_internal(jobs, callbacks, jobs_len);
    if (status != VecSim_OK) {
        return status;
    }

    context->mock_thread_pool->thread_iteration();
    context->queued_jobs_after_migration = context->mock_thread_pool->jobQ.size();

    auto *reply = VecSimIndex_TopKQuery(context->index, context->query, 2, nullptr, BY_SCORE);
    context->query_succeeded =
        reply && reply->code == VecSim_QueryReply_OK && VecSimQueryReply_Len(reply) == 2;
    VecSimQueryReply_Free(reply);
    return VecSim_OK;
}
} // namespace

TEST(SQ8TieredHNSWTest, QueryDuringSubmissionCallbackAfterOneMigration) {
    constexpr size_t dim = 4;
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = dim,
                              .metric = VecSimMetric_L2,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams primary_index_params = CreateParams(hnsw_params);
    tieredIndexMock mock_thread_pool;
    float first_vector[dim] = {1.0f, 1.0f, 1.0f, 1.0f};
    float second_vector[dim] = {2.0f, 2.0f, 2.0f, 2.0f};
    MigrationQuerySubmitContext submit_context = {.mock_thread_pool = &mock_thread_pool,
                                                  .query = second_vector};
    TieredIndexParams tiered_params = {
        .jobQueue = &mock_thread_pool.jobQ,
        .jobQueueCtx = &submit_context,
        .submitCb = executeOneMigrationThenQuery,
        .primaryIndexParams = &primary_index_params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = 2}}};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);
    submit_context.index = index;
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    ASSERT_EQ(VecSimIndex_AddVector(index, first_vector, 0), 1);
    ASSERT_EQ(VecSimIndex_AddVector(index, second_vector, 1), 1);
    EXPECT_EQ(submit_context.queued_jobs_after_migration, 1);
    EXPECT_TRUE(submit_context.query_succeeded);

    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    auto allocator = index->getAllocator();
    mock_thread_pool.reset_ctx();
}

TEST(SQ8TieredHNSWTest, BatchIteratorDoesNotRepeatLabelsDuringMigrationOverlap) {
    constexpr size_t dim = 4;
    constexpr size_t normalization_set_size = 4;
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = dim,
                              .metric = VecSimMetric_L2,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams primary_index_params = CreateParams(hnsw_params);
    tieredIndexMock mock_thread_pool;
    TieredIndexParams tiered_params = {
        .jobQueue = &mock_thread_pool.jobQ,
        .jobQueueCtx = mock_thread_pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .primaryIndexParams = &primary_index_params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = normalization_set_size}}};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    auto *tiered_index = dynamic_cast<TieredHNSWIndex<float, float> *>(index);
    ASSERT_NE(tiered_index, nullptr);

    float vectors[normalization_set_size][dim] = {
        {7.0f, 1.5f, 6.66f, 1.11f},
        {2.0f, 2.22f, 2.0f, 3.33f},
        {3.0f, 3.33f, 4.0f, 4.44f},
        {4.44f, 5.66f, 5.0f, 5.55f},
    };
    for (size_t label = 0; label < normalization_set_size - 1; label++) {
        ASSERT_EQ(VecSimIndex_AddVector(index, vectors[label], label), 1);
    }

    std::mutex overlap_mutex;
    std::condition_variable overlap_cv;
    bool backend_inserted = false;
    bool allow_flat_removal = false;
    tiered_index->setAfterBackendInsertBeforeFlatRemovalHook([&] {
        std::unique_lock lock(overlap_mutex);
        backend_inserted = true;
        overlap_cv.notify_all();
        overlap_cv.wait(lock, [&] { return allow_flat_removal; });
    });

    ASSERT_EQ(VecSimIndex_AddVector(index, vectors[normalization_set_size - 1],
                                    normalization_set_size - 1),
              1);
    std::thread migration_worker([&] { mock_thread_pool.thread_iteration(); });
    bool overlap_reached = false;
    {
        std::unique_lock lock(overlap_mutex);
        overlap_reached =
            overlap_cv.wait_for(lock, std::chrono::seconds(10), [&] { return backend_inserted; });
    }
    EXPECT_TRUE(overlap_reached);

    VecSimBatchIterator *iterator = VecSimBatchIterator_New(index, vectors[0], nullptr);
    EXPECT_NE(iterator, nullptr);
    if (iterator) {
        std::unordered_set<labelType> returned_labels;
        size_t batch_count = 0;
        while (VecSimBatchIterator_HasNext(iterator)) {
            auto *batch = VecSimBatchIterator_Next(iterator, 1, BY_SCORE);
            EXPECT_NE(batch, nullptr);
            if (!batch) {
                break;
            }
            for (const auto &result : batch->results) {
                EXPECT_TRUE(returned_labels.insert(VecSimQueryResult_GetId(&result)).second);
            }
            VecSimQueryReply_Free(batch);
            if (++batch_count > normalization_set_size) {
                ADD_FAILURE() << "batch iterator did not deplete";
                break;
            }
        }
        EXPECT_EQ(batch_count, normalization_set_size);
        EXPECT_EQ(returned_labels.size(), normalization_set_size);
        VecSimBatchIterator_Free(iterator);
    }

    {
        std::lock_guard lock(overlap_mutex);
        allow_flat_removal = true;
    }
    overlap_cv.notify_all();
    migration_worker.join();
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    auto allocator = index->getAllocator();
    mock_thread_pool.reset_ctx();
}

TEST(SQ8TieredHNSWTest, BatchIteratorCreatedBeforeNormalizationSeesMigratedLabels) {
    for (bool consume_before_training : {false, true}) {
        SCOPED_TRACE(consume_before_training);
        constexpr size_t dim = 4;
        constexpr size_t normalization_set_size = 4;
        HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                                  .dim = dim,
                                  .metric = VecSimMetric_L2,
                                  .quantType = VecSimQuant_SQ8};
        VecSimParams primary_index_params = CreateParams(hnsw_params);
        tieredIndexMock mock_thread_pool;
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .primaryIndexParams = &primary_index_params,
            .specificParams = {
                TieredHNSWParams{.QuantNormalizationSetSize = normalization_set_size}}};
        VecSimParams params = CreateParams(tiered_params);
        auto *index = VecSimIndex_New(&params);
        ASSERT_NE(index, nullptr);
        mock_thread_pool.ctx->index_strong_ref.reset(index);

        float vectors[normalization_set_size][dim] = {
            {1.0f, 1.0f, 1.0f, 1.0f},
            {2.0f, 2.0f, 2.0f, 2.0f},
            {3.0f, 3.0f, 3.0f, 3.0f},
            {4.0f, 4.0f, 4.0f, 4.0f},
        };
        for (size_t label = 0; label < normalization_set_size - 1; label++) {
            ASSERT_EQ(VecSimIndex_AddVector(index, vectors[label], label), 1);
        }

        VecSimBatchIterator *iterator = VecSimBatchIterator_New(index, vectors[0], nullptr);
        ASSERT_NE(iterator, nullptr);
        if (consume_before_training) {
            auto *batch = VecSimBatchIterator_Next(iterator, 1, BY_SCORE);
            ASSERT_EQ(VecSimQueryReply_Len(batch), 1);
            VecSimQueryReply_Free(batch);
        }

        ASSERT_EQ(VecSimIndex_AddVector(index, vectors[normalization_set_size - 1],
                                        normalization_set_size - 1),
                  1);
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }

        if (consume_before_training) {
            // Reset must discard the empty backend iterator and use the finalized mean.
            VecSimBatchIterator_Reset(iterator);
        }
        std::unordered_set<labelType> returned_labels;
        size_t batch_count = 0;
        while (VecSimBatchIterator_HasNext(iterator)) {
            auto *batch = VecSimBatchIterator_Next(iterator, 1, BY_SCORE);
            ASSERT_NE(batch, nullptr);
            const size_t batch_len = VecSimQueryReply_Len(batch);
            if (batch_len == 0) {
                VecSimQueryReply_Free(batch);
                break;
            }
            ASSERT_EQ(batch_len, 1);
            for (const auto &result : batch->results) {
                EXPECT_TRUE(returned_labels.insert(VecSimQueryResult_GetId(&result)).second);
            }
            VecSimQueryReply_Free(batch);
            ASSERT_LE(++batch_count, normalization_set_size);
        }
        EXPECT_EQ(returned_labels.size(), normalization_set_size);
        for (labelType label = 0; label < normalization_set_size; label++) {
            EXPECT_NE(returned_labels.find(label), returned_labels.end());
        }
        VecSimBatchIterator_Free(iterator);

        auto allocator = index->getAllocator();
        mock_thread_pool.reset_ctx();
    }
}

TEST(SQ8TieredHNSWTest, ConcurrentQueriesDuringNormalizationTransition) {
    constexpr size_t dim = 4;
    constexpr size_t normalization_set_size = 2;
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = dim,
                              .metric = VecSimMetric_L2,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams primary_index_params = CreateParams(hnsw_params);
    tieredIndexMock mock_thread_pool;
    TieredIndexParams tiered_params = {
        .jobQueue = &mock_thread_pool.jobQ,
        .jobQueueCtx = mock_thread_pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .primaryIndexParams = &primary_index_params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = normalization_set_size}}};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    auto *tiered_index = dynamic_cast<TieredHNSWIndex<float, float> *>(index);
    ASSERT_NE(tiered_index, nullptr);

    float first_vector[dim] = {1.0f, 1.0f, 1.0f, 1.0f};
    float second_vector[dim] = {2.0f, 2.0f, 2.0f, 2.0f};
    float query[dim] = {1.0f, 1.0f, 1.0f, 1.0f};
    ASSERT_EQ(VecSimIndex_AddVector(index, first_vector, 0), 1);

    const auto reply_is_valid = [&](VecSimQueryReply *reply) {
        if (!reply || reply->code != VecSim_QueryReply_OK ||
            VecSimQueryReply_Len(reply) != normalization_set_size) {
            return false;
        }

        labelType expected_label = 0;
        bool valid = true;
        auto *iterator = VecSimQueryReply_GetIterator(reply);
        while (VecSimQueryReply_IteratorHasNext(iterator)) {
            auto *result = VecSimQueryReply_IteratorNext(iterator);
            const labelType label = VecSimQueryResult_GetId(result);
            const double score = VecSimQueryResult_GetScore(result);
            if (label >= normalization_set_size) {
                valid = false;
                continue;
            }
            const double expected = label == 0 ? 0.0 : 4.0;
            const double tolerance = 1e-4 + expected * 0.002;
            // BY_SCORE must return label 0 (distance 0) before label 1 (distance 4).
            if (label != expected_label++ || !std::isfinite(score) ||
                std::abs(score - expected) > tolerance) {
                valid = false;
            }
        }
        VecSimQueryReply_IteratorFree(iterator);
        return valid && expected_label == normalization_set_size;
    };
    const auto batch_iterator_is_valid = [&] {
        auto *iterator = VecSimBatchIterator_New(index, query, nullptr);
        if (!iterator) {
            return false;
        }

        bool valid = VecSimBatchIterator_HasNext(iterator);
        if (valid) {
            auto *reply = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
            valid = reply_is_valid(reply);
            if (reply) {
                VecSimQueryReply_Free(reply);
            }

            // A tiered iterator may need one empty read to discover that deduplicated backend
            // results are exhausted.
            if (VecSimBatchIterator_HasNext(iterator)) {
                reply = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
                valid = valid && reply && reply->code == VecSim_QueryReply_OK &&
                        VecSimQueryReply_Len(reply) == 0;
                if (reply) {
                    VecSimQueryReply_Free(reply);
                }
            }
            valid = valid && !VecSimBatchIterator_HasNext(iterator);
        }
        VecSimBatchIterator_Free(iterator);
        return valid;
    };

    std::mutex transition_mutex;
    std::condition_variable transition_cv;
    bool finalization_entered = false;
    bool allow_finalization = false;
    tiered_index->setBeforeQuantizationFinalizationHook([&] {
        std::unique_lock lock(transition_mutex);
        finalization_entered = true;
        transition_cv.notify_all();
        transition_cv.wait(lock, [&] { return allow_finalization; });
    });

    std::thread writer([&] { EXPECT_EQ(VecSimIndex_AddVector(index, second_vector, 1), 1); });
    {
        std::unique_lock lock(transition_mutex);
        ASSERT_TRUE(transition_cv.wait_for(lock, std::chrono::seconds(10),
                                           [&] { return finalization_entered; }));
    }

    std::atomic_bool keep_reading = true;
    std::atomic_int failures = 0;
    std::condition_variable readers_cv;
    std::mutex readers_mutex;
    bool reader_completed_iteration = false;
    std::thread reader([&] {
        bool announced = false;
        while (keep_reading.load()) {
            auto *reply = VecSimIndex_TopKQuery(index, query, 2, nullptr, BY_SCORE);
            if (!reply_is_valid(reply)) {
                failures.fetch_add(1);
            }
            if (reply) {
                VecSimQueryReply_Free(reply);
            }

            reply = VecSimIndex_RangeQuery(index, query, 100.0, nullptr, BY_SCORE);
            if (!reply_is_valid(reply)) {
                failures.fetch_add(1);
            }
            if (reply) {
                VecSimQueryReply_Free(reply);
            }

            if (!batch_iterator_is_valid()) {
                failures.fetch_add(1);
            }

            (void)VecSimIndex_IndexSize(index);
            (void)VecSimIndex_DebugInfo(index);
            (void)VecSimIndex_StatsInfo(index);
            (void)VecSimIndex_PreferAdHocSearch(index, normalization_set_size, 1, true);

            if (!announced) {
                {
                    std::lock_guard lock(readers_mutex);
                    reader_completed_iteration = true;
                }
                readers_cv.notify_one();
                announced = true;
            }
        }
    });

    {
        std::unique_lock lock(readers_mutex);
        EXPECT_TRUE(readers_cv.wait_for(lock, std::chrono::seconds(10),
                                        [&] { return reader_completed_iteration; }));
    }

    mock_thread_pool.init_threads();

    {
        std::lock_guard lock(transition_mutex);
        allow_finalization = true;
    }
    transition_cv.notify_all();
    writer.join();
    mock_thread_pool.thread_pool_wait();

    keep_reading.store(false);
    reader.join();
    EXPECT_EQ(failures.load(), 0);

    auto *reply = VecSimIndex_TopKQuery(index, query, 2, nullptr, BY_SCORE);
    ASSERT_NE(reply, nullptr);
    EXPECT_TRUE(reply_is_valid(reply));
    VecSimQueryReply_Free(reply);

    reply = VecSimIndex_RangeQuery(index, query, 100.0, nullptr, BY_SCORE);
    ASSERT_NE(reply, nullptr);
    EXPECT_TRUE(reply_is_valid(reply));
    VecSimQueryReply_Free(reply);

    EXPECT_TRUE(batch_iterator_is_valid());

    // Keep the allocator alive while reset_ctx releases the index's final reference.
    auto allocator = index->getAllocator();
    mock_thread_pool.reset_ctx();
}

TEST(SQ8TieredHNSWTest, WarnsForDimensionsBelow64) {
    auto previous_log_callback = VecSimIndexInterface::logCallback;
    struct LogCallbackRestorer {
        logCallbackFunction callback;
        ~LogCallbackRestorer() { VecSimIndexInterface::logCallback = callback; }
    } restore_log_callback{previous_log_callback};

    std::vector<std::string> warnings;
    VecSimIndexInterface::logCallback = [](void *ctx, const char *level, const char *message) {
        if (strcmp(level, VecSimCommonStrings::LOG_WARNING_STRING) == 0) {
            static_cast<std::vector<std::string> *>(ctx)->emplace_back(message);
        }
    };

    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = 32,
                              .metric = VecSimMetric_L2,
                              .quantType = VecSimQuant_SQ8};
    VecSimParams primary_index_params = CreateParams(hnsw_params);
    primary_index_params.logCtx = &warnings;
    tieredIndexMock mock_thread_pool;
    TieredIndexParams tiered_params = {
        .jobQueue = &mock_thread_pool.jobQ,
        .jobQueueCtx = mock_thread_pool.ctx,
        .submitCb = tieredIndexMock::submit_callback,
        .primaryIndexParams = &primary_index_params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = 10}}};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    ASSERT_NE(index, nullptr);
    mock_thread_pool.ctx->index_strong_ref.reset(index);

    ASSERT_EQ(warnings.size(), 1);
    EXPECT_NE(warnings.front().find(
                  "HNSW SQ8 compression is not recommended for vectors with fewer than "
                  "64 dimensions because per-vector metadata overhead reduces memory savings"),
              std::string::npos);

    auto allocator = index->getAllocator();
    mock_thread_pool.reset_ctx();
}

// SQ8 kernels support only FLOAT32 and FLOAT16 input vectors.
TEST(SQ8TieredHNSWTest, RejectsUnsupportedDataType) {
    for (auto type : {VecSimType_FLOAT64, VecSimType_BFLOAT16, VecSimType_INT8, VecSimType_UINT8}) {
        HNSWParams hnsw_params = {
            .type = type, .dim = 4, .metric = VecSimMetric_L2, .quantType = VecSimQuant_SQ8};
        VecSimParams params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {
            .primaryIndexParams = &params,
            .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = 10}}};
        VecSimParams vecsim_params = CreateParams(tiered_params);
        EXPECT_EQ(VecSimIndex_New(&vecsim_params), nullptr) << "data type " << type;
        EXPECT_EQ(EstimateInitialSize(tiered_params), SIZE_MAX) << "data type " << type;
    }
}

// Value 3 has no VecSimMetric enumerator but is within the enum's representable range, so the
// factory must reject it before dispatch.
TEST(SQ8TieredHNSWTest, RejectsOutOfRangeMetric) {
    HNSWParams hnsw_params = {.type = VecSimType_FLOAT32,
                              .dim = 4,
                              .metric = static_cast<VecSimMetric>(3),
                              .quantType = VecSimQuant_SQ8};
    VecSimParams params = CreateParams(hnsw_params);
    TieredIndexParams tiered_params = {
        .primaryIndexParams = &params,
        .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = 10}}};
    VecSimParams vecsim_params = CreateParams(tiered_params);
    EXPECT_EQ(VecSimIndex_New(&vecsim_params), nullptr);
    EXPECT_EQ(EstimateInitialSize(tiered_params), SIZE_MAX);
}

TEST(SQ8TieredHNSWTest, RejectsBackendCreationFailureWithoutAccumulation) {
    constexpr size_t dim = 4;
    for (bool multi : {false, true}) {
        // The tiered type check accepts FLOAT16, but the backend rejects an unknown metric.
        // Tiered index creation must propagate that failure.
        HNSWParams hnsw_params = {.type = VecSimType_FLOAT16,
                                  .dim = dim,
                                  .metric = static_cast<VecSimMetric>(3),
                                  .multi = multi,
                                  .quantType = VecSimQuant_SQ8};
        VecSimParams primary_params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {
            .primaryIndexParams = &primary_params,
            .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = 0}}};
        VecSimParams params = CreateParams(tiered_params);
        auto *index = VecSimIndex_New(&params);
        EXPECT_EQ(index, nullptr) << "multi = " << multi;
        if (index) {
            VecSimIndex_Free(index);
        }
    }
}

TEST(SQ8TieredHNSWTest, MeanCenteredFP16L2BufferedAndMigratedValues) {
    using data_t = vecsim_types::float16;
    constexpr size_t dim = 65;
    constexpr size_t count = 8;
    for (bool multi : {false, true}) {
        tieredIndexMock mock_thread_pool;
        std::vector<float> mean(dim, 0.10001f);
        HNSWParams hnsw_params = {.type = VecSimType_FLOAT16,
                                  .dim = dim,
                                  .metric = VecSimMetric_L2,
                                  .multi = multi,
                                  .efRuntime = count,
                                  .quantType = VecSimQuant_SQ8,
                                  .quantParams = mean.data()};
        VecSimParams primary_params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {.jobQueue = &mock_thread_pool.jobQ,
                                           .jobQueueCtx = mock_thread_pool.ctx,
                                           .submitCb = tieredIndexMock::submit_callback,
                                           .flatBufferLimit = count,
                                           .primaryIndexParams = &primary_params};
        VecSimParams params = CreateParams(tiered_params);
        auto *index = VecSimIndex_New(&params);
        if (!index) {
            mock_thread_pool.reset_ctx();
            FAIL() << "bounded mean-centered FLOAT16 L2 must construct";
        }
        mock_thread_pool.ctx->index_strong_ref.reset(index);
        EXPECT_EQ(EstimateInitialSize(tiered_params), index->getAllocationSize());
        std::vector<std::vector<data_t>> vectors(count, std::vector<data_t>(dim));
        for (size_t v = 0; v < count; ++v) {
            for (size_t d = 0; d < dim; ++d) {
                const float value = -1.0f + 2.0f * v / (count - 1);
                vectors[v][d] = vecsim_types::FP32_to_FP16(d % 2 ? value : -value);
            }
            ASSERT_EQ(VecSimIndex_AddVector(index, vectors[v].data(), multi ? v / 2 : v), 1);
        }
        const auto verify_queries = [&] {
            for (size_t v = 0; v < count; ++v) {
                const auto verify = [&](size_t id, double score, size_t) {
                    EXPECT_EQ(id, multi ? v / 2 : v);
                    EXPECT_TRUE(std::isfinite(score));
                    EXPECT_NEAR(score, 0.0, dim * 1e-4);
                };
                runTopKSearchTest(index, vectors[v].data(), 1, verify);
            }
        };
        ASSERT_EQ(mock_thread_pool.jobQ.size(), count);
        verify_queries();
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        const auto info = VecSimIndex_DebugInfo(index);
        ASSERT_EQ(info.tieredInfo.backendCommonInfo.indexSize, count);
        ASSERT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
        verify_queries();
    }
}
