/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "VecSim/vec_sim.h"
#include "unit_test_utils.h"
#include "tests_utils.h"
#include "VecSim/algorithms/brute_force/brute_force_multi.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/algorithms/hnsw/hnsw_multi.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/spaces/normalize/normalize_naive.h"
#include <cmath>

class IndexTestUtilsTest : public testing::TestWithParam<std::tuple<bool, VecSimMetric>> {
protected:
    static constexpr size_t dim = 4;
    static constexpr size_t labels_count = 5;

    VecSimIndex *index;
    bool is_multi = std::get<0>(GetParam());
    VecSimMetric metric = std::get<1>(GetParam());
    size_t vec_per_label = 1;

    void SetUp(HNSWParams &params) {
        params.dim = dim;
        params.multi = is_multi;
        params.metric = metric;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        vec_per_label = this->is_multi ? 3 : 1;
    }

    void SetUp(BFParams &params) {
        params.dim = dim;
        params.multi = is_multi;
        params.metric = metric;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        vec_per_label = this->is_multi ? 3 : 1;
    }

    void TearDown() { VecSimIndex_Free(index); }

    // id should be unique as it will be used as a seed for the random vector generation
    virtual void GenerateRandomAndAddVector(size_t label, size_t id) {
        FAIL() << "IndexTestUtilsTest::GenerateRandomAndAddVector this method should be overriden";
    }

    template <typename DataType>
    void ValidateVectorsImp(std::vector<std::vector<char>> index_label_vectors,
                            std::vector<std::vector<DataType>> original_vectors, size_t label) {
        for (size_t i = 0; i < vec_per_label; i++) {
            const DataType *vec = reinterpret_cast<const DataType *>(index_label_vectors[i].data());
            for (size_t j = 0; j < dim; j++) {
                ASSERT_EQ(vec[j], original_vectors[label * vec_per_label + i][j]);
            }
        }
    }
    virtual void ValidateVectors(std::vector<std::vector<char>> vectors, size_t label) {
        FAIL() << "IndexTestUtilsTest::ValidateVectors this method should be overriden";
    }

    virtual std::vector<std::vector<char>> GetStoredVectorsData(size_t label) {
        ADD_FAILURE()
            << "IndexTestUtilsTest::GetStoredVectorsData() this method should be overriden";
        return {};
    }

    virtual size_t GetIndexDatasize() {
        ADD_FAILURE() << "IndexTestUtilsTest::GetIndexDatasize() this method should be overriden";
        return {};
    }

    // Tests
    void get_stored_vector_data_single_test();
};

class Int8IndexTestUtilsTest : public IndexTestUtilsTest {
protected:
    std::vector<std::vector<int8_t>> vectors;
    void GenerateRandomAndAddVector(size_t label, size_t id) override {
        std::vector<int8_t> v(dim);
        test_utils::populate_int8_vec(v.data(), dim, static_cast<int>(id));
        VecSimIndex_AddVector(index, v.data(), label);

        vectors.emplace_back(v);
    }

    std::vector<std::vector<char>> GetStoredVectorsData(size_t label) override {
        return (dynamic_cast<VecSimIndexAbstract<int8_t, float> *>(this->index))
            ->getStoredVectorDataByLabel(label);
    }

    size_t GetIndexDatasize() override {
        return (dynamic_cast<VecSimIndexAbstract<int8_t, float> *>(this->index))
            ->getStoredDataSize();
    }

    void ValidateVectors(std::vector<std::vector<char>> index_vectors, size_t label) override {
        IndexTestUtilsTest::ValidateVectorsImp<int8_t>(index_vectors, vectors, label);
    }

    void ValidateCosine() {
        for (size_t i = 0; i < labels_count; i++) {
            auto stored_data = GetStoredVectorsData(i);
            for (size_t j = 0; j < stored_data.size(); j++) {
                ASSERT_EQ(stored_data[j].size(), dim * sizeof(int8_t) + sizeof(float));
                const int8_t *stored_vec = reinterpret_cast<const int8_t *>(stored_data[j].data());
                // compute expected norm using the original vector
                float expected_norm =
                    test_utils::integral_compute_norm(vectors[i * vec_per_label + j].data(), dim);
                const float stored_norm = load_unaligned<float>(stored_vec + dim);
                ASSERT_EQ(stored_norm, expected_norm) << "wrong vector norm for vector id:" << j;
            }
        }
    }
};

class Float32IndexTestUtilsTest : public IndexTestUtilsTest {
protected:
    std::vector<std::vector<float>> vectors;
    void GenerateRandomAndAddVector(size_t label, size_t id) override {
        std::vector<float> v(dim);
        test_utils::populate_float_vec(v.data(), dim, id);

        VecSimIndex_AddVector(index, v.data(), label);
        VecSimMetric metric = std::get<1>(GetParam());

        if (metric == VecSimMetric_Cosine)
            VecSim_Normalize(v.data(), dim, VecSimType_FLOAT32);

        vectors.emplace_back(v);
    }

    void ValidateVectors(std::vector<std::vector<char>> index_vectors, size_t label) override {
        IndexTestUtilsTest::ValidateVectorsImp<float>(index_vectors, vectors, label);
    }

    std::vector<std::vector<char>> GetStoredVectorsData(size_t label) override {
        return (dynamic_cast<VecSimIndexAbstract<float, float> *>(this->index))
            ->getStoredVectorDataByLabel(label);
    }

    size_t GetIndexDatasize() override {
        return (dynamic_cast<VecSimIndexAbstract<float, float> *>(this->index))
            ->getStoredDataSize();
    }
};

TEST_P(Int8IndexTestUtilsTest, BF) {
    BFParams params = {.type = VecSimType_INT8, .dim = dim};
    SetUp(params);
    EXPECT_NO_FATAL_FAILURE(get_stored_vector_data_single_test());
    VecSimMetric metric = std::get<1>(GetParam());
    if (metric == VecSimMetric_Cosine) {
        EXPECT_NO_FATAL_FAILURE(ValidateCosine());
    }
}

TEST_P(Int8IndexTestUtilsTest, HNSW) {
    HNSWParams params = {.type = VecSimType_INT8, .dim = dim};
    SetUp(params);

    EXPECT_NO_FATAL_FAILURE(get_stored_vector_data_single_test());
    VecSimMetric metric = std::get<1>(GetParam());
    if (metric == VecSimMetric_Cosine) {
        EXPECT_NO_FATAL_FAILURE(ValidateCosine());
    }
}

/** Run all Int8IndexTestUtilsTest tests for each {is_multi, VecSimMetric} combination */
INSTANTIATE_TEST_SUITE_P(Int8IndexTestUtilsTest, Int8IndexTestUtilsTest,
                         testing::Combine(testing::Values(false, true), // is_multi
                                          testing::Values(VecSimMetric_L2, VecSimMetric_IP,
                                                          VecSimMetric_Cosine)),
                         [](const testing::TestParamInfo<Int8IndexTestUtilsTest::ParamType> &info) {
                             bool is_multi = std::get<0>(info.param);
                             const char *metric = VecSimMetric_ToString(std::get<1>(info.param));
                             std::string test_name(is_multi ? "Multi_" : "Single_");
                             return test_name + metric;
                         });

TEST_P(Float32IndexTestUtilsTest, BF) {
    BFParams params = {.type = VecSimType_FLOAT32, .dim = dim};
    SetUp(params);
    EXPECT_NO_FATAL_FAILURE(get_stored_vector_data_single_test());
    VecSimMetric metric = std::get<1>(GetParam());
}

TEST_P(Float32IndexTestUtilsTest, HNSW) {
    HNSWParams params = {.type = VecSimType_FLOAT32, .dim = dim};
    SetUp(params);

    EXPECT_NO_FATAL_FAILURE(get_stored_vector_data_single_test());
    VecSimMetric metric = std::get<1>(GetParam());
}

/** Run all Float32IndexTestUtilsTest tests for each {is_multi, VecSimMetric} combination */
INSTANTIATE_TEST_SUITE_P(
    Float32IndexTestUtilsTest, Float32IndexTestUtilsTest,
    testing::Combine(testing::Values(false, true), // is_multi
                     testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine)),
    [](const testing::TestParamInfo<Float32IndexTestUtilsTest::ParamType> &info) {
        bool is_multi = std::get<0>(info.param);
        const char *metric = VecSimMetric_ToString(std::get<1>(info.param));
        std::string test_name(is_multi ? "Multi_" : "Single_");
        return test_name + "_" + metric;
    });

void IndexTestUtilsTest::get_stored_vector_data_single_test() {
    size_t n = IndexTestUtilsTest::labels_count * this->vec_per_label;

    // Add vectors to the index
    int id = 0;
    for (size_t i = 0; i < IndexTestUtilsTest::labels_count; i++) {
        for (size_t j = 0; j < vec_per_label; j++) {
            this->GenerateRandomAndAddVector(i, id++);
        }
    }

    // Verify the index size
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Get stored vector data for each label
    for (size_t i = 0; i < this->labels_count; i++) {
        auto stored_data = GetStoredVectorsData(i);

        // Should return a vector of vectors for each label
        ASSERT_EQ(stored_data.size(), vec_per_label);

        // Get the size of the stored data
        size_t data_size = GetIndexDatasize();
        for (size_t j = 0; j < vec_per_label; j++) {
            ASSERT_EQ(stored_data[j].size(), data_size);
        }

        // Compare the stored data with the original vectors
        EXPECT_NO_FATAL_FAILURE(this->ValidateVectors(stored_data, i));
    }
}

TEST(QueryResultPrintTest, PrintOperators) {
    // Test VecSimQueryResult print operator
    VecSimQueryResult result = {.id = 42, .score = 3.14};
    std::ostringstream oss1;
    oss1 << result;
    EXPECT_EQ(oss1.str(), "id: 42, score: 3.14");

    // Test VecSimQueryReply print operator
    VecSimQueryReply reply(VecSimAllocator::newVecsimAllocator());
    reply.results.push_back({.id = 1, .score = 1.5});
    reply.results.push_back({.id = 2, .score = 2.5});

    std::ostringstream oss2;
    oss2 << reply;
    EXPECT_EQ(oss2.str(), "id: 1, score: 1.5\nid: 2, score: 2.5\n");
}
