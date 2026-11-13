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
#include "VecSim/algorithms/hnsw/hnsw_multi.h"
#include <cmath>
#include <map>

template <typename index_type_t>
class HNSWMultiTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

protected:
    VecSimIndex *CreateNewIndex(HNSWParams &params) {
        return test_utils::CreateNewIndex(params, index_type_t::get_index_type(), true);
    }

    HNSWIndex<data_t, dist_t> *CastToHNSW(VecSimIndex *index) {
        return reinterpret_cast<HNSWIndex<data_t, dist_t> *>(index);
    }

    HNSWIndex_Multi<data_t, dist_t> *CastToHNSW_Multi(VecSimIndex *index) {
        return reinterpret_cast<HNSWIndex_Multi<data_t, dist_t> *>(index);
    }
};

// DataTypeSet, TEST_DATA_T and TEST_DIST_T are defined in unit_test_utils.h

TYPED_TEST_SUITE(HNSWMultiTest, DataTypeSet);

TYPED_TEST(HNSWMultiTest, vector_add_multiple_test) {
    size_t dim = 4;
    size_t rep = 5;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_IP};
    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Adding multiple vectors under the same label
    for (size_t j = 0; j < rep; j++) {
        TEST_DATA_T a[dim];
        for (size_t i = 0; i < dim; i++) {
            a[i] = (TEST_DATA_T)i * j + i;
        }
        ASSERT_EQ(this->CastToHNSW_Multi(index)->addVector(a, 46), 1);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), rep);
    ASSERT_EQ(index->indexLabelCount(), 1);

    // Deleting the label. All the vectors should be deleted.
    ASSERT_EQ(this->CastToHNSW_Multi(index)->deleteVector(46), rep);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    ASSERT_EQ(index->indexLabelCount(), 0);

    VecSimIndex_Free(index);
}

// Test empty index edge cases.
TYPED_TEST(HNSWMultiTest, empty_index) {
    size_t dim = 4;
    size_t n = 20;
    size_t bs = 6;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove from an empty index - should fail because label doesn't exist.
    ASSERT_EQ(this->CastToHNSW_Multi(index)->deleteVector(0), 0);

    // Add one vector multiple times.
    for (size_t i = 0; i < 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);
    }

    // Try to remove it.
    ASSERT_EQ(this->CastToHNSW_Multi(index)->deleteVector(1), 3);

    // Size equals 0.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    VecSimIndex_DeleteVector(index, 1);

    // Size should be still zero.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, vector_search_test) {
    size_t dim = 4;
    size_t n = 1000;
    size_t n_labels = 100;
    size_t k = 11;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);

    auto verify_res = [&](size_t id, double score, size_t index) {
        size_t diff_id = (id > 50) ? (id - 50) : (50 - id);
        ASSERT_EQ(diff_id, (index + 1) / 2);
        ASSERT_EQ(score, (4 * ((index + 1) / 2) * ((index + 1) / 2)));
    };
    runTopKSearchTest(index, query, k, verify_res);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, search_more_than_there_is) {
    size_t dim = 4;
    size_t n = 5;
    size_t perLabel = 3;
    size_t n_labels = ceil((float)n / perLabel);
    size_t k = 3;
    // This test add 5 vectors under 2 labels, and then query for 3 results.
    // We want to make sure we get only 2 results back (because the results should have unique
    // labels), although the index contains 5 vectors.

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n_labels);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < n_labels; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        ASSERT_EQ(VecSimQueryResult_GetScore(el), i * perLabel * i * perLabel * dim);
        labelType element_label = VecSimQueryResult_GetId(el);
        ASSERT_EQ(element_label, i);
        auto ids = this->CastToHNSW_Multi(index)->labelLookup.at(element_label);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], i * perLabel + j);
        }
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, indexing_same_vector) {
    size_t n = 100;
    size_t k = 10;
    size_t perLabel = 10;
    size_t dim = 4;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n / perLabel; i++) {
        for (size_t j = 0; j < perLabel; j++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0.0);
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, index); };
    runTopKSearchTest(index, query, k, verify_res);
    auto res = VecSimIndex_TopKQuery(index, query, k, nullptr, BY_SCORE);
    auto it = VecSimQueryReply_GetIterator(res);
    for (size_t i = 0; i < k; i++) {
        auto el = VecSimQueryReply_IteratorNext(it);
        labelType element_label = VecSimQueryResult_GetId(el);
        ASSERT_EQ(VecSimQueryResult_GetScore(el), i * i * dim);
        ASSERT_EQ(element_label, i);
        auto ids = this->CastToHNSW_Multi(index)->labelLookup.at(element_label);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], i * perLabel + j);
        }
    }
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, find_better_score) {
    size_t n = 100;
    size_t k = 10;
    size_t n_labels = 10;
    size_t dim = 4;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    // Building the index. Each label gets 10 vectors with decreasing (by insertion order) element
    // value.
    std::map<size_t, double> scores;
    for (size_t i = 0; i < n; i++) {
        // For example, with n_labels == 10 and n == 100,
        // label 0 will get vector elements 18 -> 9 (aka (9 -> 0) + 9),
        // label 1 will get vector elements 17 -> 8 (aka (9 -> 0) + 8),
        // label 2 will get vector elements 16 -> 7 (aka (9 -> 0) + 7),
        // . . . . .
        // label 9 will get vector elements 9 -> 0 (aka (9 -> 0) + 0),
        // and so on, so each label has some common vectors with all the previous labels.
        size_t el = ((n - i - 1) % n_labels) + ((n - i - 1) / n_labels);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / n_labels, el);
        // This should be the best score for each label.
        if (i % n_labels == n_labels - 1) {
            // `el * el * dim` is the L2-squared value with the 0 vector.
            scores.emplace(i / n_labels, el * el * dim);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, k - index - 1);
        ASSERT_FLOAT_EQ(score, scores[id]);
    };

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0.0);
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, find_better_score_after_pop) {
    size_t n = 12;
    size_t n_labels = 3;
    size_t dim = 4;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    // Building the index. Each is better than the previous one.
    for (size_t i = 0; i < n; i++) {
        size_t el = n - i;
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, el);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n_labels - index - 1);
    };

    runTopKSearchTest(index, query, n_labels, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, reindexing_same_vector_different_id) {
    size_t n = 100;
    size_t k = 10;
    size_t dim = 4;
    size_t perLabel = 3;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        // i / 10 is in integer (take the "floor" value)
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, TEST_DATA_T(i / 10) + n);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

    // Run a query where all the results are supposed to be {5,5,5,5} (different ids).
    TEST_DATA_T query[] = {4.9, 4.95, 5.05, 5.1};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_TRUE(id >= 50 && id < 60 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res);

    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Reinsert the same vectors under different ids than before.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10, i / 10);
    }
    // Add more vectors under the same labels. their scores should be worst.
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < perLabel - 1; j++) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i + 10, TEST_DATA_T(i / 10) + n);
        }
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n * perLabel);

    // Run the same query again.
    auto verify_res_different_id = [&](int id, double score, size_t index) {
        ASSERT_TRUE(id >= 60 && id < 70 && score <= 1);
    };
    runTopKSearchTest(index, query, k, verify_res_different_id);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, test_hnsw_info) {
    // Build with default args.
    size_t n = 100;
    size_t d = 128;
    // Build with default args

    HNSWParams params = {.dim = d, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // Default args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(info.hnswInfo.M, HNSW_DEFAULT_M);
    ASSERT_EQ(info.hnswInfo.efConstruction, HNSW_DEFAULT_EF_C);
    ASSERT_EQ(info.hnswInfo.efRuntime, HNSW_DEFAULT_EF_RT);
    ASSERT_DOUBLE_EQ(info.hnswInfo.epsilon, HNSW_DEFAULT_EPSILON);
    VecSimIndex_Free(index);

    d = 1280;
    size_t bs = 42;
    size_t M = 16;
    size_t ef_C = 200;
    size_t ef_RT = 500;
    double epsilon = 0.005;
    params.dim = d;
    params.blockSize = bs;
    params.M = M;
    params.efConstruction = ef_C;
    params.efRuntime = ef_RT;
    params.epsilon = epsilon;

    index = this->CreateNewIndex(params);
    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, d);
    ASSERT_TRUE(info.commonInfo.basicInfo.isMulti);
    // User args.
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, bs);
    ASSERT_EQ(info.hnswInfo.M, M);
    ASSERT_EQ(info.hnswInfo.efConstruction, ef_C);
    ASSERT_EQ(info.hnswInfo.efRuntime, ef_RT);
    ASSERT_EQ(info.hnswInfo.epsilon, epsilon);
    VecSimIndex_Free(index);
}
TYPED_TEST(HNSWMultiTest, test_basic_hnsw_info_iterator) {
    size_t n = 100;
    size_t d = 128;
    VecSimMetric metrics[3] = {VecSimMetric_Cosine, VecSimMetric_IP, VecSimMetric_L2};

    for (size_t i = 0; i < 3; i++) {

        // Build with default args.
        HNSWParams params = {.dim = d, .metric = metrics[i]};
        VecSimIndex *index = this->CreateNewIndex(params);

        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
        compareHNSWIndexInfoToIterator(info, infoIter);
        VecSimDebugInfoIterator_Free(infoIter);
        VecSimIndex_Free(index);
    }
}

TYPED_TEST(HNSWMultiTest, test_dynamic_hnsw_info_iterator) {
    size_t n = 100;
    size_t d = 128;

    HNSWParams params = {.dim = d,
                         .metric = VecSimMetric_L2,
                         .M = 100,
                         .efConstruction = 250,
                         .efRuntime = 400,
                         .epsilon = 0.004};

    VecSimIndex *index = this->CreateNewIndex(params);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(100, info.hnswInfo.M);
    ASSERT_EQ(250, info.hnswInfo.efConstruction);
    ASSERT_EQ(400, info.hnswInfo.efRuntime);
    ASSERT_EQ(0.004, info.hnswInfo.epsilon);
    ASSERT_EQ(0, info.commonInfo.indexSize);
    ASSERT_EQ(-1, info.hnswInfo.max_level);
    ASSERT_EQ(-1, info.hnswInfo.entrypoint);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Add vectors.
    TEST_DATA_T v[d];
    for (size_t i = 0; i < d; i++) {
        v[i] = (TEST_DATA_T)i;
    }
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 0);
    VecSimIndex_AddVector(index, v, 1);
    VecSimIndex_AddVector(index, v, 1);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(4, info.commonInfo.indexSize);
    ASSERT_EQ(2, info.commonInfo.indexLabelCount);
    ASSERT_GE(1, info.hnswInfo.max_level);
    ASSERT_EQ(0, info.hnswInfo.entrypoint);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Delete vectors.
    VecSimIndex_DeleteVector(index, 0);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(2, info.commonInfo.indexSize);
    ASSERT_EQ(1, info.commonInfo.indexLabelCount);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Perform (or simulate) Search in all modes.
    VecSimIndex_AddVector(index, v, 0);
    auto res = VecSimIndex_TopKQuery(index, v, 1, nullptr, BY_SCORE);
    VecSimQueryReply_Free(res);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(STANDARD_KNN, info.commonInfo.lastMode);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    res = VecSimIndex_RangeQuery(index, v, 1, nullptr, BY_SCORE);
    VecSimQueryReply_Free(res);
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(RANGE_QUERY, info.commonInfo.lastMode);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 1, true));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_ADHOC_BF, info.commonInfo.lastMode);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Set the index size artificially so that BATCHES mode will be selected by the heuristics.
    auto actual_element_count = this->CastToHNSW(index)->curElementCount;
    this->CastToHNSW(index)->curElementCount = 1e6;
    vecsim_stl::vector<idType> vec(index->getAllocator());
    for (size_t i = 0; i < 1e5; i++) {
        this->CastToHNSW_Multi(index)->labelLookup.emplace(i, vec);
    }
    ASSERT_FALSE(VecSimIndex_PreferAdHocSearch(index, 10, 1, true));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_BATCHES, info.commonInfo.lastMode);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    // Simulate the case where another call to the heuristics is done after realizing that
    // the subset size is smaller, and change the policy as a result.
    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 1, 10, false));
    info = VecSimIndex_DebugInfo(index);
    infoIter = VecSimIndex_DebugInfoIterator(index);
    ASSERT_EQ(HYBRID_BATCHES_TO_ADHOC_BF, info.commonInfo.lastMode);
    compareHNSWIndexInfoToIterator(info, infoIter);
    VecSimDebugInfoIterator_Free(infoIter);

    this->CastToHNSW(index)->curElementCount = actual_element_count;
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, preferAdHocOptimization) {
    // Save the expected result for every combination that represent a different leaf in the tree.
    // map: [k, label_count, vectors per label, dim, M, r] -> res
    std::map<std::vector<float>, bool> combinations;
    combinations[{5, 100, 3, 5, 5, 0.5}] = true;
    combinations[{5, 100, 5, 5, 5, 0.5}] = true;
    combinations[{5, 100, 10, 5, 5, 0.5}] = true;
    combinations[{5, 600, 3, 5, 5, 0.1}] = true;
    combinations[{5, 600, 5, 5, 5, 0.1}] = true;
    combinations[{5, 600, 10, 5, 5, 0.1}] = false;
    combinations[{5, 600, 3, 5, 5, 0.2}] = true;
    combinations[{5, 600, 5, 5, 5, 0.2}] = true;
    combinations[{5, 600, 10, 5, 5, 0.2}] = false;
    combinations[{5, 600, 3, 60, 5, 0.5}] = true;
    combinations[{5, 600, 5, 60, 5, 0.5}] = true;
    combinations[{5, 600, 10, 60, 5, 0.5}] = false;
    combinations[{5, 600, 3, 60, 15, 0.5}] = true;
    combinations[{5, 600, 5, 60, 15, 0.5}] = true;
    combinations[{5, 600, 10, 60, 15, 0.5}] = true;
    combinations[{15, 600, 3, 50, 5, 0.5}] = true;
    combinations[{15, 600, 5, 50, 5, 0.5}] = true;
    combinations[{15, 600, 10, 50, 5, 0.5}] = true;
    combinations[{5, 70000, 3, 60, 5, 0.05}] = false;
    combinations[{5, 70000, 5, 60, 5, 0.05}] = false;
    combinations[{5, 70000, 10, 60, 5, 0.05}] = false;
    combinations[{5, 80000, 3, 60, 5, 0.05}] = false;
    combinations[{5, 80000, 5, 60, 5, 0.05}] = false;
    combinations[{5, 80000, 10, 60, 5, 0.05}] = false;
    combinations[{10, 80000, 3, 60, 5, 0.01}] = true;
    combinations[{10, 80000, 5, 60, 5, 0.01}] = true;
    combinations[{10, 80000, 10, 60, 5, 0.01}] = false;
    combinations[{10, 80000, 3, 60, 5, 0.05}] = false;
    combinations[{10, 80000, 5, 60, 5, 0.05}] = false;
    combinations[{10, 80000, 10, 60, 5, 0.05}] = false;
    combinations[{10, 80000, 3, 60, 5, 0.1}] = false;
    combinations[{10, 80000, 5, 60, 5, 0.1}] = false;
    combinations[{10, 80000, 10, 60, 5, 0.1}] = false;
    combinations[{10, 6000, 3, 100, 5, 0.1}] = false;
    combinations[{10, 6000, 5, 100, 5, 0.1}] = false;
    combinations[{10, 6000, 10, 100, 5, 0.1}] = false;
    combinations[{10, 8000, 3, 100, 5, 0.1}] = false;
    combinations[{10, 8000, 5, 100, 5, 0.1}] = false;
    combinations[{10, 8000, 10, 100, 5, 0.1}] = false;
    combinations[{10, 6000, 3, 100, 60, 0.1}] = true;
    combinations[{10, 6000, 5, 100, 60, 0.1}] = true;
    combinations[{10, 6000, 10, 100, 60, 0.1}] = false;
    combinations[{10, 6000, 3, 100, 5, 0.3}] = false;
    combinations[{10, 6000, 5, 100, 5, 0.3}] = false;
    combinations[{10, 6000, 10, 100, 5, 0.3}] = false;
    combinations[{20, 6000, 3, 100, 5, 0.1}] = true;
    combinations[{20, 6000, 5, 100, 5, 0.1}] = true;
    combinations[{20, 6000, 10, 100, 5, 0.1}] = false;
    combinations[{20, 6000, 3, 100, 5, 0.2}] = true;
    combinations[{20, 6000, 5, 100, 5, 0.2}] = true;
    combinations[{20, 6000, 10, 100, 5, 0.2}] = false;
    combinations[{20, 6000, 3, 100, 20, 0.1}] = true;
    combinations[{20, 6000, 5, 100, 20, 0.1}] = true;
    combinations[{20, 6000, 10, 100, 20, 0.1}] = true;
    combinations[{20, 35000, 3, 100, 20, 0.1}] = true;
    combinations[{20, 35000, 5, 100, 20, 0.1}] = true;
    combinations[{20, 35000, 10, 100, 20, 0.1}] = false;
    combinations[{20, 35000, 3, 100, 20, 0.2}] = true;
    combinations[{20, 35000, 5, 100, 20, 0.2}] = true;
    combinations[{20, 35000, 10, 100, 20, 0.2}] = false;

    for (auto &comb : combinations) {
        auto k = (size_t)comb.first[0];
        auto label_count = (size_t)comb.first[1];
        auto perLabel = (size_t)comb.first[2];
        auto dim = (size_t)comb.first[3];
        auto M = (size_t)comb.first[4];
        auto r = comb.first[5];

        auto index_size = label_count * perLabel;

        // Create index and check for the expected output of "prefer ad-hoc" heuristics.

        HNSWParams params = {
            .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = 1, .efRuntime = 1};

        VecSimIndex *index = this->CreateNewIndex(params);

        // Set the index size artificially to be the required one.
        this->CastToHNSW(index)->curElementCount = index_size;
        vecsim_stl::vector<idType> vec(index->getAllocator());
        for (size_t i = 0; i < label_count; i++) {
            this->CastToHNSW_Multi(index)->labelLookup.emplace(i, vec);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(index), index_size);
        bool res = VecSimIndex_PreferAdHocSearch(index, (size_t)(r * (float)index_size), k, true);
        ASSERT_EQ(res, comb.second);
        // Clean up.
        this->CastToHNSW(index)->curElementCount = 0;
        VecSimIndex_Free(index);
    }

    // Corner cases - empty index.
    HNSWParams params = {
        .dim = 4, .metric = VecSimMetric_L2, .M = 0, .efConstruction = 0, .efRuntime = 0};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_TRUE(VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    // Corner cases - subset size is greater than index size.
    ASSERT_EQ(VecSimIndex_PreferAdHocSearch(index, 42, 50, true),
              VecSimIndex_PreferAdHocSearch(index, 0, 50, true));

    VecSimIndex_Free(index);
}
TYPED_TEST(HNSWMultiTest, search_empty_index) {
    size_t dim = 4;
    size_t n = 100;
    size_t k = 11;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);
    // We do not expect any results.
    VecSimQueryReply *res = VecSimIndex_TopKQuery(index, query, k, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Iterator *it = VecSimQueryReply_GetIterator(res);
    ASSERT_EQ(VecSimQueryReply_IteratorNext(it), nullptr);
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);

    res = VecSimIndex_RangeQuery(index, query, 1.0, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // Add some vectors and remove them all from index, so it will be empty again.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 46, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    VecSimIndex_DeleteVector(index, 46);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Again - we do not expect any results.
    res = VecSimIndex_TopKQuery(index, query, k, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    it = VecSimQueryReply_GetIterator(res);
    ASSERT_EQ(VecSimQueryReply_IteratorNext(it), nullptr);
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(res);

    res = VecSimIndex_RangeQuery(index, query, 1.0, NULL, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, removeVectorWithSwaps) {
    size_t dim = 4;
    size_t n = 6;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};
    auto *index = this->CastToHNSW_Multi(this->CreateNewIndex(params));

    // Insert 3 vectors under two different labels, so that we will have:
    // {first_label->[0,1,3], second_label->[2,4,5]}
    labelType first_label = 1;
    labelType second_label = 2;

    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, first_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, second_label);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Artificially reorder the internal ids to test that we make the right changes
    // when we have an id that appears twice in the array upon deleting the ids one by one.
    ASSERT_EQ(index->labelLookup.at(second_label).size(), n / 2);
    index->labelLookup.at(second_label)[0] = 4;
    index->labelLookup.at(second_label)[1] = 2;
    index->labelLookup.at(second_label)[2] = 5;

    // Expect that the ids array of the second label will behave as following:
    // [|4, 2, 5] -> [4, |2, 4] -> [4, 2, |2] (where | marks the current position).
    index->deleteVector(second_label);
    ASSERT_EQ(index->indexLabelCount(), 1);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n / 2);

    // Check that the internal ids of the first label are as expected.
    auto ids = index->labelLookup.at(first_label);
    ASSERT_EQ(ids.size(), n / 2);
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 0) != ids.end());
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 1) != ids.end());
    ASSERT_TRUE(std::find(ids.begin(), ids.end(), 2) != ids.end());
    index->deleteVector(first_label);
    ASSERT_EQ(index->indexLabelCount(), 0);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, remove_vector_after_replacing_block) {
    size_t dim = 4;
    size_t bs = 2;
    size_t n = 6;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Setting up vectors
    TEST_DATA_T f[n][dim];
    for (size_t i = 0; i < n; i++) {
        for (size_t j = 0; j < dim; j++) {
            f[i][j] = i;
        }
    }
    // Add 1 vector with label 1
    VecSimIndex_AddVector(index, f[0], 1);

    // Add 3 vectors with label 3
    VecSimIndex_AddVector(index, f[1], 3);
    VecSimIndex_AddVector(index, f[2], 3);
    VecSimIndex_AddVector(index, f[3], 3);

    // Add 2 vectors with label 2
    VecSimIndex_AddVector(index, f[4], 2);
    VecSimIndex_AddVector(index, f[5], 2);

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Delete label 3. the following drawing present the expected changes
    // [[1, 3], [3, 3], [2, 2]] -> [[1, 2], [3, 3], [2]] -> [[1, 2], [2, 3]] -> [[1, 2], [2]]
    // [[0, 1], [2, 3], [4, 5]] -> [[0, 5], [2, 3], [4]] -> [[0, 5], [4, 3]] -> [[0, 5], [4]]
    VecSimIndex_DeleteVector(index, 3);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 3);
    ASSERT_EQ(index->indexLabelCount(), 2);
    auto hnsw_index = this->CastToHNSW_Multi(index);
    ASSERT_EQ(hnsw_index->getExternalLabel(0), 1);
    ASSERT_EQ(hnsw_index->getExternalLabel(1), 2);
    ASSERT_EQ(hnsw_index->getExternalLabel(2), 2);
    // checking the blob swaps.
    ASSERT_EQ(*(TEST_DATA_T *)(hnsw_index->getDataByInternalId(0)), 0);
    ASSERT_EQ(*(TEST_DATA_T *)(hnsw_index->getDataByInternalId(1)), 5);
    ASSERT_EQ(*(TEST_DATA_T *)(hnsw_index->getDataByInternalId(2)), 4);

    VecSimIndex_DeleteVector(index, 1);
    VecSimIndex_DeleteVector(index, 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_get_distance) {
    size_t n_labels = 2;
    size_t dim = 2;
    size_t numIndex = 3;
    VecSimIndex *index[numIndex];
    std::vector<double> distances;

    TEST_DATA_T v1_0[] = {M_PI, M_PI};
    TEST_DATA_T v2_0[] = {M_E, M_E};
    TEST_DATA_T v3_1[] = {M_PI, M_E};
    TEST_DATA_T v4_1[] = {M_SQRT2, -M_SQRT2};

    HNSWParams params = {.dim = dim};

    for (size_t i = 0; i < numIndex; i++) {
        params.metric = (VecSimMetric)i;
        index[i] = this->CreateNewIndex(params);
        VecSimIndex_AddVector(index[i], v1_0, 0);
        VecSimIndex_AddVector(index[i], v2_0, 0);
        VecSimIndex_AddVector(index[i], v3_1, 1);
        VecSimIndex_AddVector(index[i], v4_1, 1);
        ASSERT_EQ(VecSimIndex_IndexSize(index[i]), 4);
    }

    TEST_DATA_T *query = v1_0;
    TEST_DATA_T *norm = v2_0;                 // {e, e}
    VecSim_Normalize(norm, dim, params.type); // now {1/sqrt(2), 1/sqrt(2)}
    ASSERT_FLOAT_EQ(norm[0], 1.0f / sqrt(2.0));
    ASSERT_FLOAT_EQ(norm[1], 1.0f / sqrt(2.0));
    double dist;

    // VecSimMetric_L2
    // distances are [[0.000, 0.358], [0.179, 23.739]]
    // minimum of each label are:
    distances = {0, 0.1791922003030777};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], i, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_IP
    // distances are [[-18.739, -16.079], [-17.409, 1.000]]
    // minimum of each label are:
    distances = {-18.73921012878418, -17.409339904785156};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_IP], i, query);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // VecSimMetric_Cosine
    // distances are [[5.960e-08, 5.960e-08], [0.0026, 1.000]]
    // minimum of each label are:
    distances = {5.9604644775390625e-08, 0.0025991201400756836};
    for (size_t i = 0; i < n_labels; i++) {
        dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], i, norm);
        ASSERT_NEAR(dist, distances[i], 1e-5);
    }

    // Bad values
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_Cosine], -1, norm);
    ASSERT_TRUE(std::isnan(dist));
    dist = VecSimIndex_GetDistanceFrom_Unsafe(index[VecSimMetric_L2], 46, query);
    ASSERT_TRUE(std::isnan(dist));

    // Clean-up.
    for (size_t i = 0; i < numIndex; i++) {
        VecSimIndex_Free(index[i]);
    }
}

TYPED_TEST(HNSWMultiTest, testSizeEstimation) {
    size_t dim = 256;
    size_t perLabel = 1;
    size_t n_labels = 200;
    size_t bs = 256;
    size_t M = 64;

    size_t n = n_labels * perLabel;
    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs, .M = M};

    VecSimIndex *index = this->CreateNewIndex(params);
    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);
    size_t actual = index->getAllocationSize();

    ASSERT_EQ(estimation, actual);

    // Fill the initial capacity + fill the last block.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i);
    }
    idType cur = n;
    while (index->indexSize() % bs != 0) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, cur++);
    }

    // Estimate the memory delta of adding a single vector that requires a full new block.
    estimation = EstimateElementSize(params) * bs;
    size_t before = index->getAllocationSize();
    GenerateAndAddVector<TEST_DATA_T>(index, dim, bs, bs);
    actual = index->getAllocationSize() - before;

    // We check that the actual size is within 1% of the estimation.
    ASSERT_GE(estimation, actual * 0.99);
    ASSERT_LE(estimation, actual * 1.01);

    VecSimIndex_Free(index);
}

/**** resizing cases ****/

// Test empty index edge cases.
TYPED_TEST(HNSWMultiTest, emptyIndex) {
    size_t dim = 4;
    size_t bs = 6;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .blockSize = bs};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);
    size_t curr_capacity = index->indexCapacity();

    // Try to remove from an empty index - should fail because label doesn't exist.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 0), 0);

    // Add one vector.
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1.7);

    // Try to remove it.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 1);

    // The capacity should change to be zero.
    ASSERT_EQ(index->indexCapacity(), 0);

    // Size equals 0.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    // Try to remove it again.
    // The capacity should remain unchanged, as we are trying to delete a label that doesn't exist.
    ASSERT_EQ(VecSimIndex_DeleteVector(index, 1), 0);
    ASSERT_EQ(index->indexCapacity(), 0);
    // Nor the size.
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_vector_search_by_id_test) {
    size_t n = 100;
    size_t dim = 4;
    size_t k = 11;
    size_t per_label = 5;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .M = 16, .efConstruction = 200};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / per_label, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n / per_label);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 50);
    auto verify_res = [&](size_t id, double score, size_t index) { ASSERT_EQ(id, (index + 5)); };
    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, sanity_reinsert_1280) {
    size_t n_labels = 5;
    size_t per_label = 3;
    size_t d = 1280;
    size_t k = 5;

    size_t n = n_labels * per_label;

    HNSWParams params = {.dim = d, .metric = VecSimMetric_L2, .M = 16, .efConstruction = 200};

    VecSimIndex *index = this->CreateNewIndex(params);

    auto *vectors = new TEST_DATA_T[n * d];

    // Generate random vectors in every iteration and inert them under different ids.
    for (size_t iter = 1; iter <= 3; iter++) {
        for (size_t i = 0; i < n; i++) {
            for (size_t j = 0; j < d; j++) {
                (vectors + i * d)[j] = (TEST_DATA_T)rand() / (TEST_DATA_T)(RAND_MAX) / 100;
            }
        }
        auto expected_ids = std::set<size_t>();
        for (size_t i = 0; i < n; i++) {
            VecSimIndex_AddVector(index, (vectors + i * d), (i % n_labels) * iter);
            expected_ids.insert((i % n_labels) * iter);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_TRUE(expected_ids.find(id) != expected_ids.end());
            expected_ids.erase(id);
        };

        // Send arbitrary vector (the first) and search for top k. This should return all the
        // vectors that were inserted in this iteration - verify their ids.
        runTopKSearchTest(index, vectors, k, verify_res);

        // Remove vectors form current iteration.
        for (size_t i = 0; i < n_labels; i++) {
            VecSimIndex_DeleteVector(index, i * iter);
        }
    }
    delete[] vectors;
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, test_query_runtime_params_user_build_args) {
    size_t n = 100;
    size_t n_labels = 25;
    size_t per_label = 4;
    size_t d = 4;
    size_t M = 100;
    size_t efConstruction = 300;
    size_t efRuntime = 500;

    // Build with user args.

    HNSWParams params = {.dim = d,
                         .metric = VecSimMetric_L2,
                         .M = M,
                         .efConstruction = efConstruction,
                         .efRuntime = efRuntime};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, d, i / per_label, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    TEST_DATA_T query_element = (n_labels / 2) * per_label;
    auto verify_res = [&](size_t id, double score, size_t index) {
        size_t diff_id = (id < n_labels / 2) ? (n_labels / 2) - id : id - (n_labels / 2);
        double exp_elem = (id < n_labels / 2) ? id * per_label + per_label - 1 : id * per_label;
        double exp_score = d * (exp_elem - query_element) * (exp_elem - query_element);
        ASSERT_EQ(diff_id, (index + 1) / 2);
        ASSERT_EQ(score, exp_score);
    };
    size_t k = 11;
    TEST_DATA_T query[] = {query_element, query_element, query_element, query_element};
    runTopKSearchTest(index, query, k, verify_res);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    // Check that user args did not change.
    ASSERT_EQ(info.hnswInfo.M, M);
    ASSERT_EQ(info.hnswInfo.efConstruction, efConstruction);
    ASSERT_EQ(info.hnswInfo.efRuntime, efRuntime);

    // Run same query again, set efRuntime to 300.
    HNSWRuntimeParams hnswRuntimeParams = {.efRuntime = 300};
    VecSimQueryParams queryParams = CreateQueryParams(hnswRuntimeParams);
    runTopKSearchTest(index, query, k, verify_res, &queryParams);

    info = VecSimIndex_DebugInfo(index);
    // Check that user args did not change.
    ASSERT_EQ(info.hnswInfo.M, M);
    ASSERT_EQ(info.hnswInfo.efConstruction, efConstruction);
    ASSERT_EQ(info.hnswInfo.efRuntime, efRuntime);

    // Create batch iterator with query param - verify that ef_runtime is not effected.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, &queryParams);
    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.hnswInfo.efRuntime, efRuntime);
    // Run one batch for sanity.
    runBatchIteratorSearchTest(batchIterator, k, verify_res);
    // After releasing the batch iterator, ef_runtime should still be the default one.
    VecSimBatchIterator_Free(batchIterator);
    info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.hnswInfo.efRuntime, efRuntime);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_delete_entry_point) {
    size_t n = 10000;
    size_t per_label = 5;
    size_t dim = 4;
    size_t M = 2;

    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = 0, .efRuntime = 0};

    VecSimIndex *index = this->CreateNewIndex(params);

    ASSERT_TRUE(index != NULL);

    TEST_DATA_T vec[dim];
    for (size_t i = 0; i < dim; i++)
        vec[i] = i;
    for (size_t j = 0; j < n; j++)
        VecSimIndex_AddVector(index, vec, j / per_label);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);

    while (info.commonInfo.indexSize > 0) {
        ASSERT_NO_THROW(VecSimIndex_DeleteVector(index, info.hnswInfo.entrypoint));
        info = VecSimIndex_DebugInfo(index);
    }
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_batch_iterator_basic) {
    size_t dim = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n_labels = 1000;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = ef, .efRuntime = ef};

    VecSimIndex *index = this->CreateNewIndex(params);

    // For every i, add the vector (i,i,i,i) under the label i.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
    // in every iteration. The results order should be sorted by their score (distance from the
    // query vector), which means sorted from the largest id to the lowest.
    size_t n_res = 5;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        for (size_t i = 0; i < n_res; i++) {
            expected_ids[i] = (n_labels - iteration_num * n_res - i - 1);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(expected_ids[index], id);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n_labels / n_res);
    VecSimBatchIterator_Free(batchIterator);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_batch_iterator_reset) {
    size_t dim = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n_labels = 1000;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = ef, .efRuntime = ef};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Get the 100 vectors whose ids are the maximal among those that hasn't been returned yet, in
    // every iteration. Run this flow for 3 times, and reset the iterator.
    size_t n_res = 100;
    size_t re_runs = 3;

    for (size_t take = 0; take < re_runs; take++) {
        size_t iteration_num = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (n_labels - iteration_num * n_res - i - 1);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id);
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n_labels / n_res);
        VecSimBatchIterator_Reset(batchIterator);
    }
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_batch_iterator_batch_size_1) {
    size_t dim = 4;
    size_t M = 8;
    size_t ef = 2;
    size_t n_labels = 1000;
    size_t perLabel = 5;

    size_t n = n_labels * perLabel;

    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = ef, .efRuntime = ef};

    VecSimIndex *index = this->CreateNewIndex(params);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);

    for (size_t i = 0; i < n; i++) {
        // Set labels to be different than the internal ids.
        GenerateAndAddVector<TEST_DATA_T>(index, dim, (n - i - 1) / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;
    size_t n_res = 1, expected_n_res = 1;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        // Expect to get results in the reverse order of labels - which is the order of the distance
        // from the query vector. Get one result in every iteration.
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(id, iteration_num);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, expected_n_res);
        iteration_num++;
    }

    ASSERT_EQ(iteration_num, n_labels);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, hnsw_batch_iterator_advanced) {
    size_t dim = 4;
    size_t M = 8;
    size_t n_labels = 500;
    size_t perLabel = 5;
    size_t ef = n_labels;

    size_t n = n_labels * perLabel;

    HNSWParams params = {
        .dim = dim, .metric = VecSimMetric_L2, .M = M, .efConstruction = ef, .efRuntime = ef};

    VecSimIndex *index = this->CreateNewIndex(params);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Try to get results even though there are no vectors in the index.
    VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));
    VecSimBatchIterator_Free(batchIterator);

    // Insert one vector and query again. The internal id will be 0.
    VecSimIndex_AddVector(index, query, n_labels - 1);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 1);
    VecSimQueryReply_Free(res);
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator));
    VecSimBatchIterator_Free(batchIterator);

    // Insert vectors to the index and re-create the batch iterator.
    for (size_t i = 0; i < n - 1; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i / perLabel, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // Try to get 0 results.
    res = VecSimBatchIterator_Next(batchIterator, 0, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    // n_res does not divide into ef or vice versa - expect leftovers between the graph scans.
    size_t n_res = 7;
    size_t iteration_num = 0;

    while (VecSimBatchIterator_HasNext(batchIterator)) {
        iteration_num++;
        std::vector<size_t> expected_ids;
        // We ask to get the results sorted by ID in a specific batch (in ascending order), but
        // in every iteration the ids should be lower than the previous one, according to the
        // distance from the query.
        for (size_t i = 0; i < n_res; i++) {
            expected_ids.push_back(n_labels - iteration_num * n_res + i);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(expected_ids[index], id);
        };
        if (iteration_num <= n_labels / n_res) {
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID);
        } else {
            // In the last iteration there are n%n_res results left to return.
            // remove the first ids that aren't going to be returned since we pass the index size.
            for (size_t i = 0; i < n_res - n_labels % n_res; i++) {
                expected_ids.erase(expected_ids.begin());
            }
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID, n_labels % n_res);
        }
    }
    ASSERT_EQ(iteration_num, n_labels / n_res + 1);
    // Try to get more results even though there are no.
    res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), 0);
    VecSimQueryReply_Free(res);

    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, MultiBatchIteratorHeapLogic) {
    size_t n = 4;
    size_t n_labels = 3;
    size_t dim = 4;
    size_t n_res;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2, .M = 2, .efRuntime = 1};

    VecSimIndex *index = this->CreateNewIndex(params);

    // enforce max level to be 0
    this->CastToHNSW(index)->mult = 0;
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i % n_labels, i);
    }
    // enforce entry point to be 0
    this->CastToHNSW(index)->entrypointNode = 0;

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, n);
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);

    // We expect to scan the graph by order of insertion, and to find the last k vectors.
    // If the heaps update logic is true, we should get k results.
    n_res = 3;
    auto res = VecSimBatchIterator_Next(batchIterator, n_res, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res), n_res);
    VecSimQueryReply_Free(res);

    VecSimBatchIterator_Reset(batchIterator);
    n_res = 2;
    // We have 3 labels in the index. We expect to get 2 results in the first iteration and 1 in the
    // second, if the logic of extracting extras from the extras heap is true.
    auto res1 = VecSimBatchIterator_Next(batchIterator, n_res, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res1), n_res);
    auto res2 = VecSimBatchIterator_Next(batchIterator, n_res, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(res2), n_labels - n_res);

    VecSimQueryReply_Free(res1);
    VecSimQueryReply_Free(res2);

    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, testCosine) {
    size_t dim = 4;
    size_t n = 100;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_Cosine};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    // Add more worst vector for each label
    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i + n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, (n - result_rank));
        TEST_DATA_T expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
        ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
    };
    runTopKSearchTest(index, query, 10, verify_res);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, testCosineBatchIterator) {
    size_t dim = 4;
    size_t n = 100;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_Cosine};

    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    // Add more worst vector for each label
    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i + n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(index, f, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), 2 * n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // Get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
    // in every iteration. The order should be from the largest to the lowest id.
    size_t n_res = 10;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        auto verify_res_batch = [&](size_t id, double score, size_t result_rank) {
            ASSERT_EQ(id, (n - n_res * iteration_num - result_rank));
            TEST_DATA_T expected_score = index->getDistanceFrom_Unsafe(id, normalized_query);
            ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res_batch);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, rangeQuery) {
    size_t n_labels = 1000;
    size_t per_label = 5;
    size_t dim = 4;

    size_t n = n_labels * per_label;

    HNSWParams params{.dim = dim, .metric = VecSimMetric_L2, .blockSize = n / 2};
    VecSimIndex *index = this->CreateNewIndex(params);

    for (size_t i = 0; i < n_labels; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        // Add some vectors, worst than the previous vector (for the given query)
        for (size_t j = 0; j < per_label - 1; j++)
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i + n);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(index->indexLabelCount(), n_labels);

    size_t pivot_id = n_labels / 2; // the id to return vectors around it.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, pivot_id);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - pivot_id)), (index + 1) / 2);
        ASSERT_EQ(score, dim * pow((index + 1) / 2, 2));
    };
    uint expected_num_results = 11;
    // To get 11 results in the range [pivot_id - 5, pivot_id + 5], set the radius as the L2 score
    // in the boundaries.
    double radius = dim * pow(expected_num_results / 2, 2);
    runRangeQueryTest(index, query, radius, verify_res_by_score, expected_num_results, BY_SCORE);

    // Rerun with a given query params. This high epsilon value will cause the range search main
    // loop to break since we insert a candidate whose distance is within the dynamic range
    // boundaries at the beginning of the search, but when this candidate is popped out from the
    // queue, it's no longer within the dynamic range boundaries.
    VecSimQueryParams query_params = CreateQueryParams(HNSWRuntimeParams{.epsilon = 1.0});
    runRangeQueryTest(index, query, radius, verify_res_by_score, expected_num_results, BY_SCORE,
                      &query_params);

    // Get results by id.
    auto verify_res_by_id = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, pivot_id - expected_num_results / 2 + index);
        ASSERT_EQ(score, dim * pow(std::abs(int(id - pivot_id)), 2));
    };
    runRangeQueryTest(index, query, radius, verify_res_by_id, expected_num_results);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, markDelete) {
    size_t n_labels = 100;
    size_t per_label = 10;
    size_t k = 11;
    size_t dim = 4;
    VecSimBatchIterator *batchIterator;

    size_t n = n_labels * per_label;

    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};

    VecSimIndex *index = this->CreateNewIndex(params);
    // Try marking a non-existing label
    ASSERT_EQ(this->CastToHNSW(index)->markDelete(0),
              vecsim_stl::vector<idType>(index->getAllocator()));

    for (size_t i = 0; i < n_labels; i++) {
        for (size_t j = 0; j < per_label; j++)
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i * per_label + j);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

    // Search for k results from the origin. expect to find them.
    auto verify_res = [&](size_t id, double score, size_t idx) {
        ASSERT_EQ(id, idx);
        ASSERT_EQ(score, dim * per_label * per_label * idx * idx);
        auto ids = this->CastToHNSW_Multi(index)->labelLookup.at(id);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], id * per_label + j);
        }
    };
    runTopKSearchTest(index, query, k, verify_res);
    // with all vectors, this is the element of the k-th vector from the origin
    size_t all_element = per_label * (k - 1);
    runRangeQueryTest(index, query, dim * all_element * all_element, verify_res, k, BY_SCORE);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    runBatchIteratorSearchTest(batchIterator, k, verify_res);
    VecSimBatchIterator_Free(batchIterator);

    unsigned char ep_reminder = index->debugInfo().hnswInfo.entrypoint % 2;
    // Mark as deleted half of the vectors including the entrypoint.
    for (labelType label = 0; label < n_labels; label++) {
        if (label % 2 == ep_reminder) {
            vecsim_stl::vector<idType> expected_deleted_ids(index->getAllocator());
            for (size_t j = 0; j < per_label; j++)
                expected_deleted_ids.push_back(label * per_label + j);
            ASSERT_EQ(this->CastToHNSW(index)->markDelete(label), expected_deleted_ids);
        }
    }

    ASSERT_EQ(this->CastToHNSW(index)->getNumMarkedDeleted(), n / 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    ASSERT_EQ(this->CastToHNSW(index)->indexLabelCount(), n_labels / 2);

    // Add a new vector, make sure it has no link to a deleted vector (id/per_label should be even)
    // This value is very close to a deleted vector
    GenerateAndAddVector<TEST_DATA_T>(index, dim, n, n - per_label + 1);
    for (size_t level = 0; level <= this->CastToHNSW(index)->getGraphDataByInternalId(n)->toplevel;
         level++) {
        ElementLevelData &level_data = this->CastToHNSW(index)->getElementLevelData(n, level);
        for (size_t idx = 0; idx < level_data.numLinks; idx++) {
            ASSERT_TRUE((level_data.links[idx] / per_label) % 2 != ep_reminder)
                << "Got a link to " << level_data.links[idx] << " on level " << level;
        }
    }

    // Search for k results closest to the origin. expect to find only even results.
    auto verify_res_even = [&](size_t id, double score, size_t idx) {
        ASSERT_NE(id % 2, ep_reminder);
        ASSERT_EQ(id, idx * 2);
        ASSERT_EQ(score, dim * per_label * per_label * id * id);
        auto ids = this->CastToHNSW_Multi(index)->labelLookup.at(id);
        for (size_t j = 0; j < ids.size(); j++) {
            // Verifying that each vector is labeled correctly.
            // ID is calculated according to insertion order.
            ASSERT_EQ(ids[j], id * per_label + j);
        }
    };
    runTopKSearchTest(index, query, k, verify_res_even);
    // with only even vectors, this is the element of the k-th vector from the origin.
    size_t even_el = all_element * 2 + (1 - ep_reminder);
    runRangeQueryTest(index, query, dim * even_el * even_el, verify_res_even, k, BY_SCORE);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    runBatchIteratorSearchTest(batchIterator, k, verify_res_even);
    VecSimBatchIterator_Free(batchIterator);

    for (labelType label = 0; label < n_labels; label++) {
        if (label % 2 == ep_reminder) {
            for (size_t j = 0; j < per_label; j++) {
                GenerateAndAddVector<TEST_DATA_T>(index, dim, label, label * per_label + j);
            }
        }
    }
    ASSERT_EQ(this->CastToHNSW(index)->getNumMarkedDeleted(), n / 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), n + n / 2 + 1);
    ASSERT_EQ(this->CastToHNSW(index)->indexLabelCount(), n_labels + 1);

    // Search for k results closest to the origin again. expect to find the same results we found in
    // the first search (the validation over the internal ids is removed, as these internal ids
    // change). Search for k results from the origin. expect to find them.
    auto verify_res_after_reinsert = [&](size_t id, double score, size_t idx) {
        ASSERT_EQ(id, idx);
        ASSERT_EQ(score, dim * per_label * per_label * idx * idx);
    };

    runTopKSearchTest(index, query, k, verify_res_after_reinsert);
    runRangeQueryTest(index, query, dim * all_element * all_element, verify_res_after_reinsert, k,
                      BY_SCORE);
    batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    runBatchIteratorSearchTest(batchIterator, k, verify_res_after_reinsert);
    VecSimBatchIterator_Free(batchIterator);
    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, relabelVectorMulti) {
    size_t dim = 4;
    size_t per_label = 3;
    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *hnsw_index = this->CastToHNSW(index);

    // Two labels, each holding several vectors.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    auto ids_before = hnsw_index->getElementIds(0);
    ASSERT_EQ(ids_before.size(), per_label);

    ASSERT_EQ(VecSimIndex_RelabelVector(index, 0, 100), VecSimRelabel_OK);

    // Every id under the label moves together, keeping its order, and the untouched label is
    // unaffected.
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    ASSERT_FALSE(hnsw_index->isLabelExists(0));
    ASSERT_EQ(hnsw_index->getElementIds(100), ids_before);
    for (idType id : ids_before) {
        ASSERT_EQ(hnsw_index->getExternalLabel(id), 100);
    }
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), per_label);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // All `per_label` vectors are still searchable, now reported under the new label.
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);
    auto verify_res = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 100); };
    runTopKSearchTest(index, query, 1, verify_res);

    // Moving onto an occupied label is rejected without disturbing either label.
    ASSERT_EQ(VecSimIndex_RelabelVector(index, 100, 1), VecSimRelabel_NewLabelTaken);
    ASSERT_EQ(hnsw_index->getElementIds(100).size(), per_label);
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), per_label);

    VecSimIndex_Free(index);
}

TYPED_TEST(HNSWMultiTest, updateVectorsMulti) {
    size_t dim = 4;
    size_t per_label = 3;
    HNSWParams params = {.dim = dim, .metric = VecSimMetric_L2};
    VecSimIndex *index = this->CreateNewIndex(params);
    auto *hnsw_index = this->CastToHNSW(index);

    // Label 0 holds the values 0, 1, 2 and label 1 holds 10, 11, 12.
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, i);
        GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, i + 10);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label * 2);
    ASSERT_EQ(index->indexLabelCount(), 2);

    // Replace label 0's three vectors with two different ones. How many the label ends up holding
    // is decided by the update, not by what was there - which is the case `addVector` cannot
    // express at all, since in a multi-value index it appends.
    TEST_DATA_T two[2 * dim];
    GenerateVector<TEST_DATA_T>(two, dim, 100);
    GenerateVector<TEST_DATA_T>(two + dim, dim, 101);
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, two, 2), VecSimUpdate_OK);

    ASSERT_EQ(hnsw_index->getElementIds(0).size(), 2);
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label + 2);
    ASSERT_EQ(index->indexLabelCount(), 2);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // The label holds the two new vectors and none of the three it held.
    // (`getDistanceFrom_Unsafe` reports the closest vector of the label, so a zero distance means
    // the label holds that exact vector.)
    for (size_t value : {100, 101}) {
        TEST_DATA_T stored[dim];
        GenerateVector<TEST_DATA_T>(stored, dim, value);
        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(0, stored), 0) << "missing " << value;
    }
    for (size_t value : {0, 1, 2}) {
        TEST_DATA_T gone[dim];
        GenerateVector<TEST_DATA_T>(gone, dim, value);
        ASSERT_NE(hnsw_index->getDistanceFrom_Unsafe(0, gone), 0) << "kept " << value;
    }

    // The other label is untouched throughout.
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), per_label);
    for (size_t value : {10, 11, 12}) {
        TEST_DATA_T other[dim];
        GenerateVector<TEST_DATA_T>(other, dim, value);
        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(1, other), 0) << "lost " << value;
    }

    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 0);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(index, two, 1, verify_res);

    // Growing the label works the same way.
    TEST_DATA_T four[4 * dim];
    for (size_t i = 0; i < 4; i++) {
        GenerateVector<TEST_DATA_T>(four + i * dim, dim, 200 + i);
    }
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, four, 4), VecSimUpdate_OK);
    ASSERT_EQ(hnsw_index->getElementIds(0).size(), 4);
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label + 4);
    ASSERT_EQ(index->indexLabelCount(), 2);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // And an empty update removes the label altogether.
    ASSERT_EQ(VecSimIndex_UpdateVectors(index, 0, nullptr, 0), VecSimUpdate_OK);
    ASSERT_FALSE(hnsw_index->isLabelExists(0));
    ASSERT_EQ(VecSimIndex_IndexSize(index), per_label);
    ASSERT_EQ(index->indexLabelCount(), 1);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    VecSimIndex_Free(index);
}
