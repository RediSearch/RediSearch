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
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "tests_utils.h"
#include "unit_test_utils.h"
#include "mock_thread_pool.h"
#include "VecSim/vec_sim_debug.h"
#include "VecSim/spaces/L2/L2.h"
#include "VecSim/spaces/IP/IP.h"
#include "VecSim/spaces/normalize/normalize_naive.h"

class INT8Test : public ::testing::Test {
protected:
    virtual void SetUp(HNSWParams &params) {
        FAIL() << "INT8Test::SetUp(HNSWParams) this method should be overriden";
    }

    virtual void SetUp(BFParams &params) {
        FAIL() << "INT8Test::SetUp(BFParams) this method should be overriden";
    }

    virtual void SetUp(TieredIndexParams &tiered_params) {
        FAIL() << "INT8Test::SetUp(TieredIndexParams) this method should be overriden";
    }

    virtual void TearDown() { VecSimIndex_Free(index); }

    virtual const void *GetDataByInternalId(idType id) = 0;

    template <typename algo_t>
    algo_t *CastIndex() {
        return dynamic_cast<algo_t *>(index);
    }

    template <typename algo_t>
    algo_t *CastIndex(VecSimIndex *vecsim_index) {
        return dynamic_cast<algo_t *>(vecsim_index);
    }

    virtual HNSWIndex<int8_t, float> *CastToHNSW() { return CastIndex<HNSWIndex<int8_t, float>>(); }

    void PopulateRandomVector(int8_t *out_vec) {
        test_utils::populate_int8_vec(out_vec, dim, current_seed++);
    }
    int PopulateRandomAndAddVector(size_t id, int8_t *out_vec) {
        PopulateRandomVector(out_vec);
        return VecSimIndex_AddVector(index, out_vec, id);
    }

    virtual int GenerateAndAddVector(size_t id, int8_t value = 1) {
        // use unit_test_utils.h
        return ::GenerateAndAddVector<int8_t>(index, dim, id, value);
    }

    void GenerateVector(int8_t *out_vec, int8_t value) {
        // use unit_test_utils.h
        return ::GenerateVector<int8_t>(out_vec, this->dim, value);
    }

    virtual int GenerateRandomAndAddVector(size_t id) {
        int8_t v[dim];
        PopulateRandomVector(v);
        return VecSimIndex_AddVector(index, v, id);
    }

    size_t GetValidVectorsCount() {
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
        return info.commonInfo.indexLabelCount;
    }

    template <typename params_t>
    void create_index_test(params_t index_params);
    template <typename params_t>
    void element_size_test(params_t index_params);
    template <typename params_t>
    void search_by_id_test(params_t index_params);
    template <typename params_t>
    void search_by_score_test(params_t index_params);
    template <typename params_t>
    void metrics_test(params_t index_params);
    template <typename params_t>
    void search_empty_index_test(params_t index_params);
    template <typename params_t>
    void test_override(params_t index_params);
    template <typename params_t>
    void test_range_query(params_t index_params);
    template <typename params_t>
    void test_batch_iterator_basic(params_t index_params);
    template <typename params_t>
    VecSimIndexDebugInfo test_info(params_t index_params);
    template <typename params_t>
    void test_info_iterator(VecSimMetric metric);
    template <typename params_t>
    void get_element_neighbors(params_t index_params);

    VecSimIndex *index;
    size_t dim;
    int current_seed{0};
};

class INT8HNSWTest : public INT8Test {
protected:
    virtual void SetUp(HNSWParams &params) override {
        params.type = VecSimType_INT8;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        dim = params.dim;
    }

    virtual const void *GetDataByInternalId(idType id) override {
        return CastIndex<HNSWIndex_Single<int8_t, float>>()->getDataByInternalId(id);
    }

    virtual HNSWIndex<int8_t, float> *CastToHNSW() override {
        return CastIndex<HNSWIndex<int8_t, float>>(index);
    }

    HNSWIndex<int8_t, float> *CastToHNSW(VecSimIndex *new_index) {
        return CastIndex<HNSWIndex<int8_t, float>>(new_index);
    }

    void test_info(bool is_multi);
    void test_serialization(bool is_multi);
};

class INT8BruteForceTest : public INT8Test {
protected:
    virtual void SetUp(BFParams &params) override {
        params.type = VecSimType_INT8;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        dim = params.dim;
    }

    virtual const void *GetDataByInternalId(idType id) override {
        return CastIndex<BruteForceIndex_Single<int8_t, float>>()->getDataByInternalId(id);
    }

    virtual HNSWIndex<int8_t, float> *CastToHNSW() override {
        ADD_FAILURE() << "INT8BruteForceTest::CastToHNSW() this method should not be called";
        return nullptr;
    }

    void test_info(bool is_multi);
};

class INT8TieredTest : public INT8Test {
protected:
    TieredIndexParams generate_tiered_params(HNSWParams &hnsw_params, size_t swap_job_threshold = 1,
                                             size_t flat_buffer_limit = SIZE_MAX) {
        hnsw_params.type = VecSimType_INT8;
        vecsim_hnsw_params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = flat_buffer_limit,
            .primaryIndexParams = &vecsim_hnsw_params,
            .specificParams = {TieredHNSWParams{.swapJobThreshold = swap_job_threshold}}};
        return tiered_params;
    }

    virtual void SetUp(TieredIndexParams &tiered_params) override {
        ASSERT_EQ(tiered_params.primaryIndexParams->algoParams.hnswParams.type, VecSimType_INT8);
        VecSimParams params = CreateParams(tiered_params);
        index = VecSimIndex_New(&params);
        dim = tiered_params.primaryIndexParams->algoParams.hnswParams.dim;

        // Set the created tiered index in the index external context.
        mock_thread_pool.ctx->index_strong_ref.reset(index);
    }

    virtual void SetUp(HNSWParams &hnsw_params) override {
        TieredIndexParams tiered_params = generate_tiered_params(hnsw_params);
        SetUp(tiered_params);
    }

    virtual void TearDown() override {}

    virtual const void *GetDataByInternalId(idType id) override {
        return CastToBruteForce()->getDataByInternalId(id);
    }

    virtual HNSWIndex<int8_t, float> *CastToHNSW() override {
        auto tiered_index = dynamic_cast<TieredHNSWIndex<int8_t, float> *>(index);
        return tiered_index->getHNSWIndex();
    }

    virtual HNSWIndex_Single<int8_t, float> *CastToHNSWSingle() {
        return CastIndex<HNSWIndex_Single<int8_t, float>>(CastToHNSW());
    }

    BruteForceIndex<int8_t, float> *CastToBruteForce() {
        auto tiered_index = dynamic_cast<TieredHNSWIndex<int8_t, float> *>(index);
        return tiered_index->getFlatBufferIndex();
    }

    int GenerateRandomAndAddVector(size_t id) override {
        int8_t v[dim];
        PopulateRandomVector(v);
        int ret = VecSimIndex_AddVector(index, v, id);
        mock_thread_pool.thread_iteration();
        return ret;
    }

    int GenerateAndAddVector(size_t id, int8_t value) override {
        // use unit_test_utils.h
        int ret = INT8Test::GenerateAndAddVector(id, value);
        mock_thread_pool.thread_iteration();
        return ret;
    }

    void test_info(bool is_multi);
    void test_info_iterator(VecSimMetric metric);

    VecSimParams vecsim_hnsw_params;
    tieredIndexMock mock_thread_pool;
};

/* ---------------------------- Create index tests ---------------------------- */

template <typename params_t>
void INT8Test::create_index_test(params_t index_params) {
    SetUp(index_params);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    int8_t vector[dim];
    this->PopulateRandomVector(vector);
    VecSimIndex_AddVector(index, vector, 0);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);
    ASSERT_EQ(index->getDistanceFrom_Unsafe(0, vector), 0);

    ASSERT_NO_FATAL_FAILURE(
        CompareVectors(static_cast<const int8_t *>(this->GetDataByInternalId(0)), vector, dim));
}

TEST_F(INT8HNSWTest, createIndex) {
    HNSWParams params = {.dim = 40, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(create_index_test(params));
    ASSERT_EQ(index->basicInfo().type, VecSimType_INT8);
    ASSERT_EQ(index->basicInfo().algo, VecSimAlgo_HNSWLIB);
}

TEST_F(INT8BruteForceTest, createIndex) {
    BFParams params = {.dim = 40};
    EXPECT_NO_FATAL_FAILURE(create_index_test(params));
    ASSERT_EQ(index->basicInfo().type, VecSimType_INT8);
    ASSERT_EQ(index->basicInfo().algo, VecSimAlgo_BF);
}

TEST_F(INT8TieredTest, createIndex) {
    HNSWParams params = {.dim = 40, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(create_index_test(params));
    ASSERT_EQ(index->basicInfo().type, VecSimType_INT8);
    ASSERT_EQ(index->basicInfo().isTiered, true);
}

/* ---------------------------- Size Estimation tests ---------------------------- */

template <typename params_t>
void INT8Test::element_size_test(params_t index_params) {
    SetUp(index_params);

    // Estimate the memory delta of adding a single vector that requires a full new block.
    size_t estimation = EstimateElementSize(index_params) * DEFAULT_BLOCK_SIZE;
    size_t before = index->getAllocationSize();
    ASSERT_EQ(this->GenerateRandomAndAddVector(0), 1);
    size_t actual = index->getAllocationSize() - before;

    // We check that the actual size is within 1% of the estimation.
    ASSERT_GE(estimation, actual * 0.99);
    ASSERT_LE(estimation, actual * 1.01);
}

TEST_F(INT8HNSWTest, elementSizeEstimation) {
    size_t M = 64;

    HNSWParams params = {.dim = 4, .M = M};
    EXPECT_NO_FATAL_FAILURE(element_size_test(params));
}

TEST_F(INT8BruteForceTest, elementSizeEstimation) {
    BFParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(element_size_test(params));
}

TEST_F(INT8TieredTest, elementSizeEstimation) {
    size_t M = 64;
    HNSWParams hnsw_params = {.dim = 4, .M = M};
    EXPECT_NO_FATAL_FAILURE(element_size_test(hnsw_params));
}

// ======= Cosine size estimation (stored vectors include additional space for norm)
TEST_F(INT8HNSWTest, elementSizeEstimation_Cosine) {
    size_t M = 64;

    HNSWParams params = {.dim = 4, .metric = VecSimMetric_Cosine, .M = M};
    EXPECT_NO_FATAL_FAILURE(element_size_test(params));
}

TEST_F(INT8BruteForceTest, elementSizeEstimation_Cosine) {
    BFParams params = {.dim = 4, .metric = VecSimMetric_Cosine};
    EXPECT_NO_FATAL_FAILURE(element_size_test(params));
}

TEST_F(INT8TieredTest, elementSizeEstimation_Cosine) {
    size_t M = 64;
    HNSWParams hnsw_params = {.dim = 4, .metric = VecSimMetric_Cosine, .M = M};
    EXPECT_NO_FATAL_FAILURE(element_size_test(hnsw_params));
}
/* ---------------------------- Functionality tests ---------------------------- */

template <typename params_t>
void INT8Test::search_by_id_test(params_t index_params) {
    SetUp(index_params);

    size_t k = 11;
    int8_t n = 100;

    for (int8_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(i, i); // {i, i, i, i}
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    int8_t query[dim];
    this->GenerateVector(query, 50); // {50, 50, 50, 50}

    // Vectors values are equal to the id, so the 11 closest vectors are 45, 46...50
    // (closest), 51...55
    static size_t expected_res_order[] = {45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, expected_res_order[index]);    // results are sorted by ID
        ASSERT_EQ(score, 4 * (50 - id) * (50 - id)); // L2 distance
    };

    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);
}

TEST_F(INT8HNSWTest, searchByID) {
    HNSWParams params = {.dim = 4, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(search_by_id_test(params));
}

TEST_F(INT8BruteForceTest, searchByID) {
    BFParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(search_by_id_test(params));
}

TEST_F(INT8TieredTest, searchByID) {
    HNSWParams params = {.dim = 4, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(search_by_id_test(params));
}

template <typename params_t>
void INT8Test::search_by_score_test(params_t index_params) {
    SetUp(index_params);

    size_t k = 11;
    size_t n = 100;

    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(i, i); // {i, i, i, i}
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    int8_t query[dim];
    this->GenerateVector(query, 50); // {50, 50, 50, 50}

    // Vectors values are equal to the id, so the 11 closest vectors are
    // 45, 46...50 (closest), 51...55
    static size_t expected_res_order[] = {50, 49, 51, 48, 52, 47, 53, 46, 54, 45, 55};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, expected_res_order[index]);
        ASSERT_EQ(score, 4 * (50 - id) * (50 - id)); // L2 distance
    };

    // Search by score
    runTopKSearchTest(index, query, k, verify_res);
}

TEST_F(INT8HNSWTest, searchByScore) {
    HNSWParams params = {.dim = 4, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(search_by_score_test(params));
}

TEST_F(INT8BruteForceTest, searchByScore) {
    BFParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(search_by_score_test(params));
}

TEST_F(INT8TieredTest, searchByScore) {
    HNSWParams params = {.dim = 4, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(search_by_score_test(params));
}

template <typename params_t>
void INT8Test::metrics_test(params_t index_params) {
    SetUp(index_params);
    size_t n = 10;
    VecSimMetric metric = index_params.metric;
    double expected_score = 0;

    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_NEAR(score, expected_score, 1e-6f) << "failed at vector id:" << id;
    };

    for (size_t i = 0; i < n; i++) {
        int8_t vector[dim];
        this->PopulateRandomAndAddVector(i, vector);

        if (metric == VecSimMetric_Cosine) {
            // compare with the norm stored in the index vector
            const int8_t *index_vector = static_cast<const int8_t *>(this->GetDataByInternalId(i));
            float index_vector_norm = load_unaligned<float>(index_vector + dim);
            float vector_norm = spaces::IntegralType_ComputeNorm<int8_t>(vector, dim);
            ASSERT_EQ(index_vector_norm, vector_norm) << "wrong vector norm for vector id:" << i;
        } else if (metric == VecSimMetric_IP) {
            expected_score = INT8_InnerProduct(vector, vector, dim);
        }

        // query index with k = 1 expect to get the vector
        runTopKSearchTest(index, vector, 1, verify_res);
        ASSERT_EQ(VecSimIndex_IndexSize(index), i + 1);
    }
}

TEST_F(INT8HNSWTest, CosineTest) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_Cosine, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}
TEST_F(INT8HNSWTest, IPTest) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_IP, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE((metrics_test)(params));
}
TEST_F(INT8HNSWTest, L2Test) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_L2, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}

TEST_F(INT8BruteForceTest, CosineTest) {
    BFParams params = {.dim = 40, .metric = VecSimMetric_Cosine};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}
TEST_F(INT8BruteForceTest, IPTest) {
    BFParams params = {.dim = 40, .metric = VecSimMetric_IP};
    EXPECT_NO_FATAL_FAILURE((metrics_test)(params));
}
TEST_F(INT8BruteForceTest, L2Test) {
    BFParams params = {.dim = 40, .metric = VecSimMetric_L2};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}

TEST_F(INT8TieredTest, CosineTest) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_Cosine, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}
TEST_F(INT8TieredTest, IPTest) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_IP, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE((metrics_test)(params));
}
TEST_F(INT8TieredTest, L2Test) {
    HNSWParams params = {.dim = 40, .metric = VecSimMetric_L2, .M = 16, .efConstruction = 200};
    EXPECT_NO_FATAL_FAILURE(metrics_test(params));
}

template <typename params_t>
void INT8Test::search_empty_index_test(params_t params) {
    size_t n = 100;
    size_t k = 11;

    SetUp(params);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    int8_t query[dim];
    this->GenerateVector(query, 50); // {50, 50, 50, 50}

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
        this->GenerateAndAddVector(i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
    // vectors marked as deleted will be included in VecSimIndex_IndexSize
    ASSERT_EQ(GetValidVectorsCount(), 0);

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
}

TEST_F(INT8HNSWTest, SearchEmptyIndex) {
    HNSWParams params = {.dim = 4, .initialCapacity = 0};
    EXPECT_NO_FATAL_FAILURE(search_empty_index_test(params));
}

TEST_F(INT8BruteForceTest, SearchEmptyIndex) {
    BFParams params = {.dim = 4, .initialCapacity = 0};
    EXPECT_NO_FATAL_FAILURE(search_empty_index_test(params));
}

TEST_F(INT8TieredTest, SearchEmptyIndex) {
    HNSWParams params = {.dim = 4, .initialCapacity = 0};
    EXPECT_NO_FATAL_FAILURE(search_empty_index_test(params));
}

template <typename params_t>
void INT8Test::test_override(params_t params) {
    size_t n = 50;
    size_t new_n = 120;
    SetUp(params);

    // Insert n vectors.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, i), 1);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Override n vectors, the first 100 will be overwritten (deleted first).
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(i, i), 0);
    }

    // Add up to new_n vectors.
    for (size_t i = n; i < new_n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(i, i), 1);
    }

    int8_t query[dim];
    this->GenerateVector(query, new_n);

    // Vectors values equals their id, so we expect the larger the id the closest it will be to the
    // query.
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, new_n - 1 - index) << "id: " << id << " score: " << score;
        float diff = new_n - id;
        float exp_score = 4 * diff * diff;
        ASSERT_EQ(score, exp_score) << "id: " << id << " score: " << score;
    };
    runTopKSearchTest(index, query, 300, verify_res);
}

TEST_F(INT8HNSWTest, Override) {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 100, .M = 8, .efConstruction = 20, .efRuntime = 250};
    EXPECT_NO_FATAL_FAILURE(test_override(params));
}

TEST_F(INT8BruteForceTest, Override) {
    BFParams params = {.dim = 4, .initialCapacity = 100};
    EXPECT_NO_FATAL_FAILURE(test_override(params));
}

TEST_F(INT8TieredTest, Override) {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 100, .M = 8, .efConstruction = 20, .efRuntime = 250};
    EXPECT_NO_FATAL_FAILURE(test_override(params));
}

template <typename params_t>
void INT8Test::test_range_query(params_t params) {
    size_t n = 100;
    SetUp(params);

    int8_t pivot_value = 1;
    int8_t pivot_vec[dim];
    this->GenerateVector(pivot_vec, pivot_value);

    int8_t radius = 20;
    std::mt19937 gen(42);
    std::uniform_int_distribution<int16_t> dis(pivot_value - radius, pivot_value + radius);

    // insert 20 vectors near a pivot vector.
    size_t n_close = 20;
    for (size_t i = 0; i < n_close; i++) {
        int8_t random_number = static_cast<int8_t>(dis(gen));
        this->GenerateAndAddVector(i, random_number);
    }

    int8_t max_vec[dim];
    GenerateVector(max_vec, pivot_value + radius);
    float max_dist = INT8_L2Sqr(pivot_vec, max_vec, dim);

    // Add more vectors far from the pivot vector
    for (size_t i = n_close; i < n; i++) {
        int8_t random_number = static_cast<int8_t>(dis(gen));
        GenerateAndAddVector(i, 50 + random_number);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_LE(id, n_close - 1) << "score: " << score;
        ASSERT_LE(score, max_dist);
    };
    size_t expected_num_results = n_close;

    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE);
}

TEST_F(INT8HNSWTest, rangeQuery) {
    HNSWParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(test_range_query(params));
}

TEST_F(INT8BruteForceTest, rangeQuery) {
    BFParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(test_range_query(params));
}

TEST_F(INT8TieredTest, rangeQuery) {
    HNSWParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(test_range_query(params));
}

/* ---------------------------- Batch iterator tests ---------------------------- */

template <typename params_t>
void INT8Test::test_batch_iterator_basic(params_t params) {
    SetUp(params);
    size_t n = 100;

    // For every i, add the vector (i,i,i,i) under the label i.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(i, i), 1);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    int8_t query[dim];
    GenerateVector(query, n);

    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
    size_t iteration_num = 0;

    // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
    // in every iteration. The results order should be sorted by their score (distance from the
    // query vector), which means sorted from the largest id to the lowest.
    size_t n_res = 5;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        for (size_t i = 0; i < n_res; i++) {
            expected_ids[i] = (n - iteration_num * n_res - i - 1);
        }
        auto verify_res = [&](size_t id, double score, size_t index) {
            ASSERT_EQ(expected_ids[index], id)
                << "iteration_num: " << iteration_num << " index: " << index << " score: " << score;
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
}

TEST_F(INT8HNSWTest, BatchIteratorBasic) {
    HNSWParams params = {.dim = 4, .M = 8, .efConstruction = 20, .efRuntime = 100};
    EXPECT_NO_FATAL_FAILURE(test_batch_iterator_basic(params));
}

TEST_F(INT8BruteForceTest, BatchIteratorBasic) {
    BFParams params = {.dim = 4};
    EXPECT_NO_FATAL_FAILURE(test_batch_iterator_basic(params));
}

TEST_F(INT8TieredTest, BatchIteratorBasic) {
    HNSWParams params = {.dim = 4, .M = 8, .efConstruction = 20, .efRuntime = 100};
    EXPECT_NO_FATAL_FAILURE(test_batch_iterator_basic(params));
}

/* ---------------------------- Info tests ---------------------------- */

template <typename params_t>
VecSimIndexDebugInfo INT8Test::test_info(params_t params) {
    SetUp(params);
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    EXPECT_EQ(info.commonInfo.basicInfo.dim, params.dim);
    EXPECT_EQ(info.commonInfo.basicInfo.isMulti, params.multi);
    EXPECT_EQ(info.commonInfo.basicInfo.type, VecSimType_INT8);
    EXPECT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, index->getAllocationSize());
    EXPECT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_L2);

    // Validate that basic info returns the right restricted info as well.
    VecSimIndexBasicInfo s_info = VecSimIndex_BasicInfo(index);
    EXPECT_EQ(info.commonInfo.basicInfo.algo, s_info.algo);
    EXPECT_EQ(info.commonInfo.basicInfo.dim, s_info.dim);
    EXPECT_EQ(info.commonInfo.basicInfo.blockSize, s_info.blockSize);
    EXPECT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    EXPECT_EQ(info.commonInfo.basicInfo.isMulti, s_info.isMulti);
    EXPECT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    EXPECT_EQ(info.commonInfo.basicInfo.isTiered, s_info.isTiered);

    return info;
}

void INT8HNSWTest::test_info(bool is_multi) {
    HNSWParams params = {.dim = 128, .multi = is_multi};
    VecSimIndexDebugInfo info = INT8Test::test_info(params);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);

    ASSERT_EQ(info.hnswInfo.M, HNSW_DEFAULT_M);
    ASSERT_EQ(info.hnswInfo.efConstruction, HNSW_DEFAULT_EF_C);
    ASSERT_EQ(info.hnswInfo.efRuntime, HNSW_DEFAULT_EF_RT);
    ASSERT_DOUBLE_EQ(info.hnswInfo.epsilon, HNSW_DEFAULT_EPSILON);
}
TEST_F(INT8HNSWTest, testInfoSingle) { test_info(false); }
TEST_F(INT8HNSWTest, testInfoMulti) { test_info(true); }

void INT8BruteForceTest::test_info(bool is_multi) {
    BFParams params = {.dim = 128, .multi = is_multi};
    VecSimIndexDebugInfo info = INT8Test::test_info(params);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
}

TEST_F(INT8BruteForceTest, testInfoSingle) { test_info(false); }
TEST_F(INT8BruteForceTest, testInfoMulti) { test_info(true); }

void INT8TieredTest::test_info(bool is_multi) {
    size_t bufferLimit = SIZE_MAX;
    HNSWParams hnsw_params = {.dim = 128, .multi = is_multi};

    VecSimIndexDebugInfo info = INT8Test::test_info(hnsw_params);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    VecSimIndexDebugInfo frontendIndexInfo = CastToBruteForce()->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = CastToHNSW()->debugInfo();

    compareCommonInfo(info.tieredInfo.frontendCommonInfo, frontendIndexInfo.commonInfo);
    compareFlatInfo(info.tieredInfo.bfInfo, frontendIndexInfo.bfInfo);
    compareCommonInfo(info.tieredInfo.backendCommonInfo, backendIndexInfo.commonInfo);
    compareHNSWInfo(info.tieredInfo.backendInfo.hnswInfo, backendIndexInfo.hnswInfo);

    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          backendIndexInfo.commonInfo.memory +
                                          frontendIndexInfo.commonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
    EXPECT_EQ(info.tieredInfo.bufferLimit, bufferLimit);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo.pendingSwapJobsThreshold, 1);

    INT8Test::GenerateAndAddVector(1, 1);
    info = index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);

    mock_thread_pool.thread_iteration();
    info = index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);

    if (is_multi) {
        INT8Test::GenerateAndAddVector(1, 1);
        info = index->debugInfo();

        EXPECT_EQ(info.commonInfo.indexSize, 2);
        EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                              info.tieredInfo.backendCommonInfo.memory +
                                              info.tieredInfo.frontendCommonInfo.memory);
        EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);
    }

    VecSimIndex_DeleteVector(index, 1);
    info = index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
}

TEST_F(INT8TieredTest, testInfoSingle) { test_info(false); }
TEST_F(INT8TieredTest, testInfoMulti) { test_info(true); }

template <typename params_t>
void INT8Test::test_info_iterator(VecSimMetric metric) {
    params_t params = {.dim = 128, .metric = metric};
    SetUp(params);
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
    VecSimAlgo algo = info.commonInfo.basicInfo.algo;
    if (algo == VecSimAlgo_HNSWLIB) {
        compareHNSWIndexInfoToIterator(info, infoIter);
    } else if (algo == VecSimAlgo_BF) {
        compareFlatIndexInfoToIterator(info, infoIter);
    }
    VecSimDebugInfoIterator_Free(infoIter);
}

TEST_F(INT8BruteForceTest, InfoIteratorCosine) {
    test_info_iterator<BFParams>(VecSimMetric_Cosine);
}
TEST_F(INT8BruteForceTest, InfoIteratorIP) { test_info_iterator<BFParams>(VecSimMetric_IP); }
TEST_F(INT8BruteForceTest, InfoIteratorL2) { test_info_iterator<BFParams>(VecSimMetric_L2); }
TEST_F(INT8HNSWTest, InfoIteratorCosine) { test_info_iterator<HNSWParams>(VecSimMetric_Cosine); }
TEST_F(INT8HNSWTest, InfoIteratorIP) { test_info_iterator<HNSWParams>(VecSimMetric_IP); }
TEST_F(INT8HNSWTest, InfoIteratorL2) { test_info_iterator<HNSWParams>(VecSimMetric_L2); }

void INT8TieredTest::test_info_iterator(VecSimMetric metric) {
    size_t n = 100;
    size_t d = 128;
    HNSWParams params = {.dim = d, .metric = metric, .initialCapacity = n};
    SetUp(params);
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(index);
    VecSimIndexDebugInfo frontendIndexInfo = CastToBruteForce()->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = CastToHNSW()->debugInfo();
    VecSimDebugInfoIterator_Free(infoIter);
}

TEST_F(INT8TieredTest, InfoIteratorCosine) { test_info_iterator(VecSimMetric_Cosine); }
TEST_F(INT8TieredTest, InfoIteratorIP) { test_info_iterator(VecSimMetric_IP); }
TEST_F(INT8TieredTest, InfoIteratorL2) { test_info_iterator(VecSimMetric_L2); }

/* ---------------------------- HNSW specific tests ---------------------------- */

void INT8HNSWTest::test_serialization(bool is_multi) {
    size_t dim = 4;
    size_t n = 1001;
    size_t n_labels[] = {n, 100};
    size_t M = 8;
    size_t ef = 10;
    double epsilon = 0.004;
    size_t blockSize = 20;
    std::string multiToString[] = {"single", "multi_100labels_"};

    HNSWParams params{.type = VecSimType_INT8,
                      .dim = dim,
                      .metric = VecSimMetric_Cosine,
                      .multi = is_multi,
                      .initialCapacity = n,
                      .blockSize = blockSize,
                      .M = M,
                      .efConstruction = ef,
                      .efRuntime = ef,
                      .epsilon = epsilon};
    SetUp(params);

    auto *hnsw_index = this->CastToHNSW();

    int8_t data[n * dim];

    for (size_t i = 0; i < n * dim; i += dim) {
        this->PopulateRandomVector(data + i);
    }

    for (size_t j = 0; j < n; ++j) {
        VecSimIndex_AddVector(index, data + dim * j, j % n_labels[is_multi]);
    }

    auto file_name = std::string(getenv("ROOT")) + "/tests/unit/1k-d4-L2-M8-ef_c10_" +
                     VecSimType_ToString(VecSimType_INT8) + "_" + multiToString[is_multi] +
                     ".hnsw_current_version";

    // Save the index with the default version (V3).
    hnsw_index->saveIndex(file_name);

    // Fetch info after saving, as memory size change during saving.
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    ASSERT_EQ(info.hnswInfo.M, M);
    ASSERT_EQ(info.hnswInfo.efConstruction, ef);
    ASSERT_EQ(info.hnswInfo.efRuntime, ef);
    ASSERT_EQ(info.commonInfo.indexSize, n);
    ASSERT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_Cosine);
    ASSERT_EQ(info.commonInfo.basicInfo.type, VecSimType_INT8);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, dim);
    ASSERT_EQ(info.commonInfo.indexLabelCount, n_labels[is_multi]);

    // Load the index from the file.
    VecSimIndex *serialized_index = HNSWFactory::NewIndex(file_name);
    auto *serialized_hnsw_index = this->CastToHNSW(serialized_index);

    // Verify that the index was loaded as expected.
    ASSERT_TRUE(serialized_hnsw_index->checkIntegrity().valid_state);

    VecSimIndexDebugInfo info2 = VecSimIndex_DebugInfo(serialized_index);
    ASSERT_EQ(info2.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    ASSERT_EQ(info2.hnswInfo.M, M);
    ASSERT_EQ(info2.commonInfo.basicInfo.isMulti, is_multi);
    ASSERT_EQ(info2.commonInfo.basicInfo.blockSize, blockSize);
    ASSERT_EQ(info2.hnswInfo.efConstruction, ef);
    ASSERT_EQ(info2.hnswInfo.efRuntime, ef);
    ASSERT_EQ(info2.commonInfo.indexSize, n);
    ASSERT_EQ(info2.commonInfo.basicInfo.metric, VecSimMetric_Cosine);
    ASSERT_EQ(info2.commonInfo.basicInfo.type, VecSimType_INT8);
    ASSERT_EQ(info2.commonInfo.basicInfo.dim, dim);
    ASSERT_EQ(info2.commonInfo.indexLabelCount, n_labels[is_multi]);
    ASSERT_EQ(info2.hnswInfo.epsilon, epsilon);

    // Check the functionality of the loaded index.

    int8_t new_vec[dim];
    this->PopulateRandomVector(new_vec);
    VecSimIndex_AddVector(serialized_index, new_vec, n);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n) << "score: " << score;
        ASSERT_NEAR(score, 0.0, 1e-7);
    };
    runTopKSearchTest(serialized_index, new_vec, 1, verify_res);
    VecSimIndex_DeleteVector(serialized_index, 1);

    size_t n_per_label = n / n_labels[is_multi];
    ASSERT_TRUE(serialized_hnsw_index->checkIntegrity().valid_state);
    ASSERT_EQ(VecSimIndex_IndexSize(serialized_index), n + 1 - n_per_label);

    // Clean up.
    remove(file_name.c_str());
    VecSimIndex_Free(serialized_index);
}

TEST_F(INT8HNSWTest, SerializationCurrentVersion) { test_serialization(false); }

TEST_F(INT8HNSWTest, SerializationCurrentVersionMulti) { test_serialization(true); }

template <typename params_t>
void INT8Test::get_element_neighbors(params_t params) {
    size_t n = 0;

    SetUp(params);
    auto *hnsw_index = CastToHNSW();

    // Add vectors until we have at least 2 vectors at level 1.
    size_t vectors_in_higher_levels = 0;
    while (vectors_in_higher_levels < 2) {
        GenerateAndAddVector(n, n);
        if (hnsw_index->getGraphDataByInternalId(n)->toplevel > 0) {
            vectors_in_higher_levels++;
        }
        n++;
    }
    ASSERT_GE(n, 1) << "n: " << n;

    // Go over all vectors and validate that the getElementNeighbors debug command returns the
    // neighbors properly.
    for (size_t id = 0; id < n; id++) {
        ElementLevelData &cur = hnsw_index->getElementLevelData(id, 0);
        int **neighbors_output;
        VecSimDebug_GetElementNeighborsInHNSWGraph(index, id, &neighbors_output);
        auto graph_data = hnsw_index->getGraphDataByInternalId(id);
        for (size_t l = 0; l <= graph_data->toplevel; l++) {
            auto &level_data = hnsw_index->getElementLevelData(graph_data, l);
            auto &neighbours = neighbors_output[l];
            ASSERT_EQ(neighbours[0], level_data.numLinks);
            for (size_t j = 1; j <= neighbours[0]; j++) {
                ASSERT_EQ(neighbours[j], level_data.links[j - 1]);
            }
        }
        VecSimDebug_ReleaseElementNeighborsInHNSWGraph(neighbors_output);
    }
}

TEST_F(INT8HNSWTest, getElementNeighbors) {
    HNSWParams params = {.dim = 4, .M = 20};
    get_element_neighbors(params);
}

TEST_F(INT8TieredTest, getElementNeighbors) {
    HNSWParams params = {.dim = 4, .M = 20};
    get_element_neighbors(params);
}

/**
 * Tests int8_t vectors with cosine similarity in a tiered index across three scenarios:
 * 1. Verifies vector data correctness when stored in the flat buffer
 * 2. Verifies vector data correctness when inserted directly into HNSW (when flat buffer is full)
 * 3. Verifies vector data correctness after transfer from flat buffer to HNSW
 *
 * For each scenario, the test confirms:
 * - Vector data matches the expected normalized vector
 * - The norm is correctly stored at the end of the vector
 * - Search operations (topK, range, batch) return the expected results
 */

TEST_F(INT8TieredTest, CosineBlobCorrectness) {
    // Create TieredHNSW index with cosine metric
    constexpr size_t dim = 4;
    HNSWParams hnsw_params = {.dim = dim, .metric = VecSimMetric_Cosine};
    // Create tiered index with buffer limit set to 1.
    TieredIndexParams tiered_params = this->generate_tiered_params(hnsw_params, 1, 1);
    SetUp(tiered_params);

    auto frontend_index = this->CastToBruteForce();
    auto hnsw_index = this->CastToHNSW();

    int8_t vector[dim];
    PopulateRandomVector(vector);
    float vector_norm = spaces::IntegralType_ComputeNorm<int8_t>(vector, dim);

    auto verify_norm = [&](const int8_t *input_vector, float expected_norm) {
        float vectors_stored_norm = load_unaligned<float>(input_vector + dim);
        ASSERT_EQ(vectors_stored_norm, expected_norm) << "wrong vector norm";
    };

    int8_t normalized_vec[dim + sizeof(float)];
    memcpy(normalized_vec, vector, dim);
    spaces::integer_normalizeVector<int8_t>(normalized_vec, dim);
    ASSERT_NO_FATAL_FAILURE(verify_norm(normalized_vec, vector_norm));

    int8_t query[dim + sizeof(float)];
    PopulateRandomVector(query);
    float query_norm = spaces::IntegralType_ComputeNorm<int8_t>(query, dim);

    // Calculate the expected score manually.
    int ip = 0;
    for (size_t i = 0; i < dim; i++) {
        ip += vector[i] * query[i];
    }
    float expected_score = 1.0 - (float(ip) / (vector_norm * query_norm));

    auto verify_res = [&](size_t label, double score, size_t result_rank) {
        ASSERT_EQ(score, expected_score) << "label: " << label;
    };

    // ============== Scenario 1:
    // blob correctness in the flat buffer

    // Add a vector to the flat buffer.
    VecSimIndex_AddVector(index, vector, 0);
    {
        SCOPED_TRACE("Store in the flat buffer");
        // Get the stored vector data including the norm
        auto stored_vec = frontend_index->getStoredVectorDataByLabel(0);
        const int8_t *stored_vec_data = reinterpret_cast<const int8_t *>(stored_vec.at(0).data());
        // the vector should be normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec_data, normalized_vec, dim));
        // The norm should be stored in the last position.
        verify_norm(stored_vec_data, vector_norm);

        ASSERT_NO_FATAL_FAILURE(runTopKSearchTest(index, query, 1, verify_res));
        ASSERT_NO_FATAL_FAILURE(runRangeQueryTest(index, query, 2, verify_res, 1, BY_SCORE));
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        const int8_t *stored_query = static_cast<const int8_t *>(batchIterator->getQueryBlob());
        ASSERT_NO_FATAL_FAILURE(CompareVectors(query, stored_query, dim));
        verify_norm(stored_query, query_norm);
        ASSERT_NO_FATAL_FAILURE(runBatchIteratorSearchTest(batchIterator, 1, verify_res));
        VecSimBatchIterator_Free(batchIterator);
    }

    // ============== Scenario 2:
    // blob correctness when inserted directly to the hnsw

    // Add another vector and exceed the flat buffer capacity. The vector should be stored directly
    // in the hnsw index
    VecSimIndex_AddVector(index, vector, 1);
    EXPECT_EQ(frontend_index->indexSize(), 1);
    EXPECT_EQ(hnsw_index->indexSize(), 1);
    {
        SCOPED_TRACE("Full buffer; add vector directly to hnsw");
        auto stored_vec = hnsw_index->getStoredVectorDataByLabel(1);
        const int8_t *stored_vec_data = reinterpret_cast<const int8_t *>(stored_vec.at(0).data());
        // the vector should be normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec_data, normalized_vec, dim));
        // The norm should be stored in the last position.
        verify_norm(stored_vec_data, vector_norm);

        size_t k = 2;
        ASSERT_NO_FATAL_FAILURE(runTopKSearchTest(index, query, k, verify_res));
        ASSERT_NO_FATAL_FAILURE(runRangeQueryTest(index, query, 100, verify_res, k, BY_SCORE));
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        const int8_t *stored_query = static_cast<const int8_t *>(batchIterator->getQueryBlob());
        ASSERT_NO_FATAL_FAILURE(CompareVectors(query, stored_query, dim));
        verify_norm(stored_query, query_norm);
        ASSERT_NO_FATAL_FAILURE(runBatchIteratorSearchTest(batchIterator, k, verify_res));
        VecSimBatchIterator_Free(batchIterator);
    }

    // ============== Scenario 3:
    // blob correctness after transferred to the hnsw

    // Move the first vector to the hnsw index.
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(frontend_index->indexSize(), 0);
    EXPECT_EQ(hnsw_index->indexSize(), 2);
    {
        SCOPED_TRACE("Execute insertion job");
        auto stored_vec = hnsw_index->getStoredVectorDataByLabel(0);
        const int8_t *stored_vec_data = reinterpret_cast<const int8_t *>(stored_vec.at(0).data());
        // the vector should be normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec_data, normalized_vec, dim));
        // The norm should be stored in the last position.
        verify_norm(stored_vec_data, vector_norm);

        size_t k = 2;
        ASSERT_NO_FATAL_FAILURE(runTopKSearchTest(index, query, k, verify_res));
        ASSERT_NO_FATAL_FAILURE(runRangeQueryTest(index, query, 100, verify_res, k, BY_SCORE));

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(index, query, nullptr);
        // This is the only scenario where we can check the query blob in the hnsw batch iterator.
        // In all other scenarios, the hnsw iterator is depleted after the first batch, before the
        // verify function is called.
        auto verify_res_batch = [&](size_t label, double score, size_t result_rank) {
            ASSERT_EQ(score, expected_score) << "label: " << label;
            if (result_rank == 0) {
                VecSimBatchIterator *hnsw_batch_iterator =
                    dynamic_cast<TieredHNSWIndex<int8_t, float>::TieredHNSW_BatchIterator *>(
                        batchIterator)
                        ->getHNSWIterator();
                bool hnsw_iterator_not_valid =
                    ((hnsw_batch_iterator == UNINITIALIZED) || (hnsw_batch_iterator == DEPLETED));
                ASSERT_FALSE(hnsw_iterator_not_valid)
                    << "hnsw_batch_iterator status is: "
                    << (hnsw_batch_iterator == UNINITIALIZED ? "UNINITIALIZED" : "DEPLETED");
                const int8_t *stored_hnsw_query =
                    static_cast<const int8_t *>(hnsw_batch_iterator->getQueryBlob());
                ASSERT_NO_FATAL_FAILURE(CompareVectors(query, stored_hnsw_query, dim));
                verify_norm(stored_hnsw_query, query_norm);
            }
        };
        const int8_t *stored_query = static_cast<const int8_t *>(batchIterator->getQueryBlob());
        ASSERT_NO_FATAL_FAILURE(CompareVectors(query, stored_query, dim));
        verify_norm(stored_query, query_norm);
        // use batch size  of 1 to ensure the batch iterator is not depleted before the verify
        // function is called.
        ASSERT_NO_FATAL_FAILURE(runBatchIteratorSearchTest(batchIterator, 1, verify_res_batch));
        VecSimBatchIterator_Free(batchIterator);
    }
}
