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
#include "unit_test_utils.h"
#include "VecSim/utils/serializer.h"
#include "mock_thread_pool.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/vec_sim_debug.h"
#include "VecSim/spaces/L2/L2.h"

#include <random>

using bfloat16 = vecsim_types::bfloat16;

class BF16Test : public ::testing::Test {
protected:
    virtual void SetUp(HNSWParams &params) {
        FAIL() << "BF16Test::SetUp(HNSWParams) this method should be overriden";
    }

    virtual void SetUp(BFParams &params) {
        FAIL() << "BF16Test::SetUp(BFParams) this method should be overriden";
    }

    virtual void SetUp(TieredIndexParams &tiered_params) {
        FAIL() << "BF16Test::SetUp(TieredIndexParams) this method should be overriden";
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

    virtual HNSWIndex<bfloat16, float> *CastToHNSW() {
        return CastIndex<HNSWIndex<bfloat16, float>>();
    }

    void GenerateVector(bfloat16 *out_vec, float initial_value = 0.25f, float step = 0.0f) {
        for (size_t i = 0; i < dim; i++) {
            out_vec[i] = vecsim_types::float_to_bf16(initial_value + step * i);
        }
    }

    int GenerateAndAddVector(size_t id, float initial_value = 0.25f, float step = 0.0f) {
        bfloat16 v[dim];
        GenerateVector(v, initial_value, step);
        return VecSimIndex_AddVector(index, v, id);
    }

    int GenerateAndAddVector(VecSimIndex *target_index, size_t id, float initial_value = 0.25f,
                             float step = 0.0f) {
        bfloat16 v[dim];
        GenerateVector(v, initial_value, step);
        return VecSimIndex_AddVector(target_index, v, id);
    }

    template <typename params_t>
    void create_index_test(params_t index_params);
    template <typename params_t>
    void search_by_id_test(params_t index_params);
    template <typename params_t>
    void search_by_score_test(params_t index_params);
    template <typename params_t>
    void search_empty_index_test(params_t index_params);
    template <typename params_t>
    void test_range_query(params_t index_params);
    template <typename params_t>
    void test_override(params_t index_params);
    template <typename params_t>
    void test_get_distance(params_t params, VecSimMetric metric);
    template <typename params_t>
    void test_batch_iterator_basic(params_t index_params);
    template <typename params_t>
    VecSimIndexDebugInfo test_info(params_t index_params);
    template <typename params_t>
    void test_info_iterator(VecSimMetric metric);
    template <typename params_t>
    void get_element_neighbors(params_t params);

    VecSimIndex *index;
    size_t dim;
};

class BF16HNSWTest : public BF16Test {
protected:
    virtual void SetUp(HNSWParams &params) override {
        params.type = VecSimType_BFLOAT16;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        dim = params.dim;
    }

    virtual const void *GetDataByInternalId(idType id) override {
        return CastIndex<HNSWIndex_Single<bfloat16, float>>()->getDataByInternalId(id);
    }

    HNSWIndex<bfloat16, float> *CastToHNSW(VecSimIndex *new_index) {
        return CastIndex<HNSWIndex<bfloat16, float>>(new_index);
    }

    virtual HNSWIndex<bfloat16, float> *CastToHNSW() override {
        return CastIndex<HNSWIndex<bfloat16, float>>(index);
    }

    void test_info(bool is_multi);
    void test_serialization(bool is_multi);
};

class BF16BruteForceTest : public BF16Test {
protected:
    virtual void SetUp(BFParams &params) override {
        params.type = VecSimType_BFLOAT16;
        VecSimParams vecsim_params = CreateParams(params);
        index = VecSimIndex_New(&vecsim_params);
        dim = params.dim;
    }

    virtual const void *GetDataByInternalId(idType id) override {
        return CastIndex<BruteForceIndex_Single<bfloat16, float>>()->getDataByInternalId(id);
    }

    virtual HNSWIndex<bfloat16, float> *CastToHNSW() override {
        ADD_FAILURE() << "BF16BruteForceTest::CastToHNSW() this method should not be called";
        return nullptr;
    }

    void test_info(bool is_multi);
};

class BF16TieredTest : public BF16Test {
protected:
    TieredIndexParams generate_tiered_params(HNSWParams &hnsw_params, size_t swap_job_threshold = 0,
                                             size_t flat_buffer_limit = SIZE_MAX) {
        hnsw_params.type = VecSimType_BFLOAT16;
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
        return CastIndex<BruteForceIndex<bfloat16, float>>(CastToBruteForce())
            ->getDataByInternalId(id);
    }

    virtual HNSWIndex<bfloat16, float> *CastToHNSW() override {
        auto tiered_index = dynamic_cast<TieredHNSWIndex<bfloat16, float> *>(index);
        return tiered_index->getHNSWIndex();
    }

    VecSimIndexAbstract<bfloat16, float> *CastToBruteForce() {
        auto tiered_index = dynamic_cast<TieredHNSWIndex<bfloat16, float> *>(index);
        return tiered_index->getFlatBufferIndex();
    }

    void test_info_iterator(VecSimMetric metric);
    void test_info(bool is_multi);

    VecSimParams vecsim_hnsw_params;
    tieredIndexMock mock_thread_pool;
};
/* ---------------------------- Create index tests ---------------------------- */

template <typename params_t>
void BF16Test::create_index_test(params_t index_params) {
    SetUp(index_params);
    float initial_value = 0.5f;
    float step = 1.0f;

    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    bfloat16 vector[dim];
    this->GenerateVector(vector, initial_value, step);
    VecSimIndex_AddVector(index, vector, 0);

    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);
    ASSERT_EQ(index->getDistanceFrom_Unsafe(0, vector), 0);

    const void *v = this->GetDataByInternalId(0);

    for (size_t i = 0; i < dim; i++) {
        // Convert assuming little endian system.
        ASSERT_EQ(vecsim_types::bfloat16_to_float32(((bfloat16 *)v)[i]),
                  initial_value + step * float(i));
    }
}

TEST_F(BF16HNSWTest, createIndex) {
    HNSWParams params = {.dim = 40, .M = 16, .efConstruction = 200};
    create_index_test(params);
    ASSERT_EQ(index->basicInfo().type, VecSimType_BFLOAT16);
    ASSERT_EQ(index->basicInfo().algo, VecSimAlgo_HNSWLIB);
}

TEST_F(BF16BruteForceTest, createIndex) {
    BFParams params = {.dim = 40};
    create_index_test(params);
    ASSERT_EQ(index->basicInfo().type, VecSimType_BFLOAT16);
    ASSERT_EQ(index->basicInfo().algo, VecSimAlgo_BF);
}

TEST_F(BF16TieredTest, createIndex) {
    HNSWParams params = {.dim = 40, .M = 16, .efConstruction = 200};
    create_index_test(params);
    ASSERT_EQ(index->basicInfo().type, VecSimType_BFLOAT16);
    ASSERT_EQ(index->basicInfo().isTiered, true);
}

/* ---------------------------- Size Estimation tests ---------------------------- */

TEST_F(BF16HNSWTest, testSizeEstimation) {
    size_t n = 200;
    size_t bs = 256;
    size_t M = 64;

    HNSWParams params = {.dim = 4, .blockSize = bs, .M = M};
    SetUp(params);

    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);
    size_t actual = index->getAllocationSize();

    ASSERT_EQ(estimation, actual);

    // Fill the initial capacity + fill the last block.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->GenerateAndAddVector(i), 1);
    }
    idType cur = n;
    while (index->indexSize() % bs != 0) {
        this->GenerateAndAddVector(cur++);
    }

    // Estimate the memory delta of adding a single vector that requires a full new block.
    estimation = EstimateElementSize(params) * bs;
    size_t before = index->getAllocationSize();
    this->GenerateAndAddVector(bs);
    actual = index->getAllocationSize() - before;

    // We check that the actual size is within 1% of the estimation.
    ASSERT_GE(estimation, actual * 0.99);
    ASSERT_LE(estimation, actual * 1.01);
}

TEST_F(BF16BruteForceTest, testSizeEstimation) {
    size_t dim = 4;
    size_t n = 0;
    size_t bs = DEFAULT_BLOCK_SIZE;

    BFParams params = {.dim = dim, .initialCapacity = n, .blockSize = bs};
    SetUp(params);

    // EstimateInitialSize is called after CreateNewIndex because params struct is
    // changed in CreateNewIndex.
    size_t estimation = EstimateInitialSize(params);

    size_t actual = index->getAllocationSize();
    ASSERT_EQ(estimation, actual);

    estimation = EstimateElementSize(params) * bs;

    ASSERT_EQ(this->GenerateAndAddVector(0), 1);

    actual = index->getAllocationSize() - actual; // get the delta
    ASSERT_GE(estimation * 1.01, actual);
    ASSERT_LE(estimation * 0.99, actual);
}

TEST_F(BF16TieredTest, testSizeEstimation) {
    size_t n = DEFAULT_BLOCK_SIZE;
    size_t M = 32;
    size_t bs = DEFAULT_BLOCK_SIZE;

    HNSWParams hnsw_params = {.dim = 4, .initialCapacity = n, .M = M};
    SetUp(hnsw_params);
    TieredIndexParams tiered_params = generate_tiered_params(hnsw_params);
    VecSimParams params = CreateParams(tiered_params);

    // auto allocator = index->getAllocator();
    size_t initial_size_estimation = VecSimIndex_EstimateInitialSize(&params);
    auto hnsw_index = CastToHNSW();
    ASSERT_EQ(initial_size_estimation, index->getAllocationSize());

    // Add vectors up to initial capacity (initial capacity == block size).
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector(i, i);
        mock_thread_pool.thread_iteration();
    }

    // Estimate memory delta for filling up the first block and adding another block.
    size_t estimation = VecSimIndex_EstimateElementSize(&params) * bs;

    size_t before = index->getAllocationSize();
    GenerateAndAddVector(bs + n, bs + n);
    mock_thread_pool.thread_iteration();
    size_t actual = index->getAllocationSize() - before;

    // Flat index should be empty, hence the index size includes only hnsw size.
    ASSERT_EQ(index->indexSize(), hnsw_index->indexSize());
    ASSERT_EQ(index->indexCapacity(), hnsw_index->indexCapacity());
    // We added n + 1 vectors
    ASSERT_EQ(index->indexSize(), n + 1);
    // We should have 2 blocks now
    ASSERT_EQ(index->indexCapacity(), 2 * bs);

    // We check that the actual size is within 1% of the estimation.
    ASSERT_GE(estimation, actual * 0.99);
    ASSERT_LE(estimation, actual * 1.01);
}

/* ---------------------------- Functionality tests ---------------------------- */

template <typename params_t>
void BF16Test::search_by_id_test(params_t index_params) {
    SetUp(index_params);

    size_t k = 11;
    size_t n = 100;

    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(i, i); // {i, i, i, i}
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    bfloat16 query[dim];
    GenerateVector(query, 50); // {50, 50, 50, 50}

    // Vectors values are equal to the id, so the 11 closest vectors are 45, 46...50
    // (closest), 51...55
    static size_t expected_res_order[] = {45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, expected_res_order[index]);    // results are sorted by ID
        ASSERT_EQ(score, 4 * (50 - id) * (50 - id)); // L2 distance
    };

    runTopKSearchTest(index, query, k, verify_res, nullptr, BY_ID);
}

TEST_F(BF16HNSWTest, searchByID) {
    HNSWParams params = {.dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200};
    search_by_id_test(params);
}

TEST_F(BF16BruteForceTest, searchByID) {
    BFParams params = {.dim = 4, .initialCapacity = 200};
    search_by_id_test(params);
}

TEST_F(BF16TieredTest, searchByID) {
    HNSWParams params = {.dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200};
    search_by_id_test(params);
}

template <typename params_t>
void BF16Test::search_by_score_test(params_t index_params) {
    SetUp(index_params);

    size_t k = 11;
    size_t n = 100;

    for (size_t i = 0; i < n; i++) {
        this->GenerateAndAddVector(i, i); // {i, i, i, i}
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    bfloat16 query[dim];
    GenerateVector(query, 50); // {50, 50, 50, 50}
    // Vectors values are equal to the id, so the 11 closest vectors are
    // 45, 46...50 (closest), 51...55
    static size_t expected_res_order[] = {50, 49, 51, 48, 52, 47, 53, 46, 54, 45, 55};
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, expected_res_order[index]);
        ASSERT_EQ(score, 4 * (50 - id) * (50 - id)); // L2 distance
    };

    runTopKSearchTest(index, query, k, verify_res);
}

TEST_F(BF16HNSWTest, searchByScore) {
    HNSWParams params = {.dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200};
    search_by_score_test(params);
}

TEST_F(BF16BruteForceTest, searchByScore) {
    BFParams params = {.dim = 4, .initialCapacity = 200};
    search_by_score_test(params);
}

TEST_F(BF16TieredTest, searchByScore) {
    HNSWParams params = {.dim = 4, .initialCapacity = 200, .M = 16, .efConstruction = 200};
    search_by_score_test(params);
}

template <typename params_t>
void BF16Test::search_empty_index_test(params_t params) {
    size_t n = 100;
    size_t k = 11;

    SetUp(params);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 0);

    bfloat16 query[dim];
    GenerateVector(query, 50);

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
        GenerateAndAddVector(i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);
    for (size_t i = 0; i < n; i++) {
        VecSimIndex_DeleteVector(index, i);
    }
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
}

TEST_F(BF16HNSWTest, SearchEmptyIndex) {
    HNSWParams params = {.dim = 4, .initialCapacity = 0};
    search_empty_index_test(params);
}

TEST_F(BF16BruteForceTest, SearchEmptyIndex) {
    BFParams params = {.dim = 4, .initialCapacity = 0};
    search_empty_index_test(params);
}

TEST_F(BF16TieredTest, SearchEmptyIndex) {
    HNSWParams params = {.dim = 4, .initialCapacity = 0};
    search_empty_index_test(params);
}

// We run this test up to 250 vectors since for larger values we may lose accuracy when converting
// from bfloat16 to float and the order of results can't be guaranteed. for example, the distances
// between id 295 and 297 to the query both equal 64.
template <typename params_t>
void BF16Test::test_override(params_t params) {
    size_t n = 100;
    size_t new_n = 250;
    SetUp(params);

    // Insert n vectors.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, i), 1);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Override n vectors, the first 100 will be overwritten (deleted first).
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, i), 0);
    }

    // Add up to new_n vectors.
    for (size_t i = n; i < new_n; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, i), 1);
    }

    bfloat16 query[dim];
    GenerateVector(query, new_n);

    // Vectors values equals their id, so we expect the larger the id the closest it will be to the
    // query.
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, new_n - 1 - index) << "id: " << id << " score: " << score;
        bfloat16 a = vecsim_types::float_to_bf16(new_n);
        bfloat16 b = vecsim_types::float_to_bf16(id);
        // Convert assuming little endian system.
        float diff = vecsim_types::bfloat16_to_float32(a) - vecsim_types::bfloat16_to_float32(b);
        float exp_score = 4 * diff * diff;
        ASSERT_EQ(score, exp_score) << "id: " << id << " score: " << score;
    };
    runTopKSearchTest(index, query, 300, verify_res);
}

TEST_F(BF16HNSWTest, Override) {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 100, .M = 8, .efConstruction = 20, .efRuntime = 250};
    test_override(params);
}

TEST_F(BF16BruteForceTest, Override) {
    BFParams params = {.dim = 4, .initialCapacity = 100};
    test_override(params);
}

TEST_F(BF16TieredTest, Override) {
    HNSWParams params = {
        .dim = 4, .initialCapacity = 100, .M = 8, .efConstruction = 20, .efRuntime = 250};
    test_override(params);
}

template <typename params_t>
void BF16Test::test_range_query(params_t params) {
    size_t n = params.initialCapacity;
    SetUp(params);

    float pivot_value = 1.0f;
    bfloat16 pivot_vec[dim];
    GenerateVector(pivot_vec, pivot_value);

    float radius = 1.5f;
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis(pivot_value - radius, pivot_value + radius);

    // insert 20 vectors near a pivot vector.
    size_t n_close = 20;
    for (size_t i = 0; i < n_close; i++) {
        float random_number = dis(gen);
        GenerateAndAddVector(i, random_number);
    }
    bfloat16 max_vec[dim];
    GenerateVector(max_vec, pivot_value + radius);
    double max_dist = BF16_L2Sqr_LittleEndian(pivot_vec, max_vec, dim);

    // Add more vectors far from the pivot vector
    for (size_t i = n_close; i < n; i++) {
        float random_number = dis(gen);
        GenerateAndAddVector(i, 5.0 + random_number);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    auto verify_res_by_score = [&](size_t id, double score, size_t index) {
        ASSERT_LE(id, n_close - 1) << "score: " << score;
        ASSERT_LE(score, max_dist);
    };
    uint expected_num_results = n_close;

    runRangeQueryTest(index, pivot_vec, max_dist, verify_res_by_score, expected_num_results,
                      BY_SCORE);
}

TEST_F(BF16HNSWTest, rangeQuery) {
    HNSWParams params = {.dim = 4, .initialCapacity = 100};
    test_range_query(params);
}

TEST_F(BF16BruteForceTest, rangeQuery) {
    BFParams params = {.dim = 4, .initialCapacity = 100};
    test_range_query(params);
}

TEST_F(BF16TieredTest, rangeQuery) {
    HNSWParams params = {.dim = 4, .initialCapacity = 100};
    test_range_query(params);
}

template <typename params_t>
void BF16Test::test_get_distance(params_t params, VecSimMetric metric) {
    static double constexpr expected_dists[2] = {0.25, -1.5}; // L2, IP
    size_t n = 1;
    params.metric = metric;
    SetUp(params);

    bfloat16 vec[dim];
    GenerateVector(vec, 0.25, 0.25); // {0.25, 0.5, 0.75, 1}
    VecSimIndex_AddVector(index, vec, 0);
    ASSERT_EQ(VecSimIndex_IndexSize(index), 1);

    bfloat16 query[dim];
    GenerateVector(query, 0.5, 0.25); // {0.5, 0.75, 1, 1.25}

    double dist = VecSimIndex_GetDistanceFrom_Unsafe(index, 0, query);

    // manually calculated. Values were chosen as such that don't cause any accuracy loss in
    // conversion from bfloat16 to float.
    ASSERT_EQ(dist, expected_dists[metric]) << "metric: " << metric;
}

TEST_F(BF16HNSWTest, GetDistanceL2Test) {
    HNSWParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_L2);
}

TEST_F(BF16BruteForceTest, GetDistanceL2Test) {
    BFParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_L2);
}

TEST_F(BF16TieredTest, GetDistanceL2Test) {
    HNSWParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_L2);
}

TEST_F(BF16HNSWTest, GetDistanceIPTest) {
    HNSWParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_IP);
}

TEST_F(BF16BruteForceTest, GetDistanceIPTest) {
    BFParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_IP);
}

TEST_F(BF16TieredTest, GetDistanceIPTest) {
    HNSWParams params = {.dim = 4, .initialCapacity = 1};
    test_get_distance(params, VecSimMetric_IP);
}

/* ---------------------------- Batch iterator tests ---------------------------- */

// See comment above test_override for why we run this test up to n = 250
template <typename params_t>
void BF16Test::test_batch_iterator_basic(params_t params) {
    size_t n = params.initialCapacity;
    SetUp(params);

    // For every i, add the vector (i,i,i,i) under the label i.
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(GenerateAndAddVector(i, i), 1);
    }

    ASSERT_EQ(VecSimIndex_IndexSize(index), n);

    // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
    bfloat16 query[dim];
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

TEST_F(BF16HNSWTest, BatchIteratorBasic) {
    size_t n = 250;
    HNSWParams params = {
        .dim = 4, .initialCapacity = n, .M = 8, .efConstruction = 20, .efRuntime = n};
    test_batch_iterator_basic(params);
}

TEST_F(BF16BruteForceTest, BatchIteratorBasic) {
    size_t n = 250;
    BFParams params = {.dim = 4, .initialCapacity = n};
    test_batch_iterator_basic(params);
}

TEST_F(BF16TieredTest, BatchIteratorBasic) {
    size_t n = 250;
    HNSWParams params = {
        .dim = 4, .initialCapacity = n, .M = 8, .efConstruction = 20, .efRuntime = n};
    test_batch_iterator_basic(params);
}

/* ---------------------------- Info tests ---------------------------- */

template <typename params_t>
VecSimIndexDebugInfo BF16Test::test_info(params_t params) {
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    EXPECT_EQ(info.commonInfo.basicInfo.dim, params.dim);
    EXPECT_EQ(info.commonInfo.basicInfo.isMulti, params.multi);
    EXPECT_EQ(info.commonInfo.basicInfo.type, VecSimType_BFLOAT16);
    EXPECT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, index->getAllocationSize());
    EXPECT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_L2);

    // Validate that Static info returns the right restricted info as well.
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

void BF16HNSWTest::test_info(bool is_multi) {
    HNSWParams params = {.dim = 128, .multi = is_multi};
    SetUp(params);
    VecSimIndexDebugInfo info = BF16Test::test_info(params);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);

    ASSERT_EQ(info.hnswInfo.M, HNSW_DEFAULT_M);
    ASSERT_EQ(info.hnswInfo.efConstruction, HNSW_DEFAULT_EF_C);
    ASSERT_EQ(info.hnswInfo.efRuntime, HNSW_DEFAULT_EF_RT);
    ASSERT_DOUBLE_EQ(info.hnswInfo.epsilon, HNSW_DEFAULT_EPSILON);
}
TEST_F(BF16HNSWTest, testInfoSingle) { test_info(false); }

TEST_F(BF16HNSWTest, testInfoMulti) { test_info(true); }

void BF16BruteForceTest::test_info(bool is_multi) {
    BFParams params = {.dim = 128, .multi = is_multi};
    SetUp(params);
    VecSimIndexDebugInfo info = BF16Test::test_info(params);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_BF);
}

TEST_F(BF16BruteForceTest, testInfoSingle) { test_info(false); }
TEST_F(BF16BruteForceTest, testInfoMulti) { test_info(true); }

void BF16TieredTest::test_info(bool is_multi) {
    size_t bufferLimit = 1000;
    HNSWParams hnsw_params = {.dim = 128, .multi = is_multi};
    TieredIndexParams params = generate_tiered_params(hnsw_params, 1, bufferLimit);
    SetUp(params);

    VecSimIndexDebugInfo info = BF16Test::test_info(hnsw_params);
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

    GenerateAndAddVector(1, 1);
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
        GenerateAndAddVector(1, 1);
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

TEST_F(BF16TieredTest, testInfoSingle) { test_info(false); }
TEST_F(BF16TieredTest, testInfoMulti) { test_info(true); }

template <typename params_t>
void BF16Test::test_info_iterator(VecSimMetric metric) {
    size_t n = 100;
    size_t d = 128;
    params_t params = {.dim = d, .metric = metric, .initialCapacity = n};
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

TEST_F(BF16BruteForceTest, InfoIteratorCosine) {
    test_info_iterator<BFParams>(VecSimMetric_Cosine);
}
TEST_F(BF16BruteForceTest, InfoIteratorIP) { test_info_iterator<BFParams>(VecSimMetric_IP); }
TEST_F(BF16BruteForceTest, InfoIteratorL2) { test_info_iterator<BFParams>(VecSimMetric_L2); }
TEST_F(BF16HNSWTest, InfoIteratorCosine) { test_info_iterator<HNSWParams>(VecSimMetric_Cosine); }
TEST_F(BF16HNSWTest, InfoIteratorIP) { test_info_iterator<HNSWParams>(VecSimMetric_IP); }
TEST_F(BF16HNSWTest, InfoIteratorL2) { test_info_iterator<HNSWParams>(VecSimMetric_L2); }

void BF16TieredTest::test_info_iterator(VecSimMetric metric) {
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

TEST_F(BF16TieredTest, InfoIteratorCosine) { test_info_iterator(VecSimMetric_Cosine); }
TEST_F(BF16TieredTest, InfoIteratorIP) { test_info_iterator(VecSimMetric_IP); }
TEST_F(BF16TieredTest, InfoIteratorL2) { test_info_iterator(VecSimMetric_L2); }

/* ---------------------------- HNSW specific tests ---------------------------- */

void BF16HNSWTest::test_serialization(bool is_multi) {
    size_t dim = 4;
    size_t n = 1001;
    size_t n_labels[] = {n, 100};
    size_t M = 8;
    size_t ef = 10;
    double epsilon = 0.004;
    size_t blockSize = 20;
    std::string multiToString[] = {"single", "multi_100labels_"};
    int i = is_multi;

    HNSWParams params{.type = VecSimType_BFLOAT16,
                      .dim = dim,
                      .metric = VecSimMetric_L2,
                      .multi = is_multi,
                      .initialCapacity = n,
                      .blockSize = blockSize,
                      .M = M,
                      .efConstruction = ef,
                      .efRuntime = ef,
                      .epsilon = epsilon};
    SetUp(params);

    auto *hnsw_index = this->CastToHNSW();

    std::vector<bfloat16> data(n * dim);
    std::mt19937 gen(42);
    std::uniform_real_distribution<> dis;

    for (size_t i = 0; i < n * dim; ++i) {
        float val = dis(gen);
        data[i] = vecsim_types::float_to_bf16(val);
    }

    for (size_t j = 0; j < n; ++j) {
        VecSimIndex_AddVector(index, data.data() + dim * j, j % n_labels[i]);
    }

    auto file_name = std::string(getenv("ROOT")) + "/tests/unit/1k-d4-L2-M8-ef_c10_" +
                     VecSimType_ToString(VecSimType_BFLOAT16) + "_" + multiToString[i] +
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
    ASSERT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_L2);
    ASSERT_EQ(info.commonInfo.basicInfo.type, VecSimType_BFLOAT16);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, dim);
    ASSERT_EQ(info.commonInfo.indexLabelCount, n_labels[i]);

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
    ASSERT_EQ(info2.commonInfo.basicInfo.metric, VecSimMetric_L2);
    ASSERT_EQ(info2.commonInfo.basicInfo.type, VecSimType_BFLOAT16);
    ASSERT_EQ(info2.commonInfo.basicInfo.dim, dim);
    ASSERT_EQ(info2.commonInfo.indexLabelCount, n_labels[i]);
    ASSERT_EQ(info2.hnswInfo.epsilon, epsilon);

    // Check the functionality of the loaded index.

    bfloat16 new_vec[dim];
    GenerateVector(new_vec, 0.25);
    VecSimIndex_AddVector(serialized_index, new_vec, n);
    auto verify_res = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n) << "score: " << score;
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(serialized_index, new_vec, 1, verify_res);
    VecSimIndex_DeleteVector(serialized_index, 1);

    size_t n_per_label = n / n_labels[i];
    ASSERT_TRUE(serialized_hnsw_index->checkIntegrity().valid_state);
    ASSERT_EQ(VecSimIndex_IndexSize(serialized_index), n + 1 - n_per_label);

    // Clean up.
    remove(file_name.c_str());
    VecSimIndex_Free(serialized_index);
}

TEST_F(BF16HNSWTest, SerializationCurrentVersion) { test_serialization(false); }

TEST_F(BF16HNSWTest, SerializationCurrentVersionMulti) { test_serialization(true); }

template <typename params_t>
void BF16Test::get_element_neighbors(params_t params) {
    size_t n = 0;

    SetUp(params);
    auto *hnsw_index = CastToHNSW();

    // Add vectors until we have at least 2 vectors at level 1.
    size_t vectors_in_higher_levels = 0;
    while (vectors_in_higher_levels < 2) {
        GenerateAndAddVector(hnsw_index, n, n);
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

TEST_F(BF16HNSWTest, getElementNeighbors) {
    size_t dim = 4;
    size_t M = 20;
    HNSWParams params = {.dim = 4, .M = 20};
    get_element_neighbors(params);
}

TEST_F(BF16TieredTest, getElementNeighbors) {
    size_t dim = 4;
    size_t M = 20;
    HNSWParams params = {.dim = 4, .M = 20};
    get_element_neighbors(params);
}
