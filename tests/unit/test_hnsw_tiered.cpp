/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "VecSim/index_factories/tiered_factory.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/algorithms/hnsw/hnsw_multi.h"
#include "VecSim/types/float16.h"
#include "VecSim/vec_sim_debug.h"
#include <string>
#include <array>
#include <cmath>
#include <numeric>

#include "unit_test_utils.h"
#include "mock_thread_pool.h"

#include <thread>

// Runs the test for all combination of data type(float/double) - label type (single/multi)

template <typename index_type_t>
class HNSWTieredIndexTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

protected:
    VecSimWriteMode original_mode;
    void SetUp() { original_mode = VecSimIndexInterface::asyncWriteMode; }

    void TearDown() { VecSimIndexInterface::asyncWriteMode = original_mode; }

    HNSWIndex<data_t, dist_t> *CastToHNSW(VecSimIndex *index) {
        auto tiered_index = reinterpret_cast<TieredHNSWIndex<data_t, dist_t> *>(index);
        return tiered_index->getHNSWIndex();
    }

    BruteForceIndex<data_t, dist_t> *GetFlatIndex(TieredHNSWIndex<data_t, dist_t> *tiered_index) {
        return tiered_index->frontendIndex;
    }

    TieredHNSWIndex<data_t, dist_t> *CreateTieredHNSWIndex(VecSimParams &hnsw_params,
                                                           tieredIndexMock &mock_thread_pool,
                                                           size_t swap_job_threshold = 0,
                                                           size_t flat_buffer_limit = SIZE_MAX,
                                                           size_t normalization_set_size = 0) {
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = flat_buffer_limit,
            .primaryIndexParams = &hnsw_params,
            .specificParams = {
                TieredHNSWParams{.swapJobThreshold = swap_job_threshold,
                                 .QuantNormalizationSetSize = normalization_set_size}}};
        auto *tiered_index = reinterpret_cast<TieredHNSWIndex<data_t, dist_t> *>(
            TieredFactory::NewIndex(&tiered_params));

        // Set the created tiered index in the index external context (it will take ownership over
        // the index, and we'll need to release the ctx at the end of the test.
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);
        return tiered_index;
    }

    // A single-value index in async mode holding labels 0..n-1 (label i -> vector of i's), all
    // ingested into HNSW, with no queued jobs left.
    TieredHNSWIndex<data_t, dist_t> *CreateIngestedAsyncIndex(tieredIndexMock &mock_thread_pool,
                                                              size_t n, size_t dim) {
        HNSWParams params = {.type = index_type_t::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = false};
        VecSimParams hnsw_params = CreateParams(params);
        auto *tiered_index = CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        VecSim_SetWriteMode(VecSim_WriteAsync);
        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<data_t>(tiered_index, dim, i, i);
        }
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        EXPECT_EQ(CastToHNSW(tiered_index)->indexSize(), n);
        return tiered_index;
    }

    // A multi-value index in async mode holding labels 0..n_labels-1 with `per_label` vectors
    // each (label l's j-th vector is generated from l * 10 + j), all ingested into HNSW, with no
    // queued jobs left.
    TieredHNSWIndex<data_t, dist_t> *
    CreateIngestedAsyncMultiIndex(tieredIndexMock &mock_thread_pool, size_t n_labels,
                                  size_t per_label, size_t dim) {
        HNSWParams params = {.type = index_type_t::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = true};
        VecSimParams hnsw_params = CreateParams(params);
        auto *tiered_index = CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        VecSim_SetWriteMode(VecSim_WriteAsync);
        for (size_t label = 0; label < n_labels; label++) {
            for (size_t j = 0; j < per_label; j++) {
                GenerateAndAddVector<data_t>(tiered_index, dim, label, label * 10 + j);
            }
        }
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        EXPECT_EQ(CastToHNSW(tiered_index)->indexSize(), n_labels * per_label);
        return tiered_index;
    }
};

TYPED_TEST_SUITE(HNSWTieredIndexTest, DataTypeSetExtended);

// Runs the test for each data type(float/double). The label type should be explicitly
// set in the test.

template <typename index_type_t>
class HNSWTieredIndexTestBasic : public HNSWTieredIndexTest<index_type_t> {};
TYPED_TEST_SUITE(HNSWTieredIndexTestBasic, DataTypeSet);

TYPED_TEST(HNSWTieredIndexTest, CreateIndexInstance) {
    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = 4,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    // Add a vector to the flat index.
    TEST_DATA_T vector[tiered_index->backendIndex->getDim()];
    GenerateVector<TEST_DATA_T>(vector, tiered_index->backendIndex->getDim(), 0.5f);
    labelType vector_label = 1;
    VecSimIndex_AddVector(tiered_index->frontendIndex, vector, vector_label);

    // Create a mock job that inserts some vector into the HNSW index.
    auto insert_to_index = [](AsyncJob *job) {
        auto *my_insert_job = reinterpret_cast<HNSWInsertJob *>(job);
        auto my_index =
            reinterpret_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(my_insert_job->index);

        // Move the vector from the temp flat index into the HNSW index.
        // Note that we access the vector via its internal id since in index of type MULTI,
        // this is the only way to do so (knowing the label is not enough...)
        VecSimIndex_AddVector(my_index->backendIndex,
                              my_index->frontendIndex->getDataByInternalId(my_insert_job->id),
                              my_insert_job->label);
        // TODO: enable deleting vectors by internal id for the case of moving a single vector
        //  from the flat buffer in MULTI.
        VecSimIndex_DeleteVector(my_index->frontendIndex, my_insert_job->label);
        auto it = my_index->labelToInsertJobs.at(my_insert_job->label).begin();
        ASSERT_EQ(job, *it); // Assert pointers equation
        // Here we update labelToInsertJobs mapping, as we except that for every insert job
        // there will be a corresponding item in the map.
        my_index->labelToInsertJobs.at(my_insert_job->label).erase(it);
        delete job;
    };

    auto job = new (allocator)
        HNSWInsertJob(tiered_index->allocator, vector_label, 0, insert_to_index, tiered_index);
    auto jobs_vec = vecsim_stl::vector<TieredInsertJob *>(1, job, allocator);
    tiered_index->labelToInsertJobs.insert({vector_label, jobs_vec});

    // Wrap this job with an array and submit the jobs to the queue.
    // TODO: in the future this should be part of the tiered index "add_vector" flow, and
    //  we can replace this to avoid the breaking of the abstraction.
    tiered_index->submitSingleJob((AsyncJob *)job);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);

    // Execute the job from the queue and validate that the index was updated properly.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, vector), 0);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vector_label).size(), 0);
}

TYPED_TEST(HNSWTieredIndexTest, WrapExistingIndexWithoutPrimaryParams) {
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = 4,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    TEST_DATA_T vector[4] = {};
    VecSim_SetWriteMode(VecSim_WriteAsync);

    for (size_t normalization_set_size : {0, 100}) {
        SCOPED_TRACE(normalization_set_size);
        auto *backend =
            static_cast<HNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(HNSWFactory::NewIndex(&hnsw_params));
        VecSimIndex_AddVector(backend, vector, 1);
        auto mock_thread_pool = tieredIndexMock();
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = SIZE_MAX,
            .primaryIndexParams = nullptr,
            .specificParams = {
                TieredHNSWParams{.QuantNormalizationSetSize = normalization_set_size}}};
        auto *tiered_index = static_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(
            TieredFactory::TieredHNSWFactory::NewIndex(&tiered_params, backend));
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);

        EXPECT_EQ(tiered_index->indexSize(), 1);
        runTopKSearchTest(tiered_index, vector, 1, [](size_t label, double score, size_t) {
            EXPECT_EQ(label, 1);
            EXPECT_EQ(score, 0);
        });
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vector, 2), 1);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
        mock_thread_pool.thread_iteration();
        EXPECT_EQ(backend->indexSize(), 2);
        EXPECT_EQ(this->GetFlatIndex(tiered_index)->indexSize(), 0);
    }
}

TYPED_TEST(HNSWTieredIndexTest, UnquantizedIndexIgnoresNormalizationThreshold) {
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = 4,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 0, SIZE_MAX, 100);
    auto *backend = this->CastToHNSW(tiered_index);
    ASSERT_NE(backend, nullptr);
    TEST_DATA_T vector[4] = {};
    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vector, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(backend->indexSize(), 1);
    EXPECT_EQ(this->GetFlatIndex(tiered_index)->indexSize(), 0);
}

TYPED_TEST(HNSWTieredIndexTest, testIndexesAttributes) {
    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = 4,
                         .metric = VecSimMetric_Cosine,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    HNSWIndex<TEST_DATA_T, TEST_DIST_T> *hnsw_index = this->CastToHNSW(tiered_index);
    BruteForceIndex<TEST_DATA_T, TEST_DIST_T> *bf_index = this->GetFlatIndex(tiered_index);

    VecSimIndexBasicInfo hnsw_info = VecSimIndex_BasicInfo(hnsw_index);
    VecSimIndexBasicInfo bf_info = VecSimIndex_BasicInfo(bf_index);

    // assert metric
    ASSERT_EQ(hnsw_info.metric, params.metric);
    ASSERT_EQ(bf_info.metric, params.metric);
    // assert type
    ASSERT_EQ(hnsw_info.type, params.type);
    ASSERT_EQ(bf_info.type, params.type);
    // assert dim
    ASSERT_EQ(hnsw_info.dim, params.dim);
    ASSERT_EQ(bf_info.dim, params.dim);
    // assert multi
    ASSERT_EQ(hnsw_info.isMulti, params.multi);
    ASSERT_EQ(bf_info.isMulti, params.multi);

    // assert containers

    // bf - multi with cosine
    IndexComponents<TEST_DATA_T, TEST_DIST_T> bf_components = bf_index->get_components();
    PreprocessorsContainerAbstract *bf_preprocessors = bf_components.preprocessors;
    const std::type_info &bf_pp_container_expected_type =
        typeid(MultiPreprocessorsContainer<TEST_DATA_T, 1>);
    const std::type_info &bf_pp_container_actual_type = typeid(*bf_preprocessors);
    ASSERT_EQ(bf_pp_container_actual_type, bf_pp_container_expected_type);

    std::array<PreprocessorInterface *, 1> pp_arr =
        dynamic_cast<MultiPreprocessorsContainer<TEST_DATA_T, 1> *>(bf_preprocessors)
            ->getPreprocessors();
    const std::type_info &bf_pp_expected_type = typeid(CosinePreprocessor<TEST_DATA_T>);
    PreprocessorInterface *bf_pp = pp_arr[0];
    const std::type_info &bf_pp_actual_type = typeid(*bf_pp);
    ASSERT_EQ(bf_pp_actual_type, bf_pp_expected_type);

    // hnsw - simple
    IndexComponents<TEST_DATA_T, TEST_DIST_T> hnsw_components = hnsw_index->get_components();
    const std::type_info &hnsw_pp_expected_type = typeid(PreprocessorsContainerAbstract);
    const std::type_info &hnsw_pp_actual_type = typeid(*hnsw_components.preprocessors);
    ASSERT_EQ(hnsw_pp_expected_type, hnsw_pp_actual_type);
}

TYPED_TEST(HNSWTieredIndexTest, testSizeEstimation) {
    size_t dim = 128;
    size_t n = DEFAULT_BLOCK_SIZE;
    size_t M = 32;
    size_t bs = DEFAULT_BLOCK_SIZE;
    bool isMulti = TypeParam::isMulti();

    HNSWParams hnsw_params = {.type = TypeParam::get_index_type(),
                              .dim = dim,
                              .metric = VecSimMetric_Cosine,
                              .multi = isMulti,
                              .M = M};
    VecSimParams vecsim_hnsw_params = CreateParams(hnsw_params);

    auto mock_thread_pool = tieredIndexMock();
    TieredIndexParams tiered_params = {.jobQueue = &mock_thread_pool.jobQ,
                                       .jobQueueCtx = mock_thread_pool.ctx,
                                       .submitCb = tieredIndexMock::submit_callback,
                                       .flatBufferLimit = SIZE_MAX,
                                       .primaryIndexParams = &vecsim_hnsw_params};
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    mock_thread_pool.ctx->index_strong_ref.reset(index);
    auto allocator = index->getAllocator();

    size_t initial_size_estimation = VecSimIndex_EstimateInitialSize(&params);

    // labels_lookup hash table has additional memory, since STL implementation chooses "an
    // appropriate prime number" higher than n as the number of allocated buckets (for n=1000, 1031
    // buckets are created)
    auto hnsw_index = this->CastToHNSW(index);
    if (isMulti == false) {
        auto hnsw = reinterpret_cast<HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(hnsw_index);
    } else { // if its a multi value index cast to HNSW_Multi
        auto hnsw = reinterpret_cast<HNSWIndex_Multi<TEST_DATA_T, TEST_DIST_T> *>(hnsw_index);
    }

    ASSERT_EQ(initial_size_estimation, index->getAllocationSize());

    // Add vectors up to initial capacity (initial capacity == block size).
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        mock_thread_pool.thread_iteration();
    }

    // Estimate memory delta for filling up the first block and adding another block.
    size_t estimation = VecSimIndex_EstimateElementSize(&params) * bs;

    size_t before = index->getAllocationSize();
    GenerateAndAddVector<TEST_DATA_T>(index, dim, bs + n, bs + n);
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

TYPED_TEST(HNSWTieredIndexTest, addVector) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    bool isMulti = TypeParam::isMulti();
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = isMulti};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();
    TieredIndexParams tiered_params = {.jobQueue = &mock_thread_pool.jobQ,
                                       .jobQueueCtx = mock_thread_pool.ctx,
                                       .submitCb = tieredIndexMock::submit_callback,
                                       .flatBufferLimit = SIZE_MAX,
                                       .primaryIndexParams = &hnsw_params};
    auto *tiered_index = reinterpret_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(
        TieredFactory::NewIndex(&tiered_params));
    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();
    // Set the created tiered index in the index external context.
    mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);

    BFParams bf_params = {.type = TypeParam::get_index_type(),
                          .dim = dim,
                          .metric = VecSimMetric_L2,
                          .multi = isMulti};

    // Validate that memory upon creating the tiered index is as expected (no more than 2%
    // above te expected, since in different platforms there are some minor additional
    // allocations).
    size_t expected_mem = TieredFactory::EstimateInitialSize(&tiered_params);
    ASSERT_LE(expected_mem, tiered_index->getAllocationSize());
    ASSERT_GE(expected_mem * 1.02, tiered_index->getAllocationSize());

    // Create a vector and add it to the tiered index.
    labelType vec_label = 1;
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    // Validate that the vector was inserted to the flat buffer properly.
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->frontendIndex->getDistanceFrom_Unsafe(vec_label, vector), 0);
    // Validate that the job was created properly
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label).size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label)[0]->label, vec_label);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label)[0]->id, 0);

    // Account for the allocation of a new block due to the vector insertion.
    expected_mem += (BruteForceFactory::EstimateElementSize(&bf_params)) * DEFAULT_BLOCK_SIZE;
    // Account for the memory that was allocated in the labelToId map (approx.)
    expected_mem += sizeof(vecsim_stl::unordered_map<labelType, idType>::value_type) +
                    sizeof(void *) + sizeof(size_t);
    // Account for the memory that was allocated in the labelToInsertJobs map (approx.)
    expected_mem +=
        sizeof(vecsim_stl::unordered_map<labelType,
                                         vecsim_stl::vector<TieredInsertJob *>>::value_type) +
        sizeof(void *) + sizeof(size_t);
    // Account for the inner buffer of the std::vector<TieredInsertJob *> in the map.
    expected_mem += sizeof(void *) + sizeof(size_t);
    // Account for the insert job that was created.
    expected_mem += sizeof(HNSWInsertJob) + sizeof(size_t);
    ASSERT_GE(expected_mem * 1.02, tiered_index->getAllocationSize());
    ASSERT_LE(expected_mem, tiered_index->getAllocationSize());

    if (isMulti) {
        // Add another vector under the same label (create another insert job)
        VecSimIndex_AddVector(tiered_index, vector, vec_label);
        ASSERT_EQ(tiered_index->indexSize(), 2);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        ASSERT_EQ(tiered_index->backendIndex->indexSize(), 0);
        ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 2);
        // Validate that the second job was created properly
        ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label).size(), 2);
        ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label)[1]->label, vec_label);
        ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label)[1]->id, 1);
    }
}

TYPED_TEST(HNSWTieredIndexTest, manageIndexOwnership) {

    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);
    size_t initial_mem = allocator->getAllocationSize();

    // Create a dummy job callback that insert one vector to the underline HNSW index.
    auto dummy_job = [](AsyncJob *job) {
        auto *my_index = reinterpret_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(job->index);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        size_t dim = 4;
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim);
        my_index->backendIndex->addVector(vector, my_index->backendIndex->indexSize());
    };

    std::atomic_int successful_executions(0);
    auto job1 =
        new (allocator) AsyncJob(allocator, HNSW_INSERT_VECTOR_JOB, dummy_job, tiered_index);
    auto job2 =
        new (allocator) AsyncJob(allocator, HNSW_INSERT_VECTOR_JOB, dummy_job, tiered_index);

    // Wrap this job with an array and submit the jobs to the queue.
    tiered_index->submitSingleJob(job1);
    tiered_index->submitSingleJob(job2);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);

    // Execute the job from the queue asynchronously, delete the index in the meantime.
    auto run_fn = [&successful_executions, &mock_thread_pool]() {
        // Create a temporary strong reference of the index from the weak reference that the
        // job holds, to ensure that the index is not deleted while the job is running.
        if (auto temp_ref = mock_thread_pool.jobQ.front().index_weak_ref.lock()) {
            // At this point we wish to validate that we have both the index strong ref (stored
            // in index_ctx) and the weak ref owned by the job (that we currently promoted).
            EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 2);

            mock_thread_pool.jobQ.front().job->Execute(mock_thread_pool.jobQ.front().job);
            successful_executions++;
        }
        mock_thread_pool.jobQ.kick();
    };
    std::thread t1(run_fn);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Delete the index while the job is still running, to ensure that the weak ref protects
    // the index.
    mock_thread_pool.reset_ctx();
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 1);
    t1.join();
    // Expect that the first job will succeed.
    ASSERT_EQ(successful_executions, 1);

    // The second job should not run, since the weak reference is not supposed to become a
    // strong references now.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 0);
    std::thread t2(run_fn);
    t2.join();
    // Expect that the second job is ot successful.
    ASSERT_EQ(successful_executions, 1);
}

TYPED_TEST(HNSWTieredIndexTest, insertJob) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Create a vector and add it to the tiered index.
    labelType vec_label = 1;
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);

    // Execute the insert job manually (in a synchronous manner).
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    auto *insertion_job = reinterpret_cast<HNSWInsertJob *>(mock_thread_pool.jobQ.front().job);
    ASSERT_EQ(insertion_job->label, vec_label);
    ASSERT_EQ(insertion_job->id, 0);
    ASSERT_EQ(insertion_job->jobType, HNSW_INSERT_VECTOR_JOB);

    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
    // HNSW index should have allocated a single block, while flat index should remove the
    // block.
    ASSERT_EQ(tiered_index->backendIndex->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->frontendIndex->indexCapacity(), 0);
    ASSERT_EQ(tiered_index->backendIndex->getDistanceFrom_Unsafe(vec_label, vector), 0);
    // After the execution, the job should be removed from the labelToInsertJobs mapping.
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, insertJobAsync) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 5000;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    // Insert vectors
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }

    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), n);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    // Verify that the vectors were inserted to HNSW as expected
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T expected_vector[dim];
        GenerateVector<TEST_DATA_T>(expected_vector, dim, i);
        ASSERT_EQ(tiered_index->backendIndex->getDistanceFrom_Unsafe(i, expected_vector), 0);
    }
}

// A multi-value label's vectors are routinely split across the tiers while an ingest job is
// pending, so reading one tier reports a subset. Nothing else covers the tiered wrapper's
// `getDataByLabel` -- the other uses in this file all call a tier directly.
TYPED_TEST(HNSWTieredIndexTestBasic, getDataByLabelSpansBothTiers) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    TEST_DATA_T first[dim];
    TEST_DATA_T second[dim];
    GenerateVector<TEST_DATA_T>(first, dim, 1);
    GenerateVector<TEST_DATA_T>(second, dim, 2);

    // Two vectors under one label, with only the first ingested: one vector per tier.
    VecSimIndex_AddVector(tiered_index, first, 0);
    mock_thread_pool.thread_iteration();
    VecSimIndex_AddVector(tiered_index, second, 0);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);

    std::vector<std::vector<TEST_DATA_T>> stored;
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 2) << "a tier was not read; the label's vectors are split across them";
    // Buffer first, then backend: `second` is the buffered one.
    ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[0].data(), second, dim));
    ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[1].data(), first, dim));

    // Once everything is ingested the backend alone holds both.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    stored.clear();
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 2);

    // An absent label yields nothing rather than throwing, which is what lets the output size
    // stand in for "does the index hold this label".
    stored.clear();
    tiered_index->getDataByLabel(12345, stored);
    ASSERT_TRUE(stored.empty());
}

// The buffered case is the one that regressed before: reading only the backend reports nothing
// for a vector written recently enough to still be queued for ingestion, which is exactly when a
// document is most likely to be written again. Determinism comes from never running the job.
TYPED_TEST(HNSWTieredIndexTestBasic, getDataByLabelWhileStillBuffered) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 1);
    VecSimIndex_AddVector(tiered_index, vector, 0);
    // Nothing ingested: the vector exists only in the buffer, with its job still pending.
    ASSERT_EQ(this->GetFlatIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(this->CastToHNSW(tiered_index)->indexSize(), 0);

    std::vector<std::vector<TEST_DATA_T>> stored;
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 1) << "the buffered tier was not read";
    ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[0].data(), vector, dim));

    // Draining moves it to the backend, which must report it exactly once -- not once per tier.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(this->GetFlatIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(this->CastToHNSW(tiered_index)->indexSize(), 1);
    stored.clear();
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 1);
    ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[0].data(), vector, dim));
}

// An ingest job inserts into the backend before dropping the buffered copy, so a label is briefly
// held by both tiers. What that means for `getDataByLabel` differs by index kind, and both halves
// are asserted here because each is a deliberate consequence of one condition in the dispatch:
// a single-value label short-circuits on the buffer hit and never reads the backend, while a
// multi-value label always reads it and so reports the caught vector twice.
TYPED_TEST(HNSWTieredIndexTestBasic, getDataByLabelInTheIngestWindow) {
    size_t dim = 4;
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 1);

    {
        HNSWParams single = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = false};
        VecSimParams hnsw_params = CreateParams(single);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        auto *hnsw_index = this->CastToHNSW(tiered_index);

        // Build the window directly rather than racing a worker into it.
        VecSimIndex_AddVector(tiered_index, vector, 0);
        hnsw_index->addVector(vector, 0);
        ASSERT_EQ(this->GetFlatIndex(tiered_index)->indexSize(), 1);
        ASSERT_EQ(hnsw_index->indexSize(), 1);

        std::vector<std::vector<TEST_DATA_T>> stored;
        tiered_index->getDataByLabel(0, stored);
        ASSERT_EQ(stored.size(), 1) << "a single-value label must not report its two tier copies";
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[0].data(), vector, dim));
    }

    {
        HNSWParams multi = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = true};
        VecSimParams hnsw_params = CreateParams(multi);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        auto *hnsw_index = this->CastToHNSW(tiered_index);

        VecSimIndex_AddVector(tiered_index, vector, 0);
        hnsw_index->addVector(vector, 0);

        std::vector<std::vector<TEST_DATA_T>> stored;
        tiered_index->getDataByLabel(0, stored);
        // Documented on the declaration: a multi-value label always reads both tiers, so a vector
        // caught mid-ingest is reported by each. Pinned here so the caveat cannot change unnoticed.
        ASSERT_EQ(stored.size(), 2);
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[0].data(), vector, dim));
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored[1].data(), vector, dim));
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, insertJobAsyncMulti) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 5000;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    size_t per_label = 5;
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors, store them in this continuous array.
    TEST_DATA_T vectors[n * dim];
    for (size_t i = 0; i < n / per_label; i++) {
        for (size_t j = 0; j < per_label; j++) {
            GenerateVector<TEST_DATA_T>(vectors + i * dim * per_label + j * dim, dim,
                                        i * per_label + j);
            tiered_index->addVector(vectors + i * dim * per_label + j * dim, i);
        }
    }

    mock_thread_pool.thread_pool_join();
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n / per_label);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
    // Verify that the vectors were inserted to HNSW as expected
    for (size_t i = 0; i < n / per_label; i++) {
        for (size_t j = 0; j < per_label; j++) {
            // The distance from every vector that is stored under the label i should be zero
            EXPECT_EQ(tiered_index->backendIndex->getDistanceFrom_Unsafe(
                          i, vectors + i * per_label * dim + j * dim),
                      0);
        }
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, KNNSearch) {
    size_t dim = 4;
    size_t k = 10;

    size_t n = k * 3;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    size_t cur_memory_usage;

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto hnsw_index = tiered_index->backendIndex;
    auto flat_index = tiered_index->frontendIndex;

    TEST_DATA_T query_0[dim];
    GenerateVector<TEST_DATA_T>(query_0, dim, 0);
    TEST_DATA_T query_1mid[dim];
    GenerateVector<TEST_DATA_T>(query_1mid, dim, n / 3);
    TEST_DATA_T query_2mid[dim];
    GenerateVector<TEST_DATA_T>(query_2mid, dim, n * 2 / 3);
    TEST_DATA_T query_n[dim];
    GenerateVector<TEST_DATA_T>(query_n, dim, n - 1);

    // Search for vectors when the index is empty.
    runTopKSearchTest(tiered_index, query_0, k, nullptr);

    // Define the verification functions.
    auto ver_res_0 = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index);
        ASSERT_DOUBLE_EQ(score, dim * id * id);
    };

    auto ver_res_1mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - query_1mid[0])), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_2mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - query_2mid[0])), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_n = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n - 1 - index);
        ASSERT_DOUBLE_EQ(score, dim * index * index);
    };

    // Insert n/2 vectors to the main index.
    for (size_t i = 0; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n / 2);
    ASSERT_EQ(tiered_index->indexSize(), hnsw_index->indexSize());

    // Search for k vectors with the flat index empty.
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Insert n/2 vectors to the flat index.
    for (size_t i = n / 2; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexSize(), hnsw_index->indexSize() + flat_index->indexSize());

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Add some overlapping vectors to the main and flat index.
    // adding directly to the underlying indexes to avoid jobs logic.
    // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
    for (size_t i = n / 3; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, i, i);
    }

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // More edge cases:

    // Search for more vectors than the index size.
    k = n + 1;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Search for less vectors than the index size, but more than the flat and main index sizes.
    k = n * 5 / 6;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the main index size, but less than the flat index size.
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(hnsw_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(hnsw_index->indexSize(), n / 2);
    k = n * 2 / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the flat index size, but less than the main index size.
    for (size_t i = n / 2; i < n; i++) {
        VecSimIndex_DeleteVector(flat_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n / 6);
    ASSERT_EQ(hnsw_index->indexSize(), n / 2);
    k = n / 4;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for vectors when the flat index is not empty but the main index is empty.
    for (size_t i = 0; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(hnsw_index, i);
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(hnsw_index->indexSize(), 0);
    k = n / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // // // // // // // // // // // //
    // Check behavior upon timeout.  //
    // // // // // // // // // // // //

    VecSimQueryReply *res;
    // Add a vector to the HNSW index so there will be a reason to query it.
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, n, n);

    // Set timeout callback to always return 1 (will fail while querying the flat buffer).
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    res = VecSimIndex_TopKQuery(tiered_index, query_0, k, nullptr, BY_SCORE);
    ASSERT_TRUE(res->results.empty());
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);

    // Set timeout callback to return 1 after n checks (will fail while querying the HNSW index).
    // Brute-force index checks for timeout after each vector.
    size_t checks_in_flat = flat_index->indexSize();
    VecSimQueryParams qparams = {.timeoutCtx = &checks_in_flat};
    VecSim_SetTimeoutCallbackFunction([](void *ctx) {
        auto count = static_cast<size_t *>(ctx);
        if (*count == 0) {
            return 1;
        }
        (*count)--;
        return 0;
    });
    res = VecSimIndex_TopKQuery(tiered_index, query_0, k, &qparams, BY_SCORE);
    ASSERT_TRUE(res->results.empty());
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_TopKQuery(flat_index, query_0, k, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Clean up.
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; });
}

TYPED_TEST(HNSWTieredIndexTest, parallelSearch) {
    size_t dim = 4;
    size_t k = 10;
    size_t n = 2000;
    bool isMulti = TypeParam::isMulti();

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = isMulti,
        .efRuntime = n,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    std::atomic_int successful_searches(0);
    auto parallel_knn_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        size_t dim = search_job->dim;
        auto query = search_job->query;

        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };
        runTopKSearchTest(job->index, query, k, verify_res);
        (*search_job->successful_searches)++;
        delete job;
    };

    size_t per_label = isMulti ? 10 : 1;
    size_t n_labels = n / per_label;

    // Fill the job queue with insert and search jobs, while filling the flat index, before
    // initializing the thread pool.
    for (size_t i = 0; i < n; i++) {
        // Insert a vector to the flat index and add a job to insert it to the main index.
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);

        // Add a search job. Make sure the query element is between k and n - k.
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, (i % (n_labels - (2 * k))) + k);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_knn_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), n_labels);
    for (auto &it : tiered_index->labelToInsertJobs) {
        EXPECT_EQ(it.second.size(), per_label);
    }
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), 0);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // All the vectors are already in the tiered index, so we expect to find the expected
    // results from the get-go.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTest, parallelInsertSearch) {
    size_t dim = 4;
    size_t k = 10;
    size_t n = 3000;

    size_t block_size = n / 100;

    bool isMulti = TypeParam::isMulti();

    // Create TieredHNSW index instance with a mock queue.
    size_t n_labels = isMulti ? n / 25 : n;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = isMulti,
        .blockSize = block_size,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // Save the number fo tasks done by thread i in the i-th entry.
    std::vector<size_t> completed_tasks(mock_thread_pool.thread_pool_size, 0);
    mock_thread_pool.init_threads();
    std::atomic_int successful_searches(0);

    auto parallel_knn_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        auto query = search_job->query;
        // In this test we don't care about the results, just that the search doesn't crash
        // and returns the correct number of valid results.
        auto verify_res = [&](size_t id, double score, size_t res_index) {};
        runTopKSearchTest(job->index, query, k, verify_res);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Insert vectors in parallel to search.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, (TEST_DATA_T)n / 4 + (i % 1000) * M_PI);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_knn_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, MergeMulti) {
    size_t dim = 4;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = true,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    auto hnsw_index = tiered_index->backendIndex;
    auto flat_index = tiered_index->frontendIndex;

    // Insert vectors with label 0 to HNSW only.
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, 0, 1);
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, 0, 2);
    // Insert vectors with label 1 to flat buffer only.
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 2);
    // Insert DIFFERENT vectors with label 2 to both HNSW and flat buffer.
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, 2, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 2, 1);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

    // Search in the tiered index for more vectors than it has. Merging the results from the two
    // indexes should result in a list of unique vectors, even if the scores of the duplicates are
    // different.
    runTopKSearchTest(tiered_index, query, 5, [](size_t _, double __, size_t ___) {});
}

// A label that is mid-ingest lives in the HNSW index and in the flat buffer at once, so
// a top-K query merges two lists that both carry it. Both lists are ordered by exact score, while
// cmpVecSimQueryResultByScoreThenId() used to call scores within 1e-6 equal and fall back to
// ordering by id. Where those two orders disagree the shared label does not reach the two merge
// cursors at the same time, and a merge that dedups by cursor alignment alone emits it twice.
TYPED_TEST(HNSWTieredIndexTestBasic, MergeSingleWithNearTiedScores) {
    size_t dim = 4;

    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = false,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Label 1 is the farther of the two from the query, and carries the smaller id. That
    // combination is what makes the orders disagree: by exact score label 1 comes second, by id it
    // comes first.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 0.5000001);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 0.5);
    // An ingest job has already inserted label 1 into HNSW but not yet erased it from the flat
    // buffer, so both indexes hold it - with the same vector, and so the same score.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->frontendIndex, dim, 1, 0.5000001);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

    // The regression needs the two distances to differ, but by less than the tolerance the merge
    // comparator used to treat as equality. Assert it, so that a change in the vectors or in the
    // data type fails here rather than silently retiring the test.
    auto backend_res =
        VecSimIndex_TopKQuery(tiered_index->backendIndex, query, 2, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(backend_res), 2);
    double closer = VecSimQueryResult_GetScore(backend_res->results.data());
    double farther = VecSimQueryResult_GetScore(backend_res->results.data() + 1);
    VecSimQueryReply_Free(backend_res);
    ASSERT_LT(closer, farther);
    ASSERT_LT(farther - closer, 1e-6);

    // k exceeds the label count on purpose: when the merge fills exactly k results the truncation
    // hides the second copy, which is why a query for as many results as the index holds does not
    // expose this.
    runTopKSearchTest(tiered_index, query, 3, [](size_t _, double __, size_t ___) {});
}

TYPED_TEST(HNSWTieredIndexTest, deleteFromHNSWBasic) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    bool isMulti = TypeParam::isMulti();

    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = isMulti};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Delete a non existing label.
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(0)), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Insert one vector to HNSW and then delete it (it should have no neighbors to repair).
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(0)), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Add another vector and remove it. Since the other vector in the index has marked deleted,
    // this vector should have no neighbors, and again, no neighbors to repair.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(1)), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Add two vectors and delete one, expect that at backendIndex one repair job will be created.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 2);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 3, 3);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(3)), 1);

    // The first job should be a repair job of the first inserted non-deleted node id (2)
    // in level 0.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 2);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);
    ASSERT_GE(tiered_index->idToRepairJobs.at(2).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs[0]->deleted_id, 3);

    ASSERT_EQ(tiered_index->indexSize(), 4);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 3);
    ASSERT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 3);
    ASSERT_EQ(tiered_index->idToSwapJob.size(), 3);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteFromHNSWMulti) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;

    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Add two vectors and delete one, expect that at least one repair job will be created.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(0)), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1)[0]->associatedSwapJobs[0]->deleted_id, 0);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 1);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
    mock_thread_pool.jobQ.pop();

    // Insert another vector under the label (1) that has not been deleted.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 2);

    // Expect to see both ids stored under this label being deleted (1 and 2), and have both
    // ids need repair (as the connection between the two vectors is mutual). However, 1 has
    // also an outgoing edge to his other (deleted) neighbor (0), so there will be no new
    // repair job created for 1, since the previous repair job is expected to have both 0 and 2 in
    // its associated swap jobs. Also, there is an edge 0->1 whose going to be repaired as well.
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(1)), 2);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 0);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
    mock_thread_pool.jobQ.pop();
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 2);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
    mock_thread_pool.jobQ.pop();
    // No new job for deleting 1->2 edge, just another associated swap job for the existing repair
    // job of 1 (in addition to 0, we have 2).
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 3);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1)[0]->associatedSwapJobs.size(), 2);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1)[0]->associatedSwapJobs[1]->deleted_id, 2);

    ASSERT_EQ(tiered_index->idToRepairJobs.at(0).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs[0]->deleted_id, 1);

    ASSERT_EQ(tiered_index->idToRepairJobs.at(2).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs[0]->deleted_id, 1);

    ASSERT_EQ(tiered_index->idToSwapJob.size(), 3);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteFromHNSWMultiLevels) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;

    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Test that repair jobs are created for multiple levels.
    size_t num_elements_with_multiple_levels = 0;
    int vec_id = -1;
    do {
        vec_id++;
        GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, vec_id, vec_id);
        if (tiered_index->getHNSWIndex()->getGraphDataByInternalId(vec_id)->toplevel > 0) {
            num_elements_with_multiple_levels++;
        }
    } while (num_elements_with_multiple_levels < 2);

    // Delete the last inserted vector, which is in level 1.
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(vec_id)), 1);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getGraphDataByInternalId(vec_id)->toplevel, 1);
    // This should be an array of length 1.
    auto &level_one = tiered_index->getHNSWIndex()->getElementLevelData(vec_id, 1);
    ASSERT_EQ(level_one.numLinks, 1);

    size_t num_repair_jobs = mock_thread_pool.jobQ.size();
    // There should be at least two nodes to repair, the neighbors of next_id in levels 0 and 1
    ASSERT_GE(num_repair_jobs, 2);
    while (mock_thread_pool.jobQ.size() > 1) {
        // First we should have jobs for repairing nodes in level 0.
        ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
        mock_thread_pool.jobQ.pop();
    }

    // The last job should be repairing the single neighbor in level 1.
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 1);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, level_one.links[0]);
}

TYPED_TEST(HNSWTieredIndexTest, deleteFromHNSWWithRepairJobExec) {
    // Create TieredHNSW index instance with a mock queue.
    size_t n = 1000;
    size_t dim = 4;
    bool isMulti = TypeParam::isMulti();

    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = isMulti,
                         .M = 4};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index->getHNSWIndex(), dim, i, i);
    }

    // Delete vectors one by one and run the resulted repair jobs.
    while (tiered_index->getHNSWIndex()->getNumMarkedDeleted() < n) {
        // Choose the current entry point each time (it should be modified after the deletion).
        idType ep = tiered_index->getHNSWIndex()->safeGetEntryPointState().first;
        auto incoming_neighbors =
            tiered_index->getHNSWIndex()->safeCollectAllNodeIncomingNeighbors(ep);
        ASSERT_EQ((tiered_index->deleteLabelFromHNSW(ep)), 1);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), incoming_neighbors.size());
        ASSERT_EQ(tiered_index->getHNSWIndex()->checkIntegrity().connections_to_repair,
                  mock_thread_pool.jobQ.size());
        ASSERT_NE(tiered_index->getHNSWIndex()->safeGetEntryPointState().first, ep);

        // Execute synchronously all the repair jobs for the current deletion.
        while (!mock_thread_pool.jobQ.empty()) {
            idType repair_node_id = ((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id;
            auto repair_node_level = ((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level;

            tiered_index->getHNSWIndex()->repairNodeConnections(repair_node_id, repair_node_level);
            ElementLevelData &node_level = tiered_index->getHNSWIndex()->getElementLevelData(
                repair_node_id, repair_node_level);
            // This makes sure that the deleted node is no longer in the neighbors set of the
            // repaired node.
            ASSERT_TRUE(std::find(node_level.links, node_level.links + node_level.numLinks, ep) ==
                        node_level.links + node_level.numLinks);
            // Remove the job from the id -> repair_jobs lookup, so we won't think that it is
            // still pending and avoid creating new jobs for nodes that already been repaired
            // as they were pointing to deleted elements.
            tiered_index->idToRepairJobs.erase(repair_node_id);
            mock_thread_pool.jobQ.kick();
        }
        ASSERT_EQ(tiered_index->getHNSWIndex()->checkIntegrity().connections_to_repair, 0);
    }
}

TYPED_TEST(HNSWTieredIndexTest, manageIndexOwnershipWithPendingJobs) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    bool isMulti = TypeParam::isMulti();

    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = isMulti};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Add a vector and create a pending insert job.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);

    // Delete the index before the job was executed (this would delete the pending job as well).
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 1);
    mock_thread_pool.reset_ctx();
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 0);
    mock_thread_pool.jobQ.pop();

    // Recreate the index with a new ctx.
    auto *ctx = new tieredIndexMock::IndexExtCtx(&mock_thread_pool);
    mock_thread_pool.reset_ctx(ctx);
    tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Add two vectors directly to HNSW, and remove one vector to create a repair job.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(0)), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);

    // Delete the index before the job was executed (this would delete the pending job as well).
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 1);
    mock_thread_pool.reset_ctx();
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, AdHocSingle) {
    size_t dim = 4;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    auto hnsw_index = tiered_index->backendIndex;
    auto flat_index = tiered_index->frontendIndex;

    TEST_DATA_T vec1[dim];
    GenerateVector<TEST_DATA_T>(vec1, dim, 1);
    TEST_DATA_T vec2[dim];
    GenerateVector<TEST_DATA_T>(vec2, dim, 2);
    TEST_DATA_T vec3[dim];
    GenerateVector<TEST_DATA_T>(vec3, dim, 3);
    TEST_DATA_T vec4[dim];
    GenerateVector<TEST_DATA_T>(vec4, dim, 4);

    // Insert vectors to the tiered index.
    VecSimIndex_AddVector(hnsw_index, vec1, 1); // vec1 is inserted to HNSW only.
    VecSimIndex_AddVector(flat_index, vec2, 2); // vec2 is inserted to flat only.

    // vec3 is inserted to both HNSW and flat, simulating a vector that was inserted
    // to HNSW and not yet removed from flat.
    VecSimIndex_AddVector(hnsw_index, vec3, 3);
    VecSimIndex_AddVector(flat_index, vec3, 3);

    // vec4 is not inserted to any index, simulating a non-existing vector.

    // copy memory context before querying the index.
    size_t cur_memory_usage = allocator->getAllocationSize();

    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 1, vec1), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 2, vec2), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 3, vec3), 0);
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 4, vec4)));

    ASSERT_EQ(cur_memory_usage, allocator->getAllocationSize());
}

TYPED_TEST(HNSWTieredIndexTestBasic, AdHocMulti) {
    size_t dim = 4;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = true,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    auto hnsw_index = tiered_index->backendIndex;
    auto flat_index = tiered_index->frontendIndex;
    auto allocator = tiered_index->getAllocator();

    TEST_DATA_T cur_element = 1;

    // vec1_* are inserted to HNSW only.
    TEST_DATA_T vec1_1[dim];
    GenerateVector<TEST_DATA_T>(vec1_1, dim, cur_element++);
    TEST_DATA_T vec1_2[dim];
    GenerateVector<TEST_DATA_T>(vec1_2, dim, cur_element++);
    TEST_DATA_T vec1_3[dim];
    GenerateVector<TEST_DATA_T>(vec1_3, dim, cur_element++);

    // vec2_* are inserted to flat only.
    TEST_DATA_T vec2_1[dim];
    GenerateVector<TEST_DATA_T>(vec2_1, dim, cur_element++);
    TEST_DATA_T vec2_2[dim];
    GenerateVector<TEST_DATA_T>(vec2_2, dim, cur_element++);
    TEST_DATA_T vec2_3[dim];
    GenerateVector<TEST_DATA_T>(vec2_3, dim, cur_element++);

    // vec3_* are inserted to both HNSW and flat (some to HNSW only, some to flat only)
    TEST_DATA_T vec3_1[dim];
    GenerateVector<TEST_DATA_T>(vec3_1, dim, cur_element++);
    TEST_DATA_T vec3_2[dim];
    GenerateVector<TEST_DATA_T>(vec3_2, dim, cur_element++);
    TEST_DATA_T vec3_3[dim];
    GenerateVector<TEST_DATA_T>(vec3_3, dim, cur_element++);

    // vec4_* are inserted to both HNSW and flat with some overlap.
    TEST_DATA_T vec4_1[dim];
    GenerateVector<TEST_DATA_T>(vec4_1, dim, cur_element++);
    TEST_DATA_T vec4_2[dim];
    GenerateVector<TEST_DATA_T>(vec4_2, dim, cur_element++);
    TEST_DATA_T vec4_3[dim];
    GenerateVector<TEST_DATA_T>(vec4_3, dim, cur_element++);

    // vec5 is not inserted to any index, simulating a non-existing vector.
    TEST_DATA_T vec5[dim];
    GenerateVector<TEST_DATA_T>(vec5, dim, cur_element++);

    // Insert vectors to the tiered index.
    VecSimIndex_AddVector(hnsw_index, vec1_1, 1);
    VecSimIndex_AddVector(hnsw_index, vec1_2, 1);
    VecSimIndex_AddVector(hnsw_index, vec1_3, 1);

    VecSimIndex_AddVector(flat_index, vec2_1, 2);
    VecSimIndex_AddVector(flat_index, vec2_2, 2);
    VecSimIndex_AddVector(flat_index, vec2_3, 2);

    VecSimIndex_AddVector(hnsw_index, vec3_1, 3);
    VecSimIndex_AddVector(flat_index, vec3_2, 3);
    VecSimIndex_AddVector(hnsw_index, vec3_3, 3);

    VecSimIndex_AddVector(hnsw_index, vec4_1, 4);
    VecSimIndex_AddVector(hnsw_index, vec4_2, 4);
    VecSimIndex_AddVector(flat_index, vec4_2, 4);
    VecSimIndex_AddVector(flat_index, vec4_3, 4);

    // vec5 is not inserted to any index, simulating a non-existing vector.

    // copy memory context before querying the index.
    size_t cur_memory_usage = allocator->getAllocationSize();

    // Distance from any vector to its label should be 0.
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 1, vec1_1), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 1, vec1_2), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 1, vec1_3), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 2, vec2_1), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 2, vec2_2), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 2, vec2_3), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 3, vec3_1), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 3, vec3_2), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 3, vec3_3), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 4, vec4_1), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 4, vec4_2), 0);
    ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 4, vec4_3), 0);
    // Distance from a non-existing label should be NaN.
    ASSERT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, 5, vec5)));

    ASSERT_EQ(cur_memory_usage, allocator->getAllocationSize());
}

TYPED_TEST(HNSWTieredIndexTest, parallelInsertAdHoc) {
    size_t dim = 4;
    size_t n = 1000;

    size_t block_size = n / 100;
    bool isMulti = TypeParam::isMulti();

    // Create TieredHNSW index instance with a mock queue.
    size_t n_labels = isMulti ? n / 50 : n;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = isMulti,
        .blockSize = block_size,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    std::atomic_int successful_searches(0);

    auto parallel_adhoc_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        auto query = search_job->query;
        size_t element = *(TEST_DATA_T *)query;
        size_t label = element % search_job->n;
        bool isMulti =
            reinterpret_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(search_job->index)
                ->backendIndex->isMultiValue();
        VecSimTieredIndex_AcquireSharedLocks(search_job->index);
        ASSERT_EQ(0, VecSimIndex_GetDistanceFrom_Unsafe(search_job->index, label, query));
        VecSimTieredIndex_ReleaseSharedLocks(search_job->index);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Insert vectors in parallel to search.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, i);
        auto search_job = new (allocator)
            tieredIndexMock::SearchJobMock(allocator, parallel_adhoc_search, tiered_index, 1, query,
                                           n_labels, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTest, deleteVector) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    labelType vec_label = 0;
    // Delete from an empty index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 0);

    // Create a vector and add it to the tiered index (expect it to go into the flat buffer).
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);

    // Expect to have one pending insert job.
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
    auto *job = tiered_index->labelToInsertJobs.at(vec_label).back();

    // Remove vector from flat buffer.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    // The insert job should become invalid, and executing it should do nothing.
    ASSERT_EQ(job->isValid, false);
    ASSERT_EQ(reinterpret_cast<HNSWInsertJob *>(job)->id, 0);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 0);

    // Create a vector and add it to HNSW in the tiered index.
    VecSimIndex_AddVector(tiered_index->backendIndex, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);

    // Remove from main index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 1);

    // Re-insert a deleted label with a different vector.
    TEST_DATA_T new_vec_val = 2.0;
    GenerateVector<TEST_DATA_T>(vector, dim, new_vec_val);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);

    // Move the vector to HNSW by executing the insert job.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 2);
    // Check that the distance from the deleted vector (of zeros) to the label is the distance
    // to the new vector (L2 distance).
    TEST_DATA_T deleted_vector[dim];
    GenerateVector<TEST_DATA_T>(deleted_vector, dim, 0);
    ASSERT_EQ(tiered_index->backendIndex->getDistanceFrom_Unsafe(vec_label, deleted_vector),
              dim * pow(new_vec_val, 2));
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteVectorMulti) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Test some more scenarios that are relevant only for multi value index.
    labelType vec_label = 0;
    labelType other_vec_val = 2.0;
    idType invalidJobsCounter = 0;
    // Create a vector and add it to HNSW in the tiered index.
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index->backendIndex, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);

    // Test deleting a label for which one of its vector's is in the flat index while the
    // second one is in HNSW.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->isValid, false);
    ASSERT_EQ(reinterpret_cast<HNSWInsertJob *>(mock_thread_pool.jobQ.front().job)->id,
              invalidJobsCounter++);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Test deleting a label for which both of its vector's is in the flat index.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, vec_label);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 2);
    ASSERT_EQ(tiered_index->indexSize(), 3);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->isValid, false);
    ASSERT_EQ(reinterpret_cast<HNSWInsertJob *>(mock_thread_pool.jobQ.front().job)->id,
              invalidJobsCounter++);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->isValid, false);
    ASSERT_EQ(reinterpret_cast<HNSWInsertJob *>(mock_thread_pool.jobQ.front().job)->id,
              invalidJobsCounter++);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Test deleting a label for which both of its vector's is in HNSW index.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, vec_label);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    mock_thread_pool.thread_iteration();
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 3);
    ASSERT_EQ(tiered_index->backendIndex->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->backendIndex->indexLabelCount(), 0);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 3);
    ASSERT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 3);

    // Expect to see two repair jobs - one for each deleted vector internal id.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    ASSERT_EQ(reinterpret_cast<HNSWRepairJob *>(mock_thread_pool.jobQ.front().job)->node_id, 2);
    mock_thread_pool.thread_iteration();
    // The job above was the last one pending for id 1, so id 1 was taken out of the graph - and
    // with it the very edge that the remaining job (on id 1) was created to remove. That job is
    // invalidated instead of executed, so its node id field now holds its invalid job key.
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    ASSERT_FALSE(mock_thread_pool.jobQ.front().job->isValid);
    ASSERT_EQ(reinterpret_cast<HNSWRepairJob *>(mock_thread_pool.jobQ.front().job)->node_id,
              invalidJobsCounter++);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->invalidJobs.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteVectorMultiFromFlatAdvanced) {

    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Insert vectors to flat buffer under two distinct labels, so that ids 0, 2 will be associated
    // with the first label, and ids 1, 3, 4 will be associated with the second label.
    labelType vec_label_first = 0;
    labelType vec_label_second = 1;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_first);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_first);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);

    // Remove the second label, expect to see that id 1 will hold id 2 eventually.
    ASSERT_EQ(tiered_index->labelToInsertJobs.erase(vec_label_second), 1);
    auto updated_ids = tiered_index->frontendIndex->deleteVectorAndGetUpdatedIds(vec_label_second);
    ASSERT_EQ(updated_ids.size(), 1);
    ASSERT_EQ(updated_ids.at(1).first, 2);
    for (auto &it : updated_ids) {
        tiered_index->updateInsertJobInternalId(it.second.first, it.first, it.second.second);
    }
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first).size(), 2);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[0]->label, vec_label_first);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[0]->id, 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[1]->label, vec_label_first);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[1]->id, 1);

    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);

    // Remove the first label, expect an empty set.
    updated_ids = tiered_index->frontendIndex->deleteVectorAndGetUpdatedIds(vec_label_first);
    ASSERT_EQ(updated_ids.size(), 0);
    ASSERT_EQ(tiered_index->indexSize(), 0);
    tiered_index->labelToInsertJobs.clear();

    // Insert vectors to flat buffer under two distinct labels, so that ids 0, 3 will be associated
    // with the first label, and ids 1, 2, 4 will be associated with the second label. This should
    // test the case of multiple moves once we delete the second label:
    // {1->4} => {1->4, 2->3} => {1->3}
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_first);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_first);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label_second);
    ASSERT_EQ(tiered_index->labelToInsertJobs.erase(vec_label_second), 1);
    updated_ids = tiered_index->frontendIndex->deleteVectorAndGetUpdatedIds(vec_label_second);
    ASSERT_EQ(updated_ids.size(), 1);
    ASSERT_EQ(updated_ids.at(1).first, 3);
    for (auto &it : updated_ids) {
        tiered_index->updateInsertJobInternalId(it.second.first, it.first, it.second.second);
    }
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first).size(), 2);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[0]->label, vec_label_first);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[0]->id, 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[1]->label, vec_label_first);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(vec_label_first)[1]->id, 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    tiered_index->labelToInsertJobs.clear();

    // Clean jobs from queue
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.jobQ.kick();
    }
}

TYPED_TEST(HNSWTieredIndexTest, deleteVectorAndRepairAsync) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    for (size_t maxSwapJobs : {(int)n + 1, 10, 1}) {
        HNSWParams params = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = TypeParam::isMulti(),
                             .blockSize = 100};
        VecSimParams hnsw_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index =
            this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, maxSwapJobs);
        auto allocator = tiered_index->getAllocator();

        size_t per_label = TypeParam::isMulti() ? 50 : 1;
        size_t n_labels = n / per_label;

        // Launch the BG threads loop that takes jobs from the queue and executes them.

        for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
            mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                      std::ref(mock_thread_pool));
        }

        // Create and insert vectors one by one, then delete them one by one.
        std::srand(10); // create pseudo random generator with any arbitrary seed.
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            VecSimIndex_AddVector(tiered_index, vector, i % n_labels);
        }
        // While a thread is ingesting a vector into HNSW, a vector may appear in both indexes
        // (hence it will be counted twice in the index size calculation).
        size_t index_size = tiered_index->indexSize();
        EXPECT_GE(index_size, n);
        EXPECT_LE(index_size, n + mock_thread_pool.thread_pool_size);
        EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
        for (size_t i = 0; i < n_labels; i++) {
            // Every vector associated with the label may appear in flat/HNSW index or in both if
            // its just being ingested.
            int num_deleted = tiered_index->deleteVector(i);
            EXPECT_GE(num_deleted, per_label);
            EXPECT_LE(num_deleted,
                      std::min(2 * per_label, per_label + mock_thread_pool.thread_pool_size));
            EXPECT_EQ(tiered_index->deleteVector(i), 0); // delete already deleted label
        }
        EXPECT_EQ(tiered_index->indexLabelCount(), 0);

        mock_thread_pool.thread_pool_join();

        EXPECT_EQ(tiered_index->getHNSWIndex()->checkIntegrity().connections_to_repair, 0);
        EXPECT_EQ(tiered_index->getHNSWIndex()->safeGetEntryPointState().first, INVALID_ID);
        // Verify that we have no pending jobs.
        EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
        EXPECT_EQ(tiered_index->idToRepairJobs.size(), 0);
        for (auto &it : tiered_index->idToSwapJob) {
            EXPECT_EQ(it.second->pending_repair_jobs_counter.load(), 0);
        }
        // Trigger swapping of vectors that hadn't been swapped yet.
        tiered_index->executeReadySwapJobs();
        ASSERT_EQ(tiered_index->idToSwapJob.size(), 0);
        EXPECT_EQ(tiered_index->indexSize(), 0);
    }
}

TYPED_TEST(HNSWTieredIndexTest, alternateInsertDeleteAsync) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 16;
    size_t n = 1000;
    for (size_t maxSwapJobs : {(int)n + 1, 10, 1}) {
        for (size_t M : {2, 16}) {
            HNSWParams params = {.type = TypeParam::get_index_type(),
                                 .dim = dim,
                                 .metric = VecSimMetric_L2,
                                 .multi = TypeParam::isMulti(),
                                 .blockSize = 100,
                                 .M = M};
            VecSimParams hnsw_params = CreateParams(params);
            auto mock_thread_pool = tieredIndexMock();

            auto *tiered_index =
                this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, maxSwapJobs);
            auto allocator = tiered_index->getAllocator();

            size_t per_label = TypeParam::isMulti() ? 5 : 1;
            size_t n_labels = n / per_label;

            // Launch the BG threads loop that takes jobs from the queue and executes them.

            for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
                mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                          std::ref(mock_thread_pool));
            }

            // Create and insert 10 vectors, then delete them right after.
            size_t batch_size = 5;
            std::srand(10); // create pseudo random generator with any arbitrary seed.
            for (size_t i = 0; i < n / batch_size; i++) {
                for (size_t l = 0; l < batch_size; l++) {
                    TEST_DATA_T vector[dim];
                    for (size_t j = 0; j < dim; j++) {
                        vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
                    }
                    tiered_index->addVector(vector, (i * batch_size + l) / per_label);
                }
                for (size_t l = 0; l < batch_size / per_label; l++) {
                    // Every vector associated with the label may appear in flat/HNSW index or in
                    // both if its just being ingested.
                    int num_deleted = tiered_index->deleteVector(i * batch_size / per_label + l);
                    EXPECT_GE(num_deleted, per_label);
                    EXPECT_LE(num_deleted, 2 * per_label);
                }
            }
            // Vectors are deleted from flat buffer in place (in HNSW they are only marked as
            // deleted).
            EXPECT_GE(tiered_index->frontendIndex->indexSize(), 0);
            EXPECT_EQ(tiered_index->indexLabelCount(), 0);

            mock_thread_pool.thread_pool_join();

            EXPECT_EQ(tiered_index->getHNSWIndex()->checkIntegrity().connections_to_repair, 0);
            EXPECT_EQ(tiered_index->getHNSWIndex()->safeGetEntryPointState().first, INVALID_ID);
            // Verify that we have no pending jobs.
            EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
            EXPECT_EQ(tiered_index->idToRepairJobs.size(), 0);
            for (auto &it : tiered_index->idToSwapJob) {
                EXPECT_EQ(it.second->pending_repair_jobs_counter.load(), 0);
            }
            // Trigger swapping of vectors that hadn't been swapped yet.
            tiered_index->executeReadySwapJobs();
            ASSERT_LE(tiered_index->idToSwapJob.size(), 0);
            ASSERT_EQ(tiered_index->indexSize(), 0);
        }
    }
}

TYPED_TEST(HNSWTieredIndexTest, swapJobBasic) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Test initialization of the pendingSwapJobsThreshold value.
    ASSERT_EQ(tiered_index->pendingSwapJobsThreshold, DEFAULT_PENDING_SWAP_JOBS_THRESHOLD);
    mock_thread_pool.ctx->index_strong_ref.reset();

    tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool,
                                               MAX_PENDING_SWAP_JOBS_THRESHOLD + 1);
    allocator = tiered_index->getAllocator();
    ASSERT_EQ(tiered_index->pendingSwapJobsThreshold, MAX_PENDING_SWAP_JOBS_THRESHOLD);
    mock_thread_pool.ctx->index_strong_ref.reset();

    tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1);
    ASSERT_EQ(tiered_index->pendingSwapJobsThreshold, 1);

    allocator = tiered_index->getAllocator();

    // Call reserve for the unordered maps that are going to be used, since upon initialization it
    // consumed 0 memory, but after insertion and deletion they will consume a minimal amount of
    // memory (that is equivalent to the memory consumption upon reserving 0 buckets).
    tiered_index->idToRepairJobs.reserve(0);
    tiered_index->idToSwapJob.reserve(0);
    tiered_index->invalidJobs.reserve(0);

    TypeParam::isMulti() ? reinterpret_cast<HNSWIndex_Multi<TEST_DATA_T, TEST_DIST_T> *>(
                               tiered_index->getHNSWIndex())
                               ->labelLookup.reserve(0)
                         : reinterpret_cast<HNSWIndex_Single<TEST_DATA_T, TEST_DIST_T> *>(
                               tiered_index->getHNSWIndex())
                               ->labelLookup.reserve(0);

    size_t initial_mem = tiered_index->getAllocationSize();
    size_t initial_mem_backend = tiered_index->backendIndex->getAllocationSize();
    size_t initial_mem_frontend = tiered_index->frontendIndex->getAllocationSize();

    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    EXPECT_EQ(tiered_index->indexLabelCount(), 2);
    EXPECT_EQ(tiered_index->indexSize(), 2);
    // Delete both vectors.
    EXPECT_EQ(tiered_index->deleteVector(0), 1);
    EXPECT_EQ(tiered_index->deleteVector(1), 1);
    EXPECT_EQ(tiered_index->idToSwapJob.size(), 2);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 2);
    ASSERT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 2);
    // Expect to have pending repair jobs, so that swap job cannot be executed yet - for each
    // deleted vector there should be a single repair job.
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 2);
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 1);
    EXPECT_EQ(tiered_index->idToSwapJob.at(1)->pending_repair_jobs_counter.load(), 1);
    mock_thread_pool.thread_iteration();
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(tiered_index->idToSwapJob.size(), 2);
    // Insert another vector and remove it. expect it to have no neighbors.
    // Threshold for is set to be 1, so now we expect that a single deleted vector (which has no
    // pending repair jobs) will be swapped - and 2 will remain.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 2);
    EXPECT_EQ(tiered_index->deleteVector(2), 1);
    EXPECT_EQ(tiered_index->idToSwapJob.size(), 2);
    EXPECT_EQ(tiered_index->indexSize(), 2);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 2);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 2);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Now swap the rest of the jobs.
    tiered_index->executeReadySwapJobs();
    EXPECT_EQ(tiered_index->idToSwapJob.size(), 0);
    EXPECT_EQ(tiered_index->indexSize(), 0);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);

    // Reserve manually 0 buckets in the hash tables so that memory would be as it was before we
    // started inserting vectors.
    tiered_index->idToRepairJobs.reserve(0);
    tiered_index->idToSwapJob.reserve(0);
    tiered_index->invalidJobs.reserve(0);
    tiered_index->getHNSWIndex()->resizeLabelLookup(0);

    // Manually shrink the vectors so that memory would be as it was before we started inserting
    dynamic_cast<DataBlocksContainer *>(tiered_index->getHNSWIndex()->vectors)->shrinkToFit();
    tiered_index->getHNSWIndex()->graphDataBlocks.shrink_to_fit();
    tiered_index->getHNSWIndex()->visitedNodesHandlerPool.clearPool();

    EXPECT_EQ(tiered_index->backendIndex->getAllocationSize(), initial_mem_backend);
    EXPECT_EQ(tiered_index->frontendIndex->getAllocationSize(), initial_mem_frontend);
    EXPECT_EQ(tiered_index->getAllocationSize(), initial_mem);
    mock_thread_pool.reset_ctx();

    // VecSimAllocator::allocation_header_size = size_t, this should be the only memory that we
    // account for at this point.
    EXPECT_EQ(allocator->getAllocationSize(), sizeof(size_t));
}

TYPED_TEST(HNSWTieredIndexTest, swapJobBasic2) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1);
    ASSERT_EQ(tiered_index->pendingSwapJobsThreshold, 1);
    auto allocator = tiered_index->getAllocator();

    // Insert 3 vectors, expect to have a fully connected graph.
    idType invalid_jobs_counter = 0;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 2);
    // Delete 0, expect to have two repair jobs pending for 1 and 2 and execute it.
    EXPECT_EQ(tiered_index->deleteVector(0), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 2);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 0);
    // Delete 2. Only the 1->2 edge is left to repair: 0 was taken out of the graph when its own
    // repair jobs completed, so it no longer points to 2 and no 0->2 job is created. Also, expect
    // that swap job for 0 will be executed, so that 2 and 0 are swapped - the single pending job is
    // then 1->0 (originally 1->2).
    EXPECT_EQ(tiered_index->deleteVector(2), 1);
    EXPECT_EQ(tiered_index->indexSize(), 2);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 1);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 1);

    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 1);
    // The repair job should remove 1->0 (originally was 1->2).
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    ASSERT_EQ(reinterpret_cast<HNSWRepairJob *>(mock_thread_pool.jobQ.front().job)->node_id, 1);
    ASSERT_EQ(reinterpret_cast<HNSWRepairJob *>(mock_thread_pool.jobQ.front().job)
                  ->associatedSwapJobs[0]
                  ->deleted_id,
              0);
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 0);
    // Delete 1. The only other element left (the "new" 0, which is the old 2) is deleted and was
    // already taken out of the graph, so nothing points to 1 and no u->1 job is created.
    // Its swap job is therefore ready right away, and the swap and removal of the previous 0 is
    // triggered - so that 1 gets id 0.
    EXPECT_EQ(tiered_index->deleteVector(1), 1);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
    EXPECT_EQ(tiered_index->idToSwapJob.size(), 1);
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->deleted_id, 0);
    EXPECT_EQ(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 0);
    EXPECT_EQ(tiered_index->indexSize(), 1);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 1);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 1);
    EXPECT_EQ(tiered_index->getHNSWIndex()->safeGetEntryPointState().first, INVALID_ID);

    // Call delete again, this should only trigger the swap and removal of 1
    // (which has already deleted)
    EXPECT_EQ(tiered_index->deleteVector(1), 0);
    EXPECT_EQ(tiered_index->indexSize(), 0);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);
}

// Covers the invalidation of a pending repair job whose node is disposed of by a swap job. A repair
// job is denoted below as the edge it removes: u->v is the job that repairs u's connections after
// its neighbour v was deleted.
// A deleted element is taken out of the graph as soon as the jobs of *its own* deletion are done,
// so for a job on it to still be pending at that point, that job has to belong to *another*
// element's deletion: 0 is deleted first, then 1 is deleted while 0 still points to it (registering
// a 0->1 job), and only then the 1->0 and 2->0 jobs complete and 0 is isolated.
TYPED_TEST(HNSWTieredIndexTest, invalidRepairJobOnSwap) {
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Threshold of 1, so that a ready swap job is executed at the first opportunity.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1);

    // Insert 3 vectors directly into HNSW, expect to have a fully connected graph.
    for (size_t i = 0; i < 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, i, i);
    }

    // Delete 0 - a u->0 job is created for every (u, level) pair that points to it, that is 1->0
    // and 2->0. Note that the number of jobs depends on the levels the elements got (and that jobs
    // of the same (u, level) are merged), so the counters are compared to each other rather than to
    // fixed values below.
    EXPECT_EQ(tiered_index->deleteVector(0), 1);
    ASSERT_GT(mock_thread_pool.jobQ.size(), 0);
    ASSERT_GT(tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load(), 0);

    // Delete 1 before those jobs run. 0 is deleted but still connected (its own repairs are
    // pending), so a 0->1 job is created here and stays pending.
    EXPECT_EQ(tiered_index->deleteVector(1), 1);
    ASSERT_TRUE(tiered_index->idToRepairJobs.contains(0));
    ASSERT_EQ(tiered_index->invalidJobs.size(), 0);
    int pending_for_1 = tiered_index->idToSwapJob.at(1)->pending_repair_jobs_counter.load();
    ASSERT_GT(pending_for_1, 0);

    // Execute the 1->0 and 2->0 jobs, so that 0 has no pending repair job left and is taken out of
    // the graph, making its swap job ready. Isolating 0 also removes the 0->1 edge, so the pending
    // 0->1 job has nothing left to repair: it is invalidated right here, and 1's swap job stops
    // waiting for it - without waiting for 0's slot to be reclaimed.
    while (tiered_index->idToSwapJob.at(0)->pending_repair_jobs_counter.load() > 0) {
        ASSERT_GT(mock_thread_pool.jobQ.size(), 0);
        mock_thread_pool.thread_iteration();
    }
    EXPECT_FALSE(tiered_index->idToRepairJobs.contains(0));
    EXPECT_EQ(tiered_index->invalidJobs.size(), 1);
    EXPECT_LT(tiered_index->idToSwapJob.at(1)->pending_repair_jobs_counter.load(), pending_for_1);

    // Drain the remaining jobs: the invalidated 0->1 job is disposed of without being executed, and
    // any job still pending on 1 completes its repairs - so 1 is taken out of the graph as well.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    EXPECT_EQ(tiered_index->invalidJobs.size(), 0);
    EXPECT_EQ(tiered_index->idToSwapJob.at(1)->pending_repair_jobs_counter.load(), 0);

    // Both deleted elements are out of the graph now, so disposing of them leaves a single element
    // in the index, with a valid graph. The GC runs a batch of at most `pendingSwapJobsThreshold`
    // swap jobs per round, which is 1 here, so it takes two rounds.
    tiered_index->runGC();
    tiered_index->runGC();
    EXPECT_EQ(tiered_index->indexSize(), 1);
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 0);
    auto state = tiered_index->getHNSWIndex()->checkIntegrity();
    EXPECT_EQ(state.valid_state, true);
    EXPECT_EQ(state.connections_to_repair, 0);

    // Both GC rounds went through defrag(), which the info reports. Other callers of defrag() may
    // have run too, hence the lower bound on the count.
    EXPECT_GE(tiered_index->defragRuns.load(), 2);
    EXPECT_GT(tiered_index->defragTimeNs.load(), 0);
    auto info = tiered_index->debugInfo().tieredInfo.specificTieredBackendInfo.hnswTieredInfo;
    EXPECT_EQ(info.defragRuns, tiered_index->defragRuns.load());
    EXPECT_EQ(info.defragTimeNs, tiered_index->defragTimeNs.load());
}

// A set of lambdas that determine whether a vector should be inserted to the
// HNSW index (returns true) or to the flat index (returns false).
inline constexpr std::array<std::pair<std::string_view, bool (*)(size_t, size_t)>, 11> lambdas = {{
    {"100% HNSW,   0% FLAT ", [](size_t idx, size_t n) -> bool { return 1; }},
    {" 50% HNSW,  50% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 2; }},
    {"  0% HNSW, 100% FLAT ", [](size_t idx, size_t n) -> bool { return 0; }},
    {" 90% HNSW,  10% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 10; }},
    {" 10% HNSW,  90% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 10); }},
    {" 99% HNSW,   1% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 100; }},
    {"  1% HNSW,  99% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 100); }},
    {"first 10% are in HNSW", [](size_t idx, size_t n) -> bool { return idx < (n / 10); }},
    {"first 10% are in FLAT", [](size_t idx, size_t n) -> bool { return idx >= (n / 10); }},
    {" last 10% are in FLAT", [](size_t idx, size_t n) -> bool { return idx < (9 * n / 10); }},
    {" last 10% are in HNSW", [](size_t idx, size_t n) -> bool { return idx >= (9 * n / 10); }},
}};

TYPED_TEST(HNSWTieredIndexTest, BatchIterator) {
    size_t d = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .efConstruction = ef,
        .efRuntime = ef,
    };
    VecSimParams params = CreateParams(hnsw_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();

        auto *hnsw = tiered_index->backendIndex;
        auto *flat = tiered_index->frontendIndex;

        // For every i, add the vector (i,i,i,i) under the label i.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? hnsw : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
        // in every iteration. The results order should be sorted by their score (distance from
        // the query vector), which means sorted from the largest id to the lowest.
        size_t n_res = 5;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n_labels / n_res) << decider_name;
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTest, BatchIteratorReset) {
    size_t d = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .efConstruction = ef,
        .efRuntime = ef,
    };
    VecSimParams params = CreateParams(hnsw_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();

        auto *hnsw = tiered_index->backendIndex;
        auto *flat = tiered_index->frontendIndex;

        // For every i, add the vector (i,i,i,i) under the label i.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? hnsw : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        ASSERT_NO_FATAL_FAILURE(VecSimBatchIterator_Reset(batchIterator));

        // Get the 100 vectors whose ids are the maximal among those that hasn't been returned yet,
        // in every iteration. Run this flow for 3 times, and reset the iterator.
        size_t n_res = 100;
        size_t re_runs = 3;

        for (size_t take = 0; take < re_runs; take++) {
            size_t iteration_num = 0;
            while (VecSimBatchIterator_HasNext(batchIterator)) {
                std::vector<size_t> expected_ids(n_res);
                for (size_t i = 0; i < n_res; i++) {
                    expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
                }
                auto verify_res = [&](size_t id, double score, size_t index) {
                    ASSERT_EQ(expected_ids[index], id) << decider_name;
                };
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
                iteration_num++;
            }
            ASSERT_EQ(iteration_num, n_labels / n_res) << decider_name;
            VecSimBatchIterator_Reset(batchIterator);
        }

        // Try resetting the iterator before it is depleted.
        n_res = 10;
        for (size_t take = 0; take < re_runs; take++) {
            size_t iteration_num = 0;
            do {
                ASSERT_TRUE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;
                std::vector<size_t> expected_ids(n_res);
                for (size_t i = 0; i < n_res; i++) {
                    expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
                }
                auto verify_res = [&](size_t id, double score, size_t index) {
                    ASSERT_EQ(expected_ids[index], id) << decider_name;
                };
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
            } while (5 > iteration_num++);
            VecSimBatchIterator_Reset(batchIterator);
        }
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTest, BatchIteratorSize1) {
    size_t d = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .efConstruction = ef,
        .efRuntime = ef,
    };
    VecSimParams params = CreateParams(hnsw_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();

        auto *hnsw = tiered_index->backendIndex;
        auto *flat = tiered_index->frontendIndex;

        // For every i, add the vector (i,i,i,i) under the label `n_labels - (i % n_labels)`.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? hnsw : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, n_labels - (i % n_labels), i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

        size_t iteration_num = 0;
        size_t n_res = 1, expected_n_res = 1;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            iteration_num++;
            // Expect to get results in the reverse order of labels - which is the order of the
            // distance from the query vector. Get one result in every iteration.
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(id, iteration_num) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, expected_n_res);
        }

        ASSERT_EQ(iteration_num, n_labels) << decider_name;
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTest, BatchIteratorAdvanced) {
    size_t d = 4;
    size_t M = 8;
    size_t ef = 1000;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .efConstruction = ef,
    };
    VecSimParams params = CreateParams(hnsw_params);
    HNSWRuntimeParams hnswRuntimeParams = {.efRuntime = ef};
    VecSimQueryParams query_params = CreateQueryParams(hnswRuntimeParams);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();

        auto *hnsw = tiered_index->backendIndex;
        auto *flat = tiered_index->frontendIndex;

        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator =
            VecSimBatchIterator_New(tiered_index, query, &query_params);

        // Try to get results even though there are no vectors in the index.
        VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
        VecSimQueryReply_Free(res);
        ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;

        // Insert one label and query again. The internal id will be 0.
        for (size_t j = 0; j < per_label; j++) {
            GenerateAndAddVector<TEST_DATA_T>(decider(n_labels, n) ? hnsw : flat, d, n_labels,
                                              n - j);
        }
        VecSimBatchIterator_Reset(batchIterator);
        res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 1) << decider_name;
        VecSimQueryReply_Free(res);
        ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;
        VecSimBatchIterator_Free(batchIterator);

        // Insert vectors to the index and re-create the batch iterator.
        for (size_t i = 1; i < n_labels; i++) {
            auto cur = decider(i, n) ? hnsw : flat;
            for (size_t j = 1; j <= per_label; j++) {
                GenerateAndAddVector<TEST_DATA_T>(cur, d, i, (i - 1) * per_label + j);
            }
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;
        batchIterator = VecSimBatchIterator_New(tiered_index, query, &query_params);

        // Try to get 0 results.
        res = VecSimBatchIterator_Next(batchIterator, 0, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
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
            for (size_t i = 1; i <= n_res; i++) {
                expected_ids.push_back(n_labels - iteration_num * n_res + i);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            if (iteration_num <= n_labels / n_res) {
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID);
            } else {
                // In the last iteration there are `n_labels % n_res` results left to return.
                size_t n_left = n_labels % n_res;
                // Remove the first `n_res - n_left` ids from the expected ids.
                while (expected_ids.size() > n_left) {
                    expected_ids.erase(expected_ids.begin());
                }
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID, n_left);
            }
        }
        ASSERT_EQ(iteration_num, n_labels / n_res + 1) << decider_name;
        // Try to get more results even though there are no.
        res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
        VecSimQueryReply_Free(res);

        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTest, BatchIteratorWithOverlaps) {
    size_t d = 4;
    size_t M = 8;
    size_t ef = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .efConstruction = ef,
        .efRuntime = ef,
    };
    VecSimParams params = CreateParams(hnsw_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();

        auto *hnsw = tiered_index->backendIndex;
        auto *flat = tiered_index->frontendIndex;

        // For every i, add the vector (i,i,i,i) under the label i.
        size_t flat_count = 0;
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? hnsw : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
            if (cur == flat) {
                flat_count++;
                // Add 10% of the vectors in FLAT to HNSW as well.
                if (flat_count % 10 == 0) {
                    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, i % n_labels, i);
                }
            }
        }
        // The index size should be 100-110% of n.
        ASSERT_LE(VecSimIndex_IndexSize(tiered_index), n * 1.1) << decider_name;
        ASSERT_GE(VecSimIndex_IndexSize(tiered_index), n) << decider_name;
        // The number of unique labels should be n_labels.
        ASSERT_EQ(tiered_index->indexLabelCount(), n_labels) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
        // in every iteration. The results order should be sorted by their score (distance from
        // the query vector), which means sorted from the largest id to the lowest.
        size_t n_res = 5;
        size_t n_expected = n_res;
        size_t excessive_iterations = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            if (iteration_num * n_res == n_labels) {
                // in some cases, the batch iterator may report that it has more results to return,
                // but it's actually not true and the next call to `VecSimBatchIterator_Next` will
                // return 0 results. This is safe because we don't guarantee how many results the
                // batch iterator will return, and a similar scenario can happen when checking
                // `VecSimBatchIterator_HasNext` on an empty index for the first time (before the
                // first call to `VecSimBatchIterator_Next`). we check that this scenario doesn't
                // happen more than once.
                ASSERT_EQ(excessive_iterations, 0) << decider_name;
                excessive_iterations = 1;
                n_expected = 0;
            }
            std::vector<size_t> expected_ids(n_expected);
            for (size_t i = 0; i < n_expected; i++) {
                expected_ids[i] = (n - iteration_num * n_expected - i - 1) % n_labels;
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, n_expected);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num - excessive_iterations, n_labels / n_res)
            << decider_name << "\nHad excessive iterations: " << (excessive_iterations != 0);
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, BatchIteratorWithOverlaps_SpacialMultiCases) {
    size_t d = 4;

    std::shared_ptr<VecSimAllocator> allocator;
    TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *tiered_index;
    VecSimIndex *hnsw, *flat;
    TEST_DATA_T query[d];
    VecSimBatchIterator *iterator;
    VecSimQueryReply *batch;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams hnsw_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = true,
    };
    VecSimParams params = CreateParams(hnsw_params);
    auto mock_thread_pool = tieredIndexMock();

    auto L2 = [&](size_t element) { return element * element * d; };

    // TEST 1:
    // first batch contains duplicates with different scores.
    tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
    allocator = tiered_index->getAllocator();
    hnsw = tiered_index->backendIndex;
    flat = tiered_index->frontendIndex;

    GenerateAndAddVector<TEST_DATA_T>(flat, d, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 2, 2);

    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 1, 3);
    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 0, 4);
    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 3, 5);

    ASSERT_EQ(tiered_index->indexLabelCount(), 4);

    GenerateVector<TEST_DATA_T>(query, d, 0);
    iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

    // batch size is 3 (the size of each index). Internally the tiered batch iterator will have to
    // handle the duplicates with different scores.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 3, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 3);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 0);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(0));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 1);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(1));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 2), 2);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 2), L2(2));
    VecSimQueryReply_Free(batch);

    // we have 1 more label in the index. we expect the tiered batch iterator to return it only and
    // filter out the duplicates.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 1);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 3);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(5));
    ASSERT_FALSE(VecSimBatchIterator_HasNext(iterator));
    VecSimQueryReply_Free(batch);
    // TEST 1 clean up.
    VecSimBatchIterator_Free(iterator);

    // TEST 2:
    // second batch contains duplicates (different scores) from the first batch.
    auto *ctx = new tieredIndexMock::IndexExtCtx(&mock_thread_pool);
    mock_thread_pool.reset_ctx(ctx);
    tiered_index = this->CreateTieredHNSWIndex(params, mock_thread_pool);
    allocator = tiered_index->getAllocator();
    hnsw = tiered_index->backendIndex;
    flat = tiered_index->frontendIndex;

    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 2, 2);
    GenerateAndAddVector<TEST_DATA_T>(hnsw, d, 3, 3);

    GenerateAndAddVector<TEST_DATA_T>(flat, d, 2, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 3, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 0, 2);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 1, 3);

    ASSERT_EQ(tiered_index->indexLabelCount(), 4);

    iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

    // ask for 2 results. The internal batch iterators will return 2 results: hnsw - [0, 1], flat -
    // [2, 3] so there are no duplicates.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 2);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 0);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(0));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 2);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(0));
    VecSimQueryReply_Free(batch);

    // first batch contained 1 result from each index, so there is one leftover from each iterator.
    // Asking for 3 results will return additional 2 results from each iterator and the tiered batch
    // iterator will have to handle the duplicates that each iterator returned (both labels that
    // were returned in the first batch and duplicates in the current batch).
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 3, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 2);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 1);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(1));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 3);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(1));
    ASSERT_FALSE(VecSimBatchIterator_HasNext(iterator));
    VecSimQueryReply_Free(batch);
    // TEST 2 clean up.
    VecSimBatchIterator_Free(iterator);
}

TYPED_TEST(HNSWTieredIndexTest, parallelBatchIteratorSearch) {
    size_t dim = 4;
    size_t ef = 500;
    size_t n = 1000;
    size_t n_res_min = 3;  // minimum number of results to return per batch
    size_t n_res_max = 15; // maximum number of results to return per batch
    bool isMulti = TypeParam::isMulti();

    size_t per_label = isMulti ? 5 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = isMulti,
        .efRuntime = ef,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    std::atomic_int successful_searches(0);
    auto parallel_10_batches = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        const size_t res_per_batch = search_job->k;
        const size_t dim = search_job->dim;
        const auto query = search_job->query;

        size_t iteration = 0;
        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            res_index += iteration * res_per_batch;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };

        // Run 10 batches of search.
        auto tiered_iterator = VecSimBatchIterator_New(search_job->index, query, nullptr);
        do {
            runBatchIteratorSearchTest(tiered_iterator, res_per_batch, verify_res);
        } while (++iteration < 10 && VecSimBatchIterator_HasNext(tiered_iterator));

        VecSimBatchIterator_Free(tiered_iterator);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Fill the job queue with insert and batch-search jobs, while filling the flat index, before
    // initializing the thread pool.
    for (size_t i = 0; i < n; i++) {
        // Insert a vector to the flat index and add a job to insert it to the main index.
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);

        // Add a search job.
        size_t cur_res_per_batch = i % (n_res_max - n_res_min) + n_res_min;
        size_t n_res = cur_res_per_batch * 10;
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        // make sure there are `n_res / 2` vectors in the index in each "side" of the query vector.
        GenerateVector<TEST_DATA_T>(query, dim, (i % (n_labels - n_res)) + (n_res / 2));
        auto search_job = new (allocator)
            tieredIndexMock::SearchJobMock(allocator, parallel_10_batches, tiered_index,
                                           cur_res_per_batch, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), n_labels);
    for (auto &it : tiered_index->labelToInsertJobs) {
        EXPECT_EQ(it.second.size(), per_label);
    }
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), 0);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // All the vectors are already in the tiered index, so we expect to find the expected
    // results from the get-go.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, overwriteVectorAsync) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    for (size_t maxSwapJobs : {(int)n + 1, 1}) {
        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index =
            this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, maxSwapJobs);
        auto allocator = tiered_index->getAllocator();

        // Launch the BG threads loop that takes jobs from the queue and executes them.

        for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
            mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                      std::ref(mock_thread_pool));
        }

        // Insert vectors and overwrite them multiple times while thread run in the background.
        std::srand(10); // create pseudo random generator with any arbitrary seed.
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            tiered_index->addVector(vector, i);
        }
        EXPECT_EQ(tiered_index->indexLabelCount(), n);

        size_t num_overwrites = 1000;
        for (size_t i = 0; i < num_overwrites; i++) {
            size_t label_to_overwrite = std::rand() % n;
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            EXPECT_EQ(tiered_index->addVector(vector, label_to_overwrite), 0);
        }

        mock_thread_pool.thread_pool_join();

        EXPECT_EQ(tiered_index->indexSize() - tiered_index->statisticInfo().numberOfMarkedDeleted,
                  n);
        EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
        EXPECT_EQ(tiered_index->indexLabelCount(), n);
        auto report = tiered_index->getHNSWIndex()->checkIntegrity();
        EXPECT_EQ(report.connections_to_repair, 0);
        EXPECT_EQ(report.valid_state, true);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsAsync) {
    // The same shape as `overwriteVectorAsync`, for the update path: workers ingest, repair and
    // swap while the main thread keeps replacing labels, so every update races the ingestion of the
    // vectors it is replacing. Runs for a single-value and a multi-value index, and for a swap job
    // threshold that keeps tombstones around and one that disposes of them as they come.
    size_t dim = 4;
    size_t n = 500;
    size_t num_updates = 1000;
    for (bool is_multi : {false, true}) {
        for (size_t maxSwapJobs : {n + 1, (size_t)1}) {
            SCOPED_TRACE(is_multi ? "multi" : "single");
            SCOPED_TRACE(maxSwapJobs);
            HNSWParams params = {.type = TypeParam::get_index_type(),
                                 .dim = dim,
                                 .metric = VecSimMetric_L2,
                                 .multi = is_multi};
            VecSimParams hnsw_params = CreateParams(params);
            auto mock_thread_pool = tieredIndexMock();
            auto *tiered_index =
                this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, maxSwapJobs);

            for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
                mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                          std::ref(mock_thread_pool));
            }

            // Each label starts with a vector per label, or two for a multi-value index.
            std::srand(10); // create pseudo random generator with any arbitrary seed.
            const size_t initial_per_label = is_multi ? 2 : 1;
            std::vector<size_t> per_label(n, initial_per_label);
            auto random_vectors = [&](TEST_DATA_T *into, size_t count) {
                for (size_t j = 0; j < count * dim; j++) {
                    into[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
                }
            };
            for (size_t i = 0; i < n; i++) {
                TEST_DATA_T vectors[initial_per_label * dim];
                random_vectors(vectors, initial_per_label);
                for (size_t j = 0; j < initial_per_label; j++) {
                    tiered_index->addVector(vectors + j * dim, i);
                }
            }
            EXPECT_EQ(tiered_index->indexLabelCount(), n);

            // Replace labels while the workers are still catching up with the inserts above.
            for (size_t i = 0; i < num_updates; i++) {
                const labelType label = std::rand() % n;
                const size_t count = is_multi ? 1 + std::rand() % 3 : 1;
                TEST_DATA_T vectors[3 * dim];
                random_vectors(vectors, count);
                EXPECT_EQ(tiered_index->updateVectors(label, vectors, count), VecSimUpdate_OK);
                per_label[label] = count;
            }

            mock_thread_pool.thread_pool_join();

            // Every label is still there, holding exactly the vectors of the update that last
            // touched it - nothing was lost to a race with an ingestion, and nothing was left over
            // from a previous value.
            const size_t expected_live =
                std::accumulate(per_label.begin(), per_label.end(), (size_t)0);
            EXPECT_EQ(tiered_index->indexLabelCount(), n);
            EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
            EXPECT_EQ(tiered_index->indexSize() -
                          tiered_index->statisticInfo().numberOfMarkedDeleted,
                      expected_live);
            auto *hnsw_index = this->CastToHNSW(tiered_index);
            for (size_t i = 0; i < n; i++) {
                EXPECT_EQ(hnsw_index->getElementIds(i).size(), per_label[i]) << "label " << i;
            }
            auto report = hnsw_index->checkIntegrity();
            EXPECT_EQ(report.connections_to_repair, 0);
            EXPECT_EQ(report.valid_state, true);
        }
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, addRelabelAndUpdateTogetherUnderLoad) {
    // The three writes a partial update puts through an index - inserting a new document, moving a
    // label onto a new doc id, and replacing a label's vectors - against one index while the
    // workers ingest, repair and swap. Each has its own test; what this covers is their
    // interaction: a label updated while its insert job is pending and then moved before that job
    // runs, a label moved and only then updated, and fresh inserts throughout so the workers never
    // run dry.
    size_t dim = 4;
    size_t n = 200;
    for (bool is_multi : {false, true}) {
        SCOPED_TRACE(is_multi ? "multi" : "single");
        HNSWParams params = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = is_multi};
        VecSimParams hnsw_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        auto *hnsw_index = this->CastToHNSW(tiered_index);

        // Kept apart so no write can land on a label another one owns.
        const labelType moved_offset = 10 * n;
        const labelType fresh_base = 5 * n;
        // Each role writes into its own value range, so the checks at the end can tell which
        // write a vector came from.
        const size_t per_label = is_multi ? 2 : 1;

        for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
            mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                      std::ref(mock_thread_pool));
        }
        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
        }

        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T replacements[2 * dim];
            for (size_t j = 0; j < per_label; j++) {
                GenerateVector<TEST_DATA_T>(replacements + j * dim, dim,
                                            1000.0f + 3000.0f * j + 10.0f * i);
            }
            const labelType moved = i + moved_offset;

            // Both orders, because they are different races: updating a label whose insert job is
            // still pending and then moving it, against moving it first and updating what the move
            // left behind.
            if (i % 2 == 0) {
                EXPECT_EQ(tiered_index->updateVectors(i, replacements, per_label), VecSimUpdate_OK)
                    << "label " << i;
                EXPECT_EQ(tiered_index->relabelVector(i, moved), VecSimRelabel_OK) << "label " << i;
            } else {
                EXPECT_EQ(tiered_index->relabelVector(i, moved), VecSimRelabel_OK) << "label " << i;
                EXPECT_EQ(tiered_index->updateVectors(moved, replacements, per_label),
                          VecSimUpdate_OK)
                    << "label " << moved;
            }

            TEST_DATA_T fresh[dim];
            GenerateVector<TEST_DATA_T>(fresh, dim, 7000.0f + 10.0f * i);
            EXPECT_EQ(tiered_index->addVector(fresh, fresh_base + i), 1);
        }
        mock_thread_pool.thread_pool_join();

        // The join drains the queue, so everything has been ingested and the flat buffer is empty.
        EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
        EXPECT_EQ(tiered_index->indexLabelCount(), 2 * n);
        EXPECT_EQ(tiered_index->indexSize() - tiered_index->statisticInfo().numberOfMarkedDeleted,
                  n * per_label + n);

        // Every label holds what the writes left it, under the name they left it under.
        for (size_t i = 0; i < n; i++) {
            EXPECT_EQ(hnsw_index->getElementIds(i + moved_offset).size(), per_label)
                << "label " << i + moved_offset;
            EXPECT_TRUE(hnsw_index->getElementIds(i).empty()) << "stale label " << i;
            EXPECT_EQ(hnsw_index->getElementIds(fresh_base + i).size(), 1)
                << "label " << fresh_base + i;
        }

        // And holds the values they wrote, asked of each label directly. Not through a query: a
        // k-nearest search over a graph this churned is approximate and does miss the vector
        // itself, which says nothing about whether the write landed.
        for (size_t i : {(size_t)0, (size_t)1, n / 2, n - 1}) {
            for (size_t j = 0; j < per_label; j++) {
                TEST_DATA_T stored[dim];
                GenerateVector<TEST_DATA_T>(stored, dim, 1000.0f + 3000.0f * j + 10.0f * i);
                EXPECT_EQ(tiered_index->getDistanceFrom_Unsafe(i + moved_offset, stored), 0)
                    << "label " << i + moved_offset << " does not hold what the update gave it";
            }
            TEST_DATA_T fresh[dim];
            GenerateVector<TEST_DATA_T>(fresh, dim, 7000.0f + 10.0f * i);
            EXPECT_EQ(tiered_index->getDistanceFrom_Unsafe(fresh_base + i, fresh), 0)
                << "label " << fresh_base + i << " does not hold what it was added with";
        }

        // Disposing of the tombstones the updates left behind must leave a whole graph.
        tiered_index->runGC();
        EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);
        auto report = hnsw_index->checkIntegrity();
        EXPECT_EQ(report.connections_to_repair, 0);
        EXPECT_EQ(report.valid_state, true);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, queryWhileUpdating) {
    // Searches in flight while labels are being replaced. `parallelInsertSearch` covers reading
    // against insertion; this covers reading against an update, which is where a reader is most
    // exposed: the element it may be walking is marked deleted and a replacement inserted under
    // the same label, and for a multi-value label the removal lands before the insertion.
    //
    // Deliberately not asserting *what* a search returns. A query that overlaps an update may see
    // the label's old vectors, its new ones, or - for a multi-value label, between the two halves
    // - neither, and a nearest-neighbour search over a graph being churned is approximate anyway.
    // What it does assert is that no reply invents anything: every label returned is one this test
    // wrote, which is what a use-after-free on a swapped id or a torn label lookup would break,
    // and that the searches actually ran.
    size_t dim = 4;
    size_t n = 500;
    size_t k = 10;
    for (bool is_multi : {false, true}) {
        SCOPED_TRACE(is_multi ? "multi" : "single");
        HNSWParams params = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = is_multi};
        VecSimParams hnsw_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        auto allocator = tiered_index->getAllocator();
        const size_t per_label = is_multi ? 2 : 1;
        const labelType fresh_base = 10 * n;

        std::atomic_int successful_searches(0);
        std::atomic_int invalid_labels(0);
        auto search_while_writing = [](AsyncJob *job) {
            auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
            auto *invalid = reinterpret_cast<std::atomic_int *>(search_job->all_results);
            const size_t universe = search_job->n;
            auto verify_res = [&](size_t id, double score, size_t res_index) {
                // Every label this index ever held is below `universe`; anything else came from
                // reading memory that had been reused or freed.
                if (id >= universe) {
                    (*invalid)++;
                }
            };
            runTopKSearchTest(job->index, search_job->query, search_job->k, verify_res);
            (*search_job->successful_searches)++;
            delete job;
        };

        for (size_t i = 0; i < n; i++) {
            GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
        }
        mock_thread_pool.init_threads();

        for (size_t i = 0; i < n; i++) {
            // A search over the range the writes are touching, queued so a worker picks it up
            // while the writes below are still going.
            auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
            GenerateVector<TEST_DATA_T>(query, dim, 1000.0f + 10.0f * (i % n));
            auto *search_job = new (allocator)
                tieredIndexMock::SearchJobMock(allocator, search_while_writing, tiered_index, k,
                                               query, fresh_base + n, dim, &successful_searches);
            // `all_results` is unused by this callback's contract, so it carries the counter the
            // verifier reports through.
            search_job->all_results = reinterpret_cast<VecSimQueryReply **>(&invalid_labels);
            tiered_index->submitSingleJob(search_job);

            TEST_DATA_T replacements[2 * dim];
            for (size_t j = 0; j < per_label; j++) {
                GenerateVector<TEST_DATA_T>(replacements + j * dim, dim, 1000.0f + 10.0f * i + j);
            }
            EXPECT_EQ(tiered_index->updateVectors(i, replacements, per_label), VecSimUpdate_OK)
                << "label " << i;

            // An insert alongside, so the readers also overlap a growing index.
            TEST_DATA_T fresh[dim];
            GenerateVector<TEST_DATA_T>(fresh, dim, 7000.0f + 10.0f * i);
            EXPECT_EQ(tiered_index->addVector(fresh, fresh_base + i), 1);
        }
        mock_thread_pool.thread_pool_join();

        EXPECT_EQ(successful_searches, n) << "searches did not all run, so they raced nothing";
        EXPECT_EQ(invalid_labels, 0) << "a search returned a label this index never held";

        // The writes still landed correctly through all of it.
        EXPECT_EQ(tiered_index->indexLabelCount(), 2 * n);
        EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
        EXPECT_EQ(tiered_index->indexSize() - tiered_index->statisticInfo().numberOfMarkedDeleted,
                  n * per_label + n);
        tiered_index->runGC();
        auto report = this->CastToHNSW(tiered_index)->checkIntegrity();
        EXPECT_EQ(report.connections_to_repair, 0);
        EXPECT_EQ(report.valid_state, true);
    }
}

TYPED_TEST(HNSWTieredIndexTest, testInfo) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1, 1000);
    auto allocator = tiered_index->getAllocator();

    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    VecSimIndexStatsInfo stats = tiered_index->statisticInfo();
    EXPECT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_HNSWLIB);
    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, tiered_index->getAllocationSize());
    EXPECT_EQ(info.commonInfo.basicInfo.isMulti, TypeParam::isMulti());
    EXPECT_EQ(info.commonInfo.basicInfo.dim, dim);
    EXPECT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_L2);
    EXPECT_EQ(info.commonInfo.basicInfo.type, TypeParam::get_index_type());
    EXPECT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    VecSimIndexDebugInfo frontendIndexInfo = tiered_index->frontendIndex->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->backendIndex->debugInfo();

    compareCommonInfo(info.tieredInfo.frontendCommonInfo, frontendIndexInfo.commonInfo);
    compareFlatInfo(info.tieredInfo.bfInfo, frontendIndexInfo.bfInfo);
    compareCommonInfo(info.tieredInfo.backendCommonInfo, backendIndexInfo.commonInfo);
    compareHNSWInfo(info.tieredInfo.backendInfo.hnswInfo, backendIndexInfo.hnswInfo);

    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          backendIndexInfo.commonInfo.memory +
                                          frontendIndexInfo.commonInfo.memory);
    EXPECT_EQ(info.commonInfo.memory, stats.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
    EXPECT_EQ(info.tieredInfo.bufferLimit, 1000);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.hnswTieredInfo.pendingSwapJobsThreshold, 1);
    // Verify new tiered-specific stats
    EXPECT_EQ(stats.flatBufferSize, 0);
    EXPECT_EQ(stats.directHNSWInsertions, 0);

    // Validate that Static info returns the right restricted info as well.
    VecSimIndexBasicInfo s_info = VecSimIndex_BasicInfo(tiered_index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, s_info.algo);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, s_info.dim);
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, s_info.blockSize);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isMulti, s_info.isMulti);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isTiered, s_info.isTiered);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    info = tiered_index->debugInfo();
    stats = tiered_index->statisticInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.commonInfo.memory, stats.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);
    // Vector is in flat buffer, no direct insertions yet
    EXPECT_EQ(stats.flatBufferSize, 1);
    EXPECT_EQ(stats.directHNSWInsertions, 0);

    mock_thread_pool.thread_iteration();
    info = tiered_index->debugInfo();
    stats = tiered_index->statisticInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.commonInfo.memory, stats.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
    // Vector moved from flat buffer to HNSW by background thread
    EXPECT_EQ(stats.flatBufferSize, 0);
    EXPECT_EQ(stats.directHNSWInsertions, 0);

    if (TypeParam::isMulti()) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
        info = tiered_index->debugInfo();
        stats = tiered_index->statisticInfo();

        EXPECT_EQ(info.commonInfo.indexSize, 2);
        EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                              info.tieredInfo.backendCommonInfo.memory +
                                              info.tieredInfo.frontendCommonInfo.memory);
        EXPECT_EQ(info.commonInfo.memory, stats.memory);
        EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);
    }

    VecSimIndex_DeleteVector(tiered_index, 1);
    info = tiered_index->debugInfo();
    stats = tiered_index->statisticInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.commonInfo.memory, stats.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
}

TYPED_TEST(HNSWTieredIndexTest, testDirectHNSWInsertionsStats) {
    // Test that directHNSWInsertions counter is incremented when flat buffer is full.
    size_t dim = 4;
    size_t buffer_limit = 5;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Create index with small buffer limit
    auto *tiered_index =
        this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1, buffer_limit);

    // Fill the flat buffer
    for (size_t i = 0; i < buffer_limit; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }

    VecSimIndexStatsInfo stats = tiered_index->statisticInfo();
    EXPECT_EQ(stats.flatBufferSize, buffer_limit);
    EXPECT_EQ(stats.directHNSWInsertions, 0);

    // Add more vectors - these should go directly to HNSW
    size_t extra_vectors = 3;
    for (size_t i = buffer_limit; i < buffer_limit + extra_vectors; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }

    stats = tiered_index->statisticInfo();
    EXPECT_EQ(stats.flatBufferSize, buffer_limit);
    EXPECT_EQ(stats.directHNSWInsertions, extra_vectors);

    // Drain the flat buffer by starting threads and waiting for them to finish
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    stats = tiered_index->statisticInfo();
    EXPECT_EQ(stats.flatBufferSize, 0);
    // Direct insertions counter should be preserved
    EXPECT_EQ(stats.directHNSWInsertions, extra_vectors);

    // Test write-in-place mode: vectors should go directly to HNSW even when buffer is not full
    VecSim_SetWriteMode(VecSim_WriteInPlace);

    size_t write_in_place_vectors = 4;
    size_t label_offset = buffer_limit + extra_vectors;
    for (size_t i = 0; i < write_in_place_vectors; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, label_offset + i, label_offset + i);
    }

    stats = tiered_index->statisticInfo();
    // Flat buffer should still be empty (vectors went directly to HNSW)
    EXPECT_EQ(stats.flatBufferSize, 0);
    // Direct insertions counter should include the write-in-place vectors
    EXPECT_EQ(stats.directHNSWInsertions, extra_vectors + write_in_place_vectors);

    // Verify all vectors are in the backend index
    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize,
              buffer_limit + extra_vectors + write_in_place_vectors);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);

    // Reset to async mode
    VecSim_SetWriteMode(VecSim_WriteAsync);
}

TYPED_TEST(HNSWTieredIndexTest, testInfoIterator) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};

    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 1);
    auto allocator = tiered_index->getAllocator();

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    VecSimIndexDebugInfo frontendIndexInfo = tiered_index->frontendIndex->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->backendIndex->debugInfo();

    // Use the C API wrapper (as RediSearch does) so the process-wide SHARED_MEMORY
    // field is appended at the top level — compareTieredIndexInfoToIterator expects it.
    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(tiered_index);
    compareTieredIndexInfoToIterator(info, frontendIndexInfo, backendIndexInfo, infoIterator);

    VecSimDebugInfoIterator_Free(infoIterator);
}

TYPED_TEST(HNSWTieredIndexTest, debugInfoIteratorFieldOrder) {

    namespace expected_output = test_utils::test_debug_info_iterator_order;

    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams hnsw_params = {.type = TypeParam::get_index_type(),
                              .dim = dim,
                              .metric = VecSimMetric_L2,
                              .multi = TypeParam::isMulti()};
    auto mock_thread_pool = tieredIndexMock();
    auto index = test_utils::CreateNewTieredVecSimIndex(hnsw_params, mock_thread_pool);
    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1);
    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(index);

    // Test the field order using the common function
    expected_output::testDebugInfoIteratorFieldOrder(infoIterator,
                                                     expected_output::getTieredHNSWFields());

    VecSimDebugInfoIterator_Free(infoIterator);
}

TYPED_TEST(HNSWTieredIndexTest, writeInPlaceMode) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;

    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    VecSim_SetWriteMode(VecSim_WriteInPlace);
    // Validate that the vector was inserted directly to the HNSW index.
    labelType vec_label = 0;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, 0);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);

    // Overwrite inplace - only in single-value mode
    if (!TypeParam::isMulti()) {
        TEST_DATA_T overwritten_vec[] = {1, 1, 1, 1};
        tiered_index->addVector(overwritten_vec, vec_label);
        ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
        ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
        ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(vec_label, overwritten_vec), 0);
    }

    // Validate that the vector is removed in place.
    tiered_index->deleteVector(vec_label);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);
}

TYPED_TEST(HNSWTieredIndexTest, switchWriteModes) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 500;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti(),
                         .M = 32,
                         .efConstruction = 3 * n,
                         .efRuntime = 3 * n};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    VecSim_SetWriteMode(VecSim_WriteAsync);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors one by one async.
    size_t per_label = TypeParam::isMulti() ? 5 : 1;
    size_t n_labels = n / per_label;
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i % n_labels);
    }

    // Insert another n more vectors INPLACE, while the previous vectors are still being indexed.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    EXPECT_LE(tiered_index->backendIndex->indexSize(), n);
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i % n_labels + n_labels);
    }
    mock_thread_pool.thread_pool_join();
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), 2 * n);

    // Now delete the last n inserted vectors of the index using async jobs.
    VecSim_SetWriteMode(VecSim_WriteAsync);
    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n_labels; i++) {
        VecSimIndex_DeleteVector(tiered_index, n_labels + i);
    }
    // At this point, repair jobs should be executed in the background.
    EXPECT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), n);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, n);

    // Insert INPLACE another n vector (instead of the ones that were deleted).
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    auto hnsw_index = tiered_index->getHNSWIndex();
    // Run twice, at first run we insert non-existing labels, in the second run we overwrite them
    // (for single-value index only).
    for (auto overwrite : {0, 1}) {
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            labelType cur_label = i % n_labels + n_labels;
            EXPECT_EQ(tiered_index->addVector(vector, cur_label),
                      TypeParam::isMulti() ? 1 : 1 - overwrite);
            // Run a query over hnsw index and see that we only receive ids with label < n_labels+i
            // (the label that we just inserted), and the first result should be this vector
            // (unless it is unreachable)
            auto ver_res = [&](size_t res_label, double score, size_t index) {
                if (index == 0) {
                    if (res_label == cur_label) {
                        EXPECT_DOUBLE_EQ(score, 0);
                    } else {
                        hnsw_index->lockSharedIndexDataGuard();
                        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(cur_label, vector), 0);
                        hnsw_index->unlockSharedIndexDataGuard();
                    }
                }
                if (!overwrite) {
                    ASSERT_LE(res_label, i + n_labels);
                }
            };
            // Run top K search over the tiered index so that internally in the runTopKSearchTest
            // helper function we will identify that it is an async index and expect to get <= k
            // results (rather than == k, as some nodes may be unreachable).
            runTopKSearchTest(tiered_index, vector, 10, ver_res);
        }
    }

    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->backendIndex->indexLabelCount(), 2 * n_labels);
}

TYPED_TEST(HNSWTieredIndexTest, bufferLimit) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti()};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Create tiered index with buffer limit set to 0.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool,
                                                     DEFAULT_PENDING_SWAP_JOBS_THRESHOLD, 0);
    auto allocator = tiered_index->getAllocator();

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);

    // Set the flat limit to 1 and insert another vector - expect it to go to the flat buffer.
    tiered_index->flatBufferLimit = 1;
    labelType vec_label = 1;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, 0); // vector is [0,0,0,0]
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);

    // Overwrite the vector, expect removing it from the flat buffer and replace it with the new one
    // only in single-value mode
    if (!TypeParam::isMulti()) {
        TEST_DATA_T overwritten_vec[] = {1, 1, 1, 1};
        ASSERT_EQ(tiered_index->addVector(overwritten_vec, vec_label), 0);
        ASSERT_EQ(tiered_index->backendIndex->indexSize(), 1);
        ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);
        ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(vec_label, overwritten_vec), 0);
        // The first job in Q should be the invalid overwritten insert vector job.
        ASSERT_EQ(mock_thread_pool.jobQ.front().job->isValid, false);
        ASSERT_EQ(reinterpret_cast<HNSWInsertJob *>(mock_thread_pool.jobQ.front().job)->id, 0);
        mock_thread_pool.jobQ.pop();
    }

    // Insert another vector, this one should go directly to HNSW index since the buffer limit has
    // reached.
    vec_label = 2;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, 0); // vector is [0,0,0,0]
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), 2);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 3);

    // Overwrite the vector, expect marking it as deleted in HNSW and insert the new one directly
    // to HNSW as well.
    if (!TypeParam::isMulti()) {
        TEST_DATA_T overwritten_vec[] = {1, 1, 1, 1};
        ASSERT_EQ(tiered_index->addVector(overwritten_vec, vec_label), 0);
        ASSERT_EQ(tiered_index->backendIndex->indexSize(), 3);
        ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 1);
        EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 1);
        ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 1);
        ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 1);
        ASSERT_EQ(tiered_index->indexLabelCount(), 3);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(vec_label, overwritten_vec), 0);
    }
}

TYPED_TEST(HNSWTieredIndexTest, bufferLimitAsync) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 500;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .multi = TypeParam::isMulti(),
                         .M = 64};

    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Create tiered index with buffer limit set to 100.
    size_t flat_buffer_limit = 100;
    auto *tiered_index = this->CreateTieredHNSWIndex(
        hnsw_params, mock_thread_pool, DEFAULT_PENDING_SWAP_JOBS_THRESHOLD, flat_buffer_limit);
    auto allocator = tiered_index->getAllocator();
    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    // Create and insert vectors one by one async. At some point, buffer limit gets full and vectors
    // are inserted directly to HNSW.
    size_t per_label = TypeParam::isMulti() ? 5 : 1;
    size_t n_labels = n / per_label;
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    // Run twice, at first run we insert non-existing labels, in the second run we overwrite them
    // (for single-value index only).
    for (auto overwrite : {0, 1}) {
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            EXPECT_EQ(tiered_index->addVector(vector, i % n_labels),
                      TypeParam::isMulti() ? 1 : 1 - overwrite);
            EXPECT_LE(tiered_index->frontendIndex->indexSize(), flat_buffer_limit);
        }
        // In first run, wait until all vectors are moved from flat index to HNSW backend index.
        while (tiered_index->backendIndex->indexSize() < n) {
            /*do nothing*/
        }
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    // Multi-value appends on the second run. Single-value overwrites: an overwrite that goes
    // through the flat buffer reuses its old id, while one inserted straight into HNSW (the buffer
    // being full) appends a fresh one and leaves the old id marked deleted - which of the two
    // depends on thread timing. Either way, exactly n elements are live.
    size_t backend_size = tiered_index->backendIndex->indexSize();
    size_t live = TypeParam::isMulti() ? 2 * n : n;
    EXPECT_GE(backend_size, live);
    EXPECT_LE(backend_size, 2 * n);
    EXPECT_EQ(backend_size - tiered_index->getHNSWIndex()->getNumMarkedDeleted(), live);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
}

TYPED_TEST(HNSWTieredIndexTestBasic, RangeSearchCosine) {
    size_t n = 800;
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .initialCapacity = n / 2};

    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);

    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T f[dim];
        f[0] = TEST_DATA_T(i + 1) / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        // Use as label := n - (internal id)
        VecSimIndex_AddVector(tiered_index, f, n - i);
    }

    // Move some of the vectors to the hnsw index
    for (size_t i = 0; i < 10; i++) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(frontend_index->indexSize(), n - 10);
    ASSERT_EQ(hnsw_index->indexSize(), 10);

    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n);
    TEST_DATA_T query[dim];
    TEST_DATA_T normalized_query[dim];
    for (size_t i = 0; i < dim; i++) {
        query[i] = 1.0;
        normalized_query[i] = 1.0;
    }

    VecSim_Normalize(normalized_query, dim, params.type);
    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, result_rank + 1);
        double expected_score = tiered_index->getDistanceFrom_Unsafe(id, normalized_query);
        ASSERT_EQ(score, expected_score);
    };
    uint expected_num_results = 31;
    // Calculate the score of the 31st distant vector from the query vector (whose id should be 30)
    // to get the radius.
    double radius = tiered_index->getDistanceFrom_Unsafe(31, normalized_query);
    runRangeQueryTest(tiered_index, query, radius, verify_res, expected_num_results, BY_SCORE);

    // Return results BY_ID should give the same results.
    runRangeQueryTest(tiered_index, query, radius, verify_res, expected_num_results, BY_ID);

    // Index rest of the vectors and run the same test again.
    for (size_t i = 0; i < n - 10; i++) {
        mock_thread_pool.thread_iteration();
    }
    runRangeQueryTest(tiered_index, query, radius, verify_res, expected_num_results, BY_SCORE);

    // Return results BY_ID should give the same results.
    runRangeQueryTest(tiered_index, query, radius, verify_res, expected_num_results, BY_ID);
}

TYPED_TEST(HNSWTieredIndexTestBasic, TopKCosine) {
    size_t n = 100;
    size_t dim = 128;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_Cosine,
    };

    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(tiered_index, f, i);
    }
    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n);

    // Move some of the vectors to the hnsw index
    for (size_t i = 0; i < 10; i++) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(frontend_index->indexSize(), n - 10);
    ASSERT_EQ(hnsw_index->indexSize(), 10);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, (n - result_rank));
        TEST_DATA_T expected_score = tiered_index->getDistanceFrom_Unsafe(id, normalized_query);
        ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
    };
    runTopKSearchTest(tiered_index, query, 10, verify_res);

    // Index rest of the vectors and run the same test again.
    for (size_t i = 0; i < n - 10; i++) {
        mock_thread_pool.thread_iteration();
    }
    runTopKSearchTest(tiered_index, query, 10, verify_res);
}

TYPED_TEST(HNSWTieredIndexTestBasic, BatchIteratorCosine) {
    size_t n = 100;
    size_t dim = 128;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_Cosine,
    };

    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(tiered_index, f, i);
    }

    // Move some of the vectors to the hnsw index
    for (size_t i = 0; i < 10; i++) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n);
    ASSERT_EQ(frontend_index->indexSize(), n - 10);
    ASSERT_EQ(hnsw_index->indexSize(), 10);
    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    // Test with batch iterator.
    VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
    size_t iteration_num = 0;

    // get the 10 vectors whose ids are the maximal among those that hasn't been returned yet,
    // in every iteration. The order should be from the largest to the lowest id.
    size_t n_res = 10;
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        std::vector<size_t> expected_ids(n_res);
        auto verify_res_batch = [&](size_t id, double score, size_t result_rank) {
            ASSERT_EQ(id, (n - n_res * iteration_num - result_rank));
            TEST_DATA_T expected_score = tiered_index->getDistanceFrom_Unsafe(id, normalized_query);
            ASSERT_TYPE_EQ(TEST_DATA_T(score), expected_score);
        };
        runBatchIteratorSearchTest(batchIterator, n_res, verify_res_batch);
        iteration_num++;
    }
    ASSERT_EQ(iteration_num, n / n_res);
    VecSimBatchIterator_Free(batchIterator);
}

TYPED_TEST(HNSWTieredIndexTestBasic, BatchIteratorCosineMidIngestDuplicate) {
    constexpr size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_Cosine,
        .multi = false,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);

    TEST_DATA_T query[dim] = {1, 0, 0, 0};
    TEST_DATA_T flat_only[dim] = {1, 0, 0, 0};
    TEST_DATA_T shared[dim] = {1, 1, 0, 0};
    TEST_DATA_T hnsw_only[dim] = {0, 1, 0, 0};

    VecSimIndex_AddVector(tiered_index, flat_only, 0);
    VecSimIndex_AddVector(tiered_index, shared, 1);

    // An insertion job passes the frontend's normalized blob to HNSW before erasing it from the
    // flat buffer. Reproduce that overlap without completing the queued job.
    std::vector<std::vector<TEST_DATA_T>> normalized_shared;
    frontend_index->getDataByLabel(1, normalized_shared);
    ASSERT_EQ(normalized_shared.size(), 1);
    VecSimIndex_AddVector(hnsw_index, normalized_shared[0].data(), 1);
    VecSimIndex_AddVector(hnsw_index, hnsw_only, 2);

    // The HNSW backend is configured to accept already-normalized data. With the same normalized
    // query, the shared copy must therefore have exactly the same score in both tiers.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, sizeof(query));
    VecSim_Normalize(normalized_query, dim, params.type);
    auto flat_res = VecSimIndex_TopKQuery(frontend_index, query, 2, nullptr, BY_SCORE);
    auto hnsw_res = VecSimIndex_TopKQuery(hnsw_index, normalized_query, 2, nullptr, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(flat_res), 2);
    ASSERT_EQ(VecSimQueryReply_Len(hnsw_res), 2);
    ASSERT_EQ(VecSimQueryResult_GetId(flat_res->results.data() + 1), 1);
    ASSERT_EQ(VecSimQueryResult_GetId(hnsw_res->results.data()), 1);
    ASSERT_EQ(VecSimQueryResult_GetScore(flat_res->results.data() + 1),
              VecSimQueryResult_GetScore(hnsw_res->results.data()));
    VecSimQueryReply_Free(flat_res);
    VecSimQueryReply_Free(hnsw_res);

    // A one-result batch first consumes the better flat-only result, leaving the shared HNSW
    // result buffered. The next batch must consume both copies together and emit the label once.
    VecSimBatchIterator *batch_iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
    for (labelType expected_label = 0; expected_label < 3; ++expected_label) {
        ASSERT_TRUE(VecSimBatchIterator_HasNext(batch_iterator));
        runBatchIteratorSearchTest(batch_iterator, 1,
                                   [expected_label](size_t id, double score, size_t result_rank) {
                                       ASSERT_EQ(id, expected_label);
                                   });
    }
    ASSERT_FALSE(VecSimBatchIterator_HasNext(batch_iterator));
    VecSimBatchIterator_Free(batch_iterator);
}

TYPED_TEST(HNSWTieredIndexTest, RangeSearch) {
    size_t dim = 4;
    size_t k = 11;
    size_t per_label = TypeParam::isMulti() ? 5 : 1;

    size_t n_labels = k * 3;
    size_t n = n_labels * per_label;

    auto edge_delta = (k - 0.8) * per_label;
    auto mid_delta = edge_delta / 2;
    // `range` for querying the "edges" of the index and get k results.
    double range = dim * edge_delta * edge_delta; // L2 distance.
    // `half_range` for querying a point in the "middle" of the index and get k results around it.
    double half_range = dim * mid_delta * mid_delta; // L2 distance.

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .epsilon = 3.0 * per_label,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    size_t cur_memory_usage;

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    ASSERT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto hnsw_index = tiered_index->backendIndex;
    auto flat_index = tiered_index->frontendIndex;

    TEST_DATA_T query_0[dim];
    GenerateVector<TEST_DATA_T>(query_0, dim, 0);
    TEST_DATA_T query_1mid[dim];
    GenerateVector<TEST_DATA_T>(query_1mid, dim, n / 3);
    TEST_DATA_T query_2mid[dim];
    GenerateVector<TEST_DATA_T>(query_2mid, dim, n * 2 / 3);
    TEST_DATA_T query_n[dim];
    GenerateVector<TEST_DATA_T>(query_n, dim, n - 1);

    // Search for vectors when the index is empty.
    runRangeQueryTest(tiered_index, query_0, range, nullptr, 0);

    // Define the verification functions.
    auto ver_res_0 = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, index);
        // The expected score is the distance to the first vector of `id` label.
        auto element = id * per_label;
        EXPECT_DOUBLE_EQ(score, dim * element * element);
    };

    auto ver_res_1mid_by_id = [&](size_t id, double score, size_t index) {
        size_t q_id = query_1mid[0] / per_label;
        size_t mod = query_1mid[0] - q_id * per_label;
        // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
        // branchings is simply `expected_score = abs(id - q_id)`.
        // In multi value mode, for ids higher than the query id, the score is the distance to the
        // first vector of `id` label, and for ids lower than the query id, the score is the
        // distance to the last vector of `id` label. `mod` is the distance to the first vector of
        // `q_id` label.
        double expected_score = 0;
        if (id > q_id) {
            expected_score = (id - q_id) * per_label - mod;
        } else if (id < q_id) {
            expected_score = (q_id - id) * per_label - (per_label - mod - 1);
        }
        expected_score = expected_score * expected_score * dim;
        EXPECT_DOUBLE_EQ(score, expected_score);
    };

    auto ver_res_2mid_by_id = [&](size_t id, double score, size_t index) {
        size_t q_id = query_2mid[0] / per_label;
        size_t mod = query_2mid[0] - q_id * per_label;
        // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
        // branchings is simply `expected_score = abs(id - q_id)`.
        // In multi value mode, for ids higher than the query id, the score is the distance to the
        // first vector of `id` label, and for ids lower than the query id, the score is the
        // distance to the last vector of `id` label. `mod` is the distance to the first vector of
        // `q_id` label.
        double expected_score = 0;
        if (id > q_id) {
            expected_score = (id - q_id) * per_label - mod;
        } else if (id < q_id) {
            expected_score = (q_id - id) * per_label - (per_label - mod - 1);
        }
        expected_score = expected_score * expected_score * dim;
        EXPECT_DOUBLE_EQ(score, expected_score);
    };

    auto ver_res_1mid_by_score = [&](size_t id, double score, size_t index) {
        size_t q_id = query_1mid[0] / per_label;
        EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
        ver_res_1mid_by_id(id, score, index);
    };

    auto ver_res_2mid_by_score = [&](size_t id, double score, size_t index) {
        size_t q_id = query_2mid[0] / per_label;
        EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
        ver_res_2mid_by_id(id, score, index);
    };

    auto ver_res_n = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, n_labels - 1 - index);
        auto element = index * per_label;
        EXPECT_DOUBLE_EQ(score, dim * element * element);
    };

    // Insert n/2 vectors to the main index.
    for (size_t i = 0; i < (n + 1) / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, i / per_label, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), (n + 1) / 2);
    ASSERT_EQ(tiered_index->indexSize(), hnsw_index->indexSize());

    // Search for `range` with the flat index empty.
    cur_memory_usage = allocator->getAllocationSize();
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Insert n/2 vectors to the flat index.
    for (size_t i = (n + 1) / 2; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i / per_label, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexSize(), hnsw_index->indexSize() + flat_index->indexSize());

    cur_memory_usage = allocator->getAllocationSize();
    // Search for `range` so all the vectors will be from the HNSW index.
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    // Search for `range` so all the vectors will be from the flat index.
    runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
    // Search for `range` so some of the results will be from the main and some from the flat index.
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Add some overlapping vectors to the main and flat index.
    // adding directly to the underlying indexes to avoid jobs logic.
    // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
    for (size_t i = n / 3; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i / per_label, i);
    }
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, i / per_label, i);
    }

    cur_memory_usage = allocator->getAllocationSize();
    // Search for `range` so all the vectors will be from the main index.
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    // Search for `range` so all the vectors will be from the flat index.
    runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
    // Search for `range` so some of the results will be from the main and some from the flat index.
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // // // // // // // // // // // //
    // Check behavior upon timeout.  //
    // // // // // // // // // // // //

    VecSimQueryReply *res;
    // Add a vector to the HNSW index so there will be a reason to query it.
    GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, n, n);

    // Set timeout callback to always return 1 (will fail while querying the flat buffer).
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, nullptr, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);

    // Set timeout callback to return 1 after n checks (will fail while querying the HNSW index).
    // Brute-force index checks for timeout after each vector.
    size_t checks_in_flat = flat_index->indexSize();
    VecSimQueryParams qparams = {.timeoutCtx = &checks_in_flat};
    VecSim_SetTimeoutCallbackFunction([](void *ctx) {
        auto count = static_cast<size_t *>(ctx);
        if (*count == 0) {
            return 1;
        }
        (*count)--;
        return 0;
    });
    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(flat_index, query_0, range, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Check again with BY_ID.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, &qparams, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(flat_index, query_0, range, &qparams, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Clean up.
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; });
}

TYPED_TEST(HNSWTieredIndexTest, parallelRangeSearch) {
    size_t dim = 4;
    size_t k = 11;
    size_t n = 1000;
    bool isMulti = TypeParam::isMulti();

    size_t per_label = isMulti ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredHNSW index instance with a mock queue.
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = isMulti,
        .epsilon = double(dim * k * k),
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    std::atomic_int successful_searches(0);
    auto parallel_range_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        size_t dim = search_job->dim;
        // The range that will get us k results.
        double range = dim * ((k - 0.5) / 2) * ((k - 0.5) / 2); // L2 distance.
        auto query = search_job->query;

        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };
        runRangeQueryTest(job->index, query, range, verify_res, k, BY_SCORE);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Fill the job queue with insert and search jobs, while filling the flat index, before
    // initializing the thread pool.
    for (size_t i = 0; i < n; i++) {
        // Insert a vector to the flat index and add a job to insert it to the main index.
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);

        // Add a search job. Make sure the query element is between k and n_labels - k.
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, ((n - i) % (n_labels - (2 * k))) + k);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_range_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), n_labels);
    for (auto &it : tiered_index->labelToInsertJobs) {
        EXPECT_EQ(it.second.size(), per_label);
    }
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), 0);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // All the vectors are already in the tiered index, so we expect to find the expected
    // results from the get-go.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);
    EXPECT_EQ(tiered_index->backendIndex->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    EXPECT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, preferAdHocOptimization) {
    size_t dim = 4;

    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Create tiered index with buffer limit set to 0.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    auto hnsw = tiered_index->backendIndex;
    auto flat = tiered_index->frontendIndex;

    // Insert 5 vectors to the main index.
    for (size_t i = 0; i < 5; i++) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw, dim, i, i);
    }
    // Sanity check. Should choose as HNSW.
    ASSERT_EQ(tiered_index->preferAdHocSearch(5, 5, true), hnsw->preferAdHocSearch(5, 5, true));

    // Insert 6 vectors to the flat index.
    for (size_t i = 0; i < 6; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat, dim, i, i);
    }
    // Sanity check. Should choose as flat as it has more vectors.
    ASSERT_EQ(tiered_index->preferAdHocSearch(5, 5, true), flat->preferAdHocSearch(5, 5, true));

    // Check for preference of tiered with subset (10) smaller than the tiered index size (11),
    // but larger than any of the underlying indexes.
    ASSERT_NO_THROW(tiered_index->preferAdHocSearch(10, 5, false));
}

TYPED_TEST(HNSWTieredIndexTestBasic, runGCAPI) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Test initialization of the pendingSwapJobsThreshold value.
    ASSERT_EQ(tiered_index->pendingSwapJobsThreshold, DEFAULT_PENDING_SWAP_JOBS_THRESHOLD);

    // Insert three block of vectors directly to HNSW.
    size_t n = DEFAULT_PENDING_SWAP_JOBS_THRESHOLD * 3;
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index->backendIndex, vector, i);
    }

    // Delete all the vectors and wait for the thread pool to finish running the repair jobs.
    for (size_t i = 0; i < n; i++) {
        tiered_index->deleteVector(i);
    }
    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), n);
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), n);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, n);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Run the GC API call, expect that we will clean the defined threshold number of vectors
    // each time we call the GC.
    while (tiered_index->indexSize() > 0) {
        size_t cur_size = tiered_index->indexSize();
        VecSimTieredIndex_GC(tiered_index);
        ASSERT_EQ(tiered_index->indexSize(), cur_size - DEFAULT_PENDING_SWAP_JOBS_THRESHOLD);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, getElementNeighbors) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    size_t n = 0;
    size_t M = 20;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .M = M};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Add vectors directly to HNSW index until we have at least 2 vectors at level 1.
    size_t vectors_in_higher_levels = 0;
    while (vectors_in_higher_levels < 2) {
        GenerateAndAddVector<TEST_DATA_T>(hnsw_index, dim, n, n);
        if (hnsw_index->getGraphDataByInternalId(n)->toplevel > 0) {
            vectors_in_higher_levels++;
        }
        n++;
    }
    // Go over all vectors and validate that the getElementNeighbors debug command returns the
    // neighbors properly.
    for (size_t id = 0; id < n; id++) {
        ElementLevelData &cur = hnsw_index->getElementLevelData(id, 0);
        int **neighbors_output;
        VecSimDebug_GetElementNeighborsInHNSWGraph(tiered_index, id, &neighbors_output);
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

TYPED_TEST(HNSWTieredIndexTestBasic, FitMemoryTest) {
    size_t dim = 4;
    HNSWParams params = {.dim = dim, .blockSize = DEFAULT_BLOCK_SIZE};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    // Add vector
    GenerateAndAddVector<TEST_DATA_T>(index->frontendIndex, dim, 0);
    GenerateAndAddVector<TEST_DATA_T>(index->backendIndex, dim, 0);
    size_t initial_memory = index->getAllocationSize();
    index->fitMemory();
    // Tired backendIndex index doesn't have initial capacity, so adding the first vector triggers
    // allocation.
    ASSERT_EQ(index->getAllocationSize(), initial_memory) << "fitMemory() after adding 1 vector";
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteBothAsyncAndInplace) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;

    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Insert one vector to HNSW.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0);
    // Add another vector and remove it. Expect that at HNSW index one repair job will be created.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(1)), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);

    // The first job should be a repair job of the first inserted node id (0) in level 0.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    ASSERT_TRUE(mock_thread_pool.jobQ.front().job->isValid);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 0);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->level, 0);
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);
    ASSERT_GE(tiered_index->idToRepairJobs.at(0).size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs[0]->deleted_id, 1);

    // Add one more vector and remove it, expect that the same repair job for 0 would be created
    // for repairing 0->2.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 2);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(2)), 1);
    ASSERT_TRUE(tiered_index->idToSwapJob.contains(2));
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs.size(), 2);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs[1]->deleted_id, 2);
    ASSERT_EQ(tiered_index->readySwapJobs, 0);

    tiered_index->setWriteMode(VecSim_WriteInPlace);
    // Delete inplace, expect that the repair job for 0->1 and 0->2 will not be valid anymore.
    ASSERT_EQ(tiered_index->deleteVector(0), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id, 0);
    ASSERT_FALSE(mock_thread_pool.jobQ.front().job->isValid);

    // Also expect that the swap job for 2 will not exist anymore, as 2 swapped with 0
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 2);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 2);
    ASSERT_EQ(tiered_index->idToSwapJob.size(), 2);
    ASSERT_TRUE(tiered_index->idToSwapJob.contains(0));
    ASSERT_FALSE(tiered_index->idToSwapJob.contains(2));
    // Both ids 1 and 0 (previously was 2) are now ready due to the deletion of 0 and its associated
    // jobs.
    ASSERT_EQ(tiered_index->readySwapJobs, 2);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteBothAsyncAndInplaceMulti) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Insert one vector to HNSW.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0);
    // Add another vector and remove it. Expect that at HNSW index one repair job will be created.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(1)), 1);

    // Add one more vector (id=2) under label 0.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 2);
    // Add two more vectors, and delete the first one.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 3, 3);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 4, 4);

    ASSERT_EQ(tiered_index->indexSize(), 5);

    // Remove vector with label=3, expect that the same repair job for 0
    // would be created for repairing 0->3, and a new repair jobs for 2 and 4 to repair 2->3 and
    // 4->3.
    ASSERT_EQ((tiered_index->deleteLabelFromHNSW(3)), 1);
    ASSERT_TRUE(tiered_index->idToSwapJob.contains(3));
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 3);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs.size(), 2);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(0)[0]->associatedSwapJobs[1]->deleted_id, 3);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs[0]->deleted_id, 3);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(4)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(4)[0]->associatedSwapJobs[0]->deleted_id, 3);

    tiered_index->setWriteMode(VecSim_WriteInPlace);
    // Delete inplace, expect that the repair job for 0->1 and 0->2 will not be valid anymore.
    ASSERT_EQ(tiered_index->deleteVector(0), 2);
    ASSERT_EQ(tiered_index->indexSize(), 3);
    ASSERT_FALSE(mock_thread_pool.jobQ.front().job->isValid);
    mock_thread_pool.thread_iteration();
    // The next job in the queue shoule be the repair job for 2->3 and invalidate as well.
    ASSERT_FALSE(mock_thread_pool.jobQ.front().job->isValid);
    mock_thread_pool.thread_iteration();
    // The next job in the queue shoule be the repair job for 4->3, we validate that the id of 4
    // swapped properly to a valid id (lower than index size).
    ASSERT_LT(((HNSWRepairJob *)(mock_thread_pool.jobQ.front().job))->node_id,
              tiered_index->indexSize());
    ASSERT_TRUE(mock_thread_pool.jobQ.front().job->isValid);

    // Also expect that the swap job for 3 will not exist anymore, as 2 swapped with 2.
    ASSERT_EQ(tiered_index->getHNSWIndex()->getNumMarkedDeleted(), 2);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 2);
    ASSERT_EQ(tiered_index->idToSwapJob.size(), 2);
    ASSERT_FALSE(tiered_index->idToSwapJob.contains(3));
    // Id 1 is now ready due to the deletion of 0 and its associated jobs.
    ASSERT_EQ(tiered_index->readySwapJobs, 1);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteInplaceMultiSwapId) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Insert three vector to HNSW - first and last under the same label.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0);
    tiered_index->setWriteMode(VecSim_WriteInPlace);
    // Delete in-place and validate that the second id of label 0 swapped properly with the last id
    // before the deletion, and that eventually entry point was set correctly,
    ASSERT_EQ(tiered_index->deleteVector(0), 2);
    ASSERT_EQ(tiered_index->getHNSWIndex()->safeGetEntryPointState().first, 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, deleteInplaceAvoidUpdatedMarkedDeleted) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    // Insert three vector to HNSW, expect a full graph to be created
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 2, 2);

    // Delete vector with id=0 asynchronously, expect to have a repair job for the other vectors.
    ASSERT_EQ(tiered_index->deleteVector(0), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 2);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(1)[0]->associatedSwapJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs.size(), 1);

    // Execute the repair job for 1->0. Now, 0->1 is unidirectional edge
    ASSERT_TRUE(mock_thread_pool.jobQ.front().job->isValid);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->idToRepairJobs.size(), 1);
    ASSERT_EQ(tiered_index->idToRepairJobs.at(2)[0]->associatedSwapJobs.size(), 1);

    // Insert another vector with id=3, that should be connected to both 1 and 2.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index->backendIndex, dim, 3, -1);

    // Delete in-place id=2, expect that upon repairing inplace 1 due to 1->2, there will *not* be
    // a new edge 1->0 since 0 is deleted. Also the other repair job 2->0 should be invalidated.
    // Also, expect that repairing 3 in-place will not create a new edge to marked deleted 0 and
    // vice versa.
    tiered_index->setWriteMode(VecSim_WriteInPlace);
    ASSERT_EQ(tiered_index->deleteVector(2), 1);
    ASSERT_FALSE(mock_thread_pool.jobQ.front().job->isValid);
    int **neighbours;
    ASSERT_EQ(tiered_index->getHNSWIndex()->getHNSWElementNeighbors(1, &neighbours),
              VecSimDebugCommandCode_OK);
    // Expect 1 neighbors at level 0 (id=3) and that 0 is NOT a new neighbor for 1.
    ASSERT_EQ(neighbours[0][0], 1);
    ASSERT_EQ(neighbours[0][1], 3);
    VecSimDebug_ReleaseElementNeighborsInHNSWGraph(neighbours);

    ASSERT_EQ(tiered_index->getHNSWIndex()->getHNSWElementNeighbors(3, &neighbours),
              VecSimDebugCommandCode_OK);
    ASSERT_EQ(neighbours[0][0], 1);
    // Expect 1 neighbors at level 0 (id=1) and that 0 is NOT a new neighbor for 3.
    ASSERT_EQ(neighbours[0][1], 1);
    VecSimDebug_ReleaseElementNeighborsInHNSWGraph(neighbours);

    auto &level_data = tiered_index->getHNSWIndex()->getElementLevelData((idType)0, 0);
    // Expect 1 neighbors at level 0 (id=1) and that 3 is NOT a new neighbor for 0.
    ASSERT_EQ(level_data.getNumLinks(), 1);
    ASSERT_EQ(level_data.getLinkAtPos(0), 1);

    // Expect that id=0 is a ready swap job and execute it.
    ASSERT_EQ(tiered_index->readySwapJobs, 1);
    ASSERT_TRUE(tiered_index->idToSwapJob.contains(0));
    tiered_index->runGC();
}

TYPED_TEST(HNSWTieredIndexTestBasic, switchDeleteModes) {
    // Create TieredHNSW index instance with a mock queue.
    size_t dim = 16;
    size_t n = 1000;
    size_t swap_job_threshold = 10;
    HNSWParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 100,
    };
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index =
        this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, swap_job_threshold);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors one by one inplace.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i);
    }
    EXPECT_EQ(tiered_index->backendIndex->indexSize(), n);

    // Update vectors while changing the write mode.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        EXPECT_EQ(tiered_index->addVector(vector, i), 0);
        if (i % 10 == 0) {
            // Change mode every 10 vectors.
            auto next_mode = tiered_index->getWriteMode() == VecSim_WriteInPlace
                                 ? VecSim_WriteAsync
                                 : VecSim_WriteInPlace;
            VecSim_SetWriteMode(next_mode);
        }
    }

    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->backendIndex->indexLabelCount(), n);
    auto state = tiered_index->getHNSWIndex()->checkIntegrity();
    ASSERT_EQ(state.valid_state, 1);
    ASSERT_EQ(state.connections_to_repair, 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, CosineTest) {
    // Create TieredHNSW index with cosine metric
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .multi = false};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    // Both indices' metric should be Cosine
    auto frontend_index = this->GetFlatIndex(tiered_index);
    ASSERT_EQ(frontend_index->getMetric(), VecSimMetric_Cosine);

    auto hnsw_index = this->CastToHNSW(tiered_index);
    ASSERT_EQ(hnsw_index->getMetric(), VecSimMetric_Cosine);

    // However, We assume that all the blobs are normalized before sent to the HNSW index.
    // and the hnsw index should not normalize the vectors.
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 1);

    // Add a vector directly to the hnsw index.
    VecSimIndex_AddVector(hnsw_index, vector, 0);
    // Verify that the vector was not normalized.
    ASSERT_NO_FATAL_FAILURE(CompareVectors(
        reinterpret_cast<const TEST_DATA_T *>(hnsw_index->getDataByInternalId(0)), vector, dim));

    // Add the same vector to the flat index.
    VecSimIndex_AddVector(frontend_index, vector, 0);
    // Verify that the stored vector is normalized.
    VecSim_Normalize(vector, dim, TypeParam::get_index_type());
    ASSERT_NO_FATAL_FAILURE(CompareVectors(
        reinterpret_cast<const TEST_DATA_T *>(frontend_index->getDataByInternalId(0)), vector,
        dim));
}

TYPED_TEST(HNSWTieredIndexTestBasic, AddVectorCosine) {
    // Create TieredHNSW index with cosine metric
    size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .multi = false};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();
    // Create tiered index with buffer limit set to 1.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 0, 1);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);

    TEST_DATA_T value = 1;
    TEST_DATA_T normalized_vec[dim];
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, value);
    GenerateVector<TEST_DATA_T>(normalized_vec, dim, value);
    VecSim_Normalize(normalized_vec, dim, TypeParam::get_index_type());

    // Add a vector to the flat buffer
    VecSimIndex_AddVector(tiered_index, vector, 0);
    EXPECT_EQ(frontend_index->indexSize(), 1);
    EXPECT_EQ(hnsw_index->indexSize(), 0);

    // Verify the vector is normalized in the flat index.
    {
        SCOPED_TRACE("Store in the flat buffer");
        // Verify that the vector stored in the flat buffer is normalized.
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        frontend_index->getDataByLabel(0, stored_vec);
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), normalized_vec, dim));
    }

    // Add another vector and exceed the flat buffer capacity.
    VecSimIndex_AddVector(tiered_index, vector, 1);
    // First vector was not yet moved to the hnsw index, so the new vector should be inserted
    // directly to the hnsw, due to lack of space in the flat index.
    EXPECT_EQ(frontend_index->indexSize(), 1);
    EXPECT_EQ(hnsw_index->indexSize(), 1);
    {
        SCOPED_TRACE("Full buffer; add vector directly to hnsw");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        hnsw_index->getDataByLabel(1, stored_vec);
        // Verify that the vector stored in the hnsw is normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), normalized_vec, dim));
    }

    // execute the first vector insertion job
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(frontend_index->indexSize(), 0);
    EXPECT_EQ(hnsw_index->indexSize(), 2);
    {
        SCOPED_TRACE("Execute insertion job");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        hnsw_index->getDataByLabel(0, stored_vec);
        // Verify that the vector stored in the hnsw is normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), normalized_vec, dim));
    }

    // Change the mode to WriteInPlace
    VecSim_SetWriteMode(VecSim_WriteInPlace);

    VecSimIndex_AddVector(tiered_index, vector, 2);
    // Reset mode.
    VecSim_SetWriteMode(VecSim_WriteAsync);
    EXPECT_EQ(frontend_index->indexSize(), 0);
    EXPECT_EQ(hnsw_index->indexSize(), 3);
    {
        SCOPED_TRACE("Add vector in WriteInPlace mode");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        hnsw_index->getDataByLabel(2, stored_vec);
        // Verify that the vector stored in the hnsw is normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), normalized_vec, dim));
    }
}

template <typename DataType>
class PreprocessorDoubleValue : public PreprocessorInterface {
private:
    size_t dim;

public:
    PreprocessorDoubleValue(std::shared_ptr<VecSimAllocator> allocator, size_t dim)
        : PreprocessorInterface(allocator), dim(dim) {}

    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        // This assert makes sure the current use of the preprocessor is valid,
        // i.e., both blobs are of the same size.
        // In order to use different sizes, the preprocessor should be modified.
        assert(storage_blob_size == query_blob_size);

        // One blob was already allocated by a previous preprocessor(s) that process both blobs the
        // same. The blobs are pointing to the same memory, we need to allocate another memory slot
        // to split them.
        if ((storage_blob == query_blob) && (query_blob != nullptr)) {
            storage_blob = this->allocator->allocate(storage_blob_size);
            memcpy(storage_blob, query_blob, storage_blob_size);
        }

        // Either both are nullptr or they are pointing to different memory slots. Both cases are
        // handled by the designated functions.
        this->preprocessForStorage(original_blob, storage_blob, storage_blob_size,
                                   storage_alignment);
        this->preprocessQuery(original_blob, query_blob, query_blob_size, query_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate(input_blob_size);
            memcpy(blob, original_blob, input_blob_size);
        }
        for (size_t i = 0; i < dim; i++) {
            static_cast<DataType *>(blob)[i] *= 2;
        }
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {
        for (size_t i = 0; i < dim; i++) {
            static_cast<DataType *>(blob)[i] *= 2;
        }
    }

    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate_aligned(input_blob_size, alignment);
            memcpy(blob, original_blob, input_blob_size);
        }
        for (size_t i = 0; i < dim; i++) {
            static_cast<DataType *>(blob)[i] *= 2;
        }
    }
};

TYPED_TEST(HNSWTieredIndexTestBasic, HNSWWithPreprocessor) {
    // Create TieredHNSW index with cosine metric
    constexpr size_t dim = 4;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_Cosine,
                         .multi = false};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();
    // Create tiered index with buffer limit set to 1.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 0, 1);
    auto frontend_index = this->GetFlatIndex(tiered_index);
    auto hnsw_index = this->CastToHNSW(tiered_index);
    auto allocator = tiered_index->getAllocator();

    TEST_DATA_T value = 50;
    TEST_DATA_T normalized_vec[dim];
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, value);
    GenerateVector<TEST_DATA_T>(normalized_vec, dim, value);
    VecSim_Normalize(normalized_vec, dim, TypeParam::get_index_type());
    TEST_DATA_T query[dim] = {0.1, 0.2, 0.3, 0.4};
    TEST_DATA_T normalized_query[dim] = {0.1, 0.2, 0.3, 0.4};
    VecSim_Normalize(normalized_query, dim, TypeParam::get_index_type());

    // the backend index (HNSW) was created with a basic PP container, so we need to replace it with
    // a preprocessor container that is able to hold a preprocessor array.
    constexpr size_t n_preprocessors = 1;
    auto multiPPContainer = new (allocator)
        MultiPreprocessorsContainer<TEST_DATA_T, 1>(allocator, hnsw_index->getStorageAlignment());
    auto pp_double_value = new (allocator) PreprocessorDoubleValue<TEST_DATA_T>(allocator, dim);
    ASSERT_EQ(multiPPContainer->addPreprocessor(pp_double_value), 0);

    // replace the hnsw PP container
    hnsw_index->replacePPContainer(multiPPContainer);

    // Add a vector to the flat buffer.
    VecSimIndex_AddVector(tiered_index, vector, 0);

    {
        SCOPED_TRACE("Store in the flat buffer");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        frontend_index->getDataByLabel(0, stored_vec);
        // the vector should be normalized.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), normalized_vec, dim));

        // we search in the flat index, so the the query should be only normalized
        auto verify_res = [&](size_t id, double score, size_t result_rank) {
            double expected_score = tiered_index->getDistanceFrom_Unsafe(id, normalized_query);
            ASSERT_EQ(score, expected_score);
        };
        runTopKSearchTest(tiered_index, query, 1, verify_res);
        runRangeQueryTest(tiered_index, query, 0.5, verify_res, 1, BY_SCORE);
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        runBatchIteratorSearchTest(batchIterator, 1, verify_res);
        VecSimBatchIterator_Free(batchIterator);
    }

    // Add another vector and exceed the flat buffer capacity. The vector should be stored directly
    // in the hnsw index
    VecSimIndex_AddVector(tiered_index, vector, 1);
    EXPECT_EQ(frontend_index->indexSize(), 1);
    EXPECT_EQ(hnsw_index->indexSize(), 1);
    TEST_DATA_T norm_double_vec[dim];
    TEST_DATA_T norm_double_query[dim];
    for (size_t i = 0; i < dim; i++) {
        norm_double_vec[i] = 2 * normalized_vec[i];
        norm_double_query[i] = 2 * normalized_query[i];
    }
    {
        SCOPED_TRACE("Full buffer; add vector directly to hnsw");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        hnsw_index->getDataByLabel(1, stored_vec);
        // Verify that the vector stored in the hnsw is normalized *and doubled*.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), norm_double_vec, dim));
        auto verify_res = [&](size_t label, double score, size_t result_rank) {
            double expected_score = INVALID_SCORE;
            if (label == 0) { // the result in the flat buffer
                expected_score = frontend_index->calcDistance(normalized_query, normalized_vec);
            }
            if (label == 1) { // the result in the hnsw index
                expected_score =
                    hnsw_index->calcDistanceForQuery(norm_double_vec, norm_double_query);
            }
            ASSERT_EQ(score, expected_score) << "label: " << label;
        };

        size_t k = 2;
        runTopKSearchTest(tiered_index, query, k, verify_res);
        runRangeQueryTest(tiered_index, query, 100, verify_res, k, BY_SCORE);
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        runBatchIteratorSearchTest(batchIterator, k, verify_res);
        VecSimBatchIterator_Free(batchIterator);
    }

    // Move the first vector to the hnsw index.
    mock_thread_pool.thread_iteration();
    EXPECT_EQ(frontend_index->indexSize(), 0);
    EXPECT_EQ(hnsw_index->indexSize(), 2);
    {
        SCOPED_TRACE("Execute insertion job");
        std::vector<std::vector<TEST_DATA_T>> stored_vec;
        hnsw_index->getDataByLabel(0, stored_vec);
        // Verify that the vector stored in the hnsw is normalized *and doubled*.
        ASSERT_NO_FATAL_FAILURE(CompareVectors(stored_vec[0].data(), norm_double_vec, dim));
        // Both vector were processed by the hnsw preprocessor, as well as the query.
        // The score should be the distance from the doubled query to the doubled vector.
        auto verify_res = [&](size_t label, double score, size_t result_rank) {
            double expected_score =
                hnsw_index->calcDistanceForQuery(norm_double_vec, norm_double_query);
            ASSERT_EQ(score, expected_score) << "label: " << label;
        };

        size_t k = 2;
        runTopKSearchTest(tiered_index, query, k, verify_res);
        runRangeQueryTest(tiered_index, query, 100, verify_res, k, BY_SCORE);
        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        runBatchIteratorSearchTest(batchIterator, k, verify_res);
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, HNSWResize) {
    size_t dim = 4;
    constexpr size_t blockSize = 10;
    size_t resize_operations = 0;
    HNSWParams params = {.type = TypeParam::get_index_type(),
                         .dim = dim,
                         .metric = VecSimMetric_L2,
                         .blockSize = blockSize};
    VecSimParams hnsw_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();
    TieredIndexParams tiered_params = {.jobQueue = &mock_thread_pool.jobQ,
                                       .jobQueueCtx = mock_thread_pool.ctx,
                                       .submitCb = tieredIndexMock::submit_callback,
                                       .flatBufferLimit = SIZE_MAX,
                                       .primaryIndexParams = &hnsw_params,
                                       .specificParams = {TieredHNSWParams{.swapJobThreshold = 1}}};
    auto *tiered_index = reinterpret_cast<TieredHNSWIndex<TEST_DATA_T, TEST_DIST_T> *>(
        TieredFactory::NewIndex(&tiered_params));
    mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);

    auto hnsw_index = this->CastToHNSW(tiered_index);
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), 0);

    // add a vector
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    mock_thread_pool.thread_iteration();
    resize_operations++;

    // first vector should trigger resize
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->indexCapacity(), blockSize);
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), blockSize);
    ASSERT_EQ(tiered_index->frontendIndex->indexMetaDataCapacity(), 0);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(),
              hnsw_index->indexMetaDataCapacity() +
                  tiered_index->frontendIndex->indexMetaDataCapacity());

    // add up to block size
    for (size_t i = 1; i < blockSize; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i);
        mock_thread_pool.thread_iteration();
    }
    // should not trigger resize
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), blockSize);
    ASSERT_EQ(hnsw_index->indexCapacity(), blockSize);
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), blockSize);
    ASSERT_EQ(tiered_index->frontendIndex->indexMetaDataCapacity(), 0);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(),
              hnsw_index->indexMetaDataCapacity() +
                  tiered_index->frontendIndex->indexMetaDataCapacity());

    // add one more vector to trigger another resize
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, blockSize);
    mock_thread_pool.thread_iteration();
    resize_operations++;

    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), blockSize + 1);
    ASSERT_EQ(hnsw_index->indexCapacity(), 2 * blockSize);
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), 2 * blockSize);
    ASSERT_EQ(tiered_index->frontendIndex->indexMetaDataCapacity(), 0);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(),
              hnsw_index->indexMetaDataCapacity() +
                  tiered_index->frontendIndex->indexMetaDataCapacity());

    // delete a vector to shrink data blocks
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1) << "Failed to delete vector 0";

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    tiered_index->executeReadySwapJobs();
    resize_operations++;
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), blockSize);
    ASSERT_EQ(hnsw_index->indexCapacity(), blockSize);
    // meta data capacity should not shrink
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), 2 * blockSize);

    // add this vector again and verify lock was acquired to resize
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    mock_thread_pool.thread_iteration();
    resize_operations++;
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), blockSize + 1);
    ASSERT_EQ(hnsw_index->indexCapacity(), 2 * blockSize);
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), 2 * blockSize);
    ASSERT_EQ(tiered_index->frontendIndex->indexMetaDataCapacity(), 0);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(),
              hnsw_index->indexMetaDataCapacity() +
                  tiered_index->frontendIndex->indexMetaDataCapacity());

    // add up to block size (count = 2 blockSize), the lock shouldn't be acquired because no resize
    // is required
    for (size_t i = blockSize + 1; i < 2 * blockSize; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i);
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->getMainIndexGuardWriteLockCount(), resize_operations);
    ASSERT_EQ(hnsw_index->indexSize(), 2 * blockSize);
    ASSERT_EQ(hnsw_index->indexCapacity(), 2 * blockSize);
    ASSERT_EQ(hnsw_index->indexMetaDataCapacity(), 2 * blockSize);
    ASSERT_EQ(tiered_index->frontendIndex->indexMetaDataCapacity(), 0);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(),
              hnsw_index->indexMetaDataCapacity() +
                  tiered_index->frontendIndex->indexMetaDataCapacity());
}

template <typename index_type_t>
class HNSWTieredRelabelIteratorTest : public HNSWTieredIndexTest<index_type_t> {
protected:
    auto *CreateIndex(tieredIndexMock &pool, size_t n, size_t dim) {
        HNSWParams params = {.type = index_type_t::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = index_type_t::isMulti()};
        VecSimParams hnsw_params = CreateParams(params);
        auto *index = this->CreateTieredHNSWIndex(hnsw_params, pool);
        VecSim_SetWriteMode(VecSim_WriteAsync);
        using data_t = typename index_type_t::data_t;
        std::vector<data_t> vector(dim);
        for (size_t i = 0; i < n; ++i) {
            data_t value;
            if constexpr (std::is_same_v<data_t, vecsim_types::float16>) {
                value = vecsim_types::FP32_to_FP16(static_cast<float>(i));
            } else {
                value = static_cast<data_t>(i);
            }
            std::fill(vector.begin(), vector.end(), value);
            VecSimIndex_AddVector(index, vector.data(), i);
        }
        while (!pool.jobQ.empty()) {
            pool.thread_iteration();
        }
        return index;
    }
};

using RelabelIteratorTypes =
    ::testing::Types<IndexTypeExtended<VecSimType_FLOAT32, false, float>,
                     IndexTypeExtended<VecSimType_FLOAT32, true, float>,
                     IndexTypeExtended<VecSimType_FLOAT16, false, vecsim_types::float16, float>,
                     IndexTypeExtended<VecSimType_FLOAT16, true, vecsim_types::float16, float>,
                     IndexTypeExtended<VecSimType_FLOAT64, false, double>,
                     IndexTypeExtended<VecSimType_FLOAT64, true, double>>;
TYPED_TEST_SUITE(HNSWTieredRelabelIteratorTest, RelabelIteratorTypes);

TYPED_TEST(HNSWTieredRelabelIteratorTest, relabelVectorWithPausedBatchIterator) {
    const size_t dim = 4;
    const size_t n = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *index = this->CreateIndex(mock_thread_pool, n, dim);
    TEST_DATA_T query[dim]{};
    auto *iterator = VecSimBatchIterator_New(index, query, nullptr);
    auto *first = VecSimBatchIterator_Next(iterator, 1, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(first), 1);
    VecSimQueryReply_Free(first);
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));

    // Match a cursor retaining an unfinished vector scan while indexing needs to relabel.
    // Always release the iterator before joining, so a regression fails instead of hanging.
    std::atomic<bool> completed = false;
    VecSimRelabelCode code = VecSimRelabel_Unsupported;
    std::thread writer([&] {
        code = VecSimIndex_RelabelVector(index, n - 1, n + 1);
        completed.store(true);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool completed_while_paused = completed.load();
    if (completed_while_paused) {
        // Relabeling changes labels, not saved graph IDs. The same traversal must remain usable.
        auto *next = VecSimBatchIterator_Next(iterator, 1, BY_SCORE);
        EXPECT_EQ(VecSimQueryReply_GetCode(next), VecSim_QueryReply_OK);
        EXPECT_EQ(VecSimQueryReply_Len(next), 1);
        VecSimQueryReply_Free(next);
        VecSimBatchIterator_Reset(iterator);
        size_t count = 0;
        bool saw_new_label = false;
        while (VecSimBatchIterator_HasNext(iterator)) {
            auto *batch = VecSimBatchIterator_Next(iterator, 10, BY_SCORE);
            auto *results = VecSimQueryReply_GetIterator(batch);
            while (VecSimQueryReply_IteratorHasNext(results)) {
                const auto *result = VecSimQueryReply_IteratorNext(results);
                EXPECT_NE(VecSimQueryResult_GetId(result), n - 1);
                saw_new_label |= VecSimQueryResult_GetId(result) == n + 1;
                ++count;
            }
            VecSimQueryReply_IteratorFree(results);
            VecSimQueryReply_Free(batch);
        }
        EXPECT_EQ(count, n);
        EXPECT_TRUE(saw_new_label);
    }
    VecSimBatchIterator_Free(iterator);
    writer.join();
    EXPECT_TRUE(completed_while_paused);
    EXPECT_EQ(code, VecSimRelabel_OK);
}

TYPED_TEST(HNSWTieredRelabelIteratorTest, relabelVectorWhileQuerying) {
    const size_t dim = 4;
    const size_t n = 100;
    const labelType offset = 0x5555555555550000ULL;
    auto mock_thread_pool = tieredIndexMock();
    auto *index = this->CreateIndex(mock_thread_pool, n, dim);
    TEST_DATA_T query[dim]{};
    std::atomic<bool> done = false;
    std::atomic<size_t> queries = 0;
    auto verify = [&](VecSimQueryReply *reply) {
        EXPECT_EQ(VecSimQueryReply_GetCode(reply), VecSim_QueryReply_OK);
        auto *results = VecSimQueryReply_GetIterator(reply);
        while (VecSimQueryReply_IteratorHasNext(results)) {
            const auto id = VecSimQueryResult_GetId(VecSimQueryReply_IteratorNext(results));
            EXPECT_TRUE(id < n || (id >= offset && id < offset + n));
        }
        VecSimQueryReply_IteratorFree(results);
        VecSimQueryReply_Free(reply);
    };
    std::thread reader([&] {
        while (!done.load()) {
            verify(VecSimIndex_TopKQuery(index, query, 10, nullptr, BY_SCORE));
            verify(VecSimIndex_RangeQuery(index, query, 40000, nullptr, BY_SCORE));
            auto *iterator = VecSimBatchIterator_New(index, query, nullptr);
            verify(VecSimBatchIterator_Next(iterator, 10, BY_SCORE));
            verify(VecSimBatchIterator_Next(iterator, 10, BY_SCORE));
            VecSimBatchIterator_Free(iterator);
            ++queries;
        }
    });
    while (queries.load() == 0) {
        std::this_thread::yield();
    }
    for (size_t round = 0; round < 20; ++round) {
        for (size_t i = 0; i < n; ++i) {
            const labelType old_label = i + (round % 2 ? offset : 0);
            const labelType new_label = i + (round % 2 ? 0 : offset);
            EXPECT_EQ(VecSimIndex_RelabelVector(index, old_label, new_label), VecSimRelabel_OK);
        }
    }
    done = true;
    reader.join();
    EXPECT_GT(queries.load(), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, relabelVectorFlatOnly) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // The vector lands in the flat buffer with a pending insert job; nothing is in HNSW yet.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(7).size(), 1);
    const idType flat_id = tiered_index->labelToInsertJobs.at(7)[0]->id;

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);

    // The flat tier moved, and so did both halves of the job bookkeeping: the map key and the
    // job's own copy of the label. A half-applied move would make the worker below either index
    // the vector under the stale label or throw out of `labelToInsertJobs.at`.
    ASSERT_TRUE(frontend_index->isLabelExists(70));
    ASSERT_FALSE(frontend_index->isLabelExists(7));
    ASSERT_EQ(tiered_index->labelToInsertJobs.count(7), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70).size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70)[0]->label, 70);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70)[0]->id, flat_id);
    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), 1);

    // Draining the job must ingest the vector into HNSW under the *new* label and clear the flat
    // tier, exactly as it would have for a label that was never moved.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_TRUE(hnsw_index->isLabelExists(70));
    ASSERT_FALSE(hnsw_index->isLabelExists(7));
    ASSERT_EQ(tiered_index->labelToInsertJobs.count(70), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 7);
    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 70);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(tiered_index, query, 1, verify_res);
}

TYPED_TEST(HNSWTieredIndexTestBasic, relabelVectorBothTiers) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Reproduce the ingestion window: `executeInsertJob` inserts into HNSW *before* removing the
    // vector from the flat buffer, so a label is legitimately live in both tiers at once. Build
    // that state directly so the test is deterministic.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7);
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 7);
    hnsw_index->addVector(vector, 7);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->indexSize(), 1);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);

    // Both tiers must move - an implementation treating them as mutually exclusive would leave one
    // stale copy behind under the old label.
    ASSERT_TRUE(frontend_index->isLabelExists(70));
    ASSERT_FALSE(frontend_index->isLabelExists(7));
    ASSERT_TRUE(hnsw_index->isLabelExists(70));
    ASSERT_FALSE(hnsw_index->isLabelExists(7));
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70)[0]->label, 70);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

// The multi case is where a label has more than one id in each of its three homes, so a move that
// handles only the first id of a label - or only one home - still looks right in the single-value
// tests. Assert on the whole id set of every home instead.
TYPED_TEST(HNSWTieredIndexTestBasic, relabelVectorMulti) {
    size_t dim = 4;
    size_t per_label = 3;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Label 7 gets `per_label` vectors in the flat buffer (each with a pending insert job) and
    // `per_label` more directly in HNSW, reproducing the ingestion window for every copy. Label 8
    // is an untouched neighbour with copies of its own, so a move that is too broad shows up too.
    TEST_DATA_T vector[dim];
    for (size_t i = 0; i < per_label; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, i);
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 8, i + 100);
        GenerateVector<TEST_DATA_T>(vector, dim, i + 10);
        hnsw_index->addVector(vector, 7);
    }
    ASSERT_EQ(frontend_index->indexSize(), per_label * 2);
    ASSERT_EQ(hnsw_index->indexSize(), per_label);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(7).size(), per_label);
    auto hnsw_ids_before = hnsw_index->getElementIds(7);
    ASSERT_EQ(hnsw_ids_before.size(), per_label);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);

    // Nothing was added or dropped along the way.
    ASSERT_EQ(frontend_index->indexSize(), per_label * 2);
    ASSERT_EQ(hnsw_index->indexSize(), per_label);
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);

    // Every home moved, and moved *all* of its ids.
    ASSERT_TRUE(frontend_index->isLabelExists(70));
    ASSERT_FALSE(frontend_index->isLabelExists(7));
    ASSERT_TRUE(hnsw_index->isLabelExists(70));
    ASSERT_FALSE(hnsw_index->isLabelExists(7));
    ASSERT_EQ(hnsw_index->getElementIds(70), hnsw_ids_before);
    for (idType id : hnsw_ids_before) {
        ASSERT_EQ(hnsw_index->getExternalLabel(id), 70);
    }
    ASSERT_EQ(tiered_index->labelToInsertJobs.count(7), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70).size(), per_label);
    for (TieredInsertJob *job : tiered_index->labelToInsertJobs.at(70)) {
        ASSERT_EQ(job->label, 70);
    }

    // The neighbouring label kept all of its own copies.
    ASSERT_TRUE(frontend_index->isLabelExists(8));
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(8).size(), per_label);

    // The vectors are still searchable, now reported under the new label.
    GenerateVector<TEST_DATA_T>(vector, dim, 0);
    auto verify_res = [&](size_t label, double score, size_t rank) { ASSERT_EQ(label, 70); };
    runTopKSearchTest(tiered_index, vector, 1, verify_res);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

TYPED_TEST(HNSWTieredIndexTestBasic, relabelVectorRejects) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Label 1 is ingested into HNSW, label 2 stays pending in the flat buffer. The target of a
    // relabel must be free in *both* tiers and in the pending-job map.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    mock_thread_pool.thread_iteration();
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 2);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 1);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 2, 1),
              VecSimRelabel_NewLabelTaken); // target lives in HNSW
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 1, 2),
              VecSimRelabel_NewLabelTaken); // target lives in flat + jobs
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 1, 1), VecSimRelabel_SameLabel);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 42, 43), VecSimRelabel_OldLabelMissing);
    // An absent source outranks an occupied target: a caller that resolves the conflict by
    // freeing the target must not be sent down that path for a move with nothing to move.
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 42, 1), VecSimRelabel_OldLabelMissing);

    // Every rejection left both tiers and the job map untouched.
    ASSERT_TRUE(hnsw_index->isLabelExists(1));
    ASSERT_TRUE(frontend_index->isLabelExists(2));
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(2).size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(2)[0]->label, 2);
    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), 2);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

// Relabel racing against live ingestion by the worker threads. The point of interest is the
// invariant that `addVector` and `deleteVector` rely on when they reach for
// `labelToInsertJobs.at(label)` behind an `isLabelExists(label)` guard, and that
// `executeInsertJob` relies on for `labelToInsertJobs.at(job->label)`: a label present in the flat
// buffer must be a key in the job map. A relabel that moved the flat entry without re-keying the
// job map (or without rewriting `job->label`) breaks it, and the resulting `std::out_of_range`
// would be thrown inside a worker thread - i.e. it would terminate the process, not fail an
// assertion. So reaching the end of this test at all is the real assertion.
TYPED_TEST(HNSWTieredIndexTestBasic, relabelVectorDuringIngestion) {
    size_t dim = 4;
    size_t n = 500;
    // The offset that maps an original label to its relabeled value; kept larger than `n` so the
    // two ranges cannot overlap and make a relabel fail on an occupied target.
    const labelType relabel_offset = 10000;

    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    mock_thread_pool.init_threads();

    // Interleave adds with relabels of already-added labels, so that a relabel can land while a
    // worker is anywhere in `executeInsertJob` for that same label - including the window where the
    // vector is live in both tiers at once.
    size_t relabeled = 0;
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
        if (i > 0) {
            relabeled +=
                tiered_index->relabelVector(i - 1, i - 1 + relabel_offset) == VecSimRelabel_OK;
        }
    }
    relabeled += tiered_index->relabelVector(n - 1, n - 1 + relabel_offset) == VecSimRelabel_OK;

    mock_thread_pool.thread_pool_join();

    // Every relabel targeted a distinct, unoccupied label of a vector known to exist, so all of
    // them must have been applied - otherwise the label accounting below would pass trivially.
    ASSERT_EQ(relabeled, n);

    // The index is fully ingested, holds exactly the relabeled range, and none of the original
    // labels survived anywhere.
    auto *hnsw_index = this->CastToHNSW(tiered_index);
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->backendIndex->indexSize(), n);
    ASSERT_EQ(tiered_index->frontendIndex->indexSize(), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.size(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), n);
    for (size_t i = 0; i < n; i++) {
        ASSERT_TRUE(hnsw_index->isLabelExists(i + relabel_offset)) << "missing label " << i;
        ASSERT_FALSE(hnsw_index->isLabelExists(i)) << "stale label " << i;
    }
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // Each moved label still resolves to its own vector, so no relabel crossed wires.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T expected[dim];
        GenerateVector<TEST_DATA_T>(expected, dim, i);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(i + relabel_offset, expected), 0)
            << "label " << i + relabel_offset << " does not hold its original vector";
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsBuffered) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // The vector is still in the flat buffer, with a pending insert job.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7);
    // A second buffered label, to catch the update disturbing anything but its own: removing a
    // vector from the flat buffer swaps the last one into the hole and rewrites the pending job
    // that names it, so a delete-then-insert would move this label's id as collateral.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 8, 8);
    ASSERT_EQ(frontend_index->indexSize(), 2);
    ASSERT_EQ(hnsw_index->indexSize(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    const idType flat_id = tiered_index->labelToInsertJobs.at(7)[0]->id;
    const idType other_flat_id = tiered_index->labelToInsertJobs.at(8)[0]->id;

    TEST_DATA_T buffered[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(buffered, dim, 7);
    GenerateVector<TEST_DATA_T>(replacement, dim, 70);

    ASSERT_EQ(tiered_index->updateVectors(7, replacement, 1), VecSimUpdate_OK);

    // One vector under a single-value label is an overwrite, and it is served as one: the buffered
    // copy is written over where it sits, keeping its id, and no other label's pending job is
    // touched. A delete followed by an insert would reach the same end state for label 7 while
    // moving label 8's vector to a different id on the way, and would leave label 7 without a
    // vector in between.
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(7).size(), 1);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(7)[0]->id, flat_id) << "the buffered copy moved";
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(8)[0]->id, other_flat_id)
        << "another label's buffered vector was moved as collateral";

    // Nothing had reached the graph, so nothing was marked deleted: the label's vector was dropped
    // from the buffer - taking its pending job out of circulation - and the new one buffered with a
    // job of its own.
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 2);
    ASSERT_EQ(hnsw_index->indexSize(), 0);
    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(7, replacement), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(7, buffered), 0);

    // Draining ingests the new value once; the invalidated job does nothing.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(hnsw_index->indexSize(), 2);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(7, replacement), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 7);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(tiered_index, replacement, 1, verify_res);
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsIndexed) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Ingest a few vectors, so the updated element has neighbours whose connections to it have to
    // be repaired once it is marked deleted.
    for (size_t i = 0; i < 5; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(hnsw_index->indexSize(), 5);
    ASSERT_EQ(frontend_index->indexSize(), 0);

    TEST_DATA_T indexed[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(indexed, dim, 3);
    GenerateVector<TEST_DATA_T>(replacement, dim, 50);

    ASSERT_EQ(tiered_index->updateVectors(3, replacement, 1), VecSimUpdate_OK);

    // An indexed element cannot be written over, so it becomes a tombstone and the new value is
    // buffered. The label is searchable at its new value straight away, and is counted once even
    // though both tiers now mention it.
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 5);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(3, replacement), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(3, indexed), 0);
    auto verify_res = [&](size_t id, double score, size_t rank) {
        ASSERT_EQ(id, 3);
        ASSERT_EQ(score, 0);
    };
    runTopKSearchTest(tiered_index, replacement, 1, verify_res);

    // Draining the jobs ingests the new value and repairs the tombstone's neighbours; the GC then
    // disposes of it, leaving as many elements as before the update.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    tiered_index->runGC();
    ASSERT_EQ(hnsw_index->indexSize(), 5);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 5);
    auto report = hnsw_index->checkIntegrity();
    ASSERT_TRUE(report.valid_state);
    ASSERT_EQ(report.connections_to_repair, 0);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(3, replacement), 0);

    // The other labels kept their vectors throughout.
    for (size_t i : {0, 1, 2, 4}) {
        TEST_DATA_T untouched[dim];
        GenerateVector<TEST_DATA_T>(untouched, dim, i);
        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(i, untouched), 0) << "label " << i;
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsMultiChangesCount) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // One label with two vectors, one in each tier: the first is ingested, the second stays
    // buffered. So the update has to reach both tiers to replace the label's contents.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    mock_thread_pool.thread_iteration();
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 2);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);

    TEST_DATA_T three[3 * dim];
    for (size_t i = 0; i < 3; i++) {
        GenerateVector<TEST_DATA_T>(three + i * dim, dim, 10 + i);
    }
    ASSERT_EQ(tiered_index->updateVectors(1, three, 3), VecSimUpdate_OK);

    // The buffered vector left no trace; the ingested one left a tombstone. The label now holds
    // the three new vectors, all buffered.
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 3);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    for (size_t i = 0; i < 3; i++) {
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, three + i * dim), 0) << "missing " << i;
    }
    for (size_t value : {1, 2}) {
        TEST_DATA_T gone[dim];
        GenerateVector<TEST_DATA_T>(gone, dim, value);
        ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(1, gone), 0) << "kept " << value;
    }

    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    tiered_index->runGC();
    ASSERT_EQ(hnsw_index->indexSize(), 3);
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), 3);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // Shrinking the label back to a single vector goes the same way, and now every element it
    // removes is an indexed one.
    TEST_DATA_T one[dim];
    GenerateVector<TEST_DATA_T>(one, dim, 20);
    ASSERT_EQ(tiered_index->updateVectors(1, one, 1), VecSimUpdate_OK);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 3);
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    tiered_index->runGC();
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), 1);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(1, one), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

// Shrinking a multi-value label all the way to zero vectors is the one shrink outcome the reuse
// loop above never leaves an id behind for: every other shrink keeps at least one id to reuse, so
// this is the only case that exercises `popLastIdFromLabel` actually dropping the label once its
// last id is gone, instead of just shortening its id list.
TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsMultiInPlaceShrinkToZeroRemovesLabel) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    VecSim_SetWriteMode(VecSim_WriteInPlace);

    // Padding labels bracket the target label so a wrongly-erased entry, or a leftover mapping,
    // can't hide inside another label's ids.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, 0);
    labelType label = 1;
    size_t per_label = 3;
    for (size_t j = 0; j < per_label; j++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, label, 10 + j);
    }
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 20);
    ASSERT_EQ(hnsw_index->indexSize(), per_label + 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 3);

    ASSERT_EQ(tiered_index->updateVectors(label, nullptr, 0), VecSimUpdate_OK);

    ASSERT_FALSE(hnsw_index->isLabelExists(label));
    ASSERT_EQ(hnsw_index->getElementIds(label).size(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
    ASSERT_EQ(hnsw_index->indexSize(), 2);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);

    // The bracketing labels are untouched.
    TEST_DATA_T v0[dim], v2[dim];
    GenerateVector<TEST_DATA_T>(v0, dim, 0);
    GenerateVector<TEST_DATA_T>(v2, dim, 20);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, v0), 0);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(2, v2), 0);
}

// On a single-value index, replacing a label with zero vectors must remove it, like it does on a
// multi-value one - not read the (null) blob as a vector to overwrite it with. Covers a label
// still in the flat buffer (async) and one already in HNSW (in-place).
TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsSingleValueZeroVectorsRemovesLabel) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);

    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        VecSim_SetWriteMode(mode);
        labelType label = mode == VecSim_WriteAsync ? 1 : 11;
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, label, label);
        size_t labels_before = tiered_index->indexLabelCount();
        ASSERT_EQ(labels_before, 1);

        ASSERT_EQ(tiered_index->updateVectors(label, nullptr, 0), VecSimUpdate_OK);

        ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsInTheIngestWindow) {
    size_t dim = 4;
    TEST_DATA_T ingesting[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(ingesting, dim, 1);
    GenerateVector<TEST_DATA_T>(replacement, dim, 10);

    // `executeInsertJob` inserts into HNSW *before* dropping the flat copy, so a label can be in
    // both tiers at once. An update landing in that window has to remove both copies - leaving
    // either one would keep the old value searchable under the label.
    for (bool is_multi : {false, true}) {
        SCOPED_TRACE(is_multi ? "multi" : "single");
        HNSWParams params = {.type = TypeParam::get_index_type(),
                             .dim = dim,
                             .metric = VecSimMetric_L2,
                             .multi = is_multi};
        VecSimParams hnsw_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
        auto *frontend_index = this->GetFlatIndex(tiered_index);
        auto *hnsw_index = this->CastToHNSW(tiered_index);

        // Build the window directly rather than racing a worker into it.
        VecSimIndex_AddVector(tiered_index, ingesting, 0);
        hnsw_index->addVector(ingesting, 0);
        ASSERT_EQ(frontend_index->indexSize(), 1);
        ASSERT_EQ(hnsw_index->indexSize(), 1);

        ASSERT_EQ(tiered_index->updateVectors(0, replacement, 1), VecSimUpdate_OK);

        // The buffered copy was dropped and the indexed one marked deleted, so only the new value
        // is left under the label.
        ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 1);
        ASSERT_EQ(frontend_index->indexSize(), 1);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, replacement), 0);
        ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(0, ingesting), 0);

        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        tiered_index->runGC();
        ASSERT_EQ(hnsw_index->indexSize(), 1);
        ASSERT_EQ(hnsw_index->getElementIds(0).size(), 1);
        ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
        ASSERT_EQ(frontend_index->indexSize(), 0);
        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(0, replacement), 0);
        ASSERT_NE(hnsw_index->getDistanceFrom_Unsafe(0, ingesting), 0);
        ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
    }
}

TYPED_TEST(HNSWTieredIndexTestBasic, addVectorInPlaceOverwritesLabelInBothTiers) {
    size_t dim = 4;
    TEST_DATA_T ingesting[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(ingesting, dim, 1);
    GenerateVector<TEST_DATA_T>(replacement, dim, 10);

    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // The label is in the flat buffer and in HNSW at once, as inside the ingest window of an
    // insert job, so deleting it removes two vectors.
    VecSimIndex_AddVector(tiered_index, ingesting, 0);
    hnsw_index->addVector(ingesting, 0);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->indexSize(), 1);

    // Overwriting an existing label is not adding a new one, whatever number of copies it had.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, replacement, 0), 0);

    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, replacement), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(0, ingesting), 0);
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsWithFullFlatBuffer) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // A flat buffer limit of 0 leaves every insert to go straight into HNSW, which is the phase an
    // index under write pressure spends its time in - the update's insert half has to work there
    // too, with no job to carry it.
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool, 0, 0);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    TEST_DATA_T indexed[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(indexed, dim, 1);
    GenerateVector<TEST_DATA_T>(replacement, dim, 10);

    ASSERT_EQ(tiered_index->updateVectors(1, replacement, 1), VecSimUpdate_OK);

    // Nothing is buffered on either side: the old element is a tombstone and the new one went
    // directly into the graph.
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(hnsw_index->indexSize(), 2); // the live element and the tombstone
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, replacement), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(1, indexed), 0);

    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    tiered_index->runGC();
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(1, replacement), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsTwiceBeforeIngestion) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    // Start from an indexed vector, so the first update leaves a tombstone whose repair jobs are
    // still pending when the second update runs.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 0);

    TEST_DATA_T first[dim], second[dim], third[dim];
    GenerateVector<TEST_DATA_T>(first, dim, 1);
    GenerateVector<TEST_DATA_T>(second, dim, 10);
    GenerateVector<TEST_DATA_T>(third, dim, 20);

    ASSERT_EQ(tiered_index->updateVectors(1, second, 1), VecSimUpdate_OK);
    ASSERT_EQ(tiered_index->updateVectors(1, third, 1), VecSimUpdate_OK);

    // The second update replaced a vector that was still buffered, so it cost no second tombstone -
    // it dropped the buffered copy and the job that was going to ingest it.
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(frontend_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, third), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(1, second), 0);
    ASSERT_NE(tiered_index->getDistanceFrom_Unsafe(1, first), 0);

    // Draining runs the invalidated jobs as well; only the last value is ingested.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    tiered_index->runGC();
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(hnsw_index->getElementIds(1).size(), 1);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(1, third), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsRejectsSeveralVectorsInSingle) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(hnsw_index->indexSize(), 1);

    TEST_DATA_T two_vectors[2 * dim];
    GenerateVector<TEST_DATA_T>(two_vectors, dim, 10);
    GenerateVector<TEST_DATA_T>(two_vectors + dim, dim, 11);

    // Refused before anything is removed - a single-value index cannot end up holding both, and
    // serving it by storing one of them would leave the caller believing both are there.
    ASSERT_EQ(tiered_index->updateVectors(1, two_vectors, 2), VecSimUpdate_MultiNotSupported);
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(hnsw_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    TEST_DATA_T original[dim];
    GenerateVector<TEST_DATA_T>(original, dim, 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, original), 0);
    ASSERT_TRUE(hnsw_index->checkIntegrity().valid_state);
}

TYPED_TEST(HNSWTieredIndexTestBasic, updateVectorsInPlace) {
    size_t dim = 4;
    HNSWParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams hnsw_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredHNSWIndex(hnsw_params, mock_thread_pool);
    auto *frontend_index = this->GetFlatIndex(tiered_index);
    auto *hnsw_index = this->CastToHNSW(tiered_index);

    for (size_t i = 0; i < 5; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(hnsw_index->indexSize(), 5);

    tiered_index->setWriteMode(VecSim_WriteInPlace);

    TEST_DATA_T indexed[dim], replacement[dim];
    GenerateVector<TEST_DATA_T>(indexed, dim, 3);
    GenerateVector<TEST_DATA_T>(replacement, dim, 50);

    ASSERT_EQ(tiered_index->updateVectors(3, replacement, 1), VecSimUpdate_OK);

    // Both halves happen synchronously in this mode: the old element is taken out of the graph
    // rather than marked deleted, and the new one goes straight into HNSW - no tombstone, no job,
    // and nothing left buffered.
    ASSERT_EQ(hnsw_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_EQ(frontend_index->indexSize(), 0);
    ASSERT_EQ(hnsw_index->indexSize(), 5);
    ASSERT_EQ(tiered_index->indexLabelCount(), 5);
    ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(3, replacement), 0);
    ASSERT_NE(hnsw_index->getDistanceFrom_Unsafe(3, indexed), 0);
    auto report = hnsw_index->checkIntegrity();
    ASSERT_TRUE(report.valid_state);
    ASSERT_EQ(report.connections_to_repair, 0);

    for (size_t i : {0, 1, 2, 4}) {
        TEST_DATA_T untouched[dim];
        GenerateVector<TEST_DATA_T>(untouched, dim, i);
        ASSERT_EQ(hnsw_index->getDistanceFrom_Unsafe(i, untouched), 0) << "label " << i;
    }

    VecSim_SetWriteMode(VecSim_WriteAsync);
}

using float16 = vecsim_types::float16;

// -------------------------------------------------------------------
// Type definitions for parameterized tests (float32 and float16)
// -------------------------------------------------------------------

template <VecSimType type, bool IsMulti, typename DataType, typename DistType = float>
struct SQ8IndexType {
    static VecSimType get_index_type() { return type; }
    static bool isMulti() { return IsMulti; }
    typedef DataType data_t;
    typedef DistType dist_t;
};

// -------------------------------------------------------------------
// Test fixture
// -------------------------------------------------------------------

template <typename index_type_t>
class HNSWTieredIndexTestSQ8 : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

protected:
    VecSimWriteMode original_mode;

    void SetUp() override { original_mode = VecSimIndexInterface::asyncWriteMode; }
    void TearDown() override { VecSimIndexInterface::asyncWriteMode = original_mode; }

    // Create a tiered HNSW index with SQ8 quantization and accumulation phase.
    TieredHNSWIndex<data_t, dist_t> *
    CreateSQ8TieredIndex(tieredIndexMock &mock_thread_pool, size_t dim = 16,
                         VecSimMetric metric = VecSimMetric_IP, size_t normSetSize = 100,
                         size_t flat_buffer_limit = SIZE_MAX, size_t M = 16,
                         size_t efConstruction = 200, const float *mean = nullptr) {
        HNSWParams hnsw_params = {.type = index_type_t::get_index_type(),
                                  .dim = dim,
                                  .metric = metric,
                                  .multi = index_type_t::isMulti(),
                                  .M = M,
                                  .efConstruction = efConstruction,
                                  .quantType = VecSimQuant_SQ8,
                                  .quantParams = mean};
        VecSimParams vecsim_params = CreateParams(hnsw_params);
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = flat_buffer_limit,
            .primaryIndexParams = &vecsim_params,
            .specificParams = {
                TieredHNSWParams{.swapJobThreshold = 0, .QuantNormalizationSetSize = normSetSize}}};
        auto *tiered_index = reinterpret_cast<TieredHNSWIndex<data_t, dist_t> *>(
            TieredFactory::NewIndex(&tiered_params));
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);
        return tiered_index;
    }

    HNSWIndex<data_t, dist_t> *CastToHNSW(VecSimIndex *index) {
        auto tiered_index = reinterpret_cast<TieredHNSWIndex<data_t, dist_t> *>(index);
        return tiered_index->getHNSWIndex();
    }

    // --- Accessor helpers (HNSWTieredIndexTestSQ8 is a friend of TieredHNSWIndex) ---

    bool getIsInAccumulationPhase(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->sqAccumulationState.has_value();
    }

    const vecsim_stl::vector<double> &getRunningSumVec(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->sqAccumulationState->getRunningSum();
    }

    size_t getNormalizationSetSize(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->sqAccumulationState->getNormalizationSetSize();
    }

    // Compare the actual cached dispatches and preprocessing with a backend constructed with
    // the expected mean. Stored-to-stored IP specifically exercises mean_sum_squares.
    void checkTrainedComponents(TieredHNSWIndex<data_t, dist_t> *idx,
                                const std::vector<float> &mean) {
        const size_t dim = mean.size();
        auto *backend = idx->getHNSWIndex();
        HNSWParams params = {.type = index_type_t::get_index_type(),
                             .dim = dim,
                             .metric = backend->getMetric(),
                             .multi = index_type_t::isMulti(),
                             .quantType = VecSimQuant_SQ8,
                             .quantParams = mean.data()};
        VecSimParams vecsim_params = CreateParams(params);
        std::unique_ptr<HNSWIndex<data_t, dist_t>, decltype(&VecSimIndex_Free)> reference(
            static_cast<HNSWIndex<data_t, dist_t> *>(HNSWFactory::NewIndex(&vecsim_params, true)),
            VecSimIndex_Free);
        ASSERT_NE(reference, nullptr);
        ASSERT_EQ(backend->getStoredDataSize(), reference->getStoredDataSize());
        std::vector<data_t> x(dim), y(dim);
        for (size_t d = 0; d < dim; ++d) {
            x[d] = from_fp32<data_t>(0.2f + float(d % 5) * 0.1f);
            y[d] = from_fp32<data_t>(-0.3f + float(d % 7) * 0.07f);
        }
        auto actual_x = backend->preprocessForStorage(x.data());
        auto actual_y = backend->preprocessForStorage(y.data());
        auto actual_query = backend->preprocessQuery(y.data());
        auto expected_x = reference->preprocessForStorage(x.data());
        auto expected_y = reference->preprocessForStorage(y.data());
        auto expected_query = reference->preprocessQuery(y.data());
        EXPECT_EQ(memcmp(actual_x.get(), expected_x.get(), backend->getStoredDataSize()), 0);
        EXPECT_EQ(memcmp(actual_y.get(), expected_y.get(), backend->getStoredDataSize()), 0);
        EXPECT_FLOAT_EQ(backend->calcDistance(actual_x.get(), actual_y.get()),
                        reference->calcDistance(expected_x.get(), expected_y.get()));
        EXPECT_FLOAT_EQ(backend->calcDistanceForQuery(actual_x.get(), actual_query.get()),
                        reference->calcDistanceForQuery(expected_x.get(), expected_query.get()));
    }

    BruteForceIndex<data_t, dist_t> *getFrontendIndex(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->frontendIndex;
    }

    VecSimIndexAbstract<data_t, dist_t> *getBackendIndex(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->backendIndex;
    }

    auto &getLabelToInsertJobs(TieredHNSWIndex<data_t, dist_t> *idx) {
        return idx->labelToInsertJobs;
    }

    auto &getInvalidJobs(TieredHNSWIndex<data_t, dist_t> *idx) { return idx->invalidJobs; }

    void callExecuteReadySwapJobs(TieredHNSWIndex<data_t, dist_t> *idx) {
        idx->executeReadySwapJobs();
    }

    // Generate a vector with a pattern based on label.
    void GenerateVectorData(data_t *output, size_t dim, float base_value) {
        const float angle = base_value * 0.01f;
        for (size_t i = 0; i < dim; i++) {
            float val = 0.0f;
            if (i == 0) {
                val = std::cos(angle);
            } else if (i == 1) {
                val = std::sin(angle);
            }
            if constexpr (std::is_same_v<data_t, float>) {
                output[i] = val;
            } else if constexpr (std::is_same_v<data_t, float16>) {
                output[i] = vecsim_types::FP32_to_FP16(val);
            }
        }
    }

    // Get value as float from data type.
    float ToFloat(data_t val) {
        if constexpr (std::is_same_v<data_t, float>) {
            return val;
        } else {
            return vecsim_types::FP16_to_FP32(val);
        }
    }
};

using SQ8FP32Single = SQ8IndexType<VecSimType_FLOAT32, false, float>;
using SQ8FP32Multi = SQ8IndexType<VecSimType_FLOAT32, true, float>;
using SQ8FP16Single = SQ8IndexType<VecSimType_FLOAT16, false, float16, float>;
using SQ8FP16Multi = SQ8IndexType<VecSimType_FLOAT16, true, float16, float>;

using SQ8DataTypeSet = ::testing::Types<SQ8FP32Single, SQ8FP32Multi, SQ8FP16Single, SQ8FP16Multi>;
using SQ8SingleDataTypeSet = ::testing::Types<SQ8FP32Single, SQ8FP16Single>;
using SQ8MultiDataTypeSet = ::testing::Types<SQ8FP32Multi, SQ8FP16Multi>;

template <typename index_type_t>
class HNSWTieredIndexTestSQ8Single : public HNSWTieredIndexTestSQ8<index_type_t> {};

template <typename index_type_t>
class HNSWTieredIndexTestSQ8Multi : public HNSWTieredIndexTestSQ8<index_type_t> {};

TYPED_TEST_SUITE(HNSWTieredIndexTestSQ8, SQ8DataTypeSet);
TYPED_TEST_SUITE(HNSWTieredIndexTestSQ8Single, SQ8SingleDataTypeSet);
TYPED_TEST_SUITE(HNSWTieredIndexTestSQ8Multi, SQ8MultiDataTypeSet);

// -------------------------------------------------------------------
// Accumulation Phase Core Tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, AccumulationPhaseInitialization) {
    // Verify that creating an SQ8 tiered index enters accumulation phase.
    size_t dim = 16;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Verify accumulation phase state.
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getRunningSumVec(tiered_index).size(), dim);
    ASSERT_EQ(this->getNormalizationSetSize(tiered_index), normSetSize);

    // Verify running sum is zero-initialized.
    for (size_t i = 0; i < dim; i++) {
        ASSERT_DOUBLE_EQ(this->getRunningSumVec(tiered_index)[i], 0.0);
    }

    // The real SQ8 backend exists and remains empty until training completes.
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), 0);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, InitialMemoryAndSuppliedMeanWithOptionalTraining) {
    constexpr size_t dim = 17;
    const std::vector<float> supplied_mean(dim, -0.25f);
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2, VecSimMetric_Cosine}) {
        for (size_t threshold : {0, 2}) {
            for (bool supply_mean : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << metric << " threshold=" << threshold << " mean=" << supply_mean);
                auto mock_thread_pool = tieredIndexMock();
                auto *tiered_index = this->CreateSQ8TieredIndex(
                    mock_thread_pool, dim, metric, threshold, SIZE_MAX, 16, 200,
                    supply_mean ? supplied_mean.data() : nullptr);
                HNSWParams hnsw_params = {.type = TypeParam::get_index_type(),
                                          .dim = dim,
                                          .metric = metric,
                                          .multi = TypeParam::isMulti(),
                                          .M = 16,
                                          .efConstruction = 200,
                                          .quantType = VecSimQuant_SQ8,
                                          .quantParams =
                                              supply_mean ? supplied_mean.data() : nullptr};
                VecSimParams primary = CreateParams(hnsw_params);
                TieredIndexParams tiered_params = {
                    .primaryIndexParams = &primary,
                    .specificParams = {TieredHNSWParams{.QuantNormalizationSetSize = threshold}}};
                EXPECT_EQ(TieredFactory::EstimateInitialSize(&tiered_params),
                          tiered_index->getAllocationSize());
                EXPECT_EQ(this->getIsInAccumulationPhase(tiered_index), threshold > 0);
                if (threshold > 0) {
                    // A requested training set takes precedence over a supplied mean.
                    ASSERT_NO_FATAL_FAILURE(
                        this->checkTrainedComponents(tiered_index, std::vector<float>(dim, 0.0f)));
                } else if (supply_mean) {
                    ASSERT_NO_FATAL_FAILURE(
                        this->checkTrainedComponents(tiered_index, supplied_mean));
                }
            }
        }
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, L2MeanTrainingWithBoundedValues) {
    constexpr size_t dim = 65;
    constexpr float values[] = {-1.0f, -0.7f, -0.35f, 0.1f, 0.25f, 0.6f, 1.0f};
    constexpr size_t count = std::size(values);
    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        SCOPED_TRACE(mode);
        VecSimIndexInterface::asyncWriteMode = mode;
        tieredIndexMock mock_thread_pool;
        auto *tiered_index =
            this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_L2, count);
        if (!tiered_index) {
            mock_thread_pool.reset_ctx();
            FAIL() << "FLOAT16 L2 mean training must construct";
        }
        ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
        std::vector<std::vector<TEST_DATA_T>> vectors(count, std::vector<TEST_DATA_T>(dim));
        std::vector<double> sums(dim, 0.0);
        for (size_t v = 0; v < count; ++v) {
            for (size_t d = 0; d < dim; ++d) {
                vectors[v][d] = from_fp32<TEST_DATA_T>(d % 2 ? values[v] : -values[v]);
                sums[d] += this->ToFloat(vectors[v][d]);
            }
        }
        bool mean_checked = false;
        tiered_index->setAfterQuantizationFinalizationHook([&] {
            std::vector<float> mean(dim);
            for (size_t d = 0; d < dim; ++d) {
                mean[d] = static_cast<float>(sums[d] / count);
            }
            this->checkTrainedComponents(tiered_index, mean);
            mean_checked = true;
        });
        const auto verify_queries = [&](size_t inserted_count) {
            for (size_t v = 0; v < inserted_count; ++v) {
                const auto verify = [&](size_t id, double score, size_t) {
                    EXPECT_EQ(id, TypeParam::isMulti() ? v / 2 : v);
                    EXPECT_TRUE(std::isfinite(score));
                    EXPECT_NEAR(score, 0.0, dim * 1e-4);
                };
                runTopKSearchTest(tiered_index, vectors[v].data(), 1, verify);
            }
        };
        for (size_t v = 0; v < count - 1; ++v) {
            ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vectors[v].data(),
                                            TypeParam::isMulti() ? v / 2 : v),
                      1);
        }
        ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
        ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
        ASSERT_TRUE(mock_thread_pool.jobQ.empty());
        verify_queries(count - 1);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vectors.back().data(),
                                        TypeParam::isMulti() ? (count - 1) / 2 : count - 1),
                  1);
        EXPECT_TRUE(mean_checked);
        ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));
        ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
        if (mode == VecSim_WriteAsync) {
            ASSERT_EQ(mock_thread_pool.jobQ.size(), count);
            mock_thread_pool.thread_iteration();
            ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 1);
            verify_queries(count);
            while (!mock_thread_pool.jobQ.empty()) {
                mock_thread_pool.thread_iteration();
            }
        }
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        EXPECT_EQ(this->getBackendIndex(tiered_index)->indexSize(), count);
        EXPECT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
        EXPECT_TRUE(this->getLabelToInsertJobs(tiered_index).empty());
        verify_queries(count);
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8Multi, getDataByLabelDoesNotReportPartialSQ8Label) {
    constexpr size_t dim = 4;
    for (size_t normSetSize : {0, 3}) {
        SCOPED_TRACE(normSetSize);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index =
            this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);
        ASSERT_NE(tiered_index, nullptr);

        TEST_DATA_T first[dim];
        TEST_DATA_T second[dim];
        this->GenerateVectorData(first, dim, 1.0f);
        this->GenerateVectorData(second, dim, 2.0f);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, first, 0), 1);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, second, 0), 1);

        std::vector<std::vector<TEST_DATA_T>> stored;
        if (normSetSize > 0) {
            // Retrieval follows the SQ8 policy during training as well.
            ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
            ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
            tiered_index->getDataByLabel(0, stored);
            EXPECT_TRUE(stored.empty());
            stored.clear();
        }

        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, first, 0), 1);
        ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 3);

        // SQ8 retrieval reports nothing after finalization as well.
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty());
        stored.clear();

        mock_thread_pool.thread_iteration();
        ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 1);
        ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 2);
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty()) << "the two buffered vectors are only part of the label";
        stored.clear();

        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 3);
        ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty());
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8Single, getDataByLabelReportsNothingForSQ8Backend) {
    constexpr size_t dim = 4;
    for (size_t normSetSize : {0, 2}) {
        SCOPED_TRACE(normSetSize);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index =
            this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);
        ASSERT_NE(tiered_index, nullptr);

        TEST_DATA_T vector[dim];
        this->GenerateVectorData(vector, dim, 1.0f);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vector, 0), 1);
        ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

        std::vector<std::vector<TEST_DATA_T>> stored;
        if (normSetSize > 0) {
            // SQ8 retrieval also reports nothing while all vectors are still in FLAT.
            ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
            ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
            tiered_index->getDataByLabel(0, stored);
            EXPECT_TRUE(stored.empty());
            stored.clear();
            ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vector, 1), 1);
        }

        ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty());

        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        ASSERT_TRUE(this->CastToHNSW(tiered_index)->isLabelExists(0));
        ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty());

        // Buffered overwrites follow the same policy as migrated vectors.
        this->GenerateVectorData(vector, dim, 2.0f);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vector, 0), 0);
        tiered_index->getDataByLabel(0, stored);
        EXPECT_TRUE(stored.empty());
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, RunningSumAccuracy) {
    // Verify that runningSumVec correctly accumulates vector values.
    size_t dim = 8;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add vectors and verify running sum.
    std::vector<double> expected_sum(dim, 0.0);
    for (size_t i = 0; i < 5; i++) {
        TEST_DATA_T vec[dim];
        float base = static_cast<float>(i + 1);
        this->GenerateVectorData(vec, dim, base);
        VecSimIndex_AddVector(tiered_index, vec, i);

        // Update expected sum.
        for (size_t d = 0; d < dim; d++) {
            expected_sum[d] += static_cast<double>(this->ToFloat(vec[d]));
        }
    }

    // Verify running sum matches expected.
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    for (size_t d = 0; d < dim; d++) {
        ASSERT_DOUBLE_EQ(this->getRunningSumVec(tiered_index)[d], expected_sum[d])
            << "Mismatch at dimension " << d;
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, QueryDuringAccumulation) {
    // Search should only return flat buffer results during accumulation.
    size_t dim = 8;
    size_t normSetSize = 100; // High threshold so we stay in accumulation.
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add some vectors.
    size_t n = 10;
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), n);
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);

    // Run TopK query.
    TEST_DATA_T query[dim];
    this->GenerateVectorData(query, dim, 0.0f);
    auto *results = VecSimIndex_TopKQuery(tiered_index, query, 5, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    size_t res_count = VecSimQueryReply_Len(results);
    ASSERT_GT(res_count, 0);
    ASSERT_LE(res_count, 5);
    VecSimQueryReply_Free(results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, RangeQueryDuringAccumulation) {
    // Range queries should only use flat buffer during accumulation.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add identical vectors (distance 0 from each other).
    size_t n = 5;
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, 1.0f); // Same vector
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    // Range query with large radius should find all vectors.
    TEST_DATA_T query[dim];
    this->GenerateVectorData(query, dim, 1.0f);
    auto *results = VecSimIndex_RangeQuery(tiered_index, query, 0.01, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    size_t res_count = VecSimQueryReply_Len(results);
    ASSERT_EQ(res_count, n);
    VecSimQueryReply_Free(results);
}

// -------------------------------------------------------------------
// Accumulation Phase Insert/Delete Tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, AddVectorDuringAccumulation) {
    // Vectors added during accumulation go to flat buffer; no jobs submitted to queue.
    size_t dim = 8;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    TEST_DATA_T vec[dim];
    this->GenerateVectorData(vec, dim, 1.5f);
    VecSimIndex_AddVector(tiered_index, vec, 42);

    // Vector should be in flat buffer.
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), 1);

    // Job should be created in labelToInsertJobs but NOT submitted to queue.
    ASSERT_EQ(this->getLabelToInsertJobs(tiered_index).size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, DeleteVectorDuringAccumulation) {
    // Deletion from flat buffer during accumulation subtracts from running sum.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);

    // Add two vectors.
    TEST_DATA_T vec1[dim], vec2[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    this->GenerateVectorData(vec2, dim, 2.0f);
    VecSimIndex_AddVector(tiered_index, vec1, 1);
    VecSimIndex_AddVector(tiered_index, vec2, 2);

    // Record sum before deletion.
    std::vector<float> sum_before(this->getRunningSumVec(tiered_index).begin(),
                                  this->getRunningSumVec(tiered_index).end());

    // Delete label 1.
    VecSimIndex_DeleteVector(tiered_index, 1);

    // Verify running sum was updated (subtracted vec1's values).
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    for (size_t d = 0; d < dim; d++) {
        float expected = sum_before[d] - this->ToFloat(vec1[d]);
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], expected, 1e-3f);
    }

    // Verify index size.
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 1);

    // The deleted label's pending insert job was never submitted to a worker, so it should have
    // been freed directly instead of leaking into invalidJobs.
    ASSERT_EQ(this->getInvalidJobs(tiered_index).size(), 0);
    auto &label_to_insert_jobs = this->getLabelToInsertJobs(tiered_index);
    ASSERT_EQ(label_to_insert_jobs.find(1), label_to_insert_jobs.end());
}

TYPED_TEST(HNSWTieredIndexTestSQ8Single, OverwriteDuringAccumulation) {
    // Vector overwrite should update running sum correctly (only for single-label).
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add vector with label 1.
    TEST_DATA_T vec1[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    VecSimIndex_AddVector(tiered_index, vec1, 1);

    std::vector<float> sum_after_first(this->getRunningSumVec(tiered_index).begin(),
                                       this->getRunningSumVec(tiered_index).end());

    // Overwrite with different vector.
    TEST_DATA_T vec2[dim];
    this->GenerateVectorData(vec2, dim, 3.0f);
    VecSimIndex_AddVector(tiered_index, vec2, 1);

    // Running sum should reflect: sum - vec1 + vec2 (overwrite subtracts old + adds new).
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    for (size_t d = 0; d < dim; d++) {
        float expected = sum_after_first[d] - this->ToFloat(vec1[d]) + this->ToFloat(vec2[d]);
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], expected, 1e-3f);
    }
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

    // Overwrite the same label many more times. Each overwrite during the accumulation phase
    // should reuse the pending insert job in place, rather than leaking it into invalidJobs
    // (nothing collects unsubmitted jobs from there), so the allocation size should stabilize
    // after the first extra overwrite.
    TEST_DATA_T last_vec[dim];
    size_t allocation_size_after_first_extra_overwrite = 0;
    for (size_t i = 0; i < 32; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(4 + i));
        VecSimIndex_AddVector(tiered_index, vec, 1);
        memcpy(last_vec, vec, sizeof(vec));
        if (i == 0) {
            allocation_size_after_first_extra_overwrite = tiered_index->getAllocationSize();
        }
    }
    ASSERT_EQ(tiered_index->getAllocationSize(), allocation_size_after_first_extra_overwrite);

    // No jobs should have leaked into invalidJobs, and only a single pending job (and vector)
    // should remain for the label.
    ASSERT_EQ(this->getInvalidJobs(tiered_index).size(), 0);
    ASSERT_EQ(this->getLabelToInsertJobs(tiered_index).size(), 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    // Running sum should equal the last written vector (the only live vector under the label).
    for (size_t d = 0; d < dim; d++) {
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], this->ToFloat(last_vec[d]), 1e-3f);
    }
}

// -------------------------------------------------------------------
// Backend Index Initialization Tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, TrainingFinishesAtThreshold) {
    // When accumulation reaches normalizationSetSize, backend is initialized.
    size_t dim = 4;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Leave room for a temporary vector without reaching the threshold.
    for (size_t i = 0; i < normSetSize - 2; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize - 2);

    // Exercise an overwrite and an add+delete of a fresh label while still in the accumulation
    // phase, before filling to the threshold, to make sure neither leaks a pending
    // insert job into invalidJobs (nothing collects unsubmitted jobs from there).
    if (!this->getFrontendIndex(tiered_index)->isMultiValue()) {
        // Overwrite an existing label (added at i == 0 above). Overwrite semantics differ for
        // multi-value indexes, so this part is single-only.
        TEST_DATA_T overwrite_vec[dim];
        this->GenerateVectorData(overwrite_vec, dim, static_cast<float>(normSetSize));
        VecSimIndex_AddVector(tiered_index, overwrite_vec, 0);
    }
    TEST_DATA_T fresh_vec[dim];
    labelType fresh_label = normSetSize + 1;
    this->GenerateVectorData(fresh_vec, dim, static_cast<float>(fresh_label));
    VecSimIndex_AddVector(tiered_index, fresh_vec, fresh_label);
    VecSimIndex_DeleteVector(tiered_index, fresh_label);

    // The overwrite doesn't change the flat buffer size, and the add+delete cancels out, so the
    // flat buffer should be back to normSetSize - 2. Nothing should have leaked into invalidJobs.
    ASSERT_EQ(this->getInvalidJobs(tiered_index).size(), 0);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize - 2);

    // Fill to threshold - 1 and verify that jobs are still waiting for the backend.
    TEST_DATA_T vec[dim];
    this->GenerateVectorData(vec, dim, static_cast<float>(normSetSize - 2));
    VecSimIndex_AddVector(tiered_index, vec, normSetSize - 2);
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize - 1);
    ASSERT_TRUE(mock_thread_pool.jobQ.empty());

    // Add the threshold-triggering vector.
    this->GenerateVectorData(vec, dim, static_cast<float>(normSetSize - 1));
    VecSimIndex_AddVector(tiered_index, vec, normSetSize - 1);

    // Accumulation phase should be over.
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));
    // The backend is trained and still empty because the jobs have not run.
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
    // Flat buffer should hold all vectors.
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize);
    // All vectors should have associated insert jobs.
    ASSERT_EQ(this->getLabelToInsertJobs(tiered_index).size(), normSetSize);
    // Still nothing leaked into invalidJobs, and all pending jobs were submitted to the queue.
    ASSERT_EQ(this->getInvalidJobs(tiered_index).size(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), normSetSize);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, TrainingFinalizesExistingComponentsOnce) {
    constexpr size_t dim = 17;
    constexpr size_t count = 3;
    for (auto metric : {VecSimMetric_IP, VecSimMetric_L2, VecSimMetric_Cosine}) {
        SCOPED_TRACE(metric);
        auto mock_thread_pool = tieredIndexMock();
        auto *tiered_index = this->CreateSQ8TieredIndex(mock_thread_pool, dim, metric, count);
        auto *backend = this->CastToHNSW(tiered_index);
        ASSERT_NE(backend, nullptr);
        const auto components = backend->get_components();
        ASSERT_NO_FATAL_FAILURE(
            this->checkTrainedComponents(tiered_index, std::vector<float>(dim, 0.0f)));
        std::vector<float> mean(dim);
        size_t finalizations = 0;
        tiered_index->setBeforeQuantizationFinalizationHook([&] {
            ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
            ASSERT_EQ(backend->indexSize(), 0);
            ASSERT_TRUE(mock_thread_pool.jobQ.empty());
            const auto &sum = this->getRunningSumVec(tiered_index);
            for (size_t d = 0; d < dim; ++d) {
                mean[d] = static_cast<float>(sum[d] / count);
            }
        });
        tiered_index->setAfterQuantizationFinalizationHook([&] {
            ++finalizations;
            EXPECT_FALSE(this->getIsInAccumulationPhase(tiered_index));
            EXPECT_EQ(backend, this->CastToHNSW(tiered_index));
            EXPECT_EQ(components.indexCalculator, backend->get_components().indexCalculator);
            EXPECT_EQ(components.preprocessors, backend->get_components().preprocessors);
            EXPECT_EQ(backend->indexSize(), 0);
            EXPECT_TRUE(mock_thread_pool.jobQ.empty());
            ASSERT_NO_FATAL_FAILURE(this->checkTrainedComponents(tiered_index, mean));
        });
        TEST_DATA_T vec[dim];
        for (size_t i = 0; i < count; ++i) {
            for (size_t d = 0; d < dim; ++d) {
                vec[d] = from_fp32<TEST_DATA_T>(0.05f * float(i + d % 5 + 1));
            }
            ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, i), 1);
        }
        EXPECT_EQ(finalizations, 1);
        while (!mock_thread_pool.jobQ.empty()) {
            mock_thread_pool.thread_iteration();
        }
        // An empty trained graph must not re-enter training, including when write mode changes.
        VecSimIndexInterface::asyncWriteMode = VecSim_WriteInPlace;
        for (size_t i = 0; i < count; ++i) {
            ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, i), 1);
        }
        ASSERT_EQ(backend->indexSize(), 0);
        EXPECT_FALSE(this->getIsInAccumulationPhase(tiered_index));
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, count), 1);
        EXPECT_EQ(backend->indexSize(), 1);
        EXPECT_EQ(finalizations, 1);
        ASSERT_NO_FATAL_FAILURE(this->checkTrainedComponents(tiered_index, mean));
        VecSimIndexInterface::asyncWriteMode = VecSim_WriteAsync;
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, QueryDuringPartialMigration) {
    // SQ8 scores from the flat and backend indexes are not directly comparable. Verify both
    // query types find an exact-match vector that is still in the flat index while migration is
    // in progress. For multi-value indexes, the vector shares a label with the migrated vector
    // to verify duplicate labels are merged into one result.

    size_t dim = 8;
    size_t normSetSize = 3;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i * 10));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Migrate only one threshold vector, leaving the remaining vectors in the flat index.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize - 1);

    // This post-transition vector remains in the flat index while the backend has SQ8 data.
    // In multi-value indexes, reuse the migrated label to exercise deduplication across indexes.
    TEST_DATA_T flat_vec[dim];
    labelType flat_label = TypeParam::isMulti() ? 0 : 100;
    this->GenerateVectorData(flat_vec, dim, static_cast<float>(flat_label));
    VecSimIndex_AddVector(tiered_index, flat_vec, flat_label);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize);

    auto *topk_results = VecSimIndex_TopKQuery(tiered_index, flat_vec, 1, nullptr, BY_SCORE);
    ASSERT_NE(topk_results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(topk_results), 1);
    auto *topk_iterator = VecSimQueryReply_GetIterator(topk_results);
    auto *topk_result = VecSimQueryReply_IteratorNext(topk_iterator);
    ASSERT_EQ(VecSimQueryResult_GetId(topk_result), flat_label);
    VecSimQueryReply_IteratorFree(topk_iterator);
    VecSimQueryReply_Free(topk_results);

    auto *range_results = VecSimIndex_RangeQuery(tiered_index, flat_vec, 0.001, nullptr, BY_SCORE);
    ASSERT_NE(range_results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(range_results), 1);
    auto *range_iterator = VecSimQueryReply_GetIterator(range_results);
    auto *range_result = VecSimQueryReply_IteratorNext(range_iterator);
    ASSERT_EQ(VecSimQueryResult_GetId(range_result), flat_label);
    VecSimQueryReply_IteratorFree(range_iterator);
    VecSimQueryReply_Free(range_results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, NewVectorsAfterAccumulation) {
    // Vectors added after accumulation are submitted to job queue.
    size_t dim = 4;
    size_t normSetSize = 5;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Trigger transition.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    size_t queue_size_before = mock_thread_pool.jobQ.size();

    // Add a new vector after accumulation.
    TEST_DATA_T new_vec[dim];
    this->GenerateVectorData(new_vec, dim, 99.0f);
    VecSimIndex_AddVector(tiered_index, new_vec, 99);

    // New job should be submitted to queue.
    ASSERT_GT(mock_thread_pool.jobQ.size(), queue_size_before);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, DeleteFromBackendAfterAccumulation) {
    // Delete operations work on SQ backend after accumulation.
    size_t dim = 4;
    size_t normSetSize = 5;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Trigger transition.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Execute all jobs to move vectors to HNSW backend.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(tiered_index->indexSize(), normSetSize);

    // Delete a vector (marks it for deletion in HNSW).
    int deleted = VecSimIndex_DeleteVector(tiered_index, 0);
    ASSERT_EQ(deleted, 1);

    // Execute repair jobs, then run swap jobs to physically remove the vector.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    this->callExecuteReadySwapJobs(tiered_index);

    ASSERT_EQ(tiered_index->indexSize(), normSetSize - 1);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, ConcurrentSearchDuringAccumulation) {
    // Parallel searches should work correctly during accumulation.
    size_t dim = 8;
    size_t normSetSize = 1000;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add some vectors.
    size_t n = 50;
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    // Launch parallel searches.
    std::atomic_int successful_searches(0);
    size_t n_threads = 4;
    auto search_fn = [&](size_t thread_id) {
        TEST_DATA_T query[dim];
        this->GenerateVectorData(query, dim, static_cast<float>(thread_id));
        auto *results = VecSimIndex_TopKQuery(tiered_index, query, 5, nullptr, BY_SCORE);
        if (results && VecSimQueryReply_Len(results) > 0) {
            successful_searches++;
        }
        VecSimQueryReply_Free(results);
    };

    std::vector<std::thread> threads;
    for (size_t t = 0; t < n_threads; t++) {
        threads.emplace_back(search_fn, t);
    }
    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(successful_searches, (int)n_threads);
}

// -------------------------------------------------------------------
// Memory & Size Tracking Tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, IndexSizeDuringAccumulation) {
    // indexSize() returns flat buffer size during accumulation (backend is empty).
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    for (size_t i = 0; i < 10; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(tiered_index->indexSize(), 10);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 10);
    ASSERT_NE(this->getBackendIndex(tiered_index), nullptr);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CapacityDuringAccumulation) {
    // indexCapacity() reflects flat buffer capacity during accumulation.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    TEST_DATA_T vec[dim];
    this->GenerateVectorData(vec, dim, 1.0f);
    VecSimIndex_AddVector(tiered_index, vec, 0);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    // Capacity should come from flat buffer (at least DEFAULT_BLOCK_SIZE after first insert).
    ASSERT_GE(tiered_index->indexCapacity(), 1);
    ASSERT_EQ(tiered_index->indexCapacity(),
              this->getFrontendIndex(tiered_index)->indexCapacity() +
                  this->getBackendIndex(tiered_index)->indexCapacity());
}

// -------------------------------------------------------------------
// Edge Cases
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, ZeroAccumulationThreshold) {
    // QuantNormalizationSetSize=0 should skip accumulation phase entirely.
    size_t dim = 4;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, 0 /* normSetSize=0 */);

    // Should NOT be in accumulation phase.
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Adding a vector should immediately submit job to queue.
    TEST_DATA_T vec[dim];
    this->GenerateVectorData(vec, dim, 1.0f);
    VecSimIndex_AddVector(tiered_index, vec, 0);
    ASSERT_GT(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, AllVectorsDeletedBeforeThreshold) {
    // All vectors deleted before reaching threshold - should remain in accumulation.
    size_t dim = 4;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add and delete vectors.
    for (size_t i = 0; i < 5; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    for (size_t i = 0; i < 5; i++) {
        VecSimIndex_DeleteVector(tiered_index, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), 0);

    // Running sum should be approximately zero.
    for (size_t d = 0; d < dim; d++) {
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], 0.0f, 1e-3f);
    }
}

// -------------------------------------------------------------------
// Multi-Label Accumulation
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8Multi, AccumulationMultiLabel) {
    // Multi-label: multiple vectors per label during accumulation.
    size_t dim = 4;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add multiple vectors with the same label.
    labelType shared_label = 42;
    for (size_t i = 0; i < 3; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i + 1));
        VecSimIndex_AddVector(tiered_index, vec, shared_label);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 3);
    // labelToInsertJobs should have 3 jobs for the same label.
    ASSERT_EQ(this->getLabelToInsertJobs(tiered_index).at(shared_label).size(), 3);
}

TYPED_TEST(HNSWTieredIndexTestSQ8Multi, DeleteMultiLabelDuringAccumulation) {
    // Multi-label: deleting one label removes all its vectors and adjusts the sum.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add vectors with different labels.
    TEST_DATA_T vec1[dim], vec2[dim], vec3[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    this->GenerateVectorData(vec2, dim, 2.0f);
    this->GenerateVectorData(vec3, dim, 3.0f);

    VecSimIndex_AddVector(tiered_index, vec1, 10);
    VecSimIndex_AddVector(tiered_index, vec2, 10); // Same label
    VecSimIndex_AddVector(tiered_index, vec3, 20); // Different label

    std::vector<float> sum_before(this->getRunningSumVec(tiered_index).begin(),
                                  this->getRunningSumVec(tiered_index).end());

    // Delete label 10 (should remove both vectors).
    VecSimIndex_DeleteVector(tiered_index, 10);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

    // Running sum should be adjusted by subtracting both vec1 and vec2.
    for (size_t d = 0; d < dim; d++) {
        float expected = sum_before[d] - this->ToFloat(vec1[d]) - this->ToFloat(vec2[d]);
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], expected, 1e-2f);
    }

    // Both pending insert jobs for the deleted label were never submitted to a worker, so they
    // should have been freed directly instead of leaking into invalidJobs.
    ASSERT_EQ(this->getInvalidJobs(tiered_index).size(), 0);
    auto &label_to_insert_jobs = this->getLabelToInsertJobs(tiered_index);
    ASSERT_EQ(label_to_insert_jobs.find(10), label_to_insert_jobs.end());
}

// -------------------------------------------------------------------
// Batch Iterator During Accumulation
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, BatchIteratorDuringAccumulation) {
    // Batch iterator works during accumulation (only flat buffer results).
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    size_t n = 20;
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    TEST_DATA_T query[dim];
    this->GenerateVectorData(query, dim, 0.0f);
    auto *batch_iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
    ASSERT_NE(batch_iterator, nullptr);

    size_t count = 0;
    size_t batches = 0;
    while (VecSimBatchIterator_HasNext(batch_iterator)) {
        auto *batch = VecSimBatchIterator_Next(batch_iterator, 5, BY_SCORE);
        ASSERT_NE(batch, nullptr);
        ASSERT_GT(VecSimQueryReply_Len(batch), 0);
        ASSERT_LE(VecSimQueryReply_Len(batch), 5);
        count += VecSimQueryReply_Len(batch);
        VecSimQueryReply_Free(batch);
        ASSERT_LE(++batches, n);
    }
    EXPECT_EQ(count, n);
    EXPECT_FALSE(VecSimBatchIterator_HasNext(batch_iterator));

    VecSimBatchIterator_Free(batch_iterator);
}

// -------------------------------------------------------------------
// Index Statistics During Accumulation
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, DebugInfoDuringAccumulation) {
    // Debug info during accumulation reflects flat-only state.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    for (size_t i = 0; i < 5; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    ASSERT_EQ(info.commonInfo.indexSize, 5);
}

// -------------------------------------------------------------------
// Write Mode Interactions with Accumulation
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, WriteInPlaceDuringAccumulation) {
    const size_t dim = 4;
    const size_t normSetSize = 6;
    VecSimIndexInterface::asyncWriteMode = VecSim_WriteInPlace;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    TEST_DATA_T vec[dim];
    for (size_t i = 0; i < normSetSize; ++i) {
        ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
        ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), i);
        this->GenerateVectorData(vec, dim, static_cast<float>(i * 10));
        // Multiple pending jobs per label must survive flat-id swaps during migration.
        const labelType label = TypeParam::isMulti() ? i / 2 : i;
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, label), 1);
        ASSERT_TRUE(mock_thread_pool.jobQ.empty());
    }

    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), normSetSize);
    ASSERT_TRUE(this->getLabelToInsertJobs(tiered_index).empty());
    ASSERT_TRUE(this->getInvalidJobs(tiered_index).empty());

    const size_t num_labels = TypeParam::isMulti() ? normSetSize / 2 : normSetSize;
    auto *results = VecSimIndex_TopKQuery(tiered_index, vec, num_labels, nullptr, BY_ID);
    EXPECT_EQ(VecSimQueryReply_Len(results), num_labels);
    auto *iterator = VecSimQueryReply_GetIterator(results);
    for (size_t i = 0; i < num_labels; ++i) {
        auto *result = VecSimQueryReply_IteratorNext(iterator);
        EXPECT_NE(result, nullptr);
        if (result) {
            EXPECT_EQ(VecSimQueryResult_GetId(result), i);
        }
    }
    VecSimQueryReply_IteratorFree(iterator);
    VecSimQueryReply_Free(results);

    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, num_labels), 1);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), normSetSize + 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_TRUE(mock_thread_pool.jobQ.empty());
}

TYPED_TEST(HNSWTieredIndexTestSQ8, SwitchToWriteInPlaceDuringAccumulation) {
    const size_t dim = 4;
    const size_t normSetSize = 4;
    VecSimIndexInterface::asyncWriteMode = VecSim_WriteAsync;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    TEST_DATA_T vec[dim];
    for (size_t i = 0; i < normSetSize - 1; ++i) {
        this->GenerateVectorData(vec, dim, static_cast<float>(i * 10));
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, i), 1);
    }
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1);
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_TRUE(mock_thread_pool.jobQ.empty());

    VecSimIndexInterface::asyncWriteMode = VecSim_WriteInPlace;
    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, 0), 1);
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec, normSetSize - 1), 1);

    ASSERT_TRUE(mock_thread_pool.jobQ.empty());
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), normSetSize);
    ASSERT_TRUE(this->getLabelToInsertJobs(tiered_index).empty());
    ASSERT_TRUE(this->getInvalidJobs(tiered_index).empty());
}

TYPED_TEST(HNSWTieredIndexTestSQ8, WriteInPlaceAfterAccumulation) {
    // After accumulation, WriteInPlace inserts directly to SQ backend.
    size_t dim = 4;
    size_t normSetSize = 5;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Trigger transition.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Execute all pending jobs (submitted after finalizing training).
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    // Switch to write-in-place mode.
    VecSimIndexInterface::asyncWriteMode = VecSim_WriteInPlace;

    // Add vector - should go directly to HNSW backend.
    TEST_DATA_T new_vec[dim];
    this->GenerateVectorData(new_vec, dim, 99.0f);
    VecSimIndex_AddVector(tiered_index, new_vec, 99);

    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), normSetSize + 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
}

// -------------------------------------------------------------------
// Buffer Limit Interactions
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, BufferLimitWithAccumulation) {
    // The accumulation threshold takes precedence over the flat buffer limit.
    size_t dim = 4;
    size_t normSetSize = 100; // High normalization set size.
    size_t buffer_limit = 10; // Small buffer limit.
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP,
                                                    normSetSize, buffer_limit);

    // During accumulation, buffer limit should not trigger direct insert to backend
    // (since backend is not ready).
    for (size_t i = 0; i < 15; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    // Should still be in accumulation phase.
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    // All vectors should be in flat buffer (accumulation overrides buffer limit behavior).
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 15);
}

// -------------------------------------------------------------------
// Quantization Quality Validation
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, QuantizedSearchQuality) {
    // After accumulation, SQ8 backend should produce reasonable search results.
    size_t dim = 16;
    size_t normSetSize = 50;
    size_t n = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add enough vectors to trigger transition and more.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Execute all jobs.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    // Query with the same vector as label 0 - should find label 0 as nearest.
    TEST_DATA_T query[dim];
    this->GenerateVectorData(query, dim, 0.0f);
    auto *results = VecSimIndex_TopKQuery(tiered_index, query, 1, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(results), 1);

    auto it = VecSimQueryReply_GetIterator(results);
    auto *entry = VecSimQueryReply_IteratorNext(it);
    // The closest vector should be label 0 (same as query).
    ASSERT_EQ(VecSimQueryResult_GetId(entry), 0);
    // Distance should be very small (quantization introduces some error).
    ASSERT_LT(VecSimQueryResult_GetScore(entry), 1.0);

    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(results);
}

// -------------------------------------------------------------------
// End-to-end flow tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, FullFlowAsyncInsertAndSearch) {
    // Full end-to-end test: accumulation -> transition -> async insert -> search.
    size_t dim = 8;
    size_t normSetSize = 20;
    size_t total_vectors = 50;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Phase 1: Accumulation.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Phase 2: Post-accumulation inserts.
    for (size_t i = normSetSize; i < total_vectors; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    // Phase 3: Execute all jobs (including accumulation-phase jobs submitted during transition).
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), total_vectors);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), total_vectors);

    // Phase 4: Search.
    TEST_DATA_T query[dim];
    this->GenerateVectorData(query, dim, 0.0f);
    auto *results = VecSimIndex_TopKQuery(tiered_index, query, 10, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(results), 10);
    VecSimQueryReply_Free(results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, updateVectorsAfterAccumulation) {
    // The phase after the quantizer is trained: vectors live in the SQ8 backend, and an update has
    // to work against the quantized elements - which it never compares against, so there is nothing
    // here that a lossy stored form could confuse.
    size_t dim = 4;
    size_t normSetSize = 5;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Trigger the transition, then let the vectors reach the backend.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        this->GenerateVectorData(vec, dim, static_cast<float>(i + 1));
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->indexSize(), normSetSize);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);

    TEST_DATA_T replacement[dim];
    this->GenerateVectorData(replacement, dim, 50.0f);
    ASSERT_EQ(tiered_index->updateVectors(0, replacement, 1), VecSimUpdate_OK);

    // The backend element became a tombstone and the new value is buffered, as for any other
    // backend vector.
    ASSERT_EQ(this->CastToHNSW(tiered_index)->getNumMarkedDeleted(), 1);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), normSetSize);

    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    this->callExecuteReadySwapJobs(tiered_index);
    ASSERT_EQ(tiered_index->indexSize(), normSetSize);
    ASSERT_EQ(this->CastToHNSW(tiered_index)->getNumMarkedDeleted(), 0);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), normSetSize);
    ASSERT_TRUE(this->CastToHNSW(tiered_index)->isLabelExists(0));
    ASSERT_TRUE(this->CastToHNSW(tiered_index)->checkIntegrity().valid_state);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, updateVectorsDuringAccumulation) {
    // Nothing here is compared against the stored vectors, so a quantized index needs no special
    // case - but the update still has to leave the accumulation bookkeeping right, since the
    // running sum that trains the quantizer is maintained per insert and per delete.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    TEST_DATA_T vec1[dim], vec2[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    this->GenerateVectorData(vec2, dim, 5.0f);
    ASSERT_EQ(VecSimIndex_AddVector(tiered_index, vec1, 0), 1);

    ASSERT_EQ(tiered_index->updateVectors(0, vec2, 1), VecSimUpdate_OK);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    // The sum holds the new vector alone: the replaced one was subtracted as it left.
    for (size_t d = 0; d < dim; d++) {
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], this->ToFloat(vec2[d]), 1e-3f);
    }
}

// The in-place reuse fast path (`updateMultiValueInPlace`) writes straight into the HNSW backend
// and skips the running-sum/insert-job bookkeeping that `addVector`/`deleteVector` maintain during
// accumulation - unlike them, it never checks `sqAccumulationState`. It must not run while
// accumulating, even when write mode is in-place; the label has to fall back to the ordinary
// delete-then-add path below it, which does route through both.
TYPED_TEST(HNSWTieredIndexTestSQ8Multi, updateVectorsMultiInPlaceDuringAccumulationStaysInFlat) {
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    VecSim_SetWriteMode(VecSim_WriteInPlace);

    labelType label = 7;
    TEST_DATA_T vec1[dim], vec2[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    this->GenerateVectorData(vec2, dim, 2.0f);
    VecSimIndex_AddVector(tiered_index, vec1, label);
    VecSimIndex_AddVector(tiered_index, vec2, label);
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 2);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);

    TEST_DATA_T replacement[2 * dim];
    this->GenerateVectorData(replacement, dim, 10.0f);
    this->GenerateVectorData(replacement + dim, dim, 20.0f);
    ASSERT_EQ(tiered_index->updateVectors(label, replacement, 2), VecSimUpdate_OK);

    // Still accumulating, and still entirely in FLAT - the backend must never have been touched.
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 2);
    ASSERT_EQ(this->getBackendIndex(tiered_index)->indexSize(), 0);

    // The running sum reflects only the replacement vectors - the originals were subtracted.
    for (size_t d = 0; d < dim; d++) {
        float expected = this->ToFloat(replacement[d]) + this->ToFloat(replacement[dim + d]);
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], expected, 1e-2f);
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, DeleteThenReinsertDuringAccumulation) {
    // Delete and re-insert a vector during accumulation.
    size_t dim = 4;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    // Add vector.
    TEST_DATA_T vec1[dim];
    this->GenerateVectorData(vec1, dim, 1.0f);
    VecSimIndex_AddVector(tiered_index, vec1, 0);

    // Delete it.
    VecSimIndex_DeleteVector(tiered_index, 0);
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 0);

    // Re-insert with different data.
    TEST_DATA_T vec2[dim];
    this->GenerateVectorData(vec2, dim, 5.0f);
    VecSimIndex_AddVector(tiered_index, vec2, 0);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

    // Running sum should only contain vec2's values (vec1 was subtracted, vec2 was added).
    for (size_t d = 0; d < dim; d++) {
        ASSERT_NEAR(this->getRunningSumVec(tiered_index)[d], this->ToFloat(vec2[d]), 1e-3f);
    }
}

// -------------------------------------------------------------------
// Accumulation precision tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, RunningSumPrecision) {
    // Values smaller than the FP32 ULP of a large running sum must not be discarded. Both values
    // are exactly representable in FP32 and FP16, as is their expected sum in FP64.
    size_t dim = 4;
    size_t normSetSize = 100;
    constexpr size_t n_small_vectors = 32;
    constexpr float large_value = 32768.0f;
    constexpr float small_value = 1.0f / 1024.0f;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_IP, normSetSize);

    for (size_t i = 0; i <= n_small_vectors; i++) {
        TEST_DATA_T vec[dim];
        const float value = i == 0 ? large_value : small_value;
        for (size_t d = 0; d < dim; d++) {
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = value;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(value);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    const double expected_sum =
        static_cast<double>(large_value) + n_small_vectors * static_cast<double>(small_value);
    for (size_t d = 0; d < dim; d++) {
        ASSERT_DOUBLE_EQ(this->getRunningSumVec(tiered_index)[d], expected_sum)
            << "Precision loss at dim " << d;
    }
}

// -------------------------------------------------------------------
// Cosine Metric Tests
// -------------------------------------------------------------------

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineAccumulationPhase) {
    // Verify accumulation phase works correctly with Cosine metric.
    // Cosine normalizes vectors before storage, so addToSum must use stored data.
    size_t dim = 8;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    // Add vectors with varying magnitudes (normalization will make them unit vectors).
    for (size_t i = 0; i < normSetSize - 1; i++) {
        TEST_DATA_T vec[dim];
        float scale = static_cast<float>(i + 1); // Different magnitudes
        this->GenerateVectorData(vec, dim, scale);
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), normSetSize - 1);

    // Verify the running sum is computed from STORED (normalized) vectors.
    // After normalization, each stored vector has unit length, so each component
    // should be bounded by [-1, 1]. The sum of N unit vectors has bounded magnitude.
    const auto &running_sum = this->getRunningSumVec(tiered_index);
    float sum_norm_sq = 0.0f;
    for (size_t d = 0; d < dim; d++) {
        sum_norm_sq += running_sum[d] * running_sum[d];
    }
    // The magnitude of the sum of (normSetSize-1) unit vectors is at most (normSetSize-1).
    float sum_norm = std::sqrt(sum_norm_sq);
    ASSERT_LE(sum_norm, static_cast<float>(normSetSize - 1) + 0.1f);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineTransitionAndQuery) {
    // Full flow: accumulation -> transition -> query with Cosine metric.
    size_t dim = 16;
    size_t normSetSize = 20;
    size_t total_vectors = 50;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Phase 1: Fill up to threshold to trigger transition.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        // Create vectors with distinct directions by varying the first component.
        for (size_t d = 0; d < dim; d++) {
            float val = (d == 0) ? static_cast<float>(i + 1) : 1.0f;
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Phase 2: Add more vectors after transition.
    for (size_t i = normSetSize; i < total_vectors; i++) {
        TEST_DATA_T vec[dim];
        for (size_t d = 0; d < dim; d++) {
            float val = (d == 0) ? static_cast<float>(i + 1) : 1.0f;
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }

    // Phase 3: Execute all jobs.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(tiered_index->indexSize(), total_vectors);

    // Phase 4: Query - use same direction as highest-label vector (should be nearest).
    TEST_DATA_T query[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = (d == 0) ? static_cast<float>(total_vectors) : 1.0f;
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            query[d] = val;
        } else {
            query[d] = vecsim_types::FP32_to_FP16(val);
        }
    }

    auto *results = VecSimIndex_TopKQuery(tiered_index, query, 5, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(results), 5);

    // The nearest neighbor should be the vector with highest first-component
    // (most similar direction to query).
    auto it = VecSimQueryReply_GetIterator(results);
    auto *entry = VecSimQueryReply_IteratorNext(it);
    labelType top_label = VecSimQueryResult_GetId(entry);
    double top_score = VecSimQueryResult_GetScore(entry);

    // Score for Cosine is 1 - cosine_similarity. Should be close to 0 for nearest.
    ASSERT_LT(top_score, 0.01);
    // The top result should be one of the vectors with the largest first component.
    // With FP16+SQ8, vectors 48 and 49 are nearly indistinguishable, so allow some slack.
    ASSERT_GE(top_label, total_vectors - 3)
        << "Expected a high-label vector (near-identical direction to query)";

    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8Single, CosineOverwriteDuringAccumulation) {
    // Overwrite during accumulation with Cosine: subtractFromSum must use stored
    // (normalized) data, matching what addToSum accumulated.
    size_t dim = 8;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Add a vector with label 1 (magnitude = ~4).
    TEST_DATA_T vec1[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = static_cast<float>(d + 1) * 0.5f;
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            vec1[d] = val;
        } else {
            vec1[d] = vecsim_types::FP32_to_FP16(val);
        }
    }
    VecSimIndex_AddVector(tiered_index, vec1, 1);

    // Record running sum after first insert.
    std::vector<float> sum_after_first(this->getRunningSumVec(tiered_index).begin(),
                                       this->getRunningSumVec(tiered_index).end());

    // Overwrite label 1 with a completely different vector (different direction).
    TEST_DATA_T vec2[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = (d == 0) ? 10.0f : 0.01f;
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            vec2[d] = val;
        } else {
            vec2[d] = vecsim_types::FP32_to_FP16(val);
        }
    }
    VecSimIndex_AddVector(tiered_index, vec2, 1);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

    // After overwrite: sum should reflect only vec2's stored (normalized) data.
    // Since the running sum = 0 + stored(vec1) - stored(vec1) + stored(vec2) = stored(vec2),
    // the sum should be the normalized form of vec2.
    const auto &running_sum = this->getRunningSumVec(tiered_index);
    float sum_norm_sq = 0.0f;
    for (size_t d = 0; d < dim; d++) {
        sum_norm_sq += running_sum[d] * running_sum[d];
    }
    float sum_norm = std::sqrt(sum_norm_sq);
    // With only one normalized vector in the sum, the norm should be ~1.0.
    ASSERT_NEAR(sum_norm, 1.0f, 0.05f);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineDeleteDuringAccumulation) {
    // Delete during accumulation with Cosine: verify running sum is correctly updated.
    size_t dim = 8;
    size_t normSetSize = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Add two vectors.
    TEST_DATA_T vec1[dim], vec2[dim];
    for (size_t d = 0; d < dim; d++) {
        float v1 = static_cast<float>(d + 1);
        float v2 = static_cast<float>(dim - d);
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            vec1[d] = v1;
            vec2[d] = v2;
        } else {
            vec1[d] = vecsim_types::FP32_to_FP16(v1);
            vec2[d] = vecsim_types::FP32_to_FP16(v2);
        }
    }
    VecSimIndex_AddVector(tiered_index, vec1, 1);
    VecSimIndex_AddVector(tiered_index, vec2, 2);

    // Record sum with both vectors.
    std::vector<float> sum_with_both(this->getRunningSumVec(tiered_index).begin(),
                                     this->getRunningSumVec(tiered_index).end());

    // Delete label 1.
    VecSimIndex_DeleteVector(tiered_index, 1);

    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));
    ASSERT_EQ(this->getFrontendIndex(tiered_index)->indexSize(), 1);

    // After deleting vec1, sum should equal just stored(vec2).
    // stored(vec2) is the normalized version of vec2, so its norm ≈ 1.
    const auto &running_sum = this->getRunningSumVec(tiered_index);
    float sum_norm_sq = 0.0f;
    for (size_t d = 0; d < dim; d++) {
        sum_norm_sq += running_sum[d] * running_sum[d];
    }
    float sum_norm = std::sqrt(sum_norm_sq);
    ASSERT_NEAR(sum_norm, 1.0f, 0.05f);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineSearchAccuracyAfterTransition) {
    // Verify that SQ8+Cosine produces reasonable search accuracy after transition.
    // Compare results ordering against brute-force on the same index.
    size_t dim = 32;
    size_t normSetSize = 30;
    size_t n = 100;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine,
                                                    normSetSize, SIZE_MAX, 16, 200);

    // Insert vectors with different directions.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vec[dim];
        for (size_t d = 0; d < dim; d++) {
            // Create vectors where the i-th vector has a strong d==i%dim component.
            float val = (d == (i % dim)) ? 10.0f : 1.0f / (d + 1.0f);
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Execute all jobs to move vectors to backend.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_EQ(tiered_index->indexSize(), n);

    // Query for a vector similar to label 0 (strong component at dim 0).
    TEST_DATA_T query[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = (d == 0) ? 10.0f : 0.5f / (d + 1.0f);
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            query[d] = val;
        } else {
            query[d] = vecsim_types::FP32_to_FP16(val);
        }
    }

    size_t k = 10;
    auto *results = VecSimIndex_TopKQuery(tiered_index, query, k, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(results), k);

    // Verify results are sorted by increasing score (1 - cosine_sim).
    auto it = VecSimQueryReply_GetIterator(results);
    double prev_score = -1.0;
    while (auto *entry = VecSimQueryReply_IteratorNext(it)) {
        double score = VecSimQueryResult_GetScore(entry);
        ASSERT_GE(score, 0.0);
        ASSERT_LE(score, 2.0); // Cosine distance is in [0, 2]
        ASSERT_GE(score, prev_score);
        prev_score = score;
    }
    VecSimQueryReply_IteratorFree(it);

    // The top-1 result should be label 0 (same strong direction at dim 0).
    it = VecSimQueryReply_GetIterator(results);
    auto *first = VecSimQueryReply_IteratorNext(it);
    // Labels with strong component at dim 0 are: 0, 32, 64, 96
    labelType top_label = VecSimQueryResult_GetId(first);
    ASSERT_TRUE(top_label % dim == 0)
        << "Top result label=" << top_label << " expected a vector with strong dim-0 component";
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineRangeQueryAfterTransition) {
    // Verify range query with Cosine metric after accumulation transition.
    size_t dim = 8;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Insert parallel vectors (identical direction, different magnitudes) - should have distance 0.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        float scale = static_cast<float>(i + 1);
        for (size_t d = 0; d < dim; d++) {
            float val = scale * (d + 1.0f);
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Execute all jobs.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    // Query with same direction - all vectors should be at distance ~0.
    TEST_DATA_T query[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = static_cast<float>(d + 1);
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            query[d] = val;
        } else {
            query[d] = vecsim_types::FP32_to_FP16(val);
        }
    }

    // Range query with small radius should find all parallel vectors.
    auto *results = VecSimIndex_RangeQuery(tiered_index, query, 0.1, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    // All vectors have the same direction, so cosine distance ≈ 0 for all.
    ASSERT_EQ(VecSimQueryReply_Len(results), normSetSize);
    VecSimQueryReply_Free(results);
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineMeanCorrectness) {
    // Verify the mean used for SQ8 quantization is computed from normalized vectors.
    // The mean of N unit vectors with the same direction should be that unit vector itself.
    size_t dim = 4;
    size_t normSetSize = 5;
    size_t addedVectorCount = normSetSize - 1;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Add parallel vectors (same direction [1,2,3,4], different magnitudes).
    for (size_t i = 0; i < addedVectorCount; i++) {
        TEST_DATA_T vec[dim];
        float scale = static_cast<float>(i + 1);
        for (size_t d = 0; d < dim; d++) {
            float val = scale * (d + 1.0f);
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_TRUE(this->getIsInAccumulationPhase(tiered_index));

    // The running sum / addedVectorCount should be the mean of the normalized vectors.
    // Since all vectors have the same direction [1,2,3,4], after normalization they're all
    // [1,2,3,4]/sqrt(1+4+9+16) = [1,2,3,4]/sqrt(30). The mean is the same unit vector.
    const auto &running_sum = this->getRunningSumVec(tiered_index);
    float norm_factor = std::sqrt(1.0f + 4.0f + 9.0f + 16.0f);
    for (size_t d = 0; d < dim; d++) {
        float expected_mean = (d + 1.0f) / norm_factor;
        float actual_mean = running_sum[d] / addedVectorCount;
        ASSERT_NEAR(actual_mean, expected_mean, 0.02f) << "Mean mismatch at dim " << d;
    }
}

TYPED_TEST(HNSWTieredIndexTestSQ8, CosineDeleteAndReinsertAfterTransition) {
    // Delete from Cosine SQ8 backend and reinsert.
    size_t dim = 16;
    size_t normSetSize = 10;
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index =
        this->CreateSQ8TieredIndex(mock_thread_pool, dim, VecSimMetric_Cosine, normSetSize);

    // Trigger transition.
    for (size_t i = 0; i < normSetSize; i++) {
        TEST_DATA_T vec[dim];
        for (size_t d = 0; d < dim; d++) {
            float val = (d == (i % dim)) ? 5.0f : 0.1f;
            if constexpr (std::is_same_v<TEST_DATA_T, float>) {
                vec[d] = val;
            } else {
                vec[d] = vecsim_types::FP32_to_FP16(val);
            }
        }
        VecSimIndex_AddVector(tiered_index, vec, i);
    }
    ASSERT_FALSE(this->getIsInAccumulationPhase(tiered_index));

    // Move all to backend.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->indexSize(), normSetSize);

    // Delete label 0.
    VecSimIndex_DeleteVector(tiered_index, 0);
    // Process repair jobs.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    this->callExecuteReadySwapJobs(tiered_index);
    ASSERT_EQ(tiered_index->indexSize(), normSetSize - 1);

    // Reinsert with same label, different vector.
    TEST_DATA_T new_vec[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = (d == 0) ? 10.0f : 0.01f;
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            new_vec[d] = val;
        } else {
            new_vec[d] = vecsim_types::FP32_to_FP16(val);
        }
    }
    VecSimIndex_AddVector(tiered_index, new_vec, 0);

    // Move to backend.
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->indexSize(), normSetSize);

    // Query for the reinserted vector's direction.
    TEST_DATA_T query[dim];
    for (size_t d = 0; d < dim; d++) {
        float val = (d == 0) ? 1.0f : 0.0f;
        if constexpr (std::is_same_v<TEST_DATA_T, float>) {
            query[d] = val;
        } else {
            query[d] = vecsim_types::FP32_to_FP16(val);
        }
    }

    auto *results = VecSimIndex_TopKQuery(tiered_index, query, 1, nullptr, BY_SCORE);
    ASSERT_NE(results, nullptr);
    ASSERT_EQ(VecSimQueryReply_Len(results), 1);
    auto it = VecSimQueryReply_GetIterator(results);
    auto *entry = VecSimQueryReply_IteratorNext(it);
    // Label 0 has the strongest dim-0 component, should be top result.
    ASSERT_EQ(VecSimQueryResult_GetId(entry), 0);
    VecSimQueryReply_IteratorFree(it);
    VecSimQueryReply_Free(results);
}
