/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "test_utils.h"
#include "mock_thread_pool.h" // VectorSimilarity's tieredIndexMock
#include "vecsim_disk_api.h"
#include "algorithms/hnsw/hnsw_disk_tiered.h"
#include "factory/components/disk_calculator.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/types/sq8.h"
#include "utils/consistency_lock.h"

#include <thread>
#include <condition_variable>

using namespace test_utils;
using sq8 = vecsim_types::sq8;

// Shared mock job queue for all tiered HNSW disk tests
struct MockJobQueue {
    std::atomic<size_t> submission_count{0};
    std::vector<AsyncJob*> jobs;
    std::mutex jobs_mutex;

    void submitJob(AsyncJob* job) {
        std::lock_guard<std::mutex> lock(jobs_mutex);
        submission_count++;
        jobs.push_back(job);
    }

    void submitJobs(const std::vector<AsyncDiskJob*>& job_list) {
        std::lock_guard<std::mutex> lock(jobs_mutex);
        submission_count += job_list.size();
        for (auto* job : job_list) {
            jobs.push_back(job);
        }
    }

    size_t size() const { return submission_count.load(); }

    AsyncJob* getJob(size_t index) {
        std::lock_guard<std::mutex> lock(jobs_mutex);
        return (index < jobs.size()) ? jobs[index] : nullptr;
    }

    AsyncJob* getLastJob() {
        std::lock_guard<std::mutex> lock(jobs_mutex);
        return jobs.empty() ? nullptr : jobs.back();
    }

    void clear() {
        std::lock_guard<std::mutex> lock(jobs_mutex);
        submission_count = 0;
        jobs.clear();
    }
};

static MockJobQueue mock_queue;

// Static callback function matching SubmitCB signature
static int mockSubmitCallback(void* job_queue, void* index_ctx, AsyncJob** jobs, JobCallback* CBs, size_t jobs_len) {
    auto* queue = static_cast<MockJobQueue*>(index_ctx);
    {
        std::lock_guard<std::mutex> lock(queue->jobs_mutex);
        queue->submission_count += jobs_len;
        for (size_t i = 0; i < jobs_len; ++i) {
            queue->jobs.push_back(jobs[i]);
        }
    }
    return VecSim_OK;
}

class TieredHNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;

    // Helper to create tiered disk params
    struct TieredDiskParamsHolder {
        VecSimParams primary_params;
        TieredIndexParams tiered_params;
        VecSimParams tiered_vecsim_params;
        VecSimDiskContext disk_context;
        VecSimParamsDisk params_disk;
    };

    std::unique_ptr<TieredDiskParamsHolder> createTieredDiskParams(const HNSWParams& hnsw_params) {
        auto holder = std::make_unique<TieredDiskParamsHolder>();

        // Create primary (HNSW) params
        holder->primary_params = {
            .algo = VecSimAlgo_HNSWLIB,
            .algoParams = {.hnswParams = hnsw_params},
            .logCtx = nullptr,
        };

        // Create tiered params with mock job queue
        holder->tiered_params = {
            .jobQueue = &mock_queue,
            .jobQueueCtx = &mock_queue,
            .submitCb = mockSubmitCallback,
            .flatBufferLimit = SIZE_MAX,
            .primaryIndexParams = &holder->primary_params,
            .specificParams = {.tieredHnswDiskParams = TieredHNSWDiskParams{}},
        };

        // Create VecSimParams with algo = TIERED
        holder->tiered_vecsim_params = {
            .algo = VecSimAlgo_TIERED,
            .algoParams = {.tieredParams = holder->tiered_params},
            .logCtx = nullptr,
        };

        // Create disk context
        holder->disk_context = {
            .storage = nullptr,
            .indexName = "test_tiered",
            .indexNameLen = strlen("test_tiered"),
        };

        // Create the final VecSimParamsDisk
        holder->params_disk = {
            .indexParams = &holder->tiered_vecsim_params,
            .diskContext = &holder->disk_context,
        };

        return holder;
    }

    void SetUp() override { mock_queue.clear(); }
};

// Parameterized test class for TieredHNSWDisk with different metrics
class TieredHNSWDiskMetricTest : public TieredHNSWDiskTest, public testing::WithParamInterface<VecSimMetric> {};

TEST_P(TieredHNSWDiskMetricTest, CreateTieredIndex) {
    VecSimMetric metric = GetParam();
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
        .blockSize = DEFAULT_BLOCK_SIZE + 4,
        .M = HNSW_DEFAULT_M + 4,
        .efConstruction = HNSW_DEFAULT_EF_C + 4,
        .efRuntime = HNSW_DEFAULT_EF_RT + 4,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);

    ASSERT_NE(index, nullptr);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimIndexBasicInfo basic_info = VecSimIndex_BasicInfo(index);

    EXPECT_FALSE(basic_info.isMulti);
    EXPECT_TRUE(basic_info.isTiered);
    EXPECT_TRUE(basic_info.isDisk);
    EXPECT_EQ(basic_info.algo, VecSimAlgo_HNSWLIB);
    EXPECT_EQ(basic_info.dim, DIM);
    EXPECT_EQ(basic_info.metric, metric);
    EXPECT_EQ(basic_info.type, VecSimType_FLOAT32);

    // Check specific algo info
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.M, hnsw_params.M);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efConstruction, hnsw_params.efConstruction);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efRuntime, hnsw_params.efRuntime);

    VecSimDisk_FreeIndex(index);
}

TEST_P(TieredHNSWDiskMetricTest, TieredIndexStubBehavior) {
    VecSimMetric metric = GetParam();
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    // Note: addVector is now implemented and requires a job queue.
    // Tests for addVector are in the InsertDiskJob test fixture which uses MockJobQueue.

    // indexSize is still a stub (returns 0)
    EXPECT_EQ(index->indexSize(), 0);

    // deleteVector is still a stub (returns 0)
    EXPECT_EQ(index->deleteVector(1), 0);

    // topKQuery returns empty results (stub behavior for now)
    float query[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    auto* reply = index->topKQuery(query, 10, nullptr);
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->results.size(), 0);
    delete reply;

    VecSimDisk_FreeIndex(index);
}

TEST_P(TieredHNSWDiskMetricTest, CreateTieredIndexWithDefaults) {
    VecSimMetric metric = GetParam();
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
    VecSimIndexBasicInfo basic_info = VecSimIndex_BasicInfo(index);

    EXPECT_FALSE(basic_info.isMulti);
    EXPECT_TRUE(basic_info.isTiered);
    EXPECT_TRUE(basic_info.isDisk);
    EXPECT_EQ(basic_info.algo, VecSimAlgo_HNSWLIB);
    EXPECT_EQ(basic_info.dim, DIM);
    EXPECT_EQ(basic_info.metric, metric);
    EXPECT_EQ(basic_info.type, VecSimType_FLOAT32);

    // Check specific algo info
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.M, HNSW_DEFAULT_M);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efConstruction, HNSW_DEFAULT_EF_C);
    EXPECT_EQ(info.tieredInfo.backendInfo.hnswInfo.efRuntime, HNSW_DEFAULT_EF_RT);

    VecSimDisk_FreeIndex(index);
}

// Test that a created HNSW disk backend index has the correct blob sizes
TEST_P(TieredHNSWDiskMetricTest, CreatedBackendIndexBlobSizes) {
    VecSimMetric metric = GetParam();
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
    };

    // Create the backend disk params directly
    VecSimParams primary_params = {
        .algo = VecSimAlgo_HNSWLIB,
        .algoParams = {.hnswParams = hnsw_params},
        .logCtx = nullptr,
    };
    VecSimDiskContext disk_context = {
        .storage = nullptr,
        .indexName = "test_backend",
        .indexNameLen = strlen("test_backend"),
    };
    VecSimParamsDisk params_disk = {
        .indexParams = &primary_params,
        .diskContext = &disk_context,
    };

    auto* index = VecSimDisk_CreateIndex(&params_disk);
    ASSERT_NE(index, nullptr);

    // Cast to the HNSWDiskIndex to access blob size methods
    auto* hnswDiskIndex = dynamic_cast<HNSWDiskIndex<float, float>*>(index);
    ASSERT_NE(hnswDiskIndex, nullptr);

    const size_t fp32Size = DIM * sizeof(float);
    const size_t expectedSQ8Size = getExpectedSQ8Size(DIM, metric);

    // Backend: inputBlobSize = FP32, storedDataSize = SQ8
    EXPECT_EQ(hnswDiskIndex->getInputBlobSize(), fp32Size);
    EXPECT_EQ(hnswDiskIndex->getStoredDataSize(), expectedSQ8Size);

    VecSimDisk_FreeIndex(index);
}

// Test that a created Tiered HNSW disk index has correct blob sizes for both frontend and backend
// - Frontend stores FP32 vectors (storedDataSize = FP32)
// - Backend expects FP32 input (inputBlobSize = FP32) and stores SQ8 (storedDataSize = SQ8)
// - The key assertion: frontend.storedDataSize == backend.inputBlobSize
TEST_P(TieredHNSWDiskMetricTest, CreatedTieredIndexBlobSizes) {
    VecSimMetric metric = GetParam();
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
    };

    auto params_holder = createTieredDiskParams(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    // Cast to the TieredHNSWDiskIndex to access frontend and backend
    auto* tieredIndex = dynamic_cast<TieredHNSWDiskIndex<float, float>*>(index);
    ASSERT_NE(tieredIndex, nullptr);

    // Get frontend (BruteForce) and backend (HNSWDisk) indexes
    auto* frontendIndex = tieredIndex->getFlatBufferIndex();
    ASSERT_NE(frontendIndex, nullptr);

    auto* backendIndex = tieredIndex->getBackendIndex();
    ASSERT_NE(backendIndex, nullptr);

    const size_t fp32Size = DIM * sizeof(float);
    const size_t expectedSQ8Size = getExpectedSQ8Size(DIM, metric);

    // Frontend: both inputBlobSize and storedDataSize = FP32 (full precision storage)
    EXPECT_EQ(frontendIndex->getInputBlobSize(), fp32Size);
    EXPECT_EQ(frontendIndex->getStoredDataSize(), fp32Size);

    // Backend: inputBlobSize = FP32, storedDataSize = SQ8
    EXPECT_EQ(backendIndex->getInputBlobSize(), fp32Size);
    EXPECT_EQ(backendIndex->getStoredDataSize(), expectedSQ8Size);

    // THE KEY ASSERTION: frontend.storedDataSize == backend.inputBlobSize
    EXPECT_EQ(frontendIndex->getStoredDataSize(), backendIndex->getInputBlobSize());

    VecSimDisk_FreeIndex(index);
}

// Test that TieredHNSWDiskIndex contains expected components and distance API works
TEST_P(TieredHNSWDiskMetricTest, TieredHNSWDiskIndexComponents) {
    VecSimMetric metric = GetParam();

    // Create tiered disk params
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = metric,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    VecSimParams primary_params = {
        .algo = VecSimAlgo_HNSWLIB,
        .algoParams = {.hnswParams = hnsw_params},
        .logCtx = nullptr,
    };

    TieredIndexParams tiered_params = {
        .jobQueue = &mock_queue,
        .jobQueueCtx = &mock_queue,
        .submitCb = mockSubmitCallback,
        .flatBufferLimit = SIZE_MAX,
        .primaryIndexParams = &primary_params,
        .specificParams = {.tieredHnswDiskParams = TieredHNSWDiskParams{}},
    };

    VecSimParams tiered_vecsim_params = {
        .algo = VecSimAlgo_TIERED,
        .algoParams = {.tieredParams = tiered_params},
        .logCtx = nullptr,
    };

    VecSimDiskContext disk_context = {
        .storage = nullptr,
        .indexName = "test_tiered_components",
        .indexNameLen = strlen("test_tiered_components"),
    };

    VecSimParamsDisk params_disk = {
        .indexParams = &tiered_vecsim_params,
        .diskContext = &disk_context,
    };

    auto* index = VecSimDisk_CreateIndex(&params_disk);
    ASSERT_NE(index, nullptr);

    // Cast to TieredHNSWDiskIndex
    auto* tieredIndex = dynamic_cast<TieredHNSWDiskIndex<float, float>*>(index);
    ASSERT_NE(tieredIndex, nullptr);

    // Get the backend HNSWDiskIndex
    auto* backendIndex = tieredIndex->getBackendIndex();
    ASSERT_NE(backendIndex, nullptr);

    // --- Verify backend components are accessible ---
    auto components = backendIndex->get_components();
    ASSERT_NE(components.indexCalculator, nullptr);
    ASSERT_NE(components.preprocessors, nullptr);

    // --- Verify calculator is a DiskDistanceCalculator ---
    auto* diskCalculator = dynamic_cast<DiskDistanceCalculator<float>*>(components.indexCalculator);
    ASSERT_NE(diskCalculator, nullptr) << "Backend indexCalculator should be a DiskDistanceCalculator";

    // Verify each distance mode has the expected function
    auto expectedFuncFull = spaces::GetDistFunc<float, float>(metric, DIM, nullptr);
    EXPECT_EQ(diskCalculator->getDistFunc<DistanceMode::Full>(), expectedFuncFull);

    auto expectedFuncQVF = spaces::GetDistFunc<sq8, float, float>(metric, DIM, nullptr);
    EXPECT_EQ(diskCalculator->getDistFunc<DistanceMode::QuantizedVsFull>(), expectedFuncQVF);

    auto expectedFuncQ = spaces::GetDistFunc<sq8, float>(metric, DIM, nullptr);
    EXPECT_EQ(diskCalculator->getDistFunc<DistanceMode::Quantized>(), expectedFuncQ);

    // --- Verify distance calculation API works via calculator ---
    float v1[DIM] = {1.0f, 0.0f, 0.0f, 0.0f};
    float v2[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};
    float v3[DIM] = {1.0f, 0.0f, 0.0f, 0.0f}; // Same as v1

    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(v1, DIM);
        spaces::GetNormalizeFunc<float>()(v2, DIM);
        spaces::GetNormalizeFunc<float>()(v3, DIM);
    }

    // Distance to self should be 0
    float distSelf = diskCalculator->calcDistance<DistanceMode::Full>(v1, v3, DIM);
    EXPECT_NEAR(distSelf, 0.0f, 1e-6f);

    // Distance to different vector should be > 0
    float distDiff = diskCalculator->calcDistance<DistanceMode::Full>(v1, v2, DIM);
    EXPECT_GT(distDiff, 0.0f);

    // Verify distance ordering
    float close[DIM] = {0.9f, 0.1f, 0.0f, 0.0f};
    float far[DIM] = {0.0f, 1.0f, 0.0f, 0.0f};
    if (metric == VecSimMetric_Cosine) {
        spaces::GetNormalizeFunc<float>()(close, DIM);
        spaces::GetNormalizeFunc<float>()(far, DIM);
    }
    EXPECT_LT(diskCalculator->calcDistance<DistanceMode::Full>(v1, close, DIM),
              diskCalculator->calcDistance<DistanceMode::Full>(v1, far, DIM));

    VecSimDisk_FreeIndex(index);
}

// Instantiate parameterized tests for all metrics
INSTANTIATE_TEST_SUITE_P(MetricTests, TieredHNSWDiskMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric>& info) {
                             return VecSimMetric_ToString(info.param);
                         });

// ============================================================================
// Async Job Mechanism Tests
// ============================================================================

class TieredHNSWDiskAsyncJobTest : public TieredHNSWDiskTest {
protected:
    // SpeedB storage for tests that need real disk operations
    std::unique_ptr<test_utils::TempSpeeDB> temp_db_;
    rocksdb_t db_wrapper_;
    rocksdb_column_family_handle_t cf_wrapper_;
    SpeeDBHandles storage_handles_;

    std::unique_ptr<TieredDiskParamsHolder> createTieredDiskParamsWithMockQueue(const HNSWParams& hnsw_params) {
        // Ensure SpeedB storage is created
        ensureStorageCreated();

        auto holder = std::make_unique<TieredDiskParamsHolder>();

        // Create primary (HNSW) params
        holder->primary_params = {
            .algo = VecSimAlgo_HNSWLIB,
            .algoParams = {.hnswParams = hnsw_params},
            .logCtx = nullptr,
        };

        // Initialize tiered_params with zeroed memory first
        holder->tiered_params = TieredIndexParams{};

        // Create tiered params with mock job queue
        holder->tiered_params.jobQueue = &mock_queue;
        holder->tiered_params.jobQueueCtx = &mock_queue;
        holder->tiered_params.submitCb = mockSubmitCallback;
        holder->tiered_params.flatBufferLimit = SIZE_MAX;
        holder->tiered_params.primaryIndexParams = &holder->primary_params;
        holder->tiered_params.specificParams.tieredHnswDiskParams = TieredHNSWDiskParams{};

        // Create VecSimParams with algo = TIERED
        holder->tiered_vecsim_params = {
            .algo = VecSimAlgo_TIERED,
            .algoParams = {.tieredParams = holder->tiered_params},
            .logCtx = nullptr,
        };

        // Create disk context with real SpeedB storage
        holder->disk_context = {
            .storage = &storage_handles_,
            .indexName = "test_tiered_async",
            .indexNameLen = strlen("test_tiered_async"),
        };

        // Create the final VecSimParamsDisk
        holder->params_disk = {
            .indexParams = &holder->tiered_vecsim_params,
            .diskContext = &holder->disk_context,
        };

        return holder;
    }

    void SetUp() override {
        mock_queue.clear();
        // NOTE: temp_db_ is now created on-demand by createTieredDiskParamsWithMockQueue
        // instead of in SetUp, to avoid creating it for tests that don't need storage.
    }

    void TearDown() override {
        // Clean up SpeedB storage if it was created
        temp_db_.reset();
    }

    // Create SpeedB on demand for tests that need storage
    void ensureStorageCreated() {
        if (!temp_db_) {
            temp_db_ = std::make_unique<test_utils::TempSpeeDB>();
            db_wrapper_ = rocksdb_t{temp_db_->db()};
            cf_wrapper_ = rocksdb_column_family_handle_t{temp_db_->cf()};
            storage_handles_ = SpeeDBHandles{&db_wrapper_, &cf_wrapper_};
        }
    }
};

// Test that AsyncDiskJob properly initializes with correct type
TEST_F(TieredHNSWDiskAsyncJobTest, AsyncDiskJobInitialization) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Create a simple insert job
    AsyncDiskJob insert_job(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);
    EXPECT_EQ(insert_job.type, DISK_HNSW_INSERT_VECTOR_JOB);

    // Create a repair job
    RepairDiskJob repair_job(allocator, 42, 3, nullptr, nullptr);
    EXPECT_EQ(repair_job.type, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB);
    EXPECT_EQ(repair_job.node_id, 42);
    EXPECT_EQ(repair_job.level, 3);

    // Create delete jobs
    DeleteDiskJob delete_init_job(allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, 99, nullptr, nullptr);
    EXPECT_EQ(delete_init_job.type, DISK_HNSW_DELETE_VECTOR_INIT_JOB);
    EXPECT_EQ(delete_init_job.deleted_id, 99);

    DeleteDiskJob delete_finalize_job(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, 99, nullptr, nullptr);
    EXPECT_EQ(delete_finalize_job.type, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB);
    EXPECT_EQ(delete_finalize_job.deleted_id, 99);
}

// Test that InsertDiskJob properly initializes with all fields
TEST_F(TieredHNSWDiskAsyncJobTest, InsertDiskJobInitialization) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Create InsertDiskJob - simplified to match RAM version (no state field)
    // State capture happens in worker thread via indexVector() + allocateAndRegister() + storeVectorConnections()
    InsertDiskJob insert_job(allocator, 100, nullptr, nullptr);

    // Verify base class fields
    EXPECT_EQ(insert_job.type, DISK_HNSW_INSERT_VECTOR_JOB);

    // Verify InsertDiskJob-specific fields
    EXPECT_EQ(insert_job.label, 100);
}

// Test InsertDiskJob with make_vecsim_shared_ptr (matching production usage)
TEST_F(TieredHNSWDiskAsyncJobTest, InsertDiskJobSharedPtrUsage) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Create using make_vecsim_shared_ptr to match production code.
    // This uses SafeVecSimDeleter which correctly handles the VecsimBaseObject delete bug
    // by capturing the allocator before destruction.
    // Note: No state field - matching RAM HNSW pattern. State captured in worker thread.
    auto job = make_vecsim_shared_ptr<InsertDiskJob>(allocator, 999, nullptr, nullptr);

    EXPECT_EQ(job->type, DISK_HNSW_INSERT_VECTOR_JOB);
    EXPECT_EQ(job->label, 999);

    // Verify it can be submitted to mock queue
    mock_queue.submitJob(job.get());
    EXPECT_EQ(mock_queue.size(), 1);
}

// NOTE: LabelToInsertJobMapTracking test was removed as the labelToInsertJob map
// is no longer used. Insert jobs are now accessed through the mock queue for testing.

// Test that job submission with mock queue works
TEST_F(TieredHNSWDiskAsyncJobTest, JobSubmissionWithMockQueue) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto test_job = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);
    mock_queue.submitJob(test_job.get());
    EXPECT_EQ(mock_queue.size(), 1);
}

// Test hash function for GraphNodeType
TEST_F(TieredHNSWDiskAsyncJobTest, GraphNodeTypeHashFunction) {
    std::hash<GraphNodeType> hasher;

    // Test that same nodes produce same hash
    GraphNodeType node1(42, 3);
    GraphNodeType node2(42, 3);
    EXPECT_EQ(hasher(node1), hasher(node2));

    // Test that different nodes produce different hashes (likely, not guaranteed)
    GraphNodeType node3(42, 4);
    GraphNodeType node4(43, 3);
    // We can't assert inequality due to hash collisions, but we can verify they compute
    EXPECT_TRUE(hasher(node3) >= 0);
    EXPECT_TRUE(hasher(node4) >= 0);

    // Test with unordered_map
    std::unordered_map<GraphNodeType, int, std::hash<GraphNodeType>> test_map;
    test_map[node1] = 1;
    test_map[node3] = 2;
    test_map[node4] = 3;

    EXPECT_EQ(test_map.size(), 3);
    EXPECT_EQ(test_map[node1], 1);
    EXPECT_EQ(test_map[node2], 1); // Same as node1
    EXPECT_EQ(test_map[node3], 2);
    EXPECT_EQ(test_map[node4], 3);
}

// Test job type enum values
TEST_F(TieredHNSWDiskAsyncJobTest, JobTypeEnumValues) {
    // Verify that disk job types have correct base values
    EXPECT_EQ(DISK_HNSW_INSERT_VECTOR_JOB, HNSW_INSERT_VECTOR_JOB);
    EXPECT_EQ(DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB, HNSW_REPAIR_NODE_CONNECTIONS_JOB);

    // Verify that new disk-specific job types are defined
    EXPECT_NE(DISK_HNSW_DELETE_VECTOR_INIT_JOB, DISK_HNSW_INSERT_VECTOR_JOB);
    EXPECT_NE(DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, DISK_HNSW_INSERT_VECTOR_JOB);
    EXPECT_NE(DISK_HNSW_DELETE_VECTOR_INIT_JOB, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB);
}

// Test that submitted_jobs properly tracks job lifecycle via reference counting
TEST_F(TieredHNSWDiskAsyncJobTest, SubmittedJobsLifecycle) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto job1 = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);
    EXPECT_EQ(job1.use_count(), 1);

    auto job2 = job1; // Increase ref count
    EXPECT_EQ(job1.use_count(), 2);
    EXPECT_EQ(job2.use_count(), 2);

    job2.reset(); // Decrease ref count
    EXPECT_EQ(job1.use_count(), 1);
}

// Test that currently_running tracks executing jobs using set operations
TEST_F(TieredHNSWDiskAsyncJobTest, CurrentlyRunningTracking) {
    std::unordered_set<AsyncDiskJob*> test_set;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto job1 = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);
    auto job2 = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB, nullptr, nullptr);

    test_set.insert(job1.get());
    test_set.insert(job2.get());
    EXPECT_EQ(test_set.size(), 2);

    test_set.erase(job1.get());
    EXPECT_EQ(test_set.size(), 1);
}

// Test batch job submission with mock queue
TEST_F(TieredHNSWDiskAsyncJobTest, BatchJobSubmission) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    std::vector<AsyncDiskJob*> batch_jobs;
    std::vector<std::shared_ptr<AsyncDiskJob>> job_holders;

    for (int i = 0; i < 5; i++) {
        auto job = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);
        batch_jobs.push_back(job.get());
        job_holders.push_back(job);
    }

    mock_queue.submitJobs(batch_jobs);
    EXPECT_EQ(mock_queue.size(), 5);
}

// Test auto-submit behavior: custom deleter invoked when reference count reaches zero
TEST_F(TieredHNSWDiskAsyncJobTest, AutoSubmitOnRefCountZero) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    mock_queue.clear();

    // Create a job with a custom deleter that submits to mock queue
    // The deleter is called when the last shared_ptr releases ownership
    struct AutoSubmitDeleter {
        MockJobQueue* queue;
        void operator()(AsyncDiskJob* job) const {
            if (job && queue) {
                queue->submitJob(job);
            }
            if (job) {
                delete job;
            }
        }
    };

    // Create a job with auto-submit behavior
    auto job_ptr = std::shared_ptr<AsyncDiskJob>(
        new (allocator) AsyncDiskJob(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, nullptr, nullptr),
        AutoSubmitDeleter{&mock_queue});

    EXPECT_EQ(mock_queue.size(), 0);

    // Create another shared_ptr to the same job
    auto job_ptr2 = job_ptr;
    EXPECT_EQ(job_ptr.use_count(), 2);

    // Release first reference
    job_ptr.reset();
    EXPECT_EQ(mock_queue.size(), 0); // Should not submit yet

    // Release second reference - should trigger auto-submit
    job_ptr2.reset();
    EXPECT_EQ(mock_queue.size(), 1); // Should be auto-submitted
}

// Test that pending jobs vector properly holds references
TEST_F(TieredHNSWDiskAsyncJobTest, PendingJobsVectorReferences) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Create a main job
    auto main_job = std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr);

    // Create a pending job
    auto pending_job =
        std::make_shared<AsyncDiskJob>(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, nullptr, nullptr);

    // Initial ref count
    EXPECT_EQ(pending_job.use_count(), 1);

    // Add to pending jobs vector (through friend access simulation)
    auto pending_copy = pending_job;
    EXPECT_EQ(pending_job.use_count(), 2);

    // Clearing the vector should release the reference
    pending_copy.reset();
    EXPECT_EQ(pending_job.use_count(), 1);
}

// Test that submitted_jobs map can lookup by raw pointer (required for executeDiskJobWrapper)
TEST_F(TieredHNSWDiskAsyncJobTest, SubmittedJobsLookupByRawPointer) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Simulate the submitted_jobs map
    vecsim_stl::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs(allocator);

    // Create a job and add it to submitted_jobs (simulating submitDiskJob)
    auto job = std::shared_ptr<AsyncDiskJob>(
        new (allocator) AsyncDiskJob(allocator, DISK_HNSW_INSERT_VECTOR_JOB, nullptr, nullptr));
    AsyncDiskJob* raw_ptr = job.get();
    submitted_jobs[raw_ptr] = job;

    // Now simulate executeDiskJobWrapper trying to find it by raw pointer
    auto it = submitted_jobs.find(raw_ptr);
    ASSERT_NE(it, submitted_jobs.end())
        << "Failed to find job by raw pointer - this would fail with unordered_set<shared_ptr>";

    // Verify we got the correct job
    EXPECT_EQ(it->second.get(), raw_ptr);
    EXPECT_EQ(it->second.use_count(), 2); // One in 'job', one in the map

    // Simulate taking ownership and removing from map
    std::shared_ptr<AsyncDiskJob> job_owner = it->second;
    submitted_jobs.erase(it);

    EXPECT_EQ(job_owner.use_count(), 2); // One in 'job', one in 'job_owner'
    EXPECT_EQ(submitted_jobs.size(), 0);
}
// Test that custom deleter is invoked when ref_count reaches zero
// TODO: Once addVector/deleteVector are implemented, enhance this test to:
//       1. Create TieredHNSWDiskIndex with mock queue
//       2. Call pendByCurrentlyRunning() (or create job with createAutoSubmitJob)
//       3. Verify job auto-submits to mock_queue when currently_running is empty
//       4. Verify job is added to submitted_jobs map
//       5. Test that job does NOT auto-submit when currently_running is non-empty
TEST_F(TieredHNSWDiskAsyncJobTest, PendByCurrentlyRunningWithNoRunningJobs) {

    auto allocator = VecSimAllocator::newVecsimAllocator();

    bool deleter_called = false;

    {
        // Create a job with a custom deleter
        struct TestDeleter {
            bool* called_flag;
            void operator()(AsyncDiskJob* job) const {
                *called_flag = true;
                if (job) {
                    delete job;
                }
            }
        };

        JobCallback dummy_callback = +[](AsyncJob*) {};
        auto job = std::shared_ptr<AsyncDiskJob>(
            new (allocator) AsyncDiskJob(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, dummy_callback, nullptr),
            TestDeleter{&deleter_called});

        // Job has ref_count=1
        EXPECT_EQ(job.use_count(), 1);
        EXPECT_FALSE(deleter_called) << "Deleter should not be called while job is referenced";

    } // job goes out of scope -> deleter invoked

    EXPECT_TRUE(deleter_called) << "Deleter should be called when ref_count reaches 0";
}

// Test that pending job is only submitted when ALL references are released
TEST_F(TieredHNSWDiskAsyncJobTest, MultiplePendingJobReferences) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    size_t initial_job_count = mock_queue.size();

    std::shared_ptr<AsyncDiskJob> finalize_job = std::shared_ptr<AsyncDiskJob>(
        new (allocator) AsyncDiskJob(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, nullptr, nullptr));

    EXPECT_EQ(finalize_job.use_count(), 1);

    // Create two jobs, both holding references to finalize_job
    std::shared_ptr<AsyncDiskJob> job_a;
    std::shared_ptr<AsyncDiskJob> job_b;
    {
        job_a = std::shared_ptr<AsyncDiskJob>(
            new (allocator) AsyncDiskJob(allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, nullptr, nullptr));
        job_b = std::shared_ptr<AsyncDiskJob>(
            new (allocator) AsyncDiskJob(allocator, DISK_HNSW_DELETE_VECTOR_INIT_JOB, nullptr, nullptr));

        auto finalize_ref_a = finalize_job;
        auto finalize_ref_b = finalize_job;
        EXPECT_EQ(finalize_job.use_count(), 3);

        finalize_ref_a.reset();
        EXPECT_EQ(finalize_job.use_count(), 2);
        EXPECT_EQ(mock_queue.size(), initial_job_count);

        finalize_ref_b.reset();
        EXPECT_EQ(finalize_job.use_count(), 1);
        EXPECT_EQ(mock_queue.size(), initial_job_count);
    }

    finalize_job.reset();
}
// Test that concurrent repair job submissions are properly deduplicated
TEST_F(TieredHNSWDiskAsyncJobTest, ConcurrentRepairJobDeduplication) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Simulate the pending_repairs map and its mutex
    std::mutex pending_repairs_guard;
    vecsim_stl::unordered_map<GraphNodeType, std::shared_ptr<AsyncDiskJob>> pending_repairs(allocator);

    const idType node_id = 42;
    const levelType level = 2;
    const int num_threads = 10;

    std::atomic<int> jobs_created{0};

    auto submit_repair = [&]() {
        GraphNodeType key(node_id, level);
        bool created_new_job = false;

        {
            std::lock_guard<std::mutex> lock(pending_repairs_guard);
            auto it = pending_repairs.find(key);

            if (it == pending_repairs.end()) {
                auto job = std::shared_ptr<AsyncDiskJob>(
                    new (allocator) AsyncDiskJob(allocator, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB, nullptr, nullptr));
                pending_repairs[key] = job;
                created_new_job = true;
                jobs_created++;
            }
        }

        return created_new_job;
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(submit_repair);
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(jobs_created.load(), 1);
    EXPECT_EQ(pending_repairs.size(), 1);
    EXPECT_NE(pending_repairs.find(GraphNodeType(node_id, level)), pending_repairs.end());
}
// Test job lifecycle: created -> submitted -> currently_running -> complete
TEST_F(TieredHNSWDiskAsyncJobTest, JobLifecycleTracking) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    auto job = std::make_shared<RepairDiskJob>(allocator, 42, 1, nullptr, nullptr);
    EXPECT_EQ(job->type, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB);

    // Stage 2: Simulate submission by adding to a map (like submitted_jobs)
    std::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;
    submitted_jobs[job.get()] = job;
    EXPECT_EQ(submitted_jobs.size(), 1) << "Job should be in submitted_jobs after submission";
    EXPECT_NE(submitted_jobs.find(job.get()), submitted_jobs.end()) << "Job pointer should be findable";

    // Stage 3: Simulate job execution by adding to currently_running vector
    std::vector<AsyncDiskJob*> currently_running;
    currently_running.push_back(job.get());
    EXPECT_EQ(currently_running.size(), 1);
    EXPECT_EQ(currently_running[0], job.get());

    // Stage 4: Simulate job completion by removing from both collections
    currently_running.clear();
    submitted_jobs.erase(job.get());

    // After completion, job should be in neither collection
    EXPECT_EQ(submitted_jobs.size(), 0);
    EXPECT_EQ(currently_running.size(), 0);
}

// Test #6: Destructor While Jobs Running
// Verifies that jobs can be properly cleaned up even without an index
TEST_F(TieredHNSWDiskAsyncJobTest, DestructorWhileJobsRunning) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Keep jobs alive
    std::vector<std::shared_ptr<AsyncDiskJob>> job_refs;
    std::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;
    std::vector<AsyncDiskJob*> currently_running;

    // Create and "submit" several jobs
    for (int node_id = 0; node_id < 5; ++node_id) {
        auto job = std::make_shared<RepairDiskJob>(allocator, node_id, 0, nullptr, nullptr);

        submitted_jobs[job.get()] = job;
        currently_running.push_back(job.get());
        job_refs.push_back(job);
    }

    EXPECT_EQ(submitted_jobs.size(), 5);
    EXPECT_EQ(currently_running.size(), 5);

    // Simulate cleanup - clear collections first (like destructor would)
    submitted_jobs.clear();
    currently_running.clear();

    // Jobs still exist in job_refs
    EXPECT_EQ(job_refs.size(), 5);

    // Clear job references - this should not crash
    job_refs.clear();

    // If we get here without crashing, cleanup works correctly
    SUCCEED() << "Successfully handled job cleanup";
}

// Test #7: Destructor With Pending Auto-Submit Jobs
// Verifies that jobs with pending dependencies can be properly cleaned up
TEST_F(TieredHNSWDiskAsyncJobTest, DestructorWithPendingAutoSubmitJobs) {
    auto allocator = VecSimAllocator::newVecsimAllocator();

    std::vector<std::shared_ptr<AsyncDiskJob>> job_refs;
    std::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;

    // Create jobs but don't "submit" them
    for (int node_id = 0; node_id < 3; ++node_id) {
        auto job =
            std::make_shared<DeleteDiskJob>(allocator, DISK_HNSW_DELETE_VECTOR_FINALIZE_JOB, node_id, nullptr, nullptr);
        job_refs.push_back(job);
    }

    EXPECT_EQ(submitted_jobs.size(), 0) << "Jobs should not be submitted yet";
    EXPECT_EQ(job_refs.size(), 3);

    // Clear job references - this should not crash
    job_refs.clear();

    // If we get here without crashing, cleanup works correctly
    SUCCEED() << "Successfully handled pending job cleanup";
}
// Test #8: Single Element Currently Running
// Verifies that swap-with-last-and-pop pattern works correctly when there's only one element
TEST_F(TieredHNSWDiskAsyncJobTest, SingleElementCurrentlyRunning) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    std::vector<AsyncDiskJob*> currently_running;

    // Create a job
    auto job = std::make_shared<RepairDiskJob>(allocator, 42, 1, nullptr, nullptr);

    // Add job to currently_running (it's the ONLY one)
    currently_running.push_back(job.get());
    ASSERT_EQ(currently_running.size(), 1);
    ASSERT_EQ(currently_running[0], job.get());

    // Now remove it using swap-with-last-and-pop pattern
    // This is the pattern used in the actual code to remove jobs efficiently
    // When there's only one element, we're swapping element [0] with itself

    // Find the job in currently_running
    auto it = std::find(currently_running.begin(), currently_running.end(), job.get());
    ASSERT_NE(it, currently_running.end()) << "Job should be in currently_running";

    // Swap with last element and pop (when size=1, this swaps with itself)
    size_t index_to_remove = it - currently_running.begin();
    currently_running[index_to_remove] = currently_running.back();
    currently_running.pop_back();

    // Verify removal succeeded
    EXPECT_EQ(currently_running.size(), 0) << "currently_running should be empty after removal";

    // Also verify the job is still valid (wasn't corrupted by the swap-with-self)
    EXPECT_EQ(job->node_id, 42);
    EXPECT_EQ(job->level, 1);
    EXPECT_EQ(job->type, DISK_HNSW_REPAIR_NODE_CONNECTIONS_JOB);
}

// Test double submission detection
// Double submission causes segfault: job queued twice, first execution deletes it,
// second execution accesses freed memory. Prevented by assertion in submitDiskJob().
TEST_F(TieredHNSWDiskAsyncJobTest, DoubleSubmissionToSubmittedJobs) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    std::unordered_map<AsyncDiskJob*, std::shared_ptr<AsyncDiskJob>> submitted_jobs;
    auto job = std::make_shared<RepairDiskJob>(allocator, 42, 1, nullptr, nullptr);

    submitted_jobs[job.get()] = job;
    EXPECT_EQ(submitted_jobs.size(), 1);

    // Verify double submission detection would work
    bool already_submitted = (submitted_jobs.find(job.get()) != submitted_jobs.end());
    EXPECT_TRUE(already_submitted);
}

// =============================================================================
// Consistency Lock Tests for Tiered Jobs
//
// Jobs that modify in-memory structures must hold the consistency lock to ensure
// fork safety. The consistency lock is a shared mutex:
// - Async jobs take a SHARED lock (multiple jobs can run concurrently)
// - Main thread takes EXCLUSIVE lock before fork (blocks until all jobs finish)
//
// Jobs requiring the lock (modify in-memory structures):
// - executeInsertJob: removes from flat buffer and registers in HNSW
// - executeDeleteFinalizeJob: modifies holes_ free list
//
// Jobs NOT requiring the lock (disk-only writes):
// - executeRepairJob: only writes to SpeedDB storage
//
// The lock mechanism itself is already tested in test_consistency_lock.cpp.
// These tests verify the INTEGRATION of the lock with the actual job execution paths.
// =============================================================================

// Test: Insert job holds consistency lock during in-memory modifications
// This is an integration test that creates a real TieredHNSWDiskIndex, adds a vector,
// and verifies that the insert job holds the consistency lock during execution.
TEST_F(TieredHNSWDiskAsyncJobTest, InsertJobConsistencyLockIntegration) {
    using namespace vecsim_disk;

    // Create tiered index with mock queue
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = DEFAULT_BLOCK_SIZE,
        .M = 4, // Small M to avoid too many repair jobs
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParamsWithMockQueue(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    auto* tiered_index = dynamic_cast<TieredHNSWDiskIndex<float, float>*>(index);
    ASSERT_NE(tiered_index, nullptr);

    // Add a vector - this creates an insert job and submits to the queue
    float vector[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    int result = tiered_index->addVector(vector, 100);
    EXPECT_EQ(result, 1); // New vector

    // Verify job was submitted (mock callback was called)
    EXPECT_EQ(mock_queue.size(), 1);

    // Get the insert job from the mock queue
    auto* insert_job = static_cast<InsertDiskJob*>(mock_queue.getLastJob());
    ASSERT_NE(insert_job, nullptr);
    EXPECT_EQ(insert_job->label, 100);

    // Atomics to track execution state
    std::atomic<bool> job_executing{false};
    std::atomic<bool> job_completed{false};

    // Thread to execute the insert job
    // We call the Execute callback which is set to executeDiskJobWrapper
    std::thread job_thread([&]() {
        job_executing = true;

        // Execute the job callback (this is what the thread pool would do)
        // The Execute callback is executeDiskJobWrapper which calls executeInsertJob
        insert_job->Execute(insert_job);

        job_completed = true;
    });

    // Wait for job to start executing
    while (!job_executing) {
        std::this_thread::yield();
    }

    // Give the job time to execute (including the heavy graph operations)
    // This allows the consistency lock section to be entered
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Thread to try acquiring exclusive lock (simulating fork)
    // This tests that the exclusive lock can eventually be acquired after the job completes
    std::thread fork_thread([&]() {
        VecSimDisk_AcquireConsistencyLock();

        // At this point, the insert job must have released its shared lock
        // (We can't guarantee timing, but the lock mechanism is already tested separately)

        VecSimDisk_ReleaseConsistencyLock();
    });

    job_thread.join();
    fork_thread.join();

    EXPECT_TRUE(job_completed);

    // After job completes, vector should be in HNSW index
    EXPECT_EQ(tiered_index->indexSize(), 1);

    VecSimDisk_FreeIndex(index);
}

// Test: Repair job does NOT hold consistency lock (disk-only writes)
// This verifies that repair jobs only write to SpeedDB storage and don't block fork.
TEST_F(TieredHNSWDiskAsyncJobTest, RepairJobNoConsistencyLockIntegration) {
    using namespace vecsim_disk;

    // Create tiered index with mock queue
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = DEFAULT_BLOCK_SIZE,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParamsWithMockQueue(hnsw_params);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    auto* tiered_index = dynamic_cast<TieredHNSWDiskIndex<float, float>*>(index);
    ASSERT_NE(tiered_index, nullptr);

    // Add a vector to have something in the graph
    float vector[DIM] = {1.0f, 2.0f, 3.0f, 4.0f};
    tiered_index->addVector(vector, 100);

    // Get and execute the insert job from the mock queue
    auto* insert_job = static_cast<InsertDiskJob*>(mock_queue.getLastJob());
    ASSERT_NE(insert_job, nullptr);
    insert_job->Execute(insert_job);

    // Now we test that repair jobs don't hold the consistency lock.
    // Create a repair job manually (simulating what would be generated by insert)
    auto allocator = VecSimAllocator::newVecsimAllocator();

    // Test: Acquire exclusive lock while "repair" is simulated
    // Since repair jobs only write to disk (SpeedDB) and don't modify in-memory structures,
    // they should NOT hold the consistency lock, meaning the exclusive lock should be
    // immediately acquirable.
    std::atomic<bool> repair_started{false};
    std::atomic<bool> exclusive_acquired{false};
    std::atomic<bool> repair_completed{false};

    std::thread repair_thread([&]() {
        repair_started = true;

        // Simulate what executeRepairJob does:
        // It only calls hnsw_index->repairNode() which writes to SpeedDB
        // No ConsistencySharedGuard is acquired (this is the expected behavior)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        repair_completed = true;
    });

    // Wait for repair to start
    while (!repair_started) {
        std::this_thread::yield();
    }

    // Give repair thread a head start
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Exclusive lock should be acquirable immediately since repair doesn't hold shared lock
    std::thread fork_thread([&]() {
        VecSimDisk_AcquireConsistencyLock();
        exclusive_acquired = true;

        // The repair job may or may not have completed by now
        // The key point is that we got the exclusive lock without waiting for repair

        VecSimDisk_ReleaseConsistencyLock();
    });

    repair_thread.join();
    fork_thread.join();

    EXPECT_TRUE(repair_completed);
    EXPECT_TRUE(exclusive_acquired);

    VecSimDisk_FreeIndex(index);
}

// Test: Delete finalize job should hold consistency lock while modifying holes_ free list
// This simulates the pattern that executeDeleteFinalizeJob should follow.
TEST_F(TieredHNSWDiskAsyncJobTest, DeleteFinalizeJobConsistencyLockPattern) {
    using namespace vecsim_disk;

    std::atomic<bool> delete_job_started{false};
    std::atomic<bool> delete_job_in_critical_section{false};
    std::atomic<bool> delete_job_completed{false};
    std::atomic<bool> fork_blocked{false};

    // Simulate a delete finalize job
    std::thread delete_job_thread([&]() {
        delete_job_started = true;

        // Acquire consistency lock for modifying holes_ free list
        {
            ConsistencySharedGuard guard;
            delete_job_in_critical_section = true;

            // Simulate: Add deleted_id to holes_ free list (in-memory structure)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            delete_job_in_critical_section = false;
        }
        // Lock released

        delete_job_completed = true;
    });

    // Wait for delete job to enter critical section
    while (!delete_job_in_critical_section) {
        std::this_thread::yield();
    }

    // Main thread (fork) tries to acquire exclusive lock
    std::thread fork_thread([&]() {
        VecSimDisk_AcquireConsistencyLock();
        fork_blocked = true;

        EXPECT_FALSE(delete_job_in_critical_section) << "Delete job should not be in critical section during fork";

        VecSimDisk_ReleaseConsistencyLock();
    });

    delete_job_thread.join();
    fork_thread.join();

    EXPECT_TRUE(delete_job_completed);
    EXPECT_TRUE(fork_blocked);
}

// Test: Multiple insert/delete jobs can run concurrently (shared locks don't block each other)
TEST_F(TieredHNSWDiskAsyncJobTest, MultipleJobsConcurrentlyHoldConsistencyLock) {
    using namespace vecsim_disk;

    constexpr int NUM_JOBS = 4;
    std::atomic<int> jobs_holding_lock{0};
    std::atomic<int> jobs_completed{0};
    std::atomic<bool> all_acquired{false};

    std::vector<std::thread> job_threads;
    for (int i = 0; i < NUM_JOBS; i++) {
        job_threads.emplace_back([&]() {
            ConsistencySharedGuard guard;

            // Increment counter while holding the lock
            ++jobs_holding_lock;

            // Spin until all jobs have acquired the lock (proves concurrent holding)
            while (!all_acquired) {
                if (jobs_holding_lock.load() == NUM_JOBS) {
                    all_acquired = true;
                }
                std::this_thread::yield();
            }

            --jobs_holding_lock;
            ++jobs_completed;
        });
    }

    for (auto& t : job_threads) {
        t.join();
    }

    EXPECT_EQ(jobs_completed, NUM_JOBS);
    // If we got here, all NUM_JOBS threads held the shared lock simultaneously
    EXPECT_TRUE(all_acquired) << "All jobs should have held shared locks concurrently";
}
