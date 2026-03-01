/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * @file test_hnsw_disk_tiered_throttle.cpp
 * @brief Tests for throttle callback invocation in TieredHNSWDiskIndex.
 *
 * These tests verify that VecSimDisk_InvokeEnableThrottle() and
 * VecSimDisk_InvokeDisableThrottle() are called at the correct times:
 * - Enable: when flat buffer reaches flatBufferLimit (after addVector)
 * - Disable: when flat buffer drops below limit (after executeInsertJob or deleteVector)
 *
 * Design doc: docs/design/PR2_throttle_callback_invocation.md
 */

#include "gtest/gtest.h"
#include "test_utils.h"
#include "tests_utils.h"    // VectorSimilarity test utilities (populate_float_vec, etc.)
#include "mock_job_queue.h" // Shared mock job queue
#include "vecsim_disk_api.h"
#include "throttle.h"
#include "algorithms/hnsw/hnsw_disk_tiered.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace test_utils;

// ============================================================================
// Mock Throttle Callbacks
// ============================================================================

// Global counters for mock callbacks - use atomics for thread safety
static std::atomic<int> g_enableCount{0};
static std::atomic<int> g_disableCount{0};

extern "C" int MockEnableThrottle() {
    g_enableCount.fetch_add(1, std::memory_order_seq_cst);
    return 0;
}

extern "C" int MockDisableThrottle() {
    g_disableCount.fetch_add(1, std::memory_order_seq_cst);
    return 0;
}

// Helper class to capture counts before test and verify deltas after.
// This handles the case where other modules may also use the throttle API.
class ThrottleCountSnapshot {
    int enable_before_;
    int disable_before_;

public:
    ThrottleCountSnapshot()
        : enable_before_(g_enableCount.load(std::memory_order_seq_cst)),
          disable_before_(g_disableCount.load(std::memory_order_seq_cst)) {}

    int enableDelta() const { return g_enableCount.load(std::memory_order_seq_cst) - enable_before_; }
    int disableDelta() const { return g_disableCount.load(std::memory_order_seq_cst) - disable_before_; }
    bool isBalanced() const { return enableDelta() == disableDelta(); }
};

// Shared mock job queue for throttle tests
static MockJobQueue mock_queue;

// ============================================================================
// Test Fixture
// ============================================================================

class TieredHNSWDiskThrottleTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;
    static constexpr size_t FLAT_BUFFER_LIMIT = 10;

    // SpeedB storage for tests that need real disk operations
    std::unique_ptr<test_utils::TempSpeeDB> temp_db_;
    rocksdb_t db_wrapper_;
    rocksdb_column_family_handle_t cf_wrapper_;
    SpeeDBHandles storage_handles_;

    // Helper to create tiered disk params with throttle test configuration
    struct TieredDiskParamsHolder {
        VecSimParams primary_params;
        TieredIndexParams tiered_params;
        VecSimParams tiered_vecsim_params;
        VecSimDiskContext disk_context;
        VecSimParamsDisk params_disk;
    };

    std::unique_ptr<TieredDiskParamsHolder> createTieredDiskParams(const HNSWParams& hnsw_params,
                                                                   size_t flatBufferLimit = FLAT_BUFFER_LIMIT) {
        ensureStorageCreated();

        auto holder = std::make_unique<TieredDiskParamsHolder>();

        holder->primary_params = {
            .algo = VecSimAlgo_HNSWLIB,
            .algoParams = {.hnswParams = hnsw_params},
            .logCtx = nullptr,
        };

        holder->tiered_params = {
            .jobQueue = &mock_queue,
            .jobQueueCtx = &mock_queue,
            .submitCb = mockSubmitCallback,
            .flatBufferLimit = flatBufferLimit,
            .primaryIndexParams = &holder->primary_params,
            .specificParams = {.tieredHnswDiskParams = TieredHNSWDiskParams{}},
        };

        holder->tiered_vecsim_params = {
            .algo = VecSimAlgo_TIERED,
            .algoParams = {.tieredParams = holder->tiered_params},
            .logCtx = nullptr,
        };

        holder->disk_context = {
            .storage = &storage_handles_,
            .indexName = "test_throttle",
            .indexNameLen = strlen("test_throttle"),
        };

        holder->params_disk = {
            .indexParams = &holder->tiered_vecsim_params,
            .diskContext = &holder->disk_context,
        };

        return holder;
    }

    void SetUp() override {
        // Set mock throttle callbacks
        VecSimDisk_SetThrottleCallbacks(MockEnableThrottle, MockDisableThrottle);
        mock_queue.clear();
    }

    void TearDown() override {
        // Clean up SpeedB storage
        temp_db_.reset();
    }

    void ensureStorageCreated() {
        if (!temp_db_) {
            temp_db_ = std::make_unique<test_utils::TempSpeeDB>();
            db_wrapper_ = rocksdb_t{temp_db_->db()};
            cf_wrapper_ = rocksdb_column_family_handle_t{temp_db_->cf()};
            storage_handles_ = SpeeDBHandles{&db_wrapper_, &cf_wrapper_};
        }
    }
};

// ============================================================================
// Core Functionality Tests
// ============================================================================

// Test: Enable is called when buffer fills, disable when space is freed via insert job
TEST_F(TieredHNSWDiskThrottleTest, ThrottleEnableAndDisableLifecycle) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = 1024,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params, FLAT_BUFFER_LIMIT);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    ThrottleCountSnapshot snapshot;

    // Step 1: Add vectors up to limit - enable should be called exactly once when size == limit
    for (size_t i = 0; i < FLAT_BUFFER_LIMIT; i++) {
        float vector[DIM];
        populate_float_vec(vector, DIM, static_cast<int>(i));
        int result = index->addVector(vector, static_cast<labelType>(i));
        EXPECT_EQ(result, 1);
    }

    // Enable called exactly once when buffer became full
    EXPECT_EQ(snapshot.enableDelta(), 1);
    // Disable not called yet (no jobs executed)
    EXPECT_EQ(snapshot.disableDelta(), 0);
    // Jobs were submitted but not executed
    EXPECT_EQ(mock_queue.size(), FLAT_BUFFER_LIMIT);

    // Step 2: Execute one insert job - moves vector from flat to HNSW, frees space
    AsyncJob* first_job = mock_queue.takeJob(0);
    ASSERT_NE(first_job, nullptr);
    first_job->Execute(first_job);

    // Disable called exactly once when size crossed from limit to limit-1
    EXPECT_EQ(snapshot.disableDelta(), 1);
    // Balance restored
    EXPECT_TRUE(snapshot.isBalanced());

    // Step 3: Execute remaining jobs - no more disable calls (already below threshold)
    mock_queue.executeAll();

    // Still balanced - no additional enable/disable calls
    EXPECT_EQ(snapshot.enableDelta(), 1);
    EXPECT_EQ(snapshot.disableDelta(), 1);
    EXPECT_TRUE(snapshot.isBalanced());

    VecSimDisk_FreeIndex(index);
}

// Test: Deleting a vector from HNSW (not from flat buffer) doesn't trigger throttle callbacks
// This verifies that only flat buffer operations affect throttling.
TEST_F(TieredHNSWDiskThrottleTest, DeleteFromHNSWNoThrottleChange) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = 1024,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params, FLAT_BUFFER_LIMIT);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    // Step 1: Add vectors to fill buffer (triggers enable)
    for (size_t i = 0; i < FLAT_BUFFER_LIMIT; i++) {
        float vector[DIM];
        populate_float_vec(vector, DIM, static_cast<int>(i));
        index->addVector(vector, static_cast<labelType>(i));
    }

    // Step 2: Execute all jobs - moves vectors from flat to HNSW
    mock_queue.executeAll();

    // Take snapshot after setup is complete
    ThrottleCountSnapshot snapshot;

    // Step 3: Delete a vector that's now in HNSW (not in flat buffer)
    // Note: deleteVector is currently a TODO stub, but this test documents expected behavior
    int delete_result = index->deleteVector(0);

    // No throttle calls should occur - HNSW delete doesn't affect flat buffer size
    EXPECT_EQ(snapshot.enableDelta(), 0);
    EXPECT_EQ(snapshot.disableDelta(), 0);

    // When deleteVector is implemented, it should return 1 for successful delete
    // For now, the stub returns 0
    (void)delete_result; // Suppress unused warning until implementation

    VecSimDisk_FreeIndex(index);
}

// Test: When multiple workers complete concurrently, only one triggers disable callback
// This verifies the lock ensures only one worker sees size crossing from limit to limit-1
TEST_F(TieredHNSWDiskThrottleTest, ConcurrentWorkersOnFullBuffer) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = 1024,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params, FLAT_BUFFER_LIMIT);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    ThrottleCountSnapshot snapshot;

    // Step 1: Add vectors to fill buffer (triggers enable)
    for (size_t i = 0; i < FLAT_BUFFER_LIMIT; i++) {
        float vector[DIM];
        populate_float_vec(vector, DIM, static_cast<int>(i));
        index->addVector(vector, static_cast<labelType>(i));
    }

    EXPECT_EQ(snapshot.enableDelta(), 1);
    EXPECT_EQ(mock_queue.size(), FLAT_BUFFER_LIMIT);

    // Step 2: Execute all jobs concurrently from separate threads
    // The lock in executeInsertJob ensures only one worker sees the threshold crossing
    mock_queue.executeAllConcurrently();

    // Step 4: Verify exactly one disable was called
    // The flatIndexGuard lock ensures only one worker sees size == flatBufferLimit - 1
    EXPECT_EQ(snapshot.disableDelta(), 1);
    EXPECT_TRUE(snapshot.isBalanced());

    VecSimDisk_FreeIndex(index);
}

// Test: Concurrent delete from flat buffer and insert job execution
// IMPORTANT: This test asserts INCORRECT behavior that exists while deleteVector is a stub.
// When deleteVector is implemented, this test SHOULD FAIL - update assertions to:
//   - delete_result == 1 (successful delete)
//   - disableDelta == 1 (only one callback, lock ensures one sees threshold)
TEST_F(TieredHNSWDiskThrottleTest, ConcurrentDeleteAndInsertJob_StubBehavior) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = 1024,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params, FLAT_BUFFER_LIMIT);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    // Fill buffer (enable called)
    for (size_t i = 0; i < FLAT_BUFFER_LIMIT; i++) {
        float vector[DIM];
        populate_float_vec(vector, DIM, static_cast<int>(i));
        index->addVector(vector, static_cast<labelType>(i));
    }

    ThrottleCountSnapshot snapshot;

    // Race: worker executes first job while main thread tries to delete
    AsyncJob* first_job = mock_queue.takeJob(0);
    ASSERT_NE(first_job, nullptr);

    std::thread worker([first_job]() { first_job->Execute(first_job); });

    // Main thread: try to delete label 5 from flat buffer
    // Currently a stub - returns 0 and does nothing
    int delete_result = index->deleteVector(5);

    worker.join();

    EXPECT_EQ(delete_result, 1);

    // Only one disable called - lock ensures only one sees threshold
    EXPECT_EQ(snapshot.disableDelta(), 1);

    VecSimDisk_FreeIndex(index);
}

// Test: When index is destroyed with full buffer, disable is called to balance the counter.
TEST_F(TieredHNSWDiskThrottleTest, BalanceThrottlingUponDestruction) {
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .blockSize = 1024,
        .M = 4,
        .efConstruction = 10,
        .efRuntime = 10,
    };

    auto params_holder = createTieredDiskParams(hnsw_params, FLAT_BUFFER_LIMIT);
    auto* index = VecSimDisk_CreateIndex(&params_holder->params_disk);
    ASSERT_NE(index, nullptr);

    ThrottleCountSnapshot snapshot;

    // Step 1: Add vectors up to limit - enable should be called exactly once when size == limit
    for (size_t i = 0; i < FLAT_BUFFER_LIMIT; i++) {
        float vector[DIM];
        populate_float_vec(vector, DIM, static_cast<int>(i));
        int result = index->addVector(vector, static_cast<labelType>(i));
        EXPECT_EQ(result, 1);
    }

    // Enable called exactly once when buffer became full
    EXPECT_EQ(snapshot.enableDelta(), 1);
    EXPECT_EQ(snapshot.disableDelta(), 0);

    // Destroy the index - disable should be called to balance the counter
    VecSimDisk_FreeIndex(index);
    EXPECT_EQ(snapshot.disableDelta(), 1);
}
