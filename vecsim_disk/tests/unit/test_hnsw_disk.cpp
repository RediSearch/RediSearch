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
#include "vecsim_disk_api.h"
#include "factory/disk_index_factory.h"
#include "VecSim/index_factories/factory_utils.h"

using namespace test_utils;

class HNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;

    VecSimIndex* createIndex(const HNSWParams& hnsw_params, const SpeeDBHandles* handles = nullptr) {
        auto params_disk_holder = createDiskParams(hnsw_params);
        // Set storage handles if provided
        if (handles) {
            params_disk_holder->diskContext.storage = const_cast<void*>(static_cast<const void*>(handles));
        }
        return VecSimDisk_CreateIndex(&params_disk_holder->params_disk);
    }
};

// Parameterized test class for HNSWDisk with different metrics
class HNSWDiskMetricTest : public HNSWDiskTest, public testing::WithParamInterface<VecSimMetric> {};

TEST_F(HNSWDiskTest, CreateIndex) {
    TestIndex<float, float> index(DIM);
    ASSERT_NE(index.get(), nullptr);
    EXPECT_EQ(index->indexSize(), 0);
}

// Stub tests - verify stub behavior until MOD-13164 implementation
TEST_F(HNSWDiskTest, AddVectorStub) {
    TestIndex<float, float> index(DIM);

    float vec[DIM] = {1.0f, 1.0f, 1.0f, 1.0f};
    // Stub returns 0 (not implemented)
    EXPECT_EQ(index->addVector(vec, 1), 0);
    EXPECT_EQ(index->indexSize(), 0);
}

TEST_F(HNSWDiskTest, TopKQueryStub) {
    TestIndex<float, float> index(DIM);

    float query[DIM] = {1.0f, 1.0f, 1.0f, 1.0f};
    auto* reply = index->topKQuery(query, 10, nullptr);

    ASSERT_NE(reply, nullptr);
    // Stub returns empty results
    EXPECT_EQ(reply->results.size(), 0);
    delete reply;
}

TEST_F(HNSWDiskTest, DeleteVectorStub) {
    TestIndex<float, float> index(DIM);

    // Stub returns 0 (not implemented)
    EXPECT_EQ(index->deleteVector(1), 0);
    EXPECT_EQ(index->indexSize(), 0);
}

// =============================================================================
// Test: Index correctly owns storage via unique_ptr
// =============================================================================
//
// This test verifies that the HNSWDiskIndex correctly takes ownership of
// storage via unique_ptr. The TestIndex helper creates a SpeeDBStore and
// passes ownership to the index.

TEST_F(HNSWDiskTest, IndexOwnsStorage) {
    TestIndex<float, float> index(DIM);

    // The index should have a valid storage pointer
    HNSWStorage<float>* storage = index->getStorage();
    EXPECT_NE(storage, nullptr);

    // The storage should be the same as what TestIndex created
    EXPECT_EQ(storage, index.storage());
}

// =============================================================================
// Factory tests
// =============================================================================
//
// NOTE: Unit tests use a stub CreateSpeeDBStore that returns nullptr.
// Real SpeeDBStore creation is tested in flow tests with the full Redis module.

TEST_F(HNSWDiskTest, FactoryWithNullStorage) {
    // Test that factory handles null storage gracefully
    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    auto* handle = this->createIndex(hnsw_params);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // With null storage, getStorage should return null
    EXPECT_EQ(index->getStorage(), nullptr);

    VecSimDisk_FreeIndex(handle);
}

TEST_F(HNSWDiskTest, FactoryWithSpeeDBHandles) {
    // Test that factory creates HNSWStorage from valid handles
    TempSpeeDB tempDb;

    // Wrap C++ pointers in C API structs for SpeeDBHandles
    rocksdb_t db_wrapper{tempDb.db()};
    rocksdb_column_family_handle_t cf_wrapper{tempDb.cf()};

    SpeeDBHandles handles;
    handles.db = &db_wrapper;
    handles.cf = &cf_wrapper;

    HNSWParams hnsw_params = {
        .type = VecSimType_FLOAT32,
        .dim = DIM,
        .metric = VecSimMetric_L2,
        .multi = false,
        .blockSize = 1024,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
    };

    // Pass handles to createIndex
    auto* handle = this->createIndex(hnsw_params, &handles);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // Verify that storage was created successfully
    HNSWStorage<float>* storage = index->getStorage();
    ASSERT_NE(storage, nullptr);

    // Verify storage is NOT the same pointer as handles (it's an HNSWStorage instance)
    EXPECT_NE(static_cast<void*>(storage), static_cast<void*>(&handles));

    VecSimDisk_FreeIndex(handle);
}

// Test that verifies correct blob size calculation for HNSWDiskIndex
TEST_P(HNSWDiskMetricTest, BlobSizeCalculation) {
    VecSimMetric metric = GetParam();
    const size_t expectedSQ8Size = getExpectedSQ8Size(DIM, metric);
    const size_t fp32Size = DIM * sizeof(float);

    // Create an actual HNSWDiskIndex and verify its blob sizes
    TestIndex<float, float> index(DIM, metric);

    // Backend: inputBlobSize = FP32, storedDataSize = SQ8
    EXPECT_EQ(index->getInputBlobSize(), fp32Size);
    EXPECT_EQ(index->getStoredDataSize(), expectedSQ8Size);
}

// Instantiate parameterized tests for all metrics
INSTANTIATE_TEST_SUITE_P(MetricTests, HNSWDiskMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric>& info) {
                             return VecSimMetric_ToString(info.param);
                         });
