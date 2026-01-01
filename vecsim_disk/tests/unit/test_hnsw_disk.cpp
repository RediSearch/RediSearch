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
#include "hnsw_disk_factory.h"

using namespace test_utils;

class HNSWDiskTest : public ::testing::Test {
protected:
    static constexpr size_t DIM = 4;
};

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
// storage via unique_ptr. The TestIndex helper creates a MockStorage and
// passes ownership to the index.

TEST_F(HNSWDiskTest, IndexOwnsStorage) {
    TestIndex<float, float> index(DIM);

    // The index should have a valid storage pointer
    VectorStore* storage = index->getStorage();
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
    VecSimHNSWDiskParams params = {
        .dim = DIM,
        .type = VecSimType_FLOAT32,
        .metric = VecSimMetric_L2,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
        .blockSize = 1024,
        .multi = false,
        .storage = nullptr, // No storage provided
        .indexName = "test",
        .indexNameLen = 4,
        .logCtx = nullptr,
    };

    auto* handle = VecSimDisk_CreateIndex(&params);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // With null storage, getStorage should return null
    EXPECT_EQ(index->getStorage(), nullptr);

    VecSimDisk_FreeIndex(handle);
}

TEST_F(HNSWDiskTest, FactoryWithSpeeDBHandles) {
    // In production, the factory creates SpeeDBStore from handles.
    // In unit tests, the stub returns nullptr (SpeedB not linked).
    SpeeDBHandles handles;
    handles.db = reinterpret_cast<rocksdb_t*>(0xDEADBEEF);
    handles.cf = reinterpret_cast<rocksdb_column_family_handle_t*>(0xCAFEBABE);

    VecSimHNSWDiskParams params = {
        .dim = DIM,
        .type = VecSimType_FLOAT32,
        .metric = VecSimMetric_L2,
        .M = 16,
        .efConstruction = 200,
        .efRuntime = 10,
        .blockSize = 1024,
        .multi = false,
        .storage = &handles, // SpeeDBHandles from FFI
        .indexName = "test",
        .indexNameLen = 4,
        .logCtx = nullptr,
    };

    auto* handle = VecSimDisk_CreateIndex(&params);
    ASSERT_NE(handle, nullptr);

    auto* index = static_cast<HNSWDiskIndex<float, float>*>(handle);

    // In unit tests, the stub returns nullptr (no SpeedB)
    // In production, this would be a valid SpeeDBStore
    // IMPORTANT: The storage is NOT the same pointer as handles (bug is fixed)
    VectorStore* storage = index->getStorage();
    EXPECT_NE(static_cast<void*>(storage), static_cast<void*>(&handles));

    VecSimDisk_FreeIndex(handle);
}