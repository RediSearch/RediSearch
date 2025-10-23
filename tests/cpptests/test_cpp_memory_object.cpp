/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <gtest/gtest.h>
#include "disk/memory_object.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
#include "redismock/redismock.h"

using namespace search::disk;

class MemoryObjectTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize Redis mock
        ctx = RedisModule_GetThreadSafeContext(NULL);
        ASSERT_TRUE(ctx != nullptr);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }

    RedisModuleCtx *ctx = nullptr;
};

TEST_F(MemoryObjectTest, EmptyMemoryObject) {
    MemoryObject memObj;
    
    EXPECT_TRUE(memObj.IsEmpty());
    EXPECT_EQ(memObj.GetIndexCount(), 0);
    EXPECT_EQ(memObj.GetIndexes().size(), 0);
    EXPECT_EQ(memObj.GetIndex("nonexistent"), nullptr);
}

TEST_F(MemoryObjectTest, AddAndRetrieveIndex) {
    MemoryObject memObj;
    auto deletedIds = std::make_shared<DeletedIds>();
    
    // Add some deleted IDs
    deletedIds->add(10);
    deletedIds->add(20);
    deletedIds->add(30);
    
    memObj.AddIndex("test_index", DocumentType_Hash, 100, deletedIds);
    
    EXPECT_FALSE(memObj.IsEmpty());
    EXPECT_EQ(memObj.GetIndexCount(), 1);
    
    const auto* indexInfo = memObj.GetIndex("test_index");
    ASSERT_NE(indexInfo, nullptr);
    EXPECT_EQ(indexInfo->name, "test_index");
    EXPECT_EQ(indexInfo->docType, DocumentType_Hash);
    EXPECT_EQ(indexInfo->maxDocId, 100);
    EXPECT_NE(indexInfo->deletedIds, nullptr);
    
    // Verify deleted IDs
    EXPECT_TRUE(indexInfo->deletedIds->contains(10));
    EXPECT_TRUE(indexInfo->deletedIds->contains(20));
    EXPECT_TRUE(indexInfo->deletedIds->contains(30));
    EXPECT_FALSE(indexInfo->deletedIds->contains(40));
    EXPECT_EQ(indexInfo->deletedIds->size(), 3);
}

TEST_F(MemoryObjectTest, UpdateExistingIndex) {
    MemoryObject memObj;
    auto deletedIds1 = std::make_shared<DeletedIds>();
    auto deletedIds2 = std::make_shared<DeletedIds>();
    
    deletedIds1->add(10);
    deletedIds2->add(20);
    deletedIds2->add(30);
    
    // Add initial index
    memObj.AddIndex("test_index", DocumentType_Hash, 50, deletedIds1);
    EXPECT_EQ(memObj.GetIndexCount(), 1);
    
    // Update the same index
    memObj.AddIndex("test_index", DocumentType_Json, 100, deletedIds2);
    EXPECT_EQ(memObj.GetIndexCount(), 1); // Should still be 1
    
    const auto* indexInfo = memObj.GetIndex("test_index");
    ASSERT_NE(indexInfo, nullptr);
    EXPECT_EQ(indexInfo->docType, DocumentType_Json); // Updated
    EXPECT_EQ(indexInfo->maxDocId, 100); // Updated
    EXPECT_TRUE(indexInfo->deletedIds->contains(20)); // Updated deleted IDs
    EXPECT_TRUE(indexInfo->deletedIds->contains(30));
    EXPECT_FALSE(indexInfo->deletedIds->contains(10)); // Old deleted ID should be gone
}

TEST_F(MemoryObjectTest, MultipleIndexes) {
    MemoryObject memObj;
    auto deletedIds1 = std::make_shared<DeletedIds>();
    auto deletedIds2 = std::make_shared<DeletedIds>();
    
    deletedIds1->add(10);
    deletedIds2->add(20);
    
    memObj.AddIndex("index1", DocumentType_Hash, 50, deletedIds1);
    memObj.AddIndex("index2", DocumentType_Json, 100, deletedIds2);
    
    EXPECT_EQ(memObj.GetIndexCount(), 2);
    
    const auto* index1 = memObj.GetIndex("index1");
    const auto* index2 = memObj.GetIndex("index2");
    
    ASSERT_NE(index1, nullptr);
    ASSERT_NE(index2, nullptr);
    
    EXPECT_EQ(index1->name, "index1");
    EXPECT_EQ(index1->docType, DocumentType_Hash);
    EXPECT_EQ(index1->maxDocId, 50);
    
    EXPECT_EQ(index2->name, "index2");
    EXPECT_EQ(index2->docType, DocumentType_Json);
    EXPECT_EQ(index2->maxDocId, 100);
}

TEST_F(MemoryObjectTest, SerializationRoundTrip) {
    MemoryObject originalMemObj;
    auto deletedIds = std::make_shared<DeletedIds>();
    
    // Add some data
    deletedIds->add(10);
    deletedIds->add(20);
    deletedIds->add(100);
    
    originalMemObj.AddIndex("test_index", DocumentType_Hash, 150, deletedIds);
    originalMemObj.AddIndex("json_index", DocumentType_Json, 200, std::make_shared<DeletedIds>());
    
    // Create RDB IO for testing
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr);
    
    // Serialize
    originalMemObj.SerializeToRDB(io);
    
    // Reset read position
    io->read_pos = 0;
    
    // Deserialize
    auto deserializedMemObj = MemoryObject::DeserializeFromRDB(io);
    ASSERT_NE(deserializedMemObj, nullptr);
    
    // Verify deserialized data
    EXPECT_EQ(deserializedMemObj->GetIndexCount(), 2);
    
    const auto* testIndex = deserializedMemObj->GetIndex("test_index");
    const auto* jsonIndex = deserializedMemObj->GetIndex("json_index");
    
    ASSERT_NE(testIndex, nullptr);
    ASSERT_NE(jsonIndex, nullptr);
    
    EXPECT_EQ(testIndex->name, "test_index");
    EXPECT_EQ(testIndex->docType, DocumentType_Hash);
    EXPECT_EQ(testIndex->maxDocId, 150);
    EXPECT_TRUE(testIndex->deletedIds->contains(10));
    EXPECT_TRUE(testIndex->deletedIds->contains(20));
    EXPECT_TRUE(testIndex->deletedIds->contains(100));
    EXPECT_EQ(testIndex->deletedIds->size(), 3);
    
    EXPECT_EQ(jsonIndex->name, "json_index");
    EXPECT_EQ(jsonIndex->docType, DocumentType_Json);
    EXPECT_EQ(jsonIndex->maxDocId, 200);
    EXPECT_EQ(jsonIndex->deletedIds->size(), 0);
    
    RMCK_FreeRdbIO(io);
}
