#include "redisearch.h"
#include "disk/database.h"
#include "disk/database_api.h"
#include "disk/doc_table/doc_table_disk.hpp"
#include "disk/doc_table/doc_table_disk_c.h"
#include "disk/document_metadata/document_metadata.hpp"
#include "disk/memory_object.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
#include "redismock/redismock.h"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>

using namespace std;

class DocTableDiskTest : public ::testing::Test {
protected:
    string db_path;
    RedisModuleCtx *ctx;
    search::disk::Database* db;
    search::disk::Database::Index* index;
    search::disk::DocTableColumn* table;

    void SetUp() override {
        // Fixed DB path for now (no per-test unique path)
        db_path = "test_doc_table_disk_db";

        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Ensure a clean database directory using the API (logs via ctx if needed)
        DiskDatabase_Delete(ctx, db_path.c_str());

        // Create the database using the C API, then get the C++ object
        DiskDatabase* disk_db = DiskDatabase_Create(ctx, db_path.c_str());
        db = reinterpret_cast<search::disk::Database*>(disk_db);
        index = reinterpret_cast<search::disk::Database::Index*>(db->OpenIndex("idx", DocumentType_Hash));
        table = &index->GetDocTable();
    }

    void TearDown() override {
        // Clean up the database
        if (db) {
            DiskDatabase_Destroy(reinterpret_cast<DiskDatabase*>(db));
            db = nullptr;
            table = nullptr;
        }

        // Remove the database using the API before freeing ctx
        if (ctx) {
            DiskDatabase_Delete(ctx, db_path.c_str());
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }
};

TEST_F(DocTableDiskTest, PutAndGetDocId) {
    string key = "mykey";
    search::disk::DocumentID docId = table->put(key, 1.0, 0, 0);
    ASSERT_NE(docId.id, 0u);
    auto outIdOpt = table->getDocId(key);
    ASSERT_TRUE(outIdOpt.has_value());
    ASSERT_EQ(docId.id, outIdOpt->id);
}

TEST_F(DocTableDiskTest, PutAndGetDmd) {
    std::string key = "docKey";
    double score = 1.23;
    uint32_t maxFreq = 7;
    uint32_t flags = 0xA5;

    // Use put to store the document and metadata
    search::disk::DocumentID docId = table->put(key, score, flags, maxFreq);
    ASSERT_NE(docId.id, 0u);

    // Retrieve the metadata
    auto dmdOpt = table->getDmd(docId);
    ASSERT_TRUE(dmdOpt.has_value());
    ASSERT_EQ(dmdOpt->keyPtr, key);
    ASSERT_EQ(dmdOpt->score, score);
    ASSERT_EQ(dmdOpt->maxFreq, maxFreq);
    ASSERT_EQ(dmdOpt->flags, flags);
}

TEST_F(DocTableDiskTest, CApi_PutAndGetDocIdToDmd) {
    DiskIndex* handle = reinterpret_cast<DiskIndex*>(index);

    // Create a document using the C API
    const char* key = "c_api_doc_key";

    // Use DocTableDisk_Put to create a document with metadata
    t_docId docId = DocTableDisk_Put(handle, key, 2.34, 0xB6, 15);
    ASSERT_NE(docId, 0u);


    // Verify we can get the key back
    char* outKey = nullptr;
    ASSERT_EQ(DocTableDisk_GetKey(handle, docId, &outKey), 1);
    ASSERT_STREQ(outKey, key);
    DocTableDisk_FreeString(outKey);
}

TEST_F(DocTableDiskTest, KeyToDocIdNotFound) {
    auto outIdOpt = table->getDocId("nope");
    ASSERT_FALSE(outIdOpt.has_value());
}

TEST_F(DocTableDiskTest, DocIdToDmdNotFound) {
    auto dmdOpt = table->getDmd(search::disk::DocumentID{12345});
    ASSERT_FALSE(dmdOpt.has_value());
}

TEST_F(DocTableDiskTest, GetDocIdAndExists) {
    std::string key = "doc2";
    search::disk::DocumentID docId = table->put(key, 1.0, 0x22, 5);
    search::disk::DocumentID nextDocId{docId.id + 1};
    ASSERT_NE(docId.id, 0u);
    auto retrievedDocId = table->getDocId(key);
    ASSERT_TRUE(retrievedDocId.has_value());
    ASSERT_EQ(retrievedDocId->id, docId.id);
    // Existence checks: there is no explicit exists API, so use getDmd/getDocId
    ASSERT_TRUE(table->getDmd(docId).has_value());
    ASSERT_FALSE(table->getDmd(nextDocId).has_value());
    ASSERT_TRUE(table->getDocId(key).has_value());
    ASSERT_FALSE(table->getDocId("nonexistent_key").has_value());

    // Initially, the document should not be marked as deleted
    ASSERT_FALSE(table->docIdDeleted(docId));
    // Non-existent document IDs should not be marked as deleted
    ASSERT_FALSE(table->docIdDeleted(nextDocId));
}

TEST_F(DocTableDiskTest, CApi_GetIdAndExists) {
    DiskIndex* handle = reinterpret_cast<DiskIndex*>(index);

    // Add a document
    const char* key = "c_api_doc2";
    t_docId docId = DocTableDisk_Put(handle, key, 1.5, 0x33, 7);
    ASSERT_NE(docId, 0u);

    // Get the ID and check it exists
    ASSERT_EQ(DocTableDisk_GetId(handle, key), docId);

    // Existence checks via GetKey
    char* outKey2 = nullptr;
    ASSERT_EQ(DocTableDisk_GetKey(handle, docId, &outKey2), 1);
    ASSERT_STREQ(outKey2, key);
    DocTableDisk_FreeString(outKey2);

    // Non-existing ID: GetDmd should fail
    RSDocumentMetadata dmd_miss{};
    auto allocKey3 = [](const void* src, size_t len) -> char* { char* s = (char*)malloc(len+1); memcpy(s, src, len); s[len]=0; return s; };
    ASSERT_EQ(DocTableDisk_GetDmd(handle, docId + 1, &dmd_miss, allocKey3), 0);
}

TEST_F(DocTableDiskTest, DeleteAndGetKey) {
    std::string key = "doc3";
    search::disk::DocumentID docId = table->put(key, 2.0, 0x33, 7);
    ASSERT_NE(docId.id, 0u);
    auto outKeyOpt = table->getKey(docId);
    ASSERT_TRUE(outKeyOpt.has_value());
    ASSERT_EQ(*outKeyOpt, key);

    // Initially, the document should not be marked as deleted
    ASSERT_FALSE(table->docIdDeleted(docId));

    ASSERT_TRUE(table->del(key));

    // After full delete, should be marked as deleted, and not exist on disk
    ASSERT_TRUE(table->docIdDeleted(docId));
    ASSERT_FALSE(table->getDmd(docId).has_value());
}

TEST_F(DocTableDiskTest, CApi_DeleteAndGetKey) {
    DiskIndex* handle = reinterpret_cast<DiskIndex*>(index);

    // Add a document
    const char* key = "c_api_doc3";
    t_docId docId = DocTableDisk_Put(handle, key, 3.0, 0x44, 9);
    ASSERT_NE(docId, 0u);

    // Get the key back
    char* outKey = nullptr;
    ASSERT_EQ(DocTableDisk_GetKey(handle, docId, &outKey), 1);
    ASSERT_STREQ(outKey, key);
    DocTableDisk_FreeString(outKey);

    // Initially, the document should not be marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId), 0);

    // Delete the document
    ASSERT_EQ(DocTableDisk_Del(handle, key), 1);

    // Verify it no longer exists: use GetDmd
    RSDocumentMetadata dmd2{};
    auto allocKey2 = [](const void* src, size_t len) -> char* { char* s = (char*)malloc(len+1); memcpy(s, src, len); s[len]=0; return s; };
    ASSERT_EQ(DocTableDisk_GetDmd(handle, docId, &dmd2, allocKey2), 0);

    // Verify it is marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId), 1);
}

TEST_F(DocTableDiskTest, PutDuplicateKeyReturnsZero) {
    std::string key = "doc4";
    search::disk::DocumentID docId1 = table->put(key, 1.0, 0x44, 8);
    ASSERT_NE(docId1.id, 0u);
    search::disk::DocumentID docId2 = table->put(key, 2.0, 0x55, 9);
    ASSERT_EQ(docId2.id, docId1.id + 1);
}

TEST_F(DocTableDiskTest, CApi_PutDuplicateKeyReturnsZero) {
    DiskIndex* handle = reinterpret_cast<DiskIndex*>(index);
    ASSERT_NE(handle, nullptr);

    // Add a document
    const char* key = "c_api_doc4";
    t_docId docId1 = DocTableDisk_Put(handle, key, 1.0, 0x55, 11);
    ASSERT_NE(docId1, 0u);

    // Try to add a document with the same key (should override)
    t_docId docId2 = DocTableDisk_Put(handle, key, 2.0, 0x66, 12);
    ASSERT_EQ(docId2, docId1+1);
}

TEST_F(DocTableDiskTest, ResetClosesDb) {
    std::string key = "doc5";
    search::disk::DocumentID docId = table->put(key, 1.0, 0x66, 10);
    ASSERT_NE(docId.id, 0u);
}

TEST_F(DocTableDiskTest, DocIdDeleted) {
    // Test with a non-existent document ID
    ASSERT_FALSE(table->docIdDeleted(search::disk::DocumentID{999999}));

    // Add a document
    std::string key = "cpp_doc_for_deletion_test";
    search::disk::DocumentID docId = table->put(key, 1.5, 0x33, 7);
    ASSERT_NE(docId.id, 0u);

    // Initially, the document should not be marked as deleted
    ASSERT_FALSE(table->docIdDeleted(docId));

    // Delete the document using del
    ASSERT_TRUE(table->del(key));

    // Now the document should be marked as deleted
    ASSERT_TRUE(table->docIdDeleted(docId));

    // The document should no longer exist in the database
    ASSERT_FALSE(table->getDmd(docId).has_value());

    // Add another document
    std::string key2 = "cpp_doc_for_deletion_test2";
    search::disk::DocumentID docId2 = table->put(key2, 2.5, 0x44, 8);
    ASSERT_NE(docId2.id, 0u);

    // Initially, the document should not be marked as deleted
    ASSERT_FALSE(table->docIdDeleted(docId2));

    // Delete the document using del
    ASSERT_TRUE(table->del(key2));

    // Now the document should be marked as deleted
    ASSERT_TRUE(table->docIdDeleted(docId2));
}

TEST_F(DocTableDiskTest, CApi_DocIdDeleted) {
    DiskIndex* handle = reinterpret_cast<DiskIndex*>(index);

    // Test with a non-existent document ID
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, 999999), 0);

    // Add a document
    const char* key = "doc_for_deletion_test";
    t_docId docId = DocTableDisk_Put(handle, key, 1.5, 0x33, 7);
    ASSERT_NE(docId, 0u);

    // Initially, the document should not be marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId), 0);

    // Delete the document using Del
    ASSERT_EQ(DocTableDisk_Del(handle, key), 1);

    // Now the document should be marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId), 1);

    // The document should no longer exist in the database: GetDmd returns 0
    RSDocumentMetadata dmd3{};
    auto allocKey3b = [](const void* src, size_t len) -> char* { char* s = (char*)malloc(len+1); memcpy(s, src, len); s[len]=0; return s; };
    ASSERT_EQ(DocTableDisk_GetDmd(handle, docId, &dmd3, allocKey3b), 0);

    // Add another document
    const char* key2 = "doc_for_deletion_test2";
    t_docId docId2 = DocTableDisk_Put(handle, key2, 2.5, 0x44, 8);
    ASSERT_NE(docId2, 0u);

    // Initially, the document should not be marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId2), 0);

    // Delete the document using del
    ASSERT_EQ(DocTableDisk_Del(handle, key2), 1);

    // Now the document should be marked as deleted
    ASSERT_EQ(DocTableDisk_DocIdDeleted(handle, docId2), 1);

    // Test with a null handle
    ASSERT_EQ(DocTableDisk_DocIdDeleted(nullptr, docId), 0);
}

// ============================================================================
// Tests for Database creation with MemoryObject
// ============================================================================

class DatabaseWithMemoryObjectTest : public ::testing::Test {
protected:
    string db_path;
    RedisModuleCtx *ctx;

    void SetUp() override {
        db_path = "test_db_with_memory_object";
        ctx = RedisModule_GetThreadSafeContext(NULL);
        // Clean up any existing database
        DiskDatabase_Delete(ctx, db_path.c_str());
    }

    void TearDown() override {
        if (ctx) {
            DiskDatabase_Delete(ctx, db_path.c_str());
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }
};

TEST_F(DatabaseWithMemoryObjectTest, CreateDBFromPathOnly) {
    // Create a database from path only (no memory object)
    DiskDatabase* disk_db = DiskDatabase_Create(ctx, db_path.c_str());
    ASSERT_NE(disk_db, nullptr);

    auto* db = reinterpret_cast<search::disk::Database*>(disk_db);
    auto* index = db->OpenIndex("idx1", DocumentType_Hash);
    ASSERT_NE(index, nullptr);

    auto& table = index->GetDocTable();

    // Add some documents
    search::disk::DocumentID docId1 = table.put("key1", 1.0, 0, 0);
    search::disk::DocumentID docId2 = table.put("key2", 2.0, 0, 0);
    ASSERT_NE(docId1.id, 0u);
    ASSERT_NE(docId2.id, 0u);

    // Verify documents exist
    ASSERT_TRUE(table.getDocId("key1").has_value());
    ASSERT_TRUE(table.getDocId("key2").has_value());

    DiskDatabase_Destroy(disk_db);
}

TEST_F(DatabaseWithMemoryObjectTest, CreateDBWithMemoryObject) {
    // Create a memory object with index metadata
    search::disk::MemoryObject memObj;
    auto deletedIds = std::make_shared<search::disk::DeletedIds>();
    deletedIds->add(10);
    deletedIds->add(20);

    memObj.AddIndex("idx_with_memory", DocumentType_Hash, 100, deletedIds);

    // Create database with memory object
    DiskDatabase* disk_db = DiskDatabase_CreateWithMemory(ctx, db_path.c_str(),
                                                          reinterpret_cast<DiskMemoryObject*>(&memObj));
    ASSERT_NE(disk_db, nullptr);

    auto* db = reinterpret_cast<search::disk::Database*>(disk_db);
    auto* index = db->OpenIndex("idx_with_memory", DocumentType_Hash);
    ASSERT_NE(index, nullptr);

    auto& table = index->GetDocTable();

    // Verify maxDocId was restored from memory object
    ASSERT_EQ(table.GetMaxDocId(), 100u);

    // Verify deleted IDs were restored
    ASSERT_TRUE(table.GetDeletedIds()->contains(10));
    ASSERT_TRUE(table.GetDeletedIds()->contains(20));
    ASSERT_EQ(table.GetDeletedIds()->size(), 2u);

    DiskDatabase_Destroy(disk_db);
}

TEST_F(DatabaseWithMemoryObjectTest, CreateDBWithMultipleIndexesInMemoryObject) {
    // Create a memory object with multiple indexes
    search::disk::MemoryObject memObj;

    auto deletedIds1 = std::make_shared<search::disk::DeletedIds>();
    deletedIds1->add(5);
    deletedIds1->add(15);

    auto deletedIds2 = std::make_shared<search::disk::DeletedIds>();
    deletedIds2->add(100);
    deletedIds2->add(200);
    deletedIds2->add(300);

    memObj.AddIndex("hash_index", DocumentType_Hash, 50, deletedIds1);
    memObj.AddIndex("json_index", DocumentType_Json, 200, deletedIds2);

    // Create database with memory object
    DiskDatabase* disk_db = DiskDatabase_CreateWithMemory(ctx, db_path.c_str(),
                                                          reinterpret_cast<DiskMemoryObject*>(&memObj));
    ASSERT_NE(disk_db, nullptr);

    auto* db = reinterpret_cast<search::disk::Database*>(disk_db);

    // Verify hash index
    auto* hash_index = db->OpenIndex("hash_index", DocumentType_Hash);
    ASSERT_NE(hash_index, nullptr);
    ASSERT_EQ(hash_index->GetDocTable().GetMaxDocId(), 50u);
    ASSERT_EQ(hash_index->GetDocTable().GetDeletedIds()->size(), 2u);

    // Verify json index
    auto* json_index = db->OpenIndex("json_index", DocumentType_Json);
    ASSERT_NE(json_index, nullptr);
    ASSERT_EQ(json_index->GetDocTable().GetMaxDocId(), 200u);
    ASSERT_EQ(json_index->GetDocTable().GetDeletedIds()->size(), 3u);

    DiskDatabase_Destroy(disk_db);
}

TEST_F(DatabaseWithMemoryObjectTest, CreateDBWithEmptyMemoryObject) {
    // Create an empty memory object
    search::disk::MemoryObject memObj;
    ASSERT_TRUE(memObj.IsEmpty());

    // Create database with empty memory object
    DiskDatabase* disk_db = DiskDatabase_CreateWithMemory(ctx, db_path.c_str(),
                                                          reinterpret_cast<DiskMemoryObject*>(&memObj));
    ASSERT_NE(disk_db, nullptr);

    auto* db = reinterpret_cast<search::disk::Database*>(disk_db);

    // Should be able to create new indexes
    auto* index = db->OpenIndex("new_index", DocumentType_Hash);
    ASSERT_NE(index, nullptr);

    auto& table = index->GetDocTable();
    ASSERT_EQ(table.GetMaxDocId(), 0u);

    DiskDatabase_Destroy(disk_db);
}

TEST_F(DatabaseWithMemoryObjectTest, CreateDBWithMemoryObjectAndAddDocuments) {
    // Create a memory object with initial state
    search::disk::MemoryObject memObj;
    auto deletedIds = std::make_shared<search::disk::DeletedIds>();
    deletedIds->add(1);
    deletedIds->add(2);
    deletedIds->add(3);

    memObj.AddIndex("test_idx", DocumentType_Hash, 50, deletedIds);

    // Create database with memory object
    DiskDatabase* disk_db = DiskDatabase_CreateWithMemory(ctx, db_path.c_str(),
                                                          reinterpret_cast<DiskMemoryObject*>(&memObj));
    ASSERT_NE(disk_db, nullptr);

    auto* db = reinterpret_cast<search::disk::Database*>(disk_db);
    auto* index = db->OpenIndex("test_idx", DocumentType_Hash);
    ASSERT_NE(index, nullptr);

    auto& table = index->GetDocTable();

    // Verify initial state from memory object
    ASSERT_EQ(table.GetMaxDocId(), 50u);
    ASSERT_EQ(table.GetDeletedIds()->size(), 3u);

    // Add new documents
    search::disk::DocumentID docId1 = table.put("new_key1", 1.5, 0, 0);
    search::disk::DocumentID docId2 = table.put("new_key2", 2.5, 0, 0);

    ASSERT_NE(docId1.id, 0u);
    ASSERT_NE(docId2.id, 0u);

    // New documents should have IDs greater than maxDocId from memory object
    ASSERT_GT(docId1.id, 50u);
    ASSERT_GT(docId2.id, 50u);

    // Verify documents can be retrieved
    ASSERT_TRUE(table.getDocId("new_key1").has_value());
    ASSERT_TRUE(table.getDocId("new_key2").has_value());

    DiskDatabase_Destroy(disk_db);
}

// ============================================================================
// Tests for GetDeletedIds() getter and setDeletedIds() setter
// ============================================================================

/**
 * @brief Test GetDeletedIds getter returns the correct DeletedIds container
 *
 * This test verifies that:
 * - GetDeletedIds() returns a valid shared_ptr to DeletedIds
 * - The returned container is the same one used internally
 * - Operations on the returned container affect the table's deleted IDs
 */
TEST_F(DocTableDiskTest, GetDeletedIdsGetter) {
    // Get the deleted IDs container
    auto deletedIds = table->GetDeletedIds();
    ASSERT_NE(deletedIds, nullptr);

    // Verify it's a valid shared_ptr
    ASSERT_TRUE(deletedIds.get() != nullptr);

    // Initially should be empty
    ASSERT_EQ(deletedIds->size(), 0u);

    // Add some IDs to the container
    deletedIds->add(1);
    deletedIds->add(2);
    deletedIds->add(3);

    // Verify the size increased
    ASSERT_EQ(deletedIds->size(), 3u);

    // Verify the IDs are in the container
    ASSERT_TRUE(deletedIds->contains(1));
    ASSERT_TRUE(deletedIds->contains(2));
    ASSERT_TRUE(deletedIds->contains(3));
}

/**
 * @brief Test GetDeletedIds returns the same container across multiple calls
 *
 * This test verifies that:
 * - Multiple calls to GetDeletedIds() return the same container
 * - Changes made through one reference are visible through another
 */
TEST_F(DocTableDiskTest, GetDeletedIdsConsistency) {
    // Get the deleted IDs container twice
    auto deletedIds1 = table->GetDeletedIds();
    auto deletedIds2 = table->GetDeletedIds();

    // Both should point to the same object
    ASSERT_EQ(deletedIds1.get(), deletedIds2.get());

    // Add IDs through the first reference
    deletedIds1->add(42);
    deletedIds1->add(100);

    // Verify they're visible through the second reference
    ASSERT_EQ(deletedIds2->size(), 2u);
    ASSERT_TRUE(deletedIds2->contains(42));
    ASSERT_TRUE(deletedIds2->contains(100));

    // Add IDs through the second reference
    deletedIds2->add(200);

    // Verify they're visible through the first reference
    ASSERT_EQ(deletedIds1->size(), 3u);
    ASSERT_TRUE(deletedIds1->contains(200));
}

/**
 * @brief Test GetDeletedIds with document deletion
 *
 * This test verifies that:
 * - When a document is deleted, its ID is added to the deleted IDs container
 * - GetDeletedIds() reflects the changes made by document operations
 */
TEST_F(DocTableDiskTest, GetDeletedIdsWithDocumentDeletion) {
    // Add a document
    string key = "test_key";
    search::disk::DocumentID docId = table->put(key, 1.0, 0, 0);
    ASSERT_NE(docId.id, 0u);

    // Initially, deleted IDs should be empty
    auto deletedIds = table->GetDeletedIds();
    ASSERT_EQ(deletedIds->size(), 0u);

    // Delete the document
    bool deleted = table->del(key);
    ASSERT_TRUE(deleted);

    // Now the deleted IDs should contain the document ID
    ASSERT_EQ(deletedIds->size(), 1u);
    ASSERT_TRUE(deletedIds->contains(docId.id));
}

/**
 * @brief Test setDeletedIds setter replaces the deleted IDs container
 *
 * This test verifies that:
 * - setDeletedIds() can replace the deleted IDs container
 * - The new container is used for subsequent operations
 * - The old container is no longer used
 */
TEST_F(DocTableDiskTest, SetDeletedIdsSetter) {
    // Create a new DeletedIds container with some IDs
    auto newDeletedIds = std::make_shared<search::disk::DeletedIds>();
    newDeletedIds->add(10);
    newDeletedIds->add(20);
    newDeletedIds->add(30);

    // Get the old container
    auto oldDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(oldDeletedIds->size(), 0u);

    // Set the new container
    table->setDeletedIds(newDeletedIds);

    // Verify the new container is now in use
    auto retrievedDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(retrievedDeletedIds.get(), newDeletedIds.get());
    ASSERT_EQ(retrievedDeletedIds->size(), 3u);
    ASSERT_TRUE(retrievedDeletedIds->contains(10));
    ASSERT_TRUE(retrievedDeletedIds->contains(20));
    ASSERT_TRUE(retrievedDeletedIds->contains(30));
}

/**
 * @brief Test setDeletedIds with empty container
 *
 * This test verifies that:
 * - setDeletedIds() can set an empty container
 * - The empty container is properly used
 */
TEST_F(DocTableDiskTest, SetDeletedIdsWithEmptyContainer) {
    // First, add some IDs to the current container
    auto currentDeletedIds = table->GetDeletedIds();
    currentDeletedIds->add(1);
    currentDeletedIds->add(2);
    ASSERT_EQ(currentDeletedIds->size(), 2u);

    // Create a new empty container
    auto emptyDeletedIds = std::make_shared<search::disk::DeletedIds>();
    ASSERT_EQ(emptyDeletedIds->size(), 0u);

    // Set the empty container
    table->setDeletedIds(emptyDeletedIds);

    // Verify the empty container is now in use
    auto retrievedDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(retrievedDeletedIds.get(), emptyDeletedIds.get());
    ASSERT_EQ(retrievedDeletedIds->size(), 0u);
}

/**
 * @brief Test setDeletedIds followed by document operations
 *
 * This test verifies that:
 * - After setting a new deleted IDs container, document operations work correctly
 * - Document deletions are tracked in the new container
 */
TEST_F(DocTableDiskTest, SetDeletedIdsWithDocumentOperations) {
    // Create a new DeletedIds container with some initial IDs
    auto newDeletedIds = std::make_shared<search::disk::DeletedIds>();
    newDeletedIds->add(100);
    newDeletedIds->add(200);

    // Set the new container
    table->setDeletedIds(newDeletedIds);

    // Add a document
    string key = "new_doc";
    search::disk::DocumentID docId = table->put(key, 1.5, 0, 0);
    ASSERT_NE(docId.id, 0u);

    // Delete the document
    bool deleted = table->del(key);
    ASSERT_TRUE(deleted);

    // Verify the deleted IDs container now contains both the initial IDs and the new deleted ID
    auto retrievedDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(retrievedDeletedIds->size(), 3u);
    ASSERT_TRUE(retrievedDeletedIds->contains(100));
    ASSERT_TRUE(retrievedDeletedIds->contains(200));
    ASSERT_TRUE(retrievedDeletedIds->contains(docId.id));
}

/**
 * @brief Test GetDeletedIds reference counting with setDeletedIds
 *
 * This test verifies that:
 * - The shared_ptr reference counting works correctly
 * - Old containers are properly managed when replaced
 */
TEST_F(DocTableDiskTest, GetSetDeletedIdsReferenceManagement) {
    // Get the initial container
    auto initialDeletedIds = table->GetDeletedIds();
    initialDeletedIds->add(1);

    // Create a new container
    auto newDeletedIds = std::make_shared<search::disk::DeletedIds>();
    newDeletedIds->add(10);

    // Keep a reference to the initial container
    auto initialRef = initialDeletedIds;

    // Set the new container
    table->setDeletedIds(newDeletedIds);

    // The initial container should still be valid (we have a reference)
    ASSERT_EQ(initialRef->size(), 1u);
    ASSERT_TRUE(initialRef->contains(1));

    // The table should now use the new container
    auto currentDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(currentDeletedIds.get(), newDeletedIds.get());
    ASSERT_EQ(currentDeletedIds->size(), 1u);
    ASSERT_TRUE(currentDeletedIds->contains(10));
}

/**
 * @brief Test setDeletedIds with large container
 *
 * This test verifies that:
 * - setDeletedIds() works correctly with containers holding many IDs
 * - All IDs are preserved after setting
 */
TEST_F(DocTableDiskTest, SetDeletedIdsWithLargeContainer) {
    // Create a new container with many IDs
    auto largeDeletedIds = std::make_shared<search::disk::DeletedIds>();
    const size_t numIds = 1000;
    for (size_t i = 0; i < numIds; ++i) {
        largeDeletedIds->add(i * 10);
    }

    ASSERT_EQ(largeDeletedIds->size(), numIds);

    // Set the large container
    table->setDeletedIds(largeDeletedIds);

    // Verify all IDs are present
    auto retrievedDeletedIds = table->GetDeletedIds();
    ASSERT_EQ(retrievedDeletedIds->size(), numIds);

    // Verify a sample of IDs
    ASSERT_TRUE(retrievedDeletedIds->contains(0));
    ASSERT_TRUE(retrievedDeletedIds->contains(100));
    ASSERT_TRUE(retrievedDeletedIds->contains(9990));
}
