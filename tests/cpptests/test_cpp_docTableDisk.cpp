#include "redisearch.h"
#include "disk/database.h"
#include "disk/database_api.h"
#include "disk/doc_table/doc_table_disk.hpp"
#include "disk/doc_table/doc_table_disk_c.h"
#include "disk/document_metadata/document_metadata.hpp"
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
