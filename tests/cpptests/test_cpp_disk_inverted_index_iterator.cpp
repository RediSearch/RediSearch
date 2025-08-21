#include "gtest/gtest.h"
#include <rocksdb/db.h>
#include "disk/inverted_index/inverted_index_api.h"
#include "redisearch.h"
#include "redismodule.h"
#include <cstdio>
#include <filesystem>
#include <string>

class InvertedIndexIteratorTest : public ::testing::Test {
protected:
    std::string db_path;
    RedisModuleCtx *ctx;
    DiskDatabase *db;
    DiskIndex* index;

    void SetUp() override {
        // Fixed DB path for now (no per-test unique path)
        db_path = "test_inverted_index_iterator_db";

        // Get Redis context and create database
        ctx = RedisModule_GetThreadSafeContext(NULL);
        DiskDatabase_Delete(ctx, db_path.c_str());
        db = DiskDatabase_Create(ctx, db_path.c_str());
        index = DiskDatabase_OpenIndex(db, "idx", DocumentType_Hash);
        ASSERT_NE(db, nullptr);
        ASSERT_NE(index, nullptr);
    }

    void TearDown() override {
        // Clean up database and Redis context
        if (db) {
            DiskDatabase_Destroy(db);
            db = nullptr;
        }
        // Delete DB before freeing ctx, since DiskDatabase_Delete may log via ctx
        if (ctx) {
            DiskDatabase_Delete(ctx, db_path.c_str());
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }

    // Helper method to index documents
    void IndexDocuments(const char* term, size_t count, size_t step = 1, bool useDocIdAsFieldMask = true) {
        for (size_t i = 1; i <= count; i += step) {
            t_fieldMask fieldMask = useDocIdAsFieldMask ? i : 1;
            bool indexed = DiskDatabase_IndexDocument(index, term, i, fieldMask);
            ASSERT_TRUE(indexed);
        }
    }
};

TEST_F(InvertedIndexIteratorTest, testBasicIteration) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Test iteration
    RSIndexResult result;
    for (size_t i = 1; i <= documents; ++i) {
        ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
        ASSERT_EQ(result.docId, i);
        ASSERT_EQ(result.fieldMask, i);
    }

    // Should return false when no more documents
    ASSERT_FALSE(InvertedIndexIterator_Next(iter, &result));

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testSkipTo) {
    // Index documents with odd IDs only
    const size_t documents = 50;
    // index documents from 1 up to 50 with a step of 3
    // 1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49
    IndexDocuments("term", documents, 3);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Skip to a document which exists, will move to next block after that
    const t_docId existingId = 13; // move to block 16 after returning 13
    RSIndexResult result;
    ASSERT_TRUE(InvertedIndexIterator_SkipTo(iter, existingId, &result));
    ASSERT_EQ(result.docId, existingId);
    ASSERT_EQ(result.fieldMask, existingId); // field mask is the same as the doc id based on argument in IndexDocuments

    // Skip to a document which does not exist, will move to next block after that
    const t_docId nonExistingId = 14; // move to block 19 after returning 16
    ASSERT_TRUE(InvertedIndexIterator_SkipTo(iter, nonExistingId, &result));
    ASSERT_EQ(result.docId, 16);
    ASSERT_EQ(result.fieldMask, 16);

    // Skip to document 101 (should return false)
    ASSERT_FALSE(InvertedIndexIterator_SkipTo(iter, 101, &result));

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testRewind) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Read first 5 documents
    RSIndexResult result;
    for (size_t i = 1; i <= 5; ++i) {
        ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
        ASSERT_EQ(result.docId, i);
    }

    // Rewind and read again
    InvertedIndexIterator_Rewind(iter);
    for (size_t i = 1; i <= documents; ++i) {
        ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
        ASSERT_EQ(result.docId, i);
        ASSERT_EQ(result.fieldMask, i);
    }

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testAbort) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Read first 5 documents
    RSIndexResult result;
    for (size_t i = 1; i <= 5; ++i) {
        ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
        ASSERT_EQ(result.docId, i);
    }

    // Abort the iterator
    InvertedIndexIterator_Abort(iter);

    // Should return false after abort
    ASSERT_FALSE(InvertedIndexIterator_Next(iter, &result));

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testLastDocId) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Initially, LastDocId should be 0
    ASSERT_EQ(InvertedIndexIterator_LastDocId(iter), 0);

    // Read a document
    RSIndexResult result;
    ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
    ASSERT_EQ(result.docId, 1);

    // LastDocId should now be 1
    ASSERT_EQ(InvertedIndexIterator_LastDocId(iter), 1);

    // Skip to document 10
    ASSERT_TRUE(InvertedIndexIterator_SkipTo(iter, 10, &result));
    ASSERT_EQ(result.docId, 10);

    // LastDocId should now be 10
    ASSERT_EQ(InvertedIndexIterator_LastDocId(iter), 10);

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testRewindAfterAbort) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Read a document
    RSIndexResult result;
    ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));

    // Abort the iterator
    InvertedIndexIterator_Abort(iter);
    ASSERT_FALSE(InvertedIndexIterator_Next(iter, &result));

    // Rewind should restore functionality
    InvertedIndexIterator_Rewind(iter);

    // Should be able to read documents again
    ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
    ASSERT_EQ(result.docId, 1);

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testSkipToAfterEnd) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Skip past the end
    RSIndexResult result;
    ASSERT_FALSE(InvertedIndexIterator_SkipTo(iter, 100, &result));

    // Rewind and try again
    InvertedIndexIterator_Rewind(iter);

    // Should be able to read documents again
    ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
    ASSERT_EQ(result.docId, 1);

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testSkipBackward) {
    // Index 20 documents
    const size_t documents = 20;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Skip to document 10
    RSIndexResult result;
    ASSERT_TRUE(InvertedIndexIterator_SkipTo(iter, 10, &result));
    ASSERT_EQ(result.docId, 10);

    // Try to skip backward (should return false and return nothing)
    result = RSIndexResult{0, 0};
    ASSERT_FALSE(InvertedIndexIterator_SkipTo(iter, 5, &result));
    ASSERT_EQ(result.docId, 0);

    // Continue reading from current position
    ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result));
    ASSERT_EQ(result.docId, 11);

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testHasNext) {
    // Index 5 documents
    const size_t documents = 5;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Initially HasNext should be true
    ASSERT_TRUE(InvertedIndexIterator_HasNext(iter));

    // Read all documents
    RSIndexResult result;
    for (size_t i = 1; i <= documents; ++i) {
        ASSERT_TRUE(InvertedIndexIterator_Next(iter, &result)) << "Failed to read document " << i;
    }

    // After reading all documents, HasNext should be false
    ASSERT_FALSE(InvertedIndexIterator_HasNext(iter));

    // Rewind should make HasNext true again
    InvertedIndexIterator_Rewind(iter);
    ASSERT_TRUE(InvertedIndexIterator_HasNext(iter));

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testNumEstimated) {
    // Index 100 documents
    const size_t documents = 100;
    IndexDocuments("term", documents);

    // Create iterator
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "term");
    ASSERT_NE(iter, nullptr);

    // Check estimation
    size_t estimation = InvertedIndexIterator_NumEstimated(iter);
    // no deletions occured so we should have 100 documents in the inverted index
    ASSERT_EQ(estimation, 100);

    // Clean up
    InvertedIndexIterator_Free(iter);
}

TEST_F(InvertedIndexIteratorTest, testNoDocument) {
    // Try to create an iterator for a non-existent term
    DiskIterator *iter = DiskDatabase_NewInvertedIndexIterator(index, "nonexistent");
    ASSERT_EQ(iter, nullptr);
}
