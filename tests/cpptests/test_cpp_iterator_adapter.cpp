#include "gtest/gtest.h"
#include "disk/index_iterator_adapter.h"
#include "disk/document_id.h"
#include "disk/document.h"
#include "disk/inverted_index/entry_metadata.h"
#include "disk/database.h"
#include "disk/database_api.h"
#include "disk/inverted_index/inverted_index_api.h"
#include "disk/doc_table/doc_table_disk_c.h"
#include "iterators/iterator_api.h"
#include "redisearch.h"
#include "redismodule.h"
#include <vector>
#include <memory>
#include <optional>

using namespace search::disk;

// Mock iterator that implements the interface expected by QueryIteratorAdapter
class MockUnderlyingIterator {
public:
    struct Entry {
        DocumentID id;
        t_fieldMask fieldMask;

        DocumentID GetID() const { return id; }
        t_fieldMask GetFieldMask() const { return fieldMask; }
    };

private:
    std::vector<Entry> entries_;
    size_t currentIndex_;
    size_t estimatedResults_;
    bool aborted_;

public:
    MockUnderlyingIterator(std::vector<Entry> entries, size_t estimatedResults = 0)
        : entries_(std::move(entries)), currentIndex_(0), estimatedResults_(estimatedResults), aborted_(false) {
        if (estimatedResults_ == 0) {
            estimatedResults_ = entries_.size();
        }
    }

    std::optional<Entry> Next() {
        if (aborted_ || currentIndex_ >= entries_.size()) {
            return std::nullopt;
        }
        return entries_[currentIndex_++];
    }

    std::optional<Entry> SkipTo(DocumentID docId) {
        if (aborted_) {
            return std::nullopt;
        }

        // Find first entry with ID >= docId
        while (currentIndex_ < entries_.size() && entries_[currentIndex_].id.id < docId.id) {
            currentIndex_++;
        }

        if (currentIndex_ >= entries_.size()) {
            return std::nullopt;
        }

        return entries_[currentIndex_++];
    }

    void Rewind() {
        currentIndex_ = 0;
        aborted_ = false;
    }

    size_t EstimateNumResults() const {
        return estimatedResults_;
    }

    void Abort() {
        aborted_ = true;
    }

    bool IsAborted() const {
        return aborted_;
    }
};

class IteratorAdapterTest : public ::testing::Test {
protected:
    RedisModuleCtx *ctx;

    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }

    // Helper to create a mock iterator with specific document IDs and field masks
    std::unique_ptr<MockUnderlyingIterator> CreateMockIterator(
        const std::vector<std::pair<t_docId, t_fieldMask>>& docs,
        size_t estimatedResults = 0) {

        std::vector<MockUnderlyingIterator::Entry> entries;
        for (const auto& [docId, fieldMask] : docs) {
            entries.push_back({{docId}, fieldMask});
        }
        return std::make_unique<MockUnderlyingIterator>(std::move(entries), estimatedResults);
    }

    // Helper to create an adapter with a mock iterator
    QueryIteratorAdapter<MockUnderlyingIterator>* CreateAdapter(
        std::unique_ptr<MockUnderlyingIterator> mockIter,
        t_fieldMask fieldMask = RS_FIELDMASK_ALL,
        double weight = 1.0) {

        QueryIteratorAdapter<MockUnderlyingIterator>* adapter = new QueryIteratorAdapter<MockUnderlyingIterator>(
            std::move(mockIter), fieldMask, weight);

        return adapter;
    }
};

TEST_F(IteratorAdapterTest, BasicConstruction) {
    auto mockIter = CreateMockIterator({{1, RS_FIELDMASK_ALL}, {2, RS_FIELDMASK_ALL}});
    auto adapter = CreateAdapter(std::move(mockIter));

    ASSERT_NE(adapter, nullptr);
    ASSERT_EQ(adapter->base.type, ID_LIST_ITERATOR);
    ASSERT_FALSE(adapter->base.atEOF);
    ASSERT_EQ(adapter->base.lastDocId, 0);
    ASSERT_NE(adapter->base.current, nullptr);
}

TEST_F(IteratorAdapterTest, NumEstimated) {
    const size_t expectedCount = 42;
    auto mockIter = CreateMockIterator({{1, RS_FIELDMASK_ALL}}, expectedCount);
    auto adapter = CreateAdapter(std::move(mockIter));

    ASSERT_EQ(adapter->NumEstimated(), expectedCount);
    ASSERT_EQ(adapter->base.NumEstimated(&adapter->base), expectedCount);
}

TEST_F(IteratorAdapterTest, BasicRead) {
    auto mockIter = CreateMockIterator({{1, 0x1}, {2, 0x2}, {3, 0x4}});
    auto adapter = CreateAdapter(std::move(mockIter));

    // Read first document
    IteratorStatus status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.lastDocId, 1);
    ASSERT_EQ(adapter->base.current->docId, 1);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x1);
    ASSERT_FALSE(adapter->base.atEOF);

    // Read second document
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.lastDocId, 2);
    ASSERT_EQ(adapter->base.current->docId, 2);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x2);

    // Read third document
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.lastDocId, 3);
    ASSERT_EQ(adapter->base.current->docId, 3);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x4);

    // Should be at EOF now
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(adapter->base.atEOF);
}

TEST_F(IteratorAdapterTest, ReadWithFieldMaskFiltering) {
    // Create documents with different field masks
    auto mockIter = CreateMockIterator({{1, 0x1}, {2, 0x2}, {3, 0x3}, {4, 0x4}});

    // Create adapter that only accepts documents with field mask 0x2
    auto adapter = CreateAdapter(std::move(mockIter), 0x2);

    // Should skip document 1 (mask 0x1 & 0x2 = 0)
    IteratorStatus status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.current->docId, 2);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x2);

    // Should read document 3 (mask 0x3 & 0x2 = 0x2)
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.current->docId, 3);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x3);

    // Should skip document 4 (mask 0x4 & 0x2 = 0) and reach EOF
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(adapter->base.atEOF);
}

TEST_F(IteratorAdapterTest, SkipTo) {
    auto mockIter = CreateMockIterator({{1, 0x1}, {3, 0x2}, {5, 0x4}, {7, 0x8}});
    auto adapter = CreateAdapter(std::move(mockIter));

    // Skip to document 3 (exact match)
    IteratorStatus status = adapter->SkipTo(3);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.current->docId, 3);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x2);

    // Skip to document 6 (should find document 7)
    status = adapter->SkipTo(6);
    ASSERT_EQ(status, ITERATOR_NOTFOUND);
    ASSERT_EQ(adapter->base.current->docId, 7);
    ASSERT_EQ(adapter->base.current->fieldMask, 0x8);

    // Skip to document 10 (beyond end)
    status = adapter->SkipTo(10);
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(adapter->base.atEOF);
}

TEST_F(IteratorAdapterTest, Rewind) {
    auto mockIter = CreateMockIterator({{1, 0x1}, {2, 0x2}, {3, 0x4}});
    auto adapter = CreateAdapter(std::move(mockIter));

    // Read first document
    IteratorStatus status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.lastDocId, 1);

    // Rewind
    adapter->Rewind();
    ASSERT_EQ(adapter->base.lastDocId, 0);
    ASSERT_FALSE(adapter->base.atEOF);

    // Should be able to read from beginning again
    status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(adapter->base.current->docId, 1);
}

TEST_F(IteratorAdapterTest, Revalidate) {
    auto mockIter = CreateMockIterator({{1, 0x1}});
    auto adapter = CreateAdapter(std::move(mockIter));

    ValidateStatus status = adapter->Revalidate();
    ASSERT_EQ(status, VALIDATE_OK);
}

TEST_F(IteratorAdapterTest, EmptyIterator) {
    auto mockIter = CreateMockIterator({});
    auto adapter = CreateAdapter(std::move(mockIter));

    IteratorStatus status = adapter->Read();
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(adapter->base.atEOF);

    status = adapter->SkipTo(1);
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(adapter->base.atEOF);
}

TEST_F(IteratorAdapterTest, FunctionPointerInterface) {
    auto mockIter = CreateMockIterator({{1, 0x1}, {2, 0x2}});
    auto adapter = CreateAdapter(std::move(mockIter));
    QueryIterator* base = &adapter->base;

    // Test NumEstimated function pointer
    ASSERT_EQ(base->NumEstimated(base), 2);

    // Test Read function pointer
    IteratorStatus status = base->Read(base);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(base->current->docId, 1);

    // Test SkipTo function pointer
    status = base->SkipTo(base, 2);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(base->current->docId, 2);

    // Test Rewind function pointer
    base->Rewind(base);
    ASSERT_EQ(base->lastDocId, 0);

    // Test Revalidate function pointer
    ValidateStatus validateStatus = base->Revalidate(base);
    ASSERT_EQ(validateStatus, VALIDATE_OK);

    // Test Free function pointer (should not crash)
    base->Free(base);
}

// Tests with actual implementations
class IteratorAdapterRealImplTest : public ::testing::Test {
protected:
    std::string db_path;
    RedisModuleCtx *ctx;
    search::disk::Database* db;
    search::disk::Database::Index* index;

    void SetUp() override {
        db_path = "test_iterator_adapter_db";
        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Clean up any existing database
        DiskDatabase_Delete(ctx, db_path.c_str());

        // Create database and index
        DiskDatabase* disk_db = DiskDatabase_Create(ctx, db_path.c_str());
        db = reinterpret_cast<search::disk::Database*>(disk_db);
        index = reinterpret_cast<search::disk::Database::Index*>(db->OpenIndex("test_idx", DocumentType_Hash));
    }

    void TearDown() override {
        if (db) {
            DiskDatabase_Destroy(reinterpret_cast<DiskDatabase*>(db));
            db = nullptr;
            index = nullptr;
        }

        if (ctx) {
            DiskDatabase_Delete(ctx, db_path.c_str());
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }

    // Helper to add documents to the inverted index
    void AddInvertedIndexDocuments(const std::string& term, const std::vector<std::pair<t_docId, t_fieldMask>>& docs) {
        for (const auto& [docId, fieldMask] : docs) {
            // Use the C API to add documents to the inverted index
            bool success = DiskDatabase_IndexDocument(
                reinterpret_cast<DiskIndex*>(index), term.c_str(), docId, fieldMask);
            ASSERT_TRUE(success);
        }
    }

    // Helper to add documents to the doc table
    void AddDocTableDocuments(const std::vector<std::pair<std::string, t_docId>>& docs) {
        for (const auto& [key, expectedDocId] : docs) {
            t_docId docId = DocTableDisk_Put(
                reinterpret_cast<DiskIndex*>(index), key.c_str(), 1.0, 0, 0);
            ASSERT_EQ(docId, expectedDocId);
        }
    }
};

TEST_F(IteratorAdapterRealImplTest, InvertedIndexIteratorAdapter) {
    // Add some documents to the inverted index
    AddInvertedIndexDocuments("test_term", {{1, 0x1}, {3, 0x2}, {5, 0x4}});

    // Create an inverted index iterator through the adapter
    QueryIterator* iter = NewDiskInvertedIndexIterator(
        reinterpret_cast<DiskIndex*>(index), "test_term", RS_FIELDMASK_ALL, 1.0);

    ASSERT_NE(iter, nullptr);
    ASSERT_EQ(iter->type, ID_LIST_ITERATOR);
    ASSERT_FALSE(iter->atEOF);

    // Test reading documents
    IteratorStatus status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 1);
    ASSERT_EQ(iter->current->fieldMask, 0x1);

    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 3);
    ASSERT_EQ(iter->current->fieldMask, 0x2);

    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 5);
    ASSERT_EQ(iter->current->fieldMask, 0x4);

    // Should be at EOF
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(iter->atEOF);

    // Test rewind
    iter->Rewind(iter);
    ASSERT_EQ(iter->lastDocId, 0);
    ASSERT_FALSE(iter->atEOF);

    // Should be able to read again
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 1);

    // Test skip to
    iter->Rewind(iter);
    status = iter->SkipTo(iter, 3);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 3);

    status = iter->SkipTo(iter, 4);
    ASSERT_EQ(status, ITERATOR_NOTFOUND);
    ASSERT_EQ(iter->current->docId, 5);

    // Clean up
    iter->Free(iter);
}

TEST_F(IteratorAdapterRealImplTest, InvertedIndexIteratorAdapterWithFieldMaskFiltering) {
    // Add documents with different field masks
    AddInvertedIndexDocuments("filter_term", {{1, 0x1}, {2, 0x2}, {3, 0x3}, {4, 0x4}});

    // Create iterator that only accepts field mask 0x2
    QueryIterator* iter = NewDiskInvertedIndexIterator(
        reinterpret_cast<DiskIndex*>(index), "filter_term", 0x2, 1.0);

    ASSERT_NE(iter, nullptr);

    // Should skip document 1 and read document 2
    IteratorStatus status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 2);

    // Should read document 3 (0x3 & 0x2 = 0x2)
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 3);

    // Should skip document 4 and reach EOF
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_EOF);

    iter->Free(iter);
}

TEST_F(IteratorAdapterRealImplTest, DocTableIteratorAdapter) {
    // Add some documents to the doc table
    AddDocTableDocuments({{"key1", 1}, {"key2", 2}, {"key3", 3}});

    // Create a doc table iterator through the adapter
    QueryIterator* iter = DocTableDisk_NewQueryIterator(
        reinterpret_cast<DiskIndex*>(index), 1.0);

    ASSERT_NE(iter, nullptr);
    ASSERT_EQ(iter->type, ID_LIST_ITERATOR);
    ASSERT_FALSE(iter->atEOF);

    // Test reading documents
    IteratorStatus status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 1);
    ASSERT_EQ(iter->current->fieldMask, RS_FIELDMASK_ALL);

    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 2);

    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 3);

    // Should be at EOF
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_EOF);
    ASSERT_TRUE(iter->atEOF);

    // Test rewind
    iter->Rewind(iter);
    ASSERT_EQ(iter->lastDocId, 0);
    ASSERT_FALSE(iter->atEOF);

    // Should be able to read again
    status = iter->Read(iter);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 1);

    // Test skip to
    iter->Rewind(iter);
    status = iter->SkipTo(iter, 2);
    ASSERT_EQ(status, ITERATOR_OK);
    ASSERT_EQ(iter->current->docId, 2);

    // Clean up
    iter->Free(iter);
}

TEST_F(IteratorAdapterRealImplTest, EmptyInvertedIndexIterator) {
    // Create iterator for non-existent term
    QueryIterator* iter = NewDiskInvertedIndexIterator(
        reinterpret_cast<DiskIndex*>(index), "nonexistent_term", RS_FIELDMASK_ALL, 1.0);

    // Should return nullptr for empty iterator
    ASSERT_EQ(iter, nullptr);
}

TEST_F(IteratorAdapterRealImplTest, NumEstimatedWithRealImplementations) {
    // Add documents to inverted index
    AddInvertedIndexDocuments("estimate_term", {{1, 0x1}, {2, 0x2}, {3, 0x4}});

    QueryIterator* iter = NewDiskInvertedIndexIterator(
        reinterpret_cast<DiskIndex*>(index), "estimate_term", RS_FIELDMASK_ALL, 1.0);

    ASSERT_NE(iter, nullptr);

    // The estimated count should be reasonable (exact value depends on implementation)
    size_t estimated = iter->NumEstimated(iter);
    ASSERT_GT(estimated, 0);

    iter->Free(iter);

    // Test with doc table
    AddDocTableDocuments({{"key1", 1}, {"key2", 2}});

    QueryIterator* docIter = DocTableDisk_NewQueryIterator(
        reinterpret_cast<DiskIndex*>(index), 1.0);

    ASSERT_NE(docIter, nullptr);

    estimated = docIter->NumEstimated(docIter);
    ASSERT_GT(estimated, 0);

    docIter->Free(docIter);
}
