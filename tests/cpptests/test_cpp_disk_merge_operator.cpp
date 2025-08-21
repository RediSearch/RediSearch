#include <gtest/gtest.h>
#include "disk/inverted_index/merge_operator.h"
#include "disk/inverted_index/inverted_index.h"
#include "disk/document.h"
#include "rocksdb/merge_operator.h"
#include "redisearch.h"
#include "disk/database_api.h"
#include "disk/inverted_index/inverted_index_api.h"
#include <memory>
#include <string>
#include <vector>

namespace search::disk {

class InvertedIndexMergeOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    merge_op_ = std::make_unique<InvertedIndexMergeOperator>(std::make_shared<DeletedIds>());
  }

  std::unique_ptr<InvertedIndexMergeOperator> merge_op_;

  // Helper to create a document with specified ID and field mask
  Document CreateDocument(DocumentID docId, t_fieldMask fieldMask) {
    Document doc;
    doc.docId = docId;
    doc.metadata.fieldMask = fieldMask;
    return doc;
  }

  // Helper to create a serialized document block
  std::string CreateSerializedBlock(const std::vector<Document>& docs) {
    return InvertedIndexBlock::Create(docs.begin(), docs.end());
  }

  // Helper to perform a merge operation with proper lifetime management
  bool PerformMerge(const std::string& key,
                   const std::string& existing_block,
                   const std::vector<std::string>& operand_blocks,
                   std::string& result) {
    // Convert strings to slices
    rocksdb::Slice key_slice(key);
    rocksdb::Slice existing_slice(existing_block);

    std::vector<rocksdb::Slice> operand_slices;
    operand_slices.reserve(operand_blocks.size());
    for (const auto& block : operand_blocks) {
      operand_slices.emplace_back(block);
    }

    // Create merge input and output
    // Use nullptr for existing_value if the existing_block is empty
    const rocksdb::Slice* existing_ptr = existing_block.empty() ? nullptr : &existing_slice;
    rocksdb::MergeOperator::MergeOperationInput merge_in(
        key_slice, existing_ptr, operand_slices, nullptr);

    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(result, existing_operand);

    return merge_op_->FullMergeV2(merge_in, &merge_out);
  }
};

TEST_F(InvertedIndexMergeOperatorTest, BasicMerge) {
  // Create test documents
  std::vector<Document> existing_docs = {
    CreateDocument(DocumentID{1}, 0x01),
    CreateDocument(DocumentID{2}, 0x02)
  };

  std::vector<Document> operand_docs = {
    CreateDocument(DocumentID{3}, 0x01),
    CreateDocument(DocumentID{4}, 0x03)
  };

  // Serialize the document blocks
  std::string existing_block = CreateSerializedBlock(existing_docs);
  std::string operand_block = CreateSerializedBlock(operand_docs);

  // Perform the merge operation
  std::string merged_result;
  EXPECT_TRUE(PerformMerge("test_term", existing_block, {operand_block}, merged_result));

  // Verify merged result is not empty
  ASSERT_FALSE(merged_result.empty());

  // Deserialize and verify the merged block
  auto merged_block = InvertedIndexBlock::Deserialize(rocksdb::Slice(merged_result));
  ASSERT_TRUE(merged_block.has_value());

  // Verify all documents are present in the merged result
  // Note: The merge operator processes operands first, then existing value
  std::vector<DocumentID> expected_ids = {
    DocumentID{3}, DocumentID{4}, DocumentID{1}, DocumentID{2}
  };

  for (const auto& expected_id : expected_ids) {
    auto doc = merged_block->Next();
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(expected_id, doc->docId);
  }

  // No more documents should be present
  EXPECT_FALSE(merged_block->Next().has_value());
}

TEST_F(InvertedIndexMergeOperatorTest, MergeWithoutExistingValue) {
  // Create operand with documents 1 and 2
  std::vector<Document> operand_docs = {
    CreateDocument(DocumentID{1}, 0x01),
    CreateDocument(DocumentID{2}, 0x02)
  };
  std::string operand_block = CreateSerializedBlock(operand_docs);

  // Perform merge without existing value (empty string)
  std::string merged_result;
  EXPECT_TRUE(PerformMerge("test_term", "", {operand_block}, merged_result));

  // Verify result
  ASSERT_FALSE(merged_result.empty());

  // Deserialize and verify the block
  auto block = InvertedIndexBlock::Deserialize(rocksdb::Slice(merged_result));
  ASSERT_TRUE(block.has_value());

  // Verify documents
  auto doc1 = block->Next();
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(DocumentID{1}, doc1->docId);

  auto doc2 = block->Next();
  ASSERT_TRUE(doc2.has_value());
  EXPECT_EQ(DocumentID{2}, doc2->docId);

  // No more documents
  EXPECT_FALSE(block->Next().has_value());
}

TEST_F(InvertedIndexMergeOperatorTest, MergeMultipleOperands) {
  // Create existing block with document 1
  std::vector<Document> existing_docs = {
    CreateDocument(DocumentID{1}, 0x01)
  };
  std::string existing_block = CreateSerializedBlock(existing_docs);

  // Create multiple operands
  std::vector<Document> operand1_docs = {
    CreateDocument(DocumentID{2}, 0x02)
  };
  std::vector<Document> operand2_docs = {
    CreateDocument(DocumentID{3}, 0x03)
  };
  std::vector<Document> operand3_docs = {
    CreateDocument(DocumentID{4}, 0x04)
  };

  std::string operand1_block = CreateSerializedBlock(operand1_docs);
  std::string operand2_block = CreateSerializedBlock(operand2_docs);
  std::string operand3_block = CreateSerializedBlock(operand3_docs);

  // Perform merge
  std::string merged_result;
  EXPECT_TRUE(PerformMerge("test_term", existing_block,
                          {operand1_block, operand2_block, operand3_block},
                          merged_result));

  // Verify merged result
  ASSERT_FALSE(merged_result.empty());

  // Deserialize and verify the merged block
  auto merged_block = InvertedIndexBlock::Deserialize(rocksdb::Slice(merged_result));
  ASSERT_TRUE(merged_block.has_value());

  // Should have 4 documents in total (operands first, then existing)
  // Order: [2, 3, 4, 1]
  std::vector<DocumentID> expected_ids = {
    DocumentID{2}, DocumentID{3}, DocumentID{4}, DocumentID{1}
  };
  std::vector<t_fieldMask> expected_masks = {0x02, 0x03, 0x04, 0x01};

  for (size_t i = 0; i < expected_ids.size(); i++) {
    auto doc = merged_block->Next();
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(expected_ids[i], doc->docId);
    EXPECT_EQ(expected_masks[i], doc->metadata.fieldMask);
  }

  // No more documents
  EXPECT_FALSE(merged_block->Next().has_value());
}

TEST_F(InvertedIndexMergeOperatorTest, MergeWithDuplicateDocIds) {
  // Create existing block with documents 1 and 2
  std::vector<Document> existing_docs = {
    CreateDocument(DocumentID{1}, 0x01),
    CreateDocument(DocumentID{2}, 0x02)
  };
  std::string existing_block = CreateSerializedBlock(existing_docs);

  // Create operand with documents 2 (duplicate) and 3
  std::vector<Document> operand_docs = {
    CreateDocument(DocumentID{2}, 0x03), // Different field mask
    CreateDocument(DocumentID{3}, 0x04)
  };
  std::string operand_block = CreateSerializedBlock(operand_docs);

  // Perform merge
  std::string merged_result;
  EXPECT_TRUE(PerformMerge("test_term", existing_block, {operand_block}, merged_result));

  // Verify merged result
  ASSERT_FALSE(merged_result.empty());

  // Deserialize and verify the merged block
  auto merged_block = InvertedIndexBlock::Deserialize(rocksdb::Slice(merged_result));
  ASSERT_TRUE(merged_block.has_value());

  // Expected order: operands first [2, 3], then existing [1, 2]
  // But document 2 appears in both, so the merge operator should handle duplicates

  // Document 2 (from operand)
  auto doc1 = merged_block->Next();
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(DocumentID{2}, doc1->docId);
  EXPECT_EQ(0x03, doc1->metadata.fieldMask);

  // Document 3 (from operand)
  auto doc2 = merged_block->Next();
  ASSERT_TRUE(doc2.has_value());
  EXPECT_EQ(DocumentID{3}, doc2->docId);
  EXPECT_EQ(0x04, doc2->metadata.fieldMask);

  // Document 1 (from existing)
  auto doc3 = merged_block->Next();
  ASSERT_TRUE(doc3.has_value());
  EXPECT_EQ(DocumentID{1}, doc3->docId);
  EXPECT_EQ(0x01, doc3->metadata.fieldMask);

  // Document 2 (from existing) - this might be merged or appear separately
  auto doc4 = merged_block->Next();
  ASSERT_TRUE(doc4.has_value());
  EXPECT_EQ(DocumentID{2}, doc4->docId);
  EXPECT_EQ(0x02, doc4->metadata.fieldMask);

  // No more documents
  EXPECT_FALSE(merged_block->Next().has_value());
}

TEST_F(InvertedIndexMergeOperatorTest, MergeInDatabase) {
  std::string db_path = "test_inverted_index_compaction_db";
  // Get Redis context and create database
  RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
  DiskDatabase_Delete(ctx, db_path.c_str());
  DiskDatabase *db = DiskDatabase_Create(ctx, db_path.c_str());
  DiskIndex *index = DiskDatabase_OpenIndex(db, "idx", DocumentType_Hash);
  ASSERT_NE(db, nullptr);
  ASSERT_NE(index, nullptr);

  size_t documentCount = 100;
  for (size_t i = 0; i < documentCount; ++i) {
    DiskDatabase_IndexDocument(index, "hello", i, i);
  }

  DiskDatabase_CompactIndex(index);

  DiskIterator* it = DiskDatabase_NewInvertedIndexIterator(index, "hello");
  ASSERT_NE(it, nullptr);
  for (size_t i = 0; i < documentCount; ++i) {
    RSIndexResult result;
    ASSERT_TRUE(InvertedIndexIterator_Next(it, &result));
    ASSERT_EQ(i, result.docId);
    ASSERT_EQ(i, result.fieldMask);
  }
  InvertedIndexIterator_Free(it);
  DiskDatabase_Destroy(db);
  DiskDatabase_Delete(ctx, db_path.c_str());
  RedisModule_FreeThreadSafeContext(ctx);
}

}  // namespace search::disk
