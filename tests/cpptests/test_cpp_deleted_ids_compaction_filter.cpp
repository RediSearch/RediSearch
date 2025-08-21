#include "gtest/gtest.h"
#include "disk/inverted_index/deleted_ids_compaction_filter.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
#include "disk/inverted_index/inverted_index.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include <memory>
#include <string>

using namespace search::disk;

class DeletedIdsCompactionFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a DeletedIds instance
        deleted_ids_ = std::make_shared<DeletedIds>();
        
        // Create the compaction filter
        filter_ = std::make_unique<DeletedIdsCompactionFilter>(deleted_ids_);
    }

    // Helper method to create a serialized inverted index block with the given document IDs
    std::string CreateSerializedBlock(const std::vector<t_docId>& doc_ids) {
        std::vector<search::disk::Document> documents;
        for (const t_docId& doc_id : doc_ids) {
            documents.emplace_back(DocumentID{doc_id}, EntryMetadata{1});
        }
        return InvertedIndexBlock::Create(documents.begin(), documents.end());
    }

    std::shared_ptr<DeletedIds> deleted_ids_;
    std::unique_ptr<DeletedIdsCompactionFilter> filter_;
};

TEST_F(DeletedIdsCompactionFilterTest, NoDeletedIds) {
    // Create a block with document IDs 1, 2, 3
    std::string block_data = CreateSerializedBlock({1, 2, 3});
    
    // No document IDs are deleted
    bool value_changed = false;
    std::string new_value;
    
    // The filter should keep the block
    ASSERT_FALSE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(block_data), 
                                &new_value, &value_changed));
    ASSERT_FALSE(value_changed);
}

TEST_F(DeletedIdsCompactionFilterTest, WithDeletedIds) {
    // Create a block with document IDs 1, 2, 3
    std::string block_data = CreateSerializedBlock({1, 2, 3});
    
    // Mark document ID 2 as deleted
    deleted_ids_->add(2);
    
    bool value_changed = false;
    std::string new_value;
    
    // The filter should remove the block
    ASSERT_FALSE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(block_data), 
                               &new_value, &value_changed));
    ASSERT_TRUE(value_changed);
    InvertedIndexBlock block = InvertedIndexBlock::Deserialize(new_value).value();
    ASSERT_EQ(block.RemainingDocumentCount(), 2);
    ASSERT_EQ(block.Next().value().docId.id, 1);
    ASSERT_EQ(block.Next().value().docId.id, 3);

    deleted_ids_->add(1);
    deleted_ids_->add(3);
    value_changed = false;
    ASSERT_TRUE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(block_data), 
                            &new_value, &value_changed));

    ASSERT_FALSE(value_changed);
}

TEST_F(DeletedIdsCompactionFilterTest, EmptyBlock) {
    // Create an empty block
    std::string block_data = CreateSerializedBlock({});
    
    bool value_changed = false;
    std::string new_value;
    
    // The filter should keep the empty block
    ASSERT_FALSE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(block_data), 
                                &new_value, &value_changed));
    ASSERT_FALSE(value_changed);
}

TEST_F(DeletedIdsCompactionFilterTest, AllDeletedIds) {
    // Create a block with document IDs 1, 2, 3
    std::string block_data = CreateSerializedBlock({1, 2, 3});
    
    // Mark all document IDs as deleted
    deleted_ids_->add(1);
    deleted_ids_->add(2);
    deleted_ids_->add(3);
    
    bool value_changed = false;
    std::string new_value;
    
    // The filter should remove the block
    ASSERT_TRUE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(block_data), 
                               &new_value, &value_changed));
    ASSERT_FALSE(value_changed);
}

TEST_F(DeletedIdsCompactionFilterTest, InvalidBlockData) {
    // Create invalid block data
    std::string invalid_data = "invalid data";
    
    bool value_changed = false;
    std::string new_value;
    
    // The filter should keep the block (fail safe)
    ASSERT_FALSE(filter_->Filter(0, rocksdb::Slice("key"), rocksdb::Slice(invalid_data), 
                                &new_value, &value_changed));
    ASSERT_FALSE(value_changed);
}

TEST_F(DeletedIdsCompactionFilterTest, FilterV2Interface) {
    // Create a block with document IDs 1, 2, 3
    std::string block_data = CreateSerializedBlock({1, 2, 3});
    
    // Mark all documents as deleted
    deleted_ids_->add(1);
    deleted_ids_->add(2);
    deleted_ids_->add(3);
    
    std::string new_value;
    
    // Test the FilterV2 interface
    auto decision = filter_->FilterV2(0, rocksdb::Slice("key"), 
                                     rocksdb::CompactionFilter::ValueType::kValue,
                                     rocksdb::Slice(block_data), &new_value, nullptr);
    
    // The filter should decide to remove the block
    ASSERT_EQ(decision, rocksdb::CompactionFilter::Decision::kRemove);
}

TEST_F(DeletedIdsCompactionFilterTest, FilterV2WithNonValueOperandType) {
    // Create a block with document IDs 1, 2, 3
    std::string block_data = CreateSerializedBlock({1, 2, 3});
    
    // Mark document ID 2 as deleted
    deleted_ids_->add(2);
    
    std::string new_value;
    
    // Test the FilterV2 interface with a non-value type - it should not operate on it
    auto decision = filter_->FilterV2(0, rocksdb::Slice("key"), 
                                     rocksdb::CompactionFilter::ValueType::kMergeOperand,
                                     rocksdb::Slice(block_data), &new_value, nullptr);
    
    // The filter should decide to keep the block for non-value types
    ASSERT_EQ(decision, rocksdb::CompactionFilter::Decision::kKeep);
    ASSERT_TRUE(new_value.empty());
}