#include "disk/inverted_index/inverted_index.h"
#include "disk/inverted_index/deleted_ids_compaction_filter.h"

namespace search::disk {

DeletedIdsCompactionFilter::DeletedIdsCompactionFilter(std::shared_ptr<DeletedIds> deleted_ids)
    : deleted_ids_(deleted_ids) {}

bool DeletedIdsCompactionFilter::Filter(
    int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value,
    std::string* new_value, bool* value_changed) const {
    // We'll implement our logic in FilterV2, so just delegate to it
    auto decision = FilterV2(level, key, rocksdb::CompactionFilter::ValueType::kValue,
                            existing_value, new_value, nullptr);
    *value_changed = decision == rocksdb::CompactionFilter::Decision::kChangeValue;
    return decision == rocksdb::CompactionFilter::Decision::kRemove;
}

rocksdb::CompactionFilter::Decision DeletedIdsCompactionFilter::FilterV2(
    int /*level*/,
    const rocksdb::Slice& key,
    rocksdb::CompactionFilter::ValueType value_type,
    const rocksdb::Slice& existing_value,
    std::string* new_value,
    std::string* /*skip_until*/) const {
    
    // We only handle our values
    if (value_type != rocksdb::CompactionFilter::ValueType::kValue) {
        return rocksdb::CompactionFilter::Decision::kKeep;
    }
    
    // Deserialize the block
    std::optional<InvertedIndexBlock> block = InvertedIndexBlock::Deserialize(existing_value);
    if (!block) {
        // If we can't deserialize the block, keep it to be safe
        // Don't assert since we test this behaviour in the unit tests
        return rocksdb::CompactionFilter::Decision::kKeep;
    }
    
    size_t count = 0;
    const size_t originalCount = block->RemainingDocumentCount();
    std::vector<Document> documents;
    documents.reserve(originalCount);

    for (auto doc = block->Next(); doc; doc = block->Next()) {
        if (deleted_ids_->contains(doc->docId.id)) {
            // document was deleted, we need to remove it
            continue;
        }
        documents.push_back(*doc);
    }

    if (documents.empty()) {
        return rocksdb::CompactionFilter::Decision::kRemove;
    } else if (documents.size() == originalCount) {
        return rocksdb::CompactionFilter::Decision::kKeep;
    } else {
        // Serialize the documents back into a new block
        *new_value = InvertedIndexBlock::Create(documents.begin(), documents.end());
        return rocksdb::CompactionFilter::Decision::kChangeValue;
    }
}

} // namespace search::disk