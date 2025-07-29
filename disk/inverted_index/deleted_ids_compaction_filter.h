#pragma once

#include "rocksdb/compaction_filter.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
#include <string>
#include <memory>
#include <mutex>

namespace search::disk {

// Compaction filter that removes entries with deleted document IDs
class DeletedIdsCompactionFilter : public rocksdb::CompactionFilter {
public:
    explicit DeletedIdsCompactionFilter(std::shared_ptr<DeletedIds> deleted_ids);
    
    // Main filter method that decides whether to keep or remove a key-value pair
    rocksdb::CompactionFilter::Decision FilterV2(
        int level,
        const rocksdb::Slice& key,
        rocksdb::CompactionFilter::ValueType value_type,
        const rocksdb::Slice& existing_value,
        std::string* new_value,
        std::string* skip_until) const override;
    
    // Legacy filter method (required by the interface)
    bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value,
                std::string* new_value, bool* value_changed) const override;
    
    const char* Name() const override { return "DeletedIdsCompactionFilter"; }

private:    
    // Reference to the DeletedIds container
    std::shared_ptr<DeletedIds> deleted_ids_;
};

} // namespace search::disk