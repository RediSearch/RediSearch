#pragma once

#include <memory>
#include <string>
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"


namespace search::disk {

class InvertedIndexMergeOperator : public ROCKSDB_NAMESPACE::MergeOperator {
 public:
    InvertedIndexMergeOperator(std::shared_ptr<DeletedIds> deletedIds);
    ~InvertedIndexMergeOperator() override = default;

    // Main merge method that combines existing value with operands
    bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

    const char* Name() const override { return "InvertedIndexMergeOperator"; }

    // Allow single operand merges (important for inverted index updates)
    bool AllowSingleOperand() const override { return true; }

 private:

    std::shared_ptr<DeletedIds> deletedIds_;
};

}  // namespace search::disk
