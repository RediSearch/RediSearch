#include <algorithm>
#include <sstream>
#include <vector>
#include "rmutil/rm_assert.h"
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "src/disk/inverted_index/merge_operator.h"
#include "src/disk/inverted_index/inverted_index.h"

namespace search::disk {

InvertedIndexMergeOperator::InvertedIndexMergeOperator(std::shared_ptr<DeletedIds> deletedIds)
  : deletedIds_(deletedIds)
{
}

static size_t appendSortedBlockDocuments(InvertedIndexBlock& block, std::shared_ptr<DeletedIds> deletedIds,
                                         std::ostringstream& ids, std::ostringstream& metadata) {
    size_t count = 0;
    while (auto doc = block.Next()) {
      if (deletedIds->contains(doc->docId.id)) {
        continue;
      }
      doc->Serialize(ids, metadata);
      ++count;
    }
    return count;
}

bool InvertedIndexMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {

  // all operands are sorted
  // highest value exists in existing value
  size_t count = 0;
  std::ostringstream ids;
  std::ostringstream metadata;

  for (const rocksdb::Slice& operand : merge_in.operand_list) {
    auto nextBlockWithLowestDocId = InvertedIndexBlock::Deserialize(operand);
    if (!nextBlockWithLowestDocId) {
      // Deserialization failed, skip this operand
      continue;
    }
    count += appendSortedBlockDocuments(*nextBlockWithLowestDocId, deletedIds_, ids, metadata);
  }

  // Start with existing value if present
  if (merge_in.existing_value) {
    auto lastBlockWithHighestDocId = InvertedIndexBlock::Deserialize(*merge_in.existing_value);
    if (lastBlockWithHighestDocId) {
      count += appendSortedBlockDocuments(*lastBlockWithHighestDocId, deletedIds_, ids, metadata);
    }
  }
  merge_out->new_value.clear();
  if (count) {
    merge_out->new_value = InvertedIndexBlock::Create(ids, metadata, count);
  }
  return true;
}

}  // namespace search::disk
