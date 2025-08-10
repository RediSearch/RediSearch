#include <algorithm>
#include <sstream>
#include <vector>
#include "rmutil/rm_assert.h"
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "disk/inverted_index/merge_operator.h"
#include "disk/inverted_index/inverted_index.h"

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

  t_docId prevDocId = 0;
  for (const rocksdb::Slice& operand : merge_in.operand_list) {
    auto nextBlockWithLowestDocId = InvertedIndexBlock::Deserialize(operand);
    if (!nextBlockWithLowestDocId) {
      // Deserialization failed, skip this operand
      RS_ABORT("failed to deserialize block");
      RedisModule_Log(nullptr, "warning", "failed to deserialize block");
      continue;
    }
    const auto lastId = nextBlockWithLowestDocId->LastId().id;
    RS_ASSERT(prevDocId < lastId);
    prevDocId = lastId;
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
  merge_out->output_key.clear();
  if (count) {
    merge_out->new_value = InvertedIndexBlock::Create(ids, metadata, count);

    std::stringstream key;

    DocumentID docId{prevDocId};
    auto view = merge_in.key.ToStringView();
    auto pos = view.find_last_of(SingleDocument::KEY_DELIMITER);
    auto prefix = view.substr(0, pos);
    key << prefix << SingleDocument::KEY_DELIMITER;
    docId.SerializeAsKey(key);
    std::string outKey = key.str();
    merge_out->output_key = std::move(outKey);
  }
  return true;
}

}  // namespace search::disk
