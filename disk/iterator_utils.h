#pragma once
#include <sstream>

namespace search::disk
{

static inline std::optional<DocumentID> lastIdFromBlockKey(std::string_view key, std::string_view prefix) {
    return DocumentID::DeserializeFromKey(key, prefix);
}

template <typename BlockType>
bool AdvanceToBlockWithDocId(std::optional<BlockType>& block, 
    rocksdb::Slice& key,
    std::optional<DocumentID> currentDocId, 
    std::optional<DocumentID> lastDocId, 
    std::string_view prefix,
    DocumentID docId, 
    rocksdb::Iterator* iter) {
    // We can only skip with a valid block in hand
    // this is because we must not skip backward so it is important to have a reference point
    if (!block) {
        // we don't have a block which means we don't have a reference point, we can't skip
        return false;
    }

    if (lastDocId && docId <= *lastDocId) {
        // we can't skip backward
        return false;
    }

    const DocumentID lastIdFromBlock = block->LastId();
#ifdef ENABLE_ASSERT
    const std::optional<DocumentID> lastIdFromKey = lastIdFromBlockKey(iter->key().ToStringView(), prefix);
    if (!lastIdFromKey ||lastIdFromBlock.id != lastIdFromKey->id) {
        if (lastIdFromKey) {
            RedisModule_Log(nullptr, "warning", "last id from block and key don't match, %lu vs %lu", lastIdFromBlock.id, lastIdFromKey->id);
        } else {
            RedisModule_Log(nullptr, "warning", "last id from key is empty vs %lu", lastIdFromBlock.id);
        }
        RS_ABORT("last id from block and key don't match");
        return false;
    }
#endif
    if (docId > lastIdFromBlock) {
        // we need to move to a different block
        // prepare the target key we need to seek to
        std::stringstream keyStream;
        keyStream << prefix;
        docId.SerializeAsKey(keyStream);
        const std::string target = keyStream.str();

        // only change block_ state after we managed to move to a new block
        // otherwise function might not be consistent
        iter->Seek(target);
        if (!iter->Valid()) {
            return false;
        }
        key = iter->key();
        if (!key.starts_with(prefix)) {
          return false;
        }
        auto newBlock = BlockType::Deserialize(iter->value());
        if (!newBlock) {
          return false;
        } else if (!newBlock->SkipTo(docId)) {
            RS_ABORT("failed to skip to doc id, not expected to occur due to previous checks");
            return false;
        }
        block = std::move(*newBlock);
    } else if (currentDocId && currentDocId->id != docId.id) {
        // docId < currentDocId <= lastIdFromBlock
        // or
        // currentDocId < docId <= lastIdFromBlock
        // we will try and skip to docId within the block
        // if it is a backward skip we will return the next document in line
        // essentially this means the user can ask us to skip backward, doesn't mean we allow it
        if (!block->SkipTo(docId)) {
            RS_ABORT("failed to skip to doc id, not expected to occur due to previous checks");
            return false;
        }
    }
    return true;
}

} // namespace search::disk