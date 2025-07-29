#include <rocksdb/db.h>
#include <charconv>
#include "rmutil/rm_assert.h"
#include "disk/inverted_index/inverted_index.h"
#include "disk/document_id.h"

namespace search::disk {

template <typename T>
static std::error_code read_element(std::string_view& buffer, T& element) {
    if (buffer.size() < sizeof(T)) {
        return std::make_error_code(std::errc::message_size);
    }
    const T* elements = reinterpret_cast<const T*>(buffer.data());
    element = boost::endian::big_to_native(elements[0]);
    buffer.remove_prefix(sizeof(T));
    return std::error_code();
}

// Caller should verify buffer size is at least sizeof(t_docId) and aligned to it
static DocumentID DeserializeLastId(std::string_view& buffer) {
    std::string_view last = buffer.substr(buffer.size() - sizeof(t_docId));
    return DocumentID::DeserializeFromValue(last);
}

bool SingleDocument::Write(Column& col, const std::string& prefix, Document& doc) {
    std::stringstream keyStream;
    keyStream << prefix;
    SerializeKey(keyStream, doc.docId);
    return col.Write(keyStream.str(), InvertedIndexBlock::Create({doc}));
}

std::optional<DocumentID> InvertedIndexBlock::CurrentId() const {
    if (docIds_.size() < sizeof(t_docId)) {
        RS_ASSERT(docIds_.empty()); // size is expected to be zero if we don't have enough space remaining
        return std::nullopt;
    }
    return DocumentID::template DeserializeFromValue<false>(docIds_);
}

DocumentID InvertedIndexBlock::LastId() const {
    return cachedLastId_;
}

std::optional<Document> InvertedIndexBlock::Next() {
    if (docIds_.size() < sizeof(t_docId) || metadata_.size() < sizeof(t_fieldMask)) {
        RS_ASSERT(docIds_.empty() || metadata_.empty());
        return std::nullopt;
    }
    return Document::Deserialize(docIds_, metadata_);
}

bool InvertedIndexBlock::SkipTo(DocumentID docId) {
    const auto bigEndianLess = [] (t_docId rawLeft, t_docId rawRight) -> bool
    {
        const t_docId left = boost::endian::big_to_native(rawLeft);
        const t_docId right = boost::endian::big_to_native(rawRight);
        return left < right;
    };
    const t_docId* start = reinterpret_cast<const t_docId*>(docIds_.data());
    const t_docId* end = reinterpret_cast<const t_docId*>(docIds_.data() + docIds_.size());
    const t_docId bigEndianDocId = boost::endian::native_to_big(docId.id);
    const t_docId* it = std::lower_bound(start, end, bigEndianDocId, bigEndianLess);
    if (it == end) {
        // reached the end of the block and we didn't find a larger docId
        // shouldn't really happen since the block last id should be bigger than docId
        // only option is we ran out of docIds
        docIds_ = std::string_view();
        metadata_ = std::string_view();
        RS_ABORT("reached the end of the block, caller should have checked doc id was in block and block was not empty in advance");
        return false;
    }
    // find the index of the found element
    // remove the prefixes from the docIds and metadata
    const size_t index = std::distance(start, it);
    docIds_.remove_prefix(index * sizeof(t_docId));
    metadata_.remove_prefix(index * sizeof(t_fieldMask));
    return true;
}

std::optional<InvertedIndexBlock> InvertedIndexBlock::Deserialize(rocksdb::Slice value) {
    std::string_view view = value.ToStringView();
    if (view.size() < 5) {
        return std::nullopt;
    }
    uint8_t version = uint8_t(view.substr(0, 1)[0]);
    std::string_view docCountView = view.substr(1, 4);
    uint32_t documentCount = 0;
    std::error_code error = read_element<uint32_t>(docCountView, documentCount);
    if (error || documentCount == 0) {
        return std::nullopt;
    }
    const size_t byteCount = documentCount * sizeof(t_docId);
    if (view.size() < 5 + byteCount) {
        return std::nullopt;
    }
    std::string_view docIds = view.substr(5, byteCount);
    std::string_view metadata = view.substr(5 + byteCount);
    if (docIds.size() < sizeof(t_docId) || metadata.size() < sizeof(t_fieldMask)) {
        return std::nullopt;
    }
    RS_ASSERT(docIds.size() % sizeof(t_docId) == 0);
    RS_ASSERT(metadata.size() % sizeof(t_fieldMask) == 0);
    return InvertedIndexBlock{value, version, docIds, metadata, DeserializeLastId(docIds)};
}

InvertedIndexIterator* InvertedIndexIterator::Create(rocksdb::Iterator* iter, const std::string& indexName, const char* term) {
    std::unique_ptr<rocksdb::Iterator> iterPtr(iter);
    if (iter == nullptr) {
        return nullptr;
    }
    if (indexName.empty() || term == nullptr) {
        return nullptr;
    }
    std::stringstream stream;
    stream << indexName << '_' << term << '_';
    std::string prefix = stream.str();
    iter->Seek(prefix);
    if (!iter->Valid()) {
        return nullptr;
    }
    std::string firstKey = iter->key().ToString();
    if (!firstKey.starts_with(prefix)) {
        return nullptr;
    }

    std::optional<InvertedIndexBlock> first = InvertedIndexBlock::Deserialize(iter->value());
    if (!first || first->Empty()) {
        return nullptr;
    }

    const size_t estimation = DocumentID::EstimateCount(*iter, prefix);
    return new InvertedIndexIterator(std::move(iterPtr), prefix, *first, estimation);
}

InvertedIndexIterator::InvertedIndexIterator(std::unique_ptr<rocksdb::Iterator> iter, std::string prefix, InvertedIndexBlock first, size_t countEstimation)
    : iter_(std::move(iter))
    , prefix_(std::move(prefix))
    , countEstimation_(countEstimation)
    , block_(std::move(first))
{
    key_ = iter_->key();
}

std::optional<DocumentID> InvertedIndexIterator::currentId() const {
    if (!block_) {
        return std::nullopt;
    }
    return block_->CurrentId();
}

std::optional<DocumentID> InvertedIndexIterator::lastIdFromBlockKey() const {
    std::string_view view = key_.ToStringView();
    return DocumentID::DeserializeFromKey(view, prefix_);
}

void InvertedIndexIterator::advanceToNextBlock() {
    // clear the current block, we are moving to the next block
    // Valid() should now return false until a new block was assigned
    block_.reset();
    // move to the next speedb value key pair
    iter_->Next();
    // check if we reached the end of speedb inverted index column
    if (!iter_->Valid()) {
        return;
    }
    // check if the key contains the prefix we are looking for
    key_ = iter_->key();
    if (!key_.starts_with(prefix_)) {
        return;
    }
    // deserialize the new block, can fail
    // on failure we won't have a block and Valid() will be false
    auto newBlock = InvertedIndexBlock::Deserialize(iter_->value());
    if (newBlock) {
        block_ = std::move(*newBlock);
    }
}

std::optional<Document> InvertedIndexIterator::Next() {
    if (!Valid()) {
        return std::nullopt;
    }
    std::optional<Document> doc = block_->Next();
    if (doc) {
      lastDocId_ = doc->docId;
    }
    if (block_->Empty()) {
        advanceToNextBlock();
    }
    return doc;
}

std::optional<Document> InvertedIndexIterator::SkipTo(DocumentID docId) {
    // We can only skip with a valid block in hand
    // this is because we must not skip backward so it is important to have a reference point
    if (!Valid()) {
        // we don't have a block which means we don't have a reference point, we can't skip
        return std::nullopt;
    }

    if (lastDocId_ && docId <= *lastDocId_) {
        // we can't skip backward
        return std::nullopt;
    }

    const auto currentDocId = currentId();
    const DocumentID lastIdFromBlock = block_->LastId();
#ifdef ENABLE_ASSERT
    const std::optional<DocumentID> lastIdFromKey = lastIdFromBlockKey();
    if (!lastIdFromKey ||lastIdFromBlock.id != lastIdFromKey->id) {
        if (lastIdFromKey) {
            RedisModule_Log(nullptr, "warning", "last id from block and key don't match, %lu vs %lu", lastIdFromBlock.id, lastIdFromKey->id);
        } else {
            RedisModule_Log(nullptr, "warning", "last id from key is empty vs %lu", lastIdFromBlock.id);
        }
        RS_ABORT("last id from block and key don't match");
        return std::nullopt;
    }
#endif
    if (docId > lastIdFromBlock) {
        // we need to move to a different block
        // prepare the target key we need to seek to
        std::stringstream keyStream;
        keyStream << prefix_;
        SingleDocument::SerializeKey(keyStream, docId);
        const std::string target = keyStream.str();

        // only change block_ state after we managed to move to a new block
        // otherwise function might not be consistent
        iter_->Seek(target);
        if (!iter_->Valid()) {
            return std::nullopt;
        }
        key_ = iter_->key();
        if (!key_.starts_with(prefix_)) {
          return std::nullopt;
        }
        auto newBlock = InvertedIndexBlock::Deserialize(iter_->value());
        if (!newBlock) {
          return std::nullopt;
        }
        block_ = std::move(*newBlock);
    } else if (currentDocId && currentDocId->id != docId.id) {
        // docId < currentDocId <= lastIdFromBlock
        // or
        // currentDocId < docId <= lastIdFromBlock
        // we will try and skip to docId within the block
        // if it is a backward skip we will return the next document in line
        // essentially this means the user can ask us to skip backward, doesn't mean we allow it
        if (!block_->SkipTo(docId)) {
            RS_ABORT("failed to skip to doc id, not expected to occur due to previous checks");
            return std::nullopt;
        }
    }
    std::optional<Document> doc = Next();
    if (doc) {
      lastDocId_ = doc->docId;
    }
    return doc;
}

void InvertedIndexIterator::Rewind() {
    block_.reset(); // Valid() will be false until block will receive a value
    iter_->Seek(prefix_);
    if (!iter_->Valid()) {
        return;
    }
    key_ = iter_->key();
    if (!key_.starts_with(prefix_)) {
        return;
    }
    auto newBlock = InvertedIndexBlock::Deserialize(iter_->value());
    if (newBlock) {
        block_ = std::move(*newBlock);
    }
    lastDocId_.reset();
}

void InvertedIndexIterator::Abort() {
    block_.reset();
}

size_t InvertedIndexIterator::EstimateNumResults() {
    return countEstimation_;
}

bool InvertedIndexIterator::Valid() const {
    return block_.has_value();
}

} // namespace search::disk
