#include <rocksdb/db.h>
#include <charconv>
#include "rmutil/rm_assert.h"
#include "disk/inverted_index/inverted_index.h"
#include "disk/document_id.h"
#include "disk/block_util.h"
#include "disk/iterator_utils.h"

namespace search::disk {

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
    return SkipToDocumentId(*this, docIds_, docId);
}

std::optional<InvertedIndexBlock> InvertedIndexBlock::Deserialize(rocksdb::Slice value) {
    std::string_view view = value.ToStringView();
    if (view.size() < 5) {
        return std::nullopt;
    }
    uint8_t version = uint8_t(view.substr(0, 1)[0]);
    std::string_view docCountView = view.substr(1, 4);
    uint32_t documentCount = 0;
    std::error_code error = ReadElement<uint32_t>(docCountView, documentCount);
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
    if (!AdvanceToBlockWithDocId<InvertedIndexBlock>(block_, key_, currentId(), lastDocId_, prefix_, docId, iter_.get())) {
        return std::nullopt;
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
