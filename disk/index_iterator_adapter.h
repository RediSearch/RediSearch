#pragma once

#include "index_iterator.h"

namespace search::disk {

// adapts the disk inverted index iterator into the index iterator API
template <typename UnderlyingIteratorType>
struct IndexIteratorAdapter {
    IndexIterator base;
    t_fieldMask fieldMask;
    std::unique_ptr<UnderlyingIteratorType> iter;

    IndexIteratorAdapter(std::unique_ptr<UnderlyingIteratorType> iter, t_fieldMask fieldMask)
        : fieldMask(fieldMask)
        , iter(std::move(iter))
    {
        base.ctx = this;
        base.isValid = true;
        base.current = NewVirtualResult(0, RS_FIELDMASK_ALL);
        base.type = ID_LIST_ITERATOR;
        base.NumEstimated = [](void* ctx) -> size_t { return reinterpret_cast<decltype(this)>(ctx)->NumEstimated(); };
        base.Free = [](indexIterator* iter) -> void { delete reinterpret_cast<decltype(this)>(iter->ctx); };
        base.LastDocId = [](void* ctx) { return reinterpret_cast<decltype(this)>(ctx)->LastDocId(); };
        base.HasNext = [](void* ctx) -> int { return reinterpret_cast<decltype(this)>(ctx)->HasNext(); };
        base.Len = [](void* ctx) { return reinterpret_cast<decltype(this)>(ctx)->Len(); };
        base.Read = [](void* ctx, RSIndexResult** result) -> int { return reinterpret_cast<decltype(this)>(ctx)->Next(result); };
        base.SkipTo = [](void* ctx, t_docId docId, RSIndexResult** result) -> int { return reinterpret_cast<decltype(this)>(ctx)->SkipTo(docId, result); };
        base.Abort = [](void* ctx) { return reinterpret_cast<decltype(this)>(ctx)->Abort(); };
        base.Rewind = [](void* ctx) { return reinterpret_cast<decltype(this)>(ctx)->Rewind(); };
    }

    size_t NumEstimated() {
        return iter->EstimateNumResults();
    }

    t_docId LastDocId() {
        const std::optional<search::disk::DocumentID> docId = iter->LastDocId();
        return docId.value_or(search::disk::DocumentID{0}).id;
    }

    bool HasNext() {
        return iter->HasNext();
    }

    size_t Len() {
        return iter->EstimateNumResults();
    }

    int Next(RSIndexResult** result) {
        do {
            std::optional<search::disk::Document> doc = iter->Next();
            if (!doc) {
                return INDEXREAD_EOF;
            }

            if ((doc->metadata.fieldMask & fieldMask) == 0) {
                continue;
            }
            *result = base.current;
            (*result)->docId = doc->docId.id;
            (*result)->fieldMask = doc->metadata.fieldMask;
            return INDEXREAD_OK;
        } while (true);
    }

    int SkipTo(t_docId docId, RSIndexResult** result) {
        std::optional<search::disk::Document> doc = iter->SkipTo(search::disk::DocumentID{docId});
        if (!doc) {
            return INDEXREAD_EOF;
        }
        (*result)->docId = doc->docId.id;
        (*result)->fieldMask = doc->metadata.fieldMask;
        return doc->docId.id == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    void Abort() {
        iter->Abort();
        base.isValid = false;
    }

    void Rewind() {
        iter->Rewind();
        base.isValid = true;
    }
};
}