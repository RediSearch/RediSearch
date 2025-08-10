#pragma once

#include "index_iterator.h"
#include "rmutil/rm_assert.h"

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
        using AdapterType = IndexIteratorAdapter<UnderlyingIteratorType>;
        base.ctx = this;
        base.isValid = true;
        base.current = NewVirtualResult(0, RS_FIELDMASK_ALL);
        base.type = ID_LIST_ITERATOR;
        base.NumEstimated = [](void* ctx) -> size_t { return reinterpret_cast<AdapterType*>(ctx)->NumEstimated(); };
        base.Free = [](indexIterator* iter) -> void { delete reinterpret_cast<AdapterType*>(iter->ctx); };
        base.LastDocId = [](void* ctx) { return reinterpret_cast<AdapterType*>(ctx)->LastDocId(); };
        base.HasNext = [](void* ctx) -> int { return reinterpret_cast<AdapterType*>(ctx)->HasNext(); };
        base.Len = [](void* ctx) { return reinterpret_cast<AdapterType*>(ctx)->Len(); };
        base.Read = [](void* ctx, RSIndexResult** result) -> int { return reinterpret_cast<AdapterType*>(ctx)->Next(result); };
        base.SkipTo = [](void* ctx, t_docId docId, RSIndexResult** result) -> int { return reinterpret_cast<AdapterType*>(ctx)->SkipTo(docId, result); };
        base.Abort = [](void* ctx) { return reinterpret_cast<AdapterType*>(ctx)->Abort(); };
        base.Rewind = [](void* ctx) { return reinterpret_cast<AdapterType*>(ctx)->Rewind(); };
    }

    size_t NumEstimated() {
        RS_ASSERT(iter);
        return iter->EstimateNumResults();
    }

    t_docId LastDocId() {
        RS_ASSERT(iter);
        const std::optional<search::disk::DocumentID> docId = iter->LastDocId();
        return docId.value_or(search::disk::DocumentID{0}).id;
    }

    bool HasNext() {
        RS_ASSERT(iter);
        return iter->HasNext();
    }

    size_t Len() {
        RS_ASSERT(iter);
        return iter->EstimateNumResults();
    }

    int Next(RSIndexResult** result) {
        RS_ASSERT(iter);
        RS_ASSERT(base.current);
        do {
            auto entry = iter->Next();
            if (!entry) {
                return INDEXREAD_EOF;
            }

            t_fieldMask mask = entry->GetFieldMask();
            if ((mask & fieldMask) == 0) {
                continue;
            }
            *result = base.current;
            (*result)->docId = entry->GetID().id;
            (*result)->fieldMask = mask;
            return INDEXREAD_OK;
        } while (true);
    }

    int SkipTo(t_docId docId, RSIndexResult** result) {
        RS_ASSERT(iter);
        RS_ASSERT(base.current);
        auto entry = iter->SkipTo(search::disk::DocumentID{docId});
        if (!entry) {
            return INDEXREAD_EOF;
        }
        *result = base.current;
        (*result)->docId = entry->GetID().id;
        (*result)->fieldMask = entry->GetFieldMask();
        return (*result)->docId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    void Abort() {
        RS_ASSERT(iter);
        iter->Abort();
        base.isValid = false;
    }

    void Rewind() {
        RS_ASSERT(iter);
        iter->Rewind();
        base.isValid = true;
    }
};
}