#pragma once

#include "iterators/iterator_api.h"
#include "rmutil/rm_assert.h"
#include "disk/document_id.h"

namespace search::disk {

// adapts the disk inverted index iterator into the index iterator API
template <typename UnderlyingIteratorType>
struct QueryIteratorAdapter {
    QueryIterator base;
    t_fieldMask fieldMask;
    std::unique_ptr<UnderlyingIteratorType> iter;

    QueryIteratorAdapter(std::unique_ptr<UnderlyingIteratorType> iter, t_fieldMask fieldMask, double weight)
        : fieldMask(fieldMask)
        , iter(std::move(iter))
    {
        using AdapterType = QueryIteratorAdapter<UnderlyingIteratorType>;
        base.current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
        base.atEOF = false;
        base.type = ID_LIST_ITERATOR;
        base.lastDocId = 0;
        base.NumEstimated = [](QueryIterator* ctx) -> size_t { return reinterpret_cast<AdapterType*>(ctx)->NumEstimated(); };
        base.Free = [](QueryIterator* ctx) -> void { return reinterpret_cast<AdapterType*>(ctx)->Free(); };
        base.Rewind = [](QueryIterator* ctx) { return reinterpret_cast<AdapterType*>(ctx)->Rewind(); };
        base.Read = [](QueryIterator* ctx) -> IteratorStatus { return reinterpret_cast<AdapterType*>(ctx)->Read(); };
        base.SkipTo = [](QueryIterator* ctx, t_docId docId) -> IteratorStatus { return reinterpret_cast<AdapterType*>(ctx)->SkipTo(docId); };
        base.Revalidate = [](QueryIterator* ctx) -> ValidateStatus { return reinterpret_cast<AdapterType*>(ctx)->Revalidate(); };
    }

    size_t NumEstimated() {
        RS_ASSERT(iter);
        return iter->EstimateNumResults();
    }

    IteratorStatus Read() {
        RS_ASSERT(iter);
        RS_ASSERT(base.current);
        do {
            auto entry = iter->Next();
            if (!entry) {
                base.atEOF = true;
                return ITERATOR_EOF;
            }

            t_fieldMask mask = entry->GetFieldMask();
            if ((mask & fieldMask) == 0) {
                continue;
            }
            base.lastDocId = entry->GetID().id;
            base.current->docId = entry->GetID().id;
            base.current->fieldMask = mask;
            return ITERATOR_OK;
        } while (true);
    }

    IteratorStatus SkipTo(t_docId docId) {
        RS_ASSERT(iter);
        RS_ASSERT(base.current);
        auto entry = iter->SkipTo(search::disk::DocumentID{docId});
        do {
          if (!entry) {
              base.atEOF = true;
              return ITERATOR_EOF;
          }
          t_fieldMask mask = entry->GetFieldMask();
          if ((mask & fieldMask) == 0) {
            entry = iter->Next();
            continue;
          }
          base.lastDocId = entry->GetID().id;
          base.current->docId = entry->GetID().id;
          base.current->fieldMask = entry->GetFieldMask();
          return base.current->docId == docId ? ITERATOR_OK : ITERATOR_NOTFOUND;
        } while (true);
    }

    void Rewind() {
        RS_ASSERT(iter);
        iter->Rewind();
        base.lastDocId = 0;
        base.atEOF = false;
    }

    ValidateStatus Revalidate() {
        //TODO: Complete this Revalidate work
        RS_ASSERT(iter);
        return VALIDATE_OK;
    }

    void Free() {
        delete this;
    }
};
}
