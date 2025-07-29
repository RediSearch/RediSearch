#include <rocksdb/db.h>
#include "disk/inverted_index/inverted_index_api.h"
#include "disk/inverted_index/inverted_index.h"
#include "disk/index_iterator_adapter.h"


#ifdef __cplusplus
extern "C" {
#endif

bool DiskDatabase_IndexDocument(DiskIndex *handle, const char *term,
                                t_docId docId, t_fieldMask fieldMask) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);

    std::stringstream prefix;
    prefix << index->Name() << "_" << term << "_";
    search::disk::Document doc;
    doc.docId.id = docId;
    doc.metadata.fieldMask = fieldMask;

    return search::disk::SingleDocument::Write(index->GetInvertedIndex(), prefix.str(), doc);
}

DiskIterator *DiskDatabase_NewInvertedIndexIterator(DiskIndex* handle, const char* term) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    search::disk::InvertedIndexIterator* iter = index->GetInvertedIndex().template CreateIterator<search::disk::InvertedIndexIterator>(index->Name(), term);
    return reinterpret_cast<DiskIterator*>(iter);
}

bool InvertedIndexIterator_Next(DiskIterator *iter, RSIndexResult* result) {
    if (iter == nullptr || result == nullptr) {
        return false;
    }
    search::disk::InvertedIndexIterator* invertedIter = reinterpret_cast<search::disk::InvertedIndexIterator*>(iter);
    std::optional<search::disk::Document> doc = invertedIter->Next();
    if (!doc) {
        return false;
    }
    result->docId = doc->docId.id;
    result->fieldMask = doc->metadata.fieldMask;
    return true;
}

bool InvertedIndexIterator_SkipTo(DiskIterator *iter, t_docId docId, RSIndexResult* result) {
    if (iter == nullptr || result == nullptr) {
        return false;
    }
    search::disk::InvertedIndexIterator* invertedIter = reinterpret_cast<search::disk::InvertedIndexIterator*>(iter);
    std::optional<search::disk::Document> doc = invertedIter->SkipTo(search::disk::DocumentID{docId});
    if (!doc) {
        return false;
    }
    result->docId = doc->docId.id;
    result->fieldMask = doc->metadata.fieldMask;
    return true;
}

void InvertedIndexIterator_Rewind(DiskIterator *iter) {
    if (iter != nullptr) {
        reinterpret_cast<search::disk::InvertedIndexIterator*>(iter)->Rewind();
    }
}

void InvertedIndexIterator_Abort(DiskIterator *iter) {
    if (iter != nullptr) {
        reinterpret_cast<search::disk::InvertedIndexIterator*>(iter)->Abort();
    }
}

size_t InvertedIndexIterator_NumEstimated(DiskIterator *iter) {
    if (iter == nullptr) {
        return 0;
    }

    return reinterpret_cast<search::disk::InvertedIndexIterator*>(iter)->EstimateNumResults();
}

size_t InvertedIndexIterator_Len(DiskIterator *iter) {
    return UINT32_MAX;
}

t_docId InvertedIndexIterator_LastDocId(DiskIterator *iter) {
    if (iter == nullptr) {
        return 0;
    }
    const std::optional<search::disk::DocumentID> docId = reinterpret_cast<search::disk::InvertedIndexIterator*>(iter)->LastDocId();
    if (!docId) {
        return 0;
    }
    return docId->id;
}

int InvertedIndexIterator_HasNext(DiskIterator *iter) {
    if (iter == nullptr) {
        return 0;
    }

    return reinterpret_cast<search::disk::InvertedIndexIterator*>(iter)->HasNext();
}

void InvertedIndexIterator_Free(DiskIterator *iter) {
    if (iter != nullptr) {
        delete reinterpret_cast<search::disk::InvertedIndexIterator*>(iter);
    }
}

IndexIterator *NewDiskInvertedIndexIterator(DiskIndex *handle, const char *term, t_fieldMask fieldMask) {
    if (handle == nullptr || term == nullptr) {
        return nullptr;
    }

    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    std::unique_ptr<search::disk::InvertedIndexIterator> iter{index->GetInvertedIndex().template CreateIterator<search::disk::InvertedIndexIterator>(index->Name(), term)};
    if (!iter) {
        return nullptr;
    }

    auto adapter = new search::disk::IndexIteratorAdapter<search::disk::InvertedIndexIterator>(std::move(iter), fieldMask);
    return &adapter->base;
}

#ifdef __cplusplus
}
#endif
