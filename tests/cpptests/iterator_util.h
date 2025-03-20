/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "src/iterators/iterator_api.h"

#include <stdint.h>
#include <vector>
#include <algorithm>
#include <stdexcept>

extern "C" {
    IteratorStatus MockIterator_Read(QueryIterator *base);
    IteratorStatus MockIterator_SkipTo(QueryIterator *base, t_docId docId);
    size_t MockIterator_NumEstimated(QueryIterator *base);
    void MockIterator_Rewind(QueryIterator *base);
    void MockIterator_Free(QueryIterator *base);
}

class MockIterator {
public:
    QueryIterator base;
    std::vector<t_docId> docIds;
    size_t nextIndex;
    IteratorStatus whenDone;
    size_t readCount;
private:

    static void setBase(QueryIterator *base) {
        base->type = READ_ITERATOR;
        base->atEOF = false;
        base->lastDocId = 0;
        base->current = NewVirtualResult(1, RS_FIELDMASK_ALL);
        base->NumEstimated = MockIterator_NumEstimated;
        base->Free = MockIterator_Free;
        base->Read = MockIterator_Read;
        base->SkipTo = MockIterator_SkipTo;
        base->Rewind = MockIterator_Rewind;
    }
public:
    // Public API
    IteratorStatus Read() {
        readCount++;
        if (nextIndex >= docIds.size() || base.atEOF) {
            base.atEOF = true;
            return whenDone;
        }
        base.lastDocId = base.current->docId = docIds[nextIndex++];
        return ITERATOR_OK;
    }
    IteratorStatus SkipTo(t_docId docId) {
        readCount++;
        // Guarantee check
        if (base.lastDocId >= docId) {
            throw std::invalid_argument("SkipTo: requested to skip backwards");
        }
        if (base.atEOF) {
            return whenDone;
        }
        while (nextIndex < docIds.size() && docIds[nextIndex] < docId) {
            nextIndex++;
        }
        readCount--; // Decrement the read count before calling Read
        auto status = Read();
        if (status == ITERATOR_OK && base.lastDocId != docId) {
            return ITERATOR_NOTFOUND;
        }
        return status;
    }
    size_t NumEstimated() {
        return docIds.size();
    }
    void Rewind() {
        nextIndex = 0;
        readCount = 0;
        base.lastDocId = base.current->docId = 0;
        base.atEOF = false;
    }

    ~MockIterator() noexcept {
        IndexResult_Free(base.current);
    }

    // Constructor
    template<typename... Args>
    MockIterator(Args&&... args)
        : MockIterator(ITERATOR_EOF, std::forward<Args>(args)...) {}

    template<typename... Args>
    MockIterator(IteratorStatus st, Args&&... ids_args)
        : docIds({ids_args...}), whenDone(st), nextIndex(0), readCount(0) {
        setBase(&base);
        std::sort(docIds.begin(), docIds.end());
        std::unique(docIds.begin(), docIds.end());
    }
};

IteratorStatus MockIterator_Read(QueryIterator *base) {
    return reinterpret_cast<MockIterator *>(base)->Read();
}
IteratorStatus MockIterator_SkipTo(QueryIterator *base, t_docId docId) {
    return reinterpret_cast<MockIterator *>(base)->SkipTo(docId);
}
size_t MockIterator_NumEstimated(QueryIterator *base) {
    return reinterpret_cast<MockIterator *>(base)->NumEstimated();
}
void MockIterator_Rewind(QueryIterator *base) {
    reinterpret_cast<MockIterator *>(base)->Rewind();
}
void MockIterator_Free(QueryIterator *base) {
    delete reinterpret_cast<MockIterator *>(base);
}
