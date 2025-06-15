/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "src/index_iterator.h"

#include <stdint.h>
#include <vector>
#include <stdexcept>
#include <algorithm>

extern "C" {
    int MockOldIterator_Read(IndexIterator *base, RSIndexResult **hit);
    int MockOldIterator_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit);
    size_t MockOldIterator_NumEstimated(IndexIterator *base);
    void MockOldIterator_Rewind(IndexIterator *base);
    void MockOldIterator_Free(IndexIterator *base);
}

class MockOldIterator {
public:
    IndexIterator base;
    std::vector<t_docId> docIds;
    size_t nextIndex;
    t_docId lastId;
    int whenDone;
    size_t readCount;
private:

    static void setBase(IndexIterator *base) {
        base->type = READ_ITERATOR;
        base->isValid = true;
        base->LastDocId = 0;
        base->current = NewVirtualResult(1, RS_FIELDMASK_ALL);
        base->NumEstimated = MockOldIterator_NumEstimated;
        base->Free = MockOldIterator_Free;
        base->Read = MockOldIterator_Read;
        base->SkipTo = MockOldIterator_SkipTo;
        base->Rewind = MockOldIterator_Rewind;
    }
public:
    // Public API
    int Read(RSIndexResult **hit) {
        readCount++;
        if (nextIndex >= docIds.size() || !base.isValid) {
            base.isValid = false;
            return whenDone;
        }
        lastId = base.current->docId = docIds[nextIndex++];
        *hit = base.current;
        return INDEXREAD_OK;
    }
    int SkipTo(t_docId docId, RSIndexResult **hit) {
        readCount++;
        if (!base.isValid) {
            return whenDone;
        }
        while (nextIndex < docIds.size() && docIds[nextIndex] < docId) {
            nextIndex++;
        }
        readCount--; // Decrement the read count before calling Read
        auto status = Read(hit);
        if (status == INDEXREAD_OK && base.current->docId != docId) {
            return INDEXREAD_NOTFOUND;
        }
        return status;
    }
    size_t NumEstimated() const {
        return docIds.size();
    }
    void Rewind() {
        nextIndex = 0;
        readCount = 0;
        lastId = 0;
        base.current->docId = 0;
        base.isValid = true;
    }

    ~MockOldIterator() noexcept {
        IndexResult_Free(base.current);
    }

    // Constructor
    template<typename... Args>
    MockOldIterator(Args&&... args)
        : MockOldIterator(INDEXREAD_EOF, std::forward<Args>(args)...) {}

    template<typename... Args>
    MockOldIterator(int st, Args&&... args)
        : whenDone(st), nextIndex(0), lastId(0) {
        setBase(&base);
        docIds = {args...};
        std::sort(docIds.begin(), docIds.end());
        auto new_end = std::unique(docIds.begin(), docIds.end());
        docIds.erase(new_end, docIds.end());
    }
};

int MockOldIterator_Read(IndexIterator *base, RSIndexResult **hit) {
    return reinterpret_cast<MockOldIterator *>(base)->Read(hit);
}
int MockOldIterator_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
    return reinterpret_cast<MockOldIterator *>(base)->SkipTo(docId, hit);
}
size_t MockOldIterator_NumEstimated(IndexIterator *base) {
    return reinterpret_cast<MockOldIterator *>(base)->NumEstimated();
}
void MockOldIterator_Rewind(IndexIterator *base) {
    reinterpret_cast<MockOldIterator *>(base)->Rewind();
}
void MockOldIterator_Free(IndexIterator *base) {
    delete reinterpret_cast<MockOldIterator *>(base);
}
