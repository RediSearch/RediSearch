/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "src/iterators/iterator_api.h"

#include <stdint.h>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <thread>
#include <optional>

extern "C" {
    IteratorStatus MockIterator_Read(QueryIterator *base);
    IteratorStatus MockIterator_SkipTo(QueryIterator *base, t_docId docId);
    size_t MockIterator_NumEstimated(const QueryIterator *base);
    void MockIterator_Rewind(QueryIterator *base);
    void MockIterator_Free(QueryIterator *base);
    ValidateStatus MockIterator_Revalidate(QueryIterator *base);
}

class MockIterator {
public:
    QueryIterator base;
    std::vector<t_docId> docIds;
    size_t nextIndex;
    IteratorStatus whenDone;
    size_t readCount;
    std::optional<std::chrono::nanoseconds> sleepTime; // Sleep for this duration before returning from Read/SkipTo
    ValidateStatus revalidateResult; // Whether to simulate a change after GC
    size_t validationCount;

private:
    void Init() {
      base.type = MAX_ITERATOR;
      base.atEOF = false;
      base.lastDocId = 0;
      base.current = NewVirtualResult(1, RS_FIELDMASK_ALL);
      base.NumEstimated = MockIterator_NumEstimated;
      base.Free = MockIterator_Free;
      base.Read = MockIterator_Read;
      base.SkipTo = MockIterator_SkipTo;
      base.Rewind = MockIterator_Rewind;
      base.Revalidate = MockIterator_Revalidate;

      std::sort(docIds.begin(), docIds.end());
      auto new_end = std::unique(docIds.begin(), docIds.end());
      docIds.erase(new_end, docIds.end());
    }
public:
    // Public API
    IteratorStatus Read() {
      if (sleepTime.has_value()) {
        std::this_thread::sleep_for(sleepTime.value());
      }
      readCount++;
      if (nextIndex >= docIds.size() || base.atEOF) {
        base.atEOF = true;
        return whenDone;
      }
      base.lastDocId = base.current->docId = docIds[nextIndex++];
      return ITERATOR_OK;
    }

    IteratorStatus SkipTo(t_docId docId) {
      if (sleepTime.has_value()) {
        std::this_thread::sleep_for(sleepTime.value());
      }
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

    size_t NumEstimated() const {
      return docIds.size();
    }

    void Rewind() {
      nextIndex = 0;
      readCount = 0;
      base.lastDocId = base.current->docId = 0;
      base.atEOF = false;
    }

    ValidateStatus Revalidate() {
      validationCount++;

      if (revalidateResult == VALIDATE_MOVED) {
        if (nextIndex < docIds.size()) {
          base.lastDocId = base.current->docId = docIds[nextIndex++]; // Simulate a move by incrementing nextIndex
        } else {
          base.atEOF = true; // If no more documents, set EOF
        }
      }

      return revalidateResult;
    }

    // Methods to configure revalidate behavior for testing
    void SetRevalidateResult(ValidateStatus result) {
      revalidateResult = result;
    }

    size_t GetValidationCount() const {
      return validationCount;
    }

    ~MockIterator() noexcept {
      IndexResult_Free(base.current);
    }

    template<typename... Args>
    MockIterator(Args&&... args)
      : docIds({std::forward<Args>(args)...}), whenDone(ITERATOR_EOF), nextIndex(0), readCount(0), sleepTime(std::nullopt), revalidateResult(VALIDATE_OK), validationCount(0) {
      Init();
    }

    template<typename... Args>
    MockIterator(std::chrono::nanoseconds sleep, Args&&... args)
      : docIds({std::forward<Args>(args)...}), whenDone(ITERATOR_EOF), nextIndex(0), readCount(0), sleepTime(sleep), revalidateResult(VALIDATE_OK), validationCount(0) {
      Init();
    }

    template<typename... Args>
    MockIterator(IteratorStatus st, Args&&... ids_args)
      : docIds({std::forward<Args>(ids_args)...}), whenDone(st), nextIndex(0), readCount(0), sleepTime(std::nullopt), revalidateResult(VALIDATE_OK), validationCount(0) {
      Init();
    }

    template<typename... Args>
    MockIterator(IteratorStatus st, std::chrono::nanoseconds sleep, Args&&... ids_args)
      : docIds({std::forward<Args>(ids_args)...}), whenDone(st), nextIndex(0), readCount(0), sleepTime(sleep), revalidateResult(VALIDATE_OK), validationCount(0) {
      Init();
    }
};
