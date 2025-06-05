/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/optional_iterator.h"
#include "src/iterators/idlist_iterator.h"
#include "src/iterators/empty_iterator.h"

class BM_OptionalIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> childDocIds;
  static bool initialized;
  const t_docId maxDocId = 100000;
  const size_t numDocs = 50000;
  const double weight = 1.0;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    // Generate child docIds based on benchmark parameters
    auto numChildDocs = state.range(0);
    std::mt19937 rng(42);
    std::uniform_int_distribution<t_docId> dist(1, maxDocId);

    childDocIds.resize(numChildDocs);
    for (int i = 0; i < numChildDocs; ++i) {
      childDocIds[i] = dist(rng);
    }
    std::sort(childDocIds.begin(), childDocIds.end());
    childDocIds.erase(std::unique(childDocIds.begin(), childDocIds.end()), childDocIds.end());
  }

  void TearDown(::benchmark::State &state) {
    // Nothing to clean up
  }

  QueryIterator* createOptionalWithChild() {
    QueryIterator *child = IT_V2(NewIdListIterator)(childDocIds.data(), childDocIds.size(), 1.0);
    return IT_V2(NewOptionalIterator_NonOptimized)(child, maxDocId, numDocs, weight);
  }

  QueryIterator* createOptionalEmpty() {
    return IT_V2(NewOptionalIterator_NonOptimized)(NULL, maxDocId, numDocs, weight);
  }
};
bool BM_OptionalIterator::initialized = false;

// Benchmark scenarios: different numbers of child documents
#define CHILD_DOCS_SCENARIOS() RangeMultiplier(2)->Range(2, 1024)->DenseRange(1500, 5000, 500)

BENCHMARK_DEFINE_F(BM_OptionalIterator, ReadEmpty)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalEmpty();

  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, ReadWithChild)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, SkipToEmpty)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalEmpty();

  t_docId docId = 1;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 100;
    if (rc == ITERATOR_EOF || docId > maxDocId) {
      iterator_base->Rewind(iterator_base);
      docId = 1;
    }
    benchmark::DoNotOptimize(rc);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, SkipToWithChild)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  t_docId docId = 1;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 100;
    if (rc == ITERATOR_EOF || docId > maxDocId) {
      iterator_base->Rewind(iterator_base);
      docId = 1;
    }
    benchmark::DoNotOptimize(rc);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, SkipToRealHits)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  size_t childIndex = 0;
  for (auto _ : state) {
    if (childIndex >= childDocIds.size()) {
      iterator_base->Rewind(iterator_base);
      childIndex = 0;
    }

    t_docId target = childDocIds[childIndex++];
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, target);
    benchmark::DoNotOptimize(rc);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, SkipToVirtualHits)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  // Generate docIds that are NOT in childDocIds (virtual hits)
  std::vector<t_docId> virtualDocIds;
  for (t_docId i = 1; i <= maxDocId; i += 137) { // Use prime step to avoid patterns
    if (std::find(childDocIds.begin(), childDocIds.end(), i) == childDocIds.end()) {
      virtualDocIds.push_back(i);
    }
  }

  size_t virtualIndex = 0;
  for (auto _ : state) {
    if (virtualIndex >= virtualDocIds.size()) {
      iterator_base->Rewind(iterator_base);
      virtualIndex = 0;
    }

    t_docId target = virtualDocIds[virtualIndex++];
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, target);
    benchmark::DoNotOptimize(rc);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, MixedOperations)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  std::mt19937 rng(123);
  std::uniform_int_distribution<int> opDist(0, 1); // 0 = Read, 1 = SkipTo
  std::uniform_int_distribution<t_docId> docDist(1, maxDocId);

  for (auto _ : state) {
    int operation = opDist(rng);
    IteratorStatus rc;

    if (operation == 0) {
      rc = iterator_base->Read(iterator_base);
    } else {
      t_docId target = docDist(rng);
      rc = iterator_base->SkipTo(iterator_base, target);
    }

    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }

    benchmark::DoNotOptimize(rc);
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, FullIteration)(benchmark::State &state) {
  for (auto _ : state) {
    QueryIterator *iterator_base = createOptionalWithChild();

    // Iterate through all documents
    while (iterator_base->Read(iterator_base) == ITERATOR_OK) {
      benchmark::DoNotOptimize(iterator_base->current->docId);
    }

    iterator_base->Free(iterator_base);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, RewindPerformance)(benchmark::State &state) {
  QueryIterator *iterator_base = createOptionalWithChild();

  // Read some documents first
  for (int i = 0; i < 1000 && iterator_base->Read(iterator_base) == ITERATOR_OK; i++) {
    // Just advance the iterator
  }

  for (auto _ : state) {
    iterator_base->Rewind(iterator_base);
    benchmark::DoNotOptimize(iterator_base->lastDocId);
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_OptionalIterator, CreateAndDestroy)(benchmark::State &state) {
  for (auto _ : state) {
    QueryIterator *iterator_base = createOptionalEmpty();
    benchmark::DoNotOptimize(iterator_base);
    iterator_base->Free(iterator_base);
  }
}

// Register benchmarks with different child document counts
BENCHMARK_REGISTER_F(BM_OptionalIterator, ReadEmpty)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, ReadWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, SkipToEmpty)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, SkipToWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, SkipToRealHits)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, SkipToVirtualHits)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, MixedOperations)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, FullIteration)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, RewindPerformance)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, CreateAndDestroy)->CHILD_DOCS_SCENARIOS();
