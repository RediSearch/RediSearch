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
#include "iterator_util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/optional_iterator.h"
#include "src/iterators/idlist_iterator.h"
#include "src/iterators/empty_iterator.h"

template<bool WithChild>
class BM_OptionalIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> childDocIds;
  static bool initialized;
  const t_docId maxDocId = 100000;
  const size_t numDocs = 50000;
  const double weight = 1.0;
  QueryIterator *iterator_base = nullptr;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    iterator_base = createOptionalIterator(state);
  }

  void TearDown(::benchmark::State &state) {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
      iterator_base = nullptr;
    }
    childDocIds.clear();
  }

  // Template to create optional iterator with or without child
  QueryIterator* createOptionalIterator(::benchmark::State &state) {
    QueryIterator *child = nullptr;
    if constexpr (WithChild) {
      // Generate child docIds based on benchmark parameters
      const auto numChildDocs = state.range(0);
      childDocIds.resize(numChildDocs);

      std::mt19937 rng(42);
      std::uniform_int_distribution<t_docId> dist(1, maxDocId);

      for (int i = 0; i < numChildDocs; ++i) {
        childDocIds[i] = dist(rng);
      }
      std::sort(childDocIds.begin(), childDocIds.end());
      childDocIds.erase(std::unique(childDocIds.begin(), childDocIds.end()), childDocIds.end());

      // Create MockIterator with the generated childDocIds
      child = (QueryIterator *)new MockIterator(childDocIds);
    }
    return IT_V2(NewOptionalIterator_NonOptimized)(child, maxDocId, numDocs, weight);
  }
};

template<bool WithChild>
bool BM_OptionalIterator<WithChild>::initialized = false;

// Type aliases for cleaner benchmark definitions
using BM_OptionalIteratorEmpty = BM_OptionalIterator<false>;
using BM_OptionalIteratorWithChild = BM_OptionalIterator<true>;

// Benchmark scenarios: different numbers of child documents
#define CHILD_DOCS_SCENARIOS() RangeMultiplier(2)->Range(2, 1024)->DenseRange(1500, 5000, 500)

BENCHMARK_DEFINE_F(BM_OptionalIteratorEmpty, ReadEmpty)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, ReadWithChild)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorEmpty, SkipToEmpty)(benchmark::State &state) {
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
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToWithChild)(benchmark::State &state) {
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
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToRealHits)(benchmark::State &state) {
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
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToVirtualHits)(benchmark::State &state) {
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
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, MixedOperations)(benchmark::State &state) {
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
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, FullIteration)(benchmark::State &state) {
  for (auto _ : state) {
    // Create a new iterator for each iteration since we need to iterate to completion
    QueryIterator *temp_iterator = createOptionalIterator(state);

    // Iterate through all documents
    while (temp_iterator->Read(temp_iterator) == ITERATOR_OK) {
      benchmark::DoNotOptimize(temp_iterator->current->docId);
    }

    temp_iterator->Free(temp_iterator);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, RewindPerformance)(benchmark::State &state) {
  // Read some documents first
  for (int i = 0; i < 1000 && iterator_base->Read(iterator_base) == ITERATOR_OK; i++) {
    // Just advance the iterator
  }

  for (auto _ : state) {
    iterator_base->Rewind(iterator_base);
    benchmark::DoNotOptimize(iterator_base->lastDocId);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorEmpty, CreateAndDestroy)(benchmark::State &state) {
  for (auto _ : state) {
    // Create a new iterator for each iteration to measure creation/destruction
    QueryIterator *temp_iterator = createOptionalIterator(state);
    benchmark::DoNotOptimize(temp_iterator);
    temp_iterator->Free(temp_iterator);
  }
}

// Register benchmarks with different child document counts
BENCHMARK_REGISTER_F(BM_OptionalIteratorEmpty, ReadEmpty)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, ReadWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorEmpty, SkipToEmpty)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToRealHits)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToVirtualHits)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, MixedOperations)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, FullIteration)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, RewindPerformance)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorEmpty, CreateAndDestroy)->CHILD_DOCS_SCENARIOS();

BENCHMARK_MAIN();
