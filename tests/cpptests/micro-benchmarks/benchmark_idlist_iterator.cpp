/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define MICRO_BENCHMARKS

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/idlist_iterator.h"

#include "src/index.h"

class BM_IdListIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> docIds;
  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    auto numDocuments = state.range(0);
    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, 2'000'000);

    docIds.resize(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      for (auto &id : docIds) {
        id = dist(rng);
      }
    }
    std::sort(docIds.begin(), docIds.end());
  }
};
bool BM_IdListIterator::initialized = false;

// Translation - exponential range from 2 to 20 (double each time), then 25, 50, 75, and 100.
// This is the number of docIds to iterate on in each scenario
#define DOCIDS_SCENARIOS() RangeMultiplier(2)->Range(2, 20)->DenseRange(25, 100, 25)

BENCHMARK_DEFINE_F(BM_IdListIterator, Read)(benchmark::State &state) {
  QueryIterator *iterator_base = IT_V2(NewIdListIterator)(docIds.data(), docIds.size(), 1.0);
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_IdListIterator, SkipTo)(benchmark::State &state) {
  QueryIterator *iterator_base = IT_V2(NewIdListIterator)(docIds.data(), docIds.size(), 1.0);

  t_offset step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read)->DOCIDS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo)->DOCIDS_SCENARIOS();

BENCHMARK_DEFINE_F(BM_IdListIterator, Read_Old)(benchmark::State &state) {
  IndexIterator *iterator_base = NewIdListIterator(docIds.data(), docIds.size(), 1.0);
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_IdListIterator, SkipTo_Old)(benchmark::State &state) {
  IndexIterator *iterator_base = NewIdListIterator(docIds.data(), docIds.size(), 1.0);
  RSIndexResult *hit = iterator_base->current;
  hit->docId = 0; // Ensure initial docId is set to 0

  t_offset step = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base, hit->docId + step, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      // Don't rely on the old iterator's Rewind to reset hit->docId
      hit = iterator_base->current;
      hit->docId = 0;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read_Old)->DOCIDS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo_Old)->DOCIDS_SCENARIOS();

BENCHMARK_MAIN();
