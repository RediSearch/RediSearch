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
#include "src/iterators/wildcard_iterator.h"

class BM_WildcardIterator : public benchmark::Fixture {
public:
  t_docId maxDocId;
  size_t numDocs;
  QueryIterator *iterator_base;
  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    numDocs = state.range(0);
    maxDocId = numDocs * 2; // Simulate sparse document IDs

    // Initialize iterators based on the test name
    iterator_base = NewWildcardIterator_NonOptimized(maxDocId, numDocs, 1.0);
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

bool BM_WildcardIterator::initialized = false;

#define DOCS_SCENARIOS() RangeMultiplier(10)->Range(10, 1'000'000)

BENCHMARK_DEFINE_F(BM_WildcardIterator, Read)(benchmark::State &state) {
  IteratorStatus rc;
  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_DEFINE_F(BM_WildcardIterator, SkipTo)(benchmark::State &state) {
  t_offset step = 10;
  IteratorStatus rc;
  for (auto _ : state) {
    rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_WildcardIterator, Read)->DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_WildcardIterator, SkipTo)->DOCS_SCENARIOS();

BENCHMARK_MAIN();
