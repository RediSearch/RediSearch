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

#include "src/index.h"

template <typename IteratorType>
class BM_WildcardIterator : public benchmark::Fixture {
public:
  t_docId maxDocId;
  size_t numDocs;
  IteratorType *iterator_base;
  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    numDocs = state.range(0);
    maxDocId = numDocs * 2; // Simulate sparse document IDs

    // Initialize iterators based on the test name
    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      iterator_base = IT_V2(NewWildcardIterator_NonOptimized)(maxDocId, numDocs);
    } else {
      iterator_base = NewWildcardIterator_NonOptimized(maxDocId, numDocs);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType>
bool BM_WildcardIterator<IteratorType>::initialized = false;

#define DOCS_SCENARIOS() Arg(10)->Arg(100)->Arg(1000)->Arg(10000)->Arg(100000)->Arg(1000000)

BENCHMARK_TEMPLATE1_DEFINE_F(BM_WildcardIterator, Read, QueryIterator)(benchmark::State &state) {
  IteratorStatus rc;
  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_WildcardIterator, SkipTo, QueryIterator)(benchmark::State &state) {
  t_docId docId = 10;
  IteratorStatus rc;
  for (auto _ : state) {
    rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }
}

BENCHMARK_REGISTER_F(BM_WildcardIterator, Read)->DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_WildcardIterator, SkipTo)->DOCS_SCENARIOS();

BENCHMARK_TEMPLATE1_DEFINE_F(BM_WildcardIterator, Read_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  int rc;
  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE_DEFINE_F(BM_WildcardIterator, SkipTo_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  t_docId docId = 10;
  int rc;
  for (auto _ : state) {
    rc = iterator_base->SkipTo(iterator_base, docId, &hit);
    docId += 10;
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }
}

BENCHMARK_REGISTER_F(BM_WildcardIterator, Read_Old)->DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_WildcardIterator, SkipTo_Old)->DOCS_SCENARIOS();

BENCHMARK_MAIN();
