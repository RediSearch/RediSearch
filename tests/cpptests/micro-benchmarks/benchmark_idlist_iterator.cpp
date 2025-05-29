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

template <typename IteratorType>
class BM_IdListIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> docIds;
  static bool initialized;
  IteratorType *iterator_base;

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
    docIds.erase(std::unique(docIds.begin(), docIds.end()), docIds.end());

    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      iterator_base = IT_V2(NewIdListIterator)(docIds.data(), docIds.size(), 1.0);
    } else if constexpr (std::is_same_v<IteratorType, IndexIterator>) {
      iterator_base = NewIdListIterator(docIds.data(), docIds.size(), 1.0);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType>
bool BM_IdListIterator<IteratorType>::initialized = false;

#define DOCIDS_SCENARIOS() Arg(10)->Arg(100)->Arg(1000)->Arg(100000)

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, Read, QueryIterator)(benchmark::State &state) {
  for (auto _ : state) {
    auto rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, SkipTo, QueryIterator)(benchmark::State &state) {
  t_docId docId = 10;
  for (auto _ : state) {
    auto rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read)->DOCIDS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo)->DOCIDS_SCENARIOS();

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, Read_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    auto rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, SkipTo_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  t_docId docId = 10;
  for (auto _ : state) {
    auto rc = iterator_base->SkipTo(iterator_base, docId, &hit);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read_Old)->DOCIDS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo_Old)->DOCIDS_SCENARIOS();

