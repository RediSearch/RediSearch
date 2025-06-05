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
#include <iostream>

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

    auto numDocuments = 100000;
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
    t_docId* ids_array = (t_docId*)rm_malloc(docIds.size() * sizeof(t_docId));
    std::copy(docIds.begin(), docIds.end(), ids_array);

    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      iterator_base = IT_V2(NewIdListIterator)(ids_array, docIds.size(), 1.0);
    } else if constexpr (std::is_same_v<IteratorType, IndexIterator>) {
      iterator_base = NewIdListIterator(ids_array, docIds.size(), 1.0);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType>
bool BM_IdListIterator<IteratorType>::initialized = false;

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, Read, QueryIterator)(benchmark::State &state) {
  for (auto _ : state) {
    auto rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, SkipTo, QueryIterator)(benchmark::State &state) {
  t_docId docId = 100;
  for (auto _ : state) {
    auto rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 100;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 100;
    }
  }
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read);
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo);

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, Read_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    auto rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_IdListIterator, SkipTo_Old, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  t_docId docId = 100;
  for (auto _ : state) {
    auto rc = iterator_base->SkipTo(iterator_base, docId, &hit);
    docId += 100;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 100;
    }
  }
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read_Old);
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo_Old);

BENCHMARK_MAIN();
