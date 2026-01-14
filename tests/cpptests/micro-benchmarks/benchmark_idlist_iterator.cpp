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
#include "iterators_rs.h"

class BM_IdListIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> docIds;
  static bool initialized;
  QueryIterator *iterator_base;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    auto numDocuments = 1000000;
    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, 2'000'000);

    docIds.resize(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      docIds[i] = dist(rng);
    }

    std::sort(docIds.begin(), docIds.end());
    docIds.erase(std::unique(docIds.begin(), docIds.end()), docIds.end());

    t_docId* ids_array = (t_docId*)rm_malloc(docIds.size() * sizeof(t_docId));
    std::copy(docIds.begin(), docIds.end(), ids_array);

    iterator_base = NewSortedIdListIterator(ids_array, docIds.size(), 1.0);
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};
bool BM_IdListIterator::initialized = false;

BENCHMARK_DEFINE_F(BM_IdListIterator, Read)(benchmark::State &state) {
  for (auto _ : state) {
    auto rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_DEFINE_F(BM_IdListIterator, SkipTo)(benchmark::State &state) {
  t_offset step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_IdListIterator, Read);
BENCHMARK_REGISTER_F(BM_IdListIterator, SkipTo);

BENCHMARK_MAIN();
