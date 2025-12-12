/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "iterator_util.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/not_iterator.h"

template <bool optimized>
class BM_NotIterator : public benchmark::Fixture {
public:
  QueryIterator *iterator_base;
  QueryIterator *child;
  std::vector<t_docId> childIds;
  std::vector<t_docId> wcIds;
  t_docId maxDocId;
  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    // The ratio between wildcard result set size and child result set size
    double ratio_multiplier = state.range(0);

    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, maxDocId);
    std::uniform_real_distribution<double> prob_dist(0.0, 1.0);

    // For the subset case, first generate the WC IDs
    wcIds.resize(maxDocId);
    for (auto &id : wcIds) {
      id = dist(rng);
    }
    std::sort(wcIds.begin(), wcIds.end());
    wcIds.erase(std::unique(wcIds.begin(), wcIds.end()), wcIds.end());

    double inclusion_probability = 1.0 / ratio_multiplier;
    childIds.clear();

    for (const auto& wcId : wcIds) {
      if (prob_dist(rng) < inclusion_probability) {
        childIds.push_back(wcId);
      }
    }

    QueryIterator *child = (QueryIterator *)new MockIterator(childIds);
    struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout

    if constexpr (optimized) {
      QueryIterator *wcii = (QueryIterator *)new MockIterator(wcIds);
      iterator_base = _New_NotIterator_With_WildCardIterator(child, wcii, maxDocId, 1.0, timeout);
    } else {
      iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, nullptr);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <bool optimized>
bool BM_NotIterator<optimized>::initialized = false;

#define RATIO_SCENARIOS() \
  RangeMultiplier(10)->Range(1, 1000000)

BENCHMARK_TEMPLATE1_DEFINE_F(BM_NotIterator, Read, false)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_NotIterator, SkipTo, false)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_NotIterator, Read_Optimized, true)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_NotIterator, SkipTo_Optimized, true)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read)->RATIO_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo)->RATIO_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, Read_Optimized)->RATIO_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Optimized)->RATIO_SCENARIOS();

BENCHMARK_MAIN();
