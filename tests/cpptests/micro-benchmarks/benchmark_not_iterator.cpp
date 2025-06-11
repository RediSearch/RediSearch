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

#include "deprecated_iterator_util.h"
#include "src/index.h"

template <typename IteratorType, bool optimized>
class BM_NotIterator : public benchmark::Fixture {
public:
  IteratorType *iterator_base;
  IteratorType *child;
  std::vector<t_docId> childIds;
  std::vector<t_docId> wcIds;
  t_docId maxDocId;
  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    auto numChildDocuments = state.range(0);
    maxDocId = state.range(1);

    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, maxDocId);

    childIds.resize(numChildDocuments);
    for (auto &id : childIds) {
      id = dist(rng);
    }
    std::sort(childIds.begin(), childIds.end());
    auto new_end = std::unique(childIds.begin(), childIds.end());
    childIds.erase(new_end, childIds.end());
    IteratorType *child = createChild();
    struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout

    if constexpr (optimized) {
      wcIds.resize(maxDocId);
      for (auto &id : wcIds) {
        id = dist(rng);
      }
      std::sort(wcIds.begin(), wcIds.end());
      auto new_end = std::unique(wcIds.begin(), wcIds.end());
      wcIds.erase(new_end, wcIds.end());
      IteratorType *wcii = createWCII();
      struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
      if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
        iterator_base = IT_V2(_New_NotIterator_With_WildCardIterator)(child, wcii, maxDocId, 1.0, timeout);
      } else {
        iterator_base = _New_NotIterator_With_WildCardIterator(child, wcii, maxDocId, 1.0, timeout);
      }
    } else {
      if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
        iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, nullptr);
      } else {
        iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, nullptr);
      }
    }
  }

  IteratorType *createChild() {
    // Create a mock child iterator, depending on the iteratortype constexpr
    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      return (QueryIterator *)new MockIterator(childIds);
    } else {
      return (IndexIterator *)new MockOldIterator(childIds);
    }
  }

  IteratorType *createWCII() {
    // Create a mock child iterator, depending on the iteratortype constexpr
    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      return (QueryIterator *)new MockIterator(wcIds);
    } else {
      return (IndexIterator *)new MockOldIterator(wcIds);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType, bool optimized>
bool BM_NotIterator<IteratorType, optimized>::initialized = false;

#define NOT_SCENARIOS() \
  Args({1'000, 100'000}) -> \
  Args({100'000, 1'000}) -> \
  Args({10'000, 500'000}) -> \
  Args({500'000, 10'000}) -> \
  Args({100'000, 1'000'000}) -> \
  Args({1'000'000, 100'000}) -> \
  Args({1'000'000, 2'000'000}) -> \
  Args({2'000'000, 1'000'000}) -> \
  Args({10'000'000, 50'000'000}) -> \
  Args({50'000'000, 10'000'000})

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, Read, QueryIterator, false)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, SkipTo, QueryIterator, false)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo)->NOT_SCENARIOS();

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, Read_Old, IndexIterator, false)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, SkipTo_Old, IndexIterator, false)(benchmark::State &state) {
  t_docId step = 10;
  RSIndexResult *hit = iterator_base->current;
  hit->docId = 0; // Ensure initial docId is set to 0
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base->ctx, hit->docId + step, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
      hit = iterator_base->current;
      hit->docId = 0;
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read_Old)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Old)->NOT_SCENARIOS();

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, Read_Optimized, QueryIterator, true)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, SkipTo_Optimized, QueryIterator, true)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read_Optimized)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Optimized)->NOT_SCENARIOS();

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, Read_Old_Optimized, IndexIterator, true)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_NotIterator, SkipTo_Old_Optimized, IndexIterator, true)(benchmark::State &state) {
  t_docId step = 10;
  RSIndexResult *hit = iterator_base->current;
  hit->docId = 0; // Ensure initial docId is set to 0
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base->ctx, hit->docId + step, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
      hit = iterator_base->current;
      hit->docId = 0;
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read_Old_Optimized)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Old_Optimized)->NOT_SCENARIOS();

BENCHMARK_MAIN();
