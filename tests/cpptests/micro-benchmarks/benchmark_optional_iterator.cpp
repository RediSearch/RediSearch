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
#include "index_utils.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/optional_iterator.h"

class BM_OptionalIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> childDocIds;
  static bool initialized;
  const t_docId maxDocId = 1'000'000;
  const double weight = 1.0;
  QueryIterator *iterator_base = nullptr;
  MockQueryEvalCtx *mockCtx = nullptr;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    // Generate child docIds based on benchmark parameters
    const double ChildDocsRatio = state.range(0) / 100.0;
    const bool Optimized = state.range(1);

    std::mt19937 rng(42);
    std::uniform_real_distribution<> dist(0.0, 1.0);

    childDocIds.clear();
    std::vector<t_docId> allDocIds;
    allDocIds.reserve(maxDocId);
    for (t_docId i = 1; i <= maxDocId; ++i) {
      allDocIds.push_back(i);
      if (dist(rng) < ChildDocsRatio) {
        childDocIds.push_back(i);
      }
    }

    if (Optimized) {
      mockCtx = new MockQueryEvalCtx(allDocIds); // Constructor for `index_all` tests
    } else {
      mockCtx = new MockQueryEvalCtx(maxDocId);
    }

    iterator_base = createOptionalIterator();
  }

  void TearDown(::benchmark::State &state) {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
      iterator_base = nullptr;
    }
    if (mockCtx) {
      delete mockCtx;
      mockCtx = nullptr;
    }
    childDocIds.clear();
  }

  QueryIterator* createOptionalIterator() {
    QueryIterator *child = (QueryIterator *)new MockIterator(childDocIds);
    return NewOptionalIterator(child, &mockCtx->qctx, weight);
  }
};

bool BM_OptionalIterator::initialized = false;



// Benchmark scenarios: different ratio of child documents
#define CHILD_DOCS_SCENARIOS() \
  ArgNames({"ChildDocsRatio", "Optimized"})-> \
  ArgsProduct({::benchmark::CreateDenseRange(0, 90, 10), {false, true}})

// Benchmark functions (assuming iterator always has child)
BENCHMARK_DEFINE_F(BM_OptionalIterator, Read)(benchmark::State &state) {
  QueryIterator *iterator = iterator_base;

  for (auto _ : state) {
    IteratorStatus rc = iterator->Read(iterator);
    if (rc == ITERATOR_EOF) {
      iterator->Rewind(iterator);
    }
    benchmark::DoNotOptimize(iterator->current);
  }
}


BENCHMARK_DEFINE_F(BM_OptionalIterator, SkipTo)(benchmark::State &state) {
  QueryIterator *iterator = iterator_base;
  t_offset step = 10;

  for (auto _ : state) {
    IteratorStatus rc = iterator->SkipTo(iterator, iterator->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator->Rewind(iterator);
    }
    benchmark::DoNotOptimize(rc);
  }
}

// Register optional iterator benchmarks
BENCHMARK_REGISTER_F(BM_OptionalIterator, Read)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIterator, SkipTo)->CHILD_DOCS_SCENARIOS();

BENCHMARK_MAIN();
