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
  t_docId maxDocId = 2'000'000;
  static bool initialized;
  QueryEvalCtx* qctx = NULL;
  IndexSpec *spec = NULL;
  DocTable* docTable = NULL;
  RedisSearchCtx* sctx = NULL;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    auto numDocuments = state.range(0);
    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, maxDocId);

    childIds.resize(numDocuments);
    for (auto &id : childIds) {
      id = dist(rng);
    }
    std::sort(childIds.begin(), childIds.end());
    auto new_end = std::unique(childIds.begin(), childIds.end());
    childIds.erase(new_end, childIds.end());
    IteratorType *child = createChild();
    struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout

    // Create QueryEvalCtx if optimized mode is enabled
    if constexpr (optimized) {
      // Create a mock QueryEvalCtx to enable optimized mode
      spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
      spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
      spec->rule->index_all = true;

      sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
      sctx->spec = spec;

      docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
      docTable->maxDocId = maxDocId;
      docTable->size = maxDocId;

      qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
      qctx->sctx = sctx;
      qctx->docTable = docTable;
    }

    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, qctx);
    } else {
      iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, qctx);
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

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
    //Delete if pointers are not NULL
    if (spec && spec->rule) rm_free(spec->rule);
    if (spec) rm_free(spec);
    if (sctx) rm_free(sctx);
    if (docTable) rm_free(docTable);
    if (qctx) rm_free(qctx);
  }
};

template <typename IteratorType, bool optimized>
bool BM_NotIterator<IteratorType, optimized>::initialized = false;

#define NOT_SCENARIOS() Arg(1000) -> Arg(10000) -> Arg(100000) -> Arg(1000000)

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
      hit->docId = 0;
    }
  }
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read_Old_Optimized)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Old_Optimized)->NOT_SCENARIOS();

BENCHMARK_MAIN();
