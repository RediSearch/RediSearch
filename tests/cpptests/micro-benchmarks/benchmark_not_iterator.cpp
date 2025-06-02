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

class BM_NotIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> childIds;
  t_docId maxDocId = 2'000'000;
  static bool initialized;

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
  }

  QueryIterator *createChild() {
    return (QueryIterator *)new MockIterator(childIds);
  }
  
  IndexIterator *createChildOld() {
    return (IndexIterator *)new MockOldIterator(childIds);
  }
};
bool BM_NotIterator::initialized = false;

// Translation - exponential range from 2 to 20 (double each time), then 25, 50, 75, and 100.
// This is the number of documents in the child iterator
#define NOT_SCENARIOS() RangeMultiplier(2)->Range(2, 20)->DenseRange(25, 100, 25)

BENCHMARK_DEFINE_F(BM_NotIterator, Read)(benchmark::State &state) {
  QueryIterator *child = createChild();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  QueryIterator *iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, NULL);
  IteratorStatus rc;
  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_NotIterator, SkipTo)(benchmark::State &state) {
  QueryIterator *child = createChild();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  QueryIterator *iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, NULL);
  
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

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo)->NOT_SCENARIOS();

BENCHMARK_DEFINE_F(BM_NotIterator, Read_Old)(benchmark::State &state) {
  IndexIterator *child = createChildOld();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  IndexIterator *iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, NULL);
  
  RSIndexResult *hit;
  int rc;
  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_NotIterator, SkipTo_Old)(benchmark::State &state) {
  IndexIterator *child = createChildOld();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  IndexIterator *iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, NULL);
  
  RSIndexResult *hit;
  t_docId docId = 10;
  int rc;
  for (auto _ : state) {
    rc = iterator_base->SkipTo(iterator_base->ctx, docId, &hit);
    docId += 10;
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base); 
}

BENCHMARK_REGISTER_F(BM_NotIterator, Read_Old)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipTo_Old)->NOT_SCENARIOS();


// Add benchmarks for optimized version (with wildcard iterator)
BENCHMARK_DEFINE_F(BM_NotIterator, ReadOptimized)(benchmark::State &state) {
  QueryIterator *child = createChild();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  
  // Create a mock QueryEvalCtx to enable optimized mode
  IndexSpec *spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
  spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
  spec->rule->index_all = true;
  
  RedisSearchCtx* sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
  sctx->spec = spec;

  DocTable* docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
  docTable->maxDocId = maxDocId;
  docTable->size = maxDocId;

  QueryEvalCtx* qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
  qctx->sctx = sctx;
  qctx->docTable = docTable;

  QueryIterator *iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, qctx);
  IteratorStatus rc;

  for (auto _ : state) {
    rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
  if (spec && spec->rule) rm_free(spec->rule);
  if (spec) rm_free(spec);
  if (sctx) rm_free(sctx);
  if (docTable) rm_free(docTable);
  if (qctx) rm_free(qctx);
}

BENCHMARK_DEFINE_F(BM_NotIterator, SkipToOptimized)(benchmark::State &state) {
  QueryIterator *child = createChild();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  
  // Create a mock QueryEvalCtx to enable optimized mode
  IndexSpec *spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
  spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
  spec->rule->index_all = true;
  
  RedisSearchCtx* sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
  sctx->spec = spec;

  DocTable* docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
  docTable->maxDocId = maxDocId;
  docTable->size = maxDocId;

  QueryEvalCtx* qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
  qctx->sctx = sctx;
  qctx->docTable = docTable;

  QueryIterator *iterator_base = IT_V2(NewNotIterator)(child, maxDocId, 1.0, timeout, qctx);
  IteratorStatus rc;
  t_docId docId = 10;
  for (auto _ : state) {
    rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
  if (spec && spec->rule) rm_free(spec->rule);
  if (spec) rm_free(spec);
  if (sctx) rm_free(sctx);
  if (docTable) rm_free(docTable);
  if (qctx) rm_free(qctx);
}

BENCHMARK_REGISTER_F(BM_NotIterator, ReadOptimized)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipToOptimized)->NOT_SCENARIOS();

BENCHMARK_DEFINE_F(BM_NotIterator, ReadOptimized_Old)(benchmark::State &state) {
  IndexIterator *child = createChildOld();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  
  // Create a mock QueryEvalCtx to enable optimized mode
  IndexSpec *spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
  spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
  spec->rule->index_all = true;
  
  RedisSearchCtx* sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
  sctx->spec = spec;

  DocTable* docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
  docTable->maxDocId = maxDocId;
  docTable->size = maxDocId;

  QueryEvalCtx* qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
  qctx->sctx = sctx;
  qctx->docTable = docTable;
  
  // Pass qctx to enable optimized mode
  IndexIterator *iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, qctx);
  
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
    }
  }

  iterator_base->Free(iterator_base);
  if (spec && spec->rule) rm_free(spec->rule);
  if (spec) rm_free(spec);
  if (sctx) rm_free(sctx);
  if (docTable) rm_free(docTable);
  if (qctx) rm_free(qctx);
}

BENCHMARK_DEFINE_F(BM_NotIterator, SkipToOptimized_Old)(benchmark::State &state) {
  IndexIterator *child = createChildOld();
  struct timespec timeout = {LONG_MAX, 999999999}; // "infinite" timeout
  
  // Create a mock QueryEvalCtx to enable optimized mode
  IndexSpec *spec = (IndexSpec*)rm_calloc(1, sizeof(IndexSpec));
  spec->rule = (SchemaRule*)rm_calloc(1, sizeof(SchemaRule));
  spec->rule->index_all = true;
  
  RedisSearchCtx* sctx = (RedisSearchCtx*)rm_calloc(1, sizeof(RedisSearchCtx));
  sctx->spec = spec;

  DocTable* docTable = (DocTable*)rm_calloc(1, sizeof(DocTable));
  docTable->maxDocId = maxDocId;
  docTable->size = maxDocId;

  QueryEvalCtx* qctx = (QueryEvalCtx*)rm_calloc(1, sizeof(QueryEvalCtx));
  qctx->sctx = sctx;
  qctx->docTable = docTable;
  
  // Pass qctx to enable optimized mode
  IndexIterator *iterator_base = NewNotIterator(child, maxDocId, 1.0, timeout, qctx);
  
  RSIndexResult *hit;
  t_docId docId = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base->ctx, docId, &hit);
    docId += 10;
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base->ctx);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
  if (spec && spec->rule) rm_free(spec->rule);
  if (spec) rm_free(spec);
  if (sctx) rm_free(sctx);
  if (docTable) rm_free(docTable);
  if (qctx) rm_free(qctx);
}

// Register the new benchmark tests
BENCHMARK_REGISTER_F(BM_NotIterator, ReadOptimized_Old)->NOT_SCENARIOS();
BENCHMARK_REGISTER_F(BM_NotIterator, SkipToOptimized_Old)->NOT_SCENARIOS();

BENCHMARK_MAIN();
