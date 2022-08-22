#pragma once

#include "query_optimizer.h"
#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"
#include "util/heap.h"
#include "util/timeout.h"

// This enum should match the VecSearchMode enum in VecSim

typedef struct {
  IndexIterator base;
  t_docId lastDocId;
  size_t numIterations;
  TimeoutCtx timeoutCtx;           // Timeout parameters

  size_t offset;

  IndexIterator *childIter;
  t_docId childLastId;
  IndexIterator *numericIter;
  t_docId numericLastId;

  QOptimizer optim;

  heap_t *heap;
  RSIndexResult *pooledResult;
} OptimizerIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewOptimizerIterator(QOptimizer *q_opt, IndexIterator *root, IndexIterator *numeric);

#ifdef __cplusplus
}
#endif
