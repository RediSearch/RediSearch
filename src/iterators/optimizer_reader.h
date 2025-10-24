/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "query_optimizer.h"
#include "iterators/iterator_api.h"
#include "redisearch.h"
#include "util/heap.h"
#include "util/timeout.h"

// This enum should match the VecSearchMode enum in VecSim
typedef int (*OptimizerCompareFunc)(const void *e1, const void *e2, const void *udata);

#define OPTIM_OWN_NF 0x01

typedef struct {
  QueryIterator base;
  QOptimizer *optim;
  int flags;

  size_t numDocs;               // total number of documents in index
  int heapOldSize;              // size of heap before last rewind
  size_t hitCounter;            // number of Read/SkipTo calls during latest iteration
  size_t numIterations;         // number iterations
  size_t childEstimate;         // results estimate on child
  int lastLimitEstimate;        // last estimation for filter

  size_t offset;

  // child iterator with old root and numeric iterator for sortby field
  QueryIterator *child;
  t_docId childLastId;
  QueryIterator *numericIter;
  t_docId numericLastId;


  heap_t *heap;                 // heap for results
  RSIndexResult *resArr;        // keeps RSIndexResult
  OptimizerCompareFunc cmp;     // compare function
  RSIndexResult *pooledResult;  // memory pool

  TimeoutCtx timeoutCtx;        // Timeout parameters

  IteratorsConfig *config;       // Copy of current RSglobalconfig.IteratorsConfig
  t_fieldIndex numericFieldIndex; // field index for numeric filter
} OptimizerIterator;

#ifdef __cplusplus
extern "C" {
#endif

QueryIterator *NewOptimizerIterator(QOptimizer *q_opt, QueryIterator *root, IteratorsConfig *config);

#ifdef __cplusplus
}
#endif
