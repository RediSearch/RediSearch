#pragma once

#include "query_optimizer.h"
#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"
#include "util/heap.h"
#include "util/timeout.h"

// This enum should match the VecSearchMode enum in VecSim
typedef int (*OptimizerCompareFunc)(const void *e1, const void *e2, const void *udata);

#define OPTIM_OWN_NF 0x01

typedef struct {
  IndexIterator base;
  QOptimizer *optim;
  t_docId lastDocId;
  int flags;
  
  size_t numDocs;               // total number of documents in index
  int heapOldSize;              // size of heap before last rewind
  size_t hitCounter;            // number of Read/SkipTo calls during latest iteration
  size_t numIterations;         // number iterations
  size_t childEstimate;         // results estimate on child 
  int lastLimitEstimate;        // last estimation for filter        

  size_t offset;

  // child iterator with old root and numeric iterator for sortby field
  IndexIterator *child;
  t_docId childLastId;
  IndexIterator *numericIter;
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

IndexIterator *NewOptimizerIterator(QOptimizer *q_opt, IndexIterator *root, IteratorsConfig *config);

#ifdef __cplusplus
}
#endif
