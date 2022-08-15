#pragma once

#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"
#include "util/heap.h"
#include "util/timeout.h"

// This enum should match the VecSearchMode enum in VecSim

struct qast_opt;

typedef struct {
  IndexIterator *child;
  IndexIterator *numeric;
  struct qast_opt *opt;

  IndexIterator base;
  t_docId lastDocId;
  size_t numIterations;
  TimeoutCtx timeoutCtx;           // Timeout parameters
} OptimizerIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewOptimizerIterator(struct qast_opt *opt);

#ifdef __cplusplus
}
#endif
