/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "index_iterator.h"
#include "vector_index.h"
#include "redisearch.h"
#include "spec.h"
#include "util/minmax_heap.h"
#include "util/timeout.h"

typedef struct {
  VecSimIndex *index;
  size_t dim;
  VecSimType elementType;
  VecSimMetric spaceMetric;
  KNNVectorQuery query;
  VecSimQueryParams qParams;
  char *vectorScoreField;
  bool ignoreDocScore;
  IndexIterator *childIt;
  struct timespec timeout;
} HybridIteratorParams;

typedef struct {
  IndexIterator base;
  VecSimIndex *index;
  size_t dimension;                // index dimension
  VecSimType vecType;              // index data type
  VecSimMetric indexMetric;        // index distance metric
  KNNVectorQuery query;
  VecSimQueryParams runtimeParams; // Evaluated runtime params.
  IndexIterator *child;
  VecSimSearchMode searchMode;
  bool resultsPrepared;            // Indicates if the results were already processed
                                   // (should occur in the first call to Read)
  VecSimQueryReply *reply;
  VecSimQueryReply_Iterator *iter;
  t_docId lastDocId;
  RSIndexResult **returnedResults; // Save the pointers to be freed in clean-up.
  char *scoreField;                // To use by the sorter, for distinguishing between different vector fields.
  mm_heap_t *topResults;           // Sorted by score (min-max heap).
  size_t numIterations;
  bool ignoreScores;               // Ignore the document scores, only vector score matters.
  TimeoutCtx timeoutCtx;           // Timeout parameters
} HybridIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewHybridVectorIterator(HybridIteratorParams hParams, QueryError *status);

#ifdef __cplusplus
}
#endif
