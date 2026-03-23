/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "iterator_api.h"
#include "vector_index.h"
#include "util/minmax_heap.h"
#include "util/timeout.h"

typedef struct {
  RedisSearchCtx *sctx;
  VecSimIndex *index;
  size_t dim;
  VecSimType elementType;
  VecSimMetric spaceMetric;
  KNNVectorQuery query;
  VecSimQueryParams qParams;
  char *vectorScoreField;
  bool canTrimDeepResults; // If true, no need to deep copy the results before adding them to the heap.
  QueryIterator *childIt;
  struct timespec timeout;
  const FieldFilterContext* filterCtx;
} HybridIteratorParams;

typedef struct {
  QueryIterator base;
  RedisSearchCtx *sctx;
  VecSimIndex *index;
  size_t dimension;                // index dimension
  VecSimType vecType;              // index data type
  VecSimMetric indexMetric;        // index distance metric
  KNNVectorQuery query;
  VecSimQueryParams runtimeParams; // Evaluated runtime params.
  QueryIterator *child;
  VecSimSearchMode searchMode;
  bool resultsPrepared;            // Indicates if the results were already processed
                                   // (should occur in the first call to Read)
  VecSimQueryReply *reply;
  VecSimQueryReply_Iterator *iter;
  RLookupKey *ownKey;              // To be used if the iterator has to yield the vector scores
  struct RLookupKeyHandle *keyHandle; // Back-reference to the handle that points to this iterator's ownKey
  
  char *scoreField;                // To use by the sorter, for distinguishing between different vector fields.
  mm_heap_t *topResults;           // Sorted by score (min-max heap).
  size_t numIterations;
  size_t maxBatchSize;             // Maximum batch size used during batches mode
  size_t maxBatchIteration;        // Iteration (zero-based) where the maximum batch size occurred
  bool canTrimDeepResults;         // Ignore the document scores, only vector score matters. No need to deep copy the results from the child iterator.
  TimeoutCtx timeoutCtx;           // Timeout parameters
  FieldFilterContext filterCtx;
} HybridIterator;

#ifdef __cplusplus
extern "C" {
#endif

QueryIterator *NewHybridVectorIterator(HybridIteratorParams hParams, QueryError *status);

#ifdef __cplusplus
}
#endif
