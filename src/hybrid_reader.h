#pragma once

#include "index_iterator.h"
#include "vector_index.h"
#include "redisearch.h"
#include "spec.h"
#include "util/heap.h"
#include "util/timeout.h"

typedef enum {
  VECSIM_STANDARD_KNN,               // Run k-nn query over the entire vector index.
  VECSIM_HYBRID_ADHOC_BF,            // Measure ad-hoc the distance for every result that passes the filters,
                                     // and take the top k results.
  VECSIM_HYBRID_BATCHES,             // Get the top vector results in batches upon demand, and keep the results that
                                     // passes the filters until we reach k results.
  VECSIM_HYBRID_BATCHES_TO_ADHOC_BF  // Start with batches and dynamically switched to ad-hoc BF.

} VecSimSearchMode;

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
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  RSIndexResult **returnedResults; // Save the pointers to be freed in clean-up.
  char *scoreField;                // To use by the sorter, for distinguishing between different vector fields.
  heap_t *topResults;              // Sorted by score (max heap).
  //heap_t *orderedResults;        // Sorted by id (min heap) - for future use.
  size_t numIterations;
  bool ignoreScores;               // Ignore the document scores, only vector score matters.
  TimeoutCb timeoutCb;             // Timeout callback function
  TimeoutCtx timeoutCtx;           // Timeout parameters
} HybridIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewHybridVectorIterator(HybridIteratorParams hParams);

#ifdef __cplusplus
}
#endif
