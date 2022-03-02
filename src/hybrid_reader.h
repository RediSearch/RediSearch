#pragma once

#include "index_iterator.h"
#include "vector_index.h"
#include "redisearch.h"
#include "spec.h"
#include "util/heap.h"

typedef enum {
  STANDARD_KNN,     // Run k-nn query over the entire vector index.
  HYBRID_ADHOC_BF,  // Measure ad-hoc the distance for every result that passes the filters,
                    // and take the top k results.
  HYBRID_BATCHES    // Get the top vector results in batches upon demand, and keep the results that
                    // passes the filters until we reach k results.
} VecSearchMode;

typedef struct {
  IndexIterator base;
  VecSimIndex *index;
  KNNVectorQuery query;
  VecSimQueryParams runtimeParams;   // Evaluated runtime params.
  IndexIterator *child;
  VecSearchMode mode;
  bool resultsPrepared;             // Indicates if the results were already processed
                         // (should occur in the first call to Read)
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  RSIndexResult **returnedResults; // Save the pointers to be freed in clean-up.
  char *scoreField;                // To use by the sorter, for distinguishing between different vector fields.
  heap_t *topResults;              // Sorted by score (max heap).
  //heap_t *orderedResults;        // Sorted by id (min heap) - for future use.
  size_t numIterations;
} HybridIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewHybridVectorIterator(VecSimIndex *index, char *score_field, KNNVectorQuery query, VecSimQueryParams qParams, IndexIterator *child_it);

#ifdef __cplusplus
}
#endif
