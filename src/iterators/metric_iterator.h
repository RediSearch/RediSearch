/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#pragma once

#include "iterator_api.h"

#ifdef __cplusplus
extern "C" {
#endif
// The metric type that this iterator yields.
typedef enum {
  VECTOR_DISTANCE,
} Metric;

typedef struct {
  QueryIterator base;
  Metric type;
  t_docId *docIds;
  double *metricList;    // metric_list[i] is the metric that ids_list[i] yields.
  size_t size;
  size_t offset;       // Index of the next doc_id to return.
} MetricIterator;

/**
 * @param ids_list - the list of doc ids to iterate over. The implementation assumes they are sorted.
 * @param metric_list - the list of scores in the iterator. The implementation assumes the number of
 * entries in this list is the same as the number of entries in ids_list.
 * @param num_results - the number of results in the iterator.
 * @param metric_type - the Metric type represented by these scores
 * @param yields_metric - whether the iterator should yield the metric as the score
 */
QueryIterator *IT_V2(NewMetricIterator)(t_docId *docIds, double *metric_list, size_t num_results, Metric metric_type, bool yields_metric);

#ifdef __cplusplus
}
#endif