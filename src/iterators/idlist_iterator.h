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

// Forward declaration to avoid circular dependency
typedef struct MetricRequest MetricRequest;

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  VECTOR_DISTANCE,
} Metric;

typedef struct {
  QueryIterator base;
  t_docId *docIds;
  t_offset size;
  t_offset offset;
} IdListIterator;

typedef struct {
  IdListIterator base;
  Metric type;
  double *metricList;    // metric_list[i] is the metric that ids_list[i] yields.
  // We store the index and pointer-to-pointer to the array
  // This avoids use-after-free when the array is reallocated
  int metricRequestIdx;
  // This is a pointer-to-pointer so we always get the current array
  struct MetricRequest **metricRequestsP;
} MetricIterator;

/**
 * @param ids - the list of doc ids to iterate over
 * @param num - the number of doc ids in the list
 * @param weight - the weight of the node (assigned to the returned result)
 */
QueryIterator *NewIdListIterator(t_docId *ids, t_offset num, double weight);

QueryIterator *NewMetricIterator(t_docId *docIds, double *metric_list, size_t num_results, Metric metric_type);

#ifdef __cplusplus
}
#endif
