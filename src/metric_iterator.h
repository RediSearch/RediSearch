/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "index_iterator.h"

// The metric type that this iterator yields.
typedef enum {
  VECTOR_DISTANCE,
} Metric;

typedef struct {
  IndexIterator base;
  Metric type;
  t_docId *idsList;
  double *metricList;    // metric_list[i] is the metric that ids_list[i] yields.
  t_docId lastDocId;
  size_t resultsNum;
  size_t curIndex;       // Index of the next doc_id to return.
} MetricIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list, Metric metric_type, bool yields_metric);

#ifdef __cplusplus
}
#endif
