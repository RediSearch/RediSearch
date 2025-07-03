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
#ifndef __ITERATOR_API_H__
typedef enum {
  VECTOR_DISTANCE,
} Metric;
#endif

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list, Metric metric_type, bool yields_metric);

Metric GetMetric(const IndexIterator *it);

#ifdef __cplusplus
}
#endif
