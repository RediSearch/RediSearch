#pragma once

#include "index_iterator.h"

typedef void* MetricResult_List;
typedef void* MetricResult_Iterator;

typedef enum {
  VECTOR_DISTANCE,
} Metric;

typedef struct {
  IndexIterator base;
  Metric type;
  MetricResult_List list;
  MetricResult_Iterator iter;
  t_docId lastDocId;
  const char *metricField;
  size_t results_num;
} MetricIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewMetricIterator(MetricResult_List list, MetricResult_Iterator iter,
                                 const char *metric_name, Metric metric_type);

#ifdef __cplusplus
}
#endif