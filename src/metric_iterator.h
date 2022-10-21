#pragma once

#include "index_iterator.h"

typedef enum {
  VECTOR_DISTANCE,
} Metric;

typedef struct {
  IndexIterator base;
  Metric type;
  t_docId *idsList;
  double *metricList;    // metric_list[i] is the metric that ids_list[i] yields.
  t_docId lastDocId;
  const char *fieldName; // The field name that corresponds to the metric in the results.
  size_t resultsNum;
  size_t curIndex;       // Index of the next doc_id to return.
} MetricIterator;

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list,
                                 const char *field_name, Metric metric_type);

#ifdef __cplusplus
}
#endif