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

struct RLookupKey; // Forward declaration
IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list, Metric metric_type, struct RLookupKey ***key_pp);

#ifdef __cplusplus
}
#endif
