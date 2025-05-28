/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "metric_iterator.h"

static inline void setEof(MetricIterator *it, bool value) {
  it->base.atEOF = value;
}

static inline bool isEof(const MetricIterator *it) {
  return it->base.atEOF;
}

static size_t MR_NumEstimated(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  return mr->size;
}

static void MR_Rewind(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  base->lastDocId = 0;
  mr->offset = 0;
  setEof(mr, false);
}

static IteratorStatus MR_Read(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  if (isEof(mr) || mr->offset >= mr->size) {
    setEof(mr, true);
    return ITERATOR_EOF;
  }

  base->lastDocId = mr->docIds[mr->offset++];
  mr->base.current->docId = base->lastDocId;
  return ITERATOR_OK;
}

static IteratorStatus MR_SkipTo(QueryIterator *base, t_docId docId) {
  MetricIterator *mr = (MetricIterator *)base;
  if (isEof(mr) || mr->offset >= mr->size) {
    setEof(mr, true);
    return ITERATOR_EOF;
  }

  if (docId > mr->docIds[mr->size - 1]) {
    setEof(mr, true);
    return ITERATOR_EOF;
  }

  int64_t top = mr->size - 1, bottom = mr->offset;
  int64_t i;
  t_docId did;
  while (bottom <= top) {
    i = (bottom + top) / 2;
    did = mr->docIds[i];

    if (did == docId) {
      break;
    }
    if (docId < did) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
  }
  if (did < docId) {
    did = mr->docIds[++i];
  }
  mr->offset = i + 1;
  if (mr->offset >= mr->size) {
    setEof(mr, true);
  }

  mr->base.current->docId = mr->base.lastDocId = did;
  return docId == did ? ITERATOR_OK : ITERATOR_NOTFOUND;
}

static void SetYield(QueryIterator *mr) {
  ResultMetrics_Reset(mr->current);
  ResultMetrics_Add(mr->current, NULL, RS_NumVal(mr->current->num.value));
}

static IteratorStatus MR_Read_With_Yield(QueryIterator *base) {
  IteratorStatus rc = MR_Read(base);
  if (ITERATOR_OK == rc) {
    MetricIterator *mr = (MetricIterator *)base;
    base->current->num.value = mr->metricList[mr->offset - 1];
    SetYield(base);
  }
  return rc;
}

static IteratorStatus MR_SkipTo_With_Yield(QueryIterator *base, t_docId docId) {
  int rc = MR_SkipTo(base, docId);
  if (ITERATOR_OK == rc || ITERATOR_NOTFOUND == rc) {
    MetricIterator *mr = (MetricIterator *)base;
    base->current->num.value = mr->metricList[mr->offset - 1];
    SetYield(base);
  }
  return rc;
}

static void MR_Free(QueryIterator *self) {
  MetricIterator *mr = (MetricIterator *)self;
  if (mr == NULL) {
    return;
  }
  IndexResult_Free(mr->base.current);

  if (mr->docIds) {
    rm_free(mr->docIds);
  }
  
  if (mr->metricList) {
    rm_free(mr->metricList);
  }

  rm_free(mr);
}

QueryIterator *IT_V2(NewMetricIterator)(t_docId *docIds, double *metric_list, size_t num_results, Metric metric_type, bool yields_metric) {
  MetricIterator *mi = rm_new(MetricIterator);
  mi->base.lastDocId = 0;
  setEof(mi, false);
  mi->docIds = docIds;
  mi->metricList = metric_list;
  mi->size = num_results;
  mi->offset = 0;

  QueryIterator *ri = &mi->base;
  ri->type = METRIC_ITERATOR;
  ri->current = NewMetricResult();
  mi->type = metric_type;
  // If we interested in yielding score
  if (yields_metric) {
    ri->Read = MR_Read_With_Yield;
    ri->SkipTo = MR_SkipTo_With_Yield;
  } else {
    ri->Read = MR_Read;
    ri->SkipTo = MR_SkipTo;
  }
  ri->Rewind = MR_Rewind;
  ri->Free = MR_Free;
  ri->NumEstimated = MR_NumEstimated;

  return ri;
}