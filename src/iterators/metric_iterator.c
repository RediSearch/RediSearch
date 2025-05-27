/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "metric_iterator.h"

static size_t MR_NumEstimated(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  return mr->resultsNum;
}

static void MR_Rewind(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  base->lastDocId = 0;
  mr->curIndex = 0;
  mr->base.atEOF = 0;
}

static IteratorStatus MR_Read(QueryIterator *base) {
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  MetricIterator *mr = (MetricIterator *)base;

  // Set the item that we read in the current RSIndexResult
  base->lastDocId = mr->idsList[mr->curIndex];

  // Advance the current index in the doc ids array, so it will point to the next id to be returned.
  // If we reached the total number of results, iterator is depleted.
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    mr->base.atEOF = 1;
  }
  return ITERATOR_OK;
}

static IteratorStatus MR_SkipTo(QueryIterator *base, t_docId docId) {
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  MetricIterator *mr = (MetricIterator *)base;
  t_docId cur_id = mr->idsList[mr->curIndex];
  while(cur_id < docId) {
    // consider binary search for next value (skip exponentially to 2,4,8,...).
    mr->curIndex++;
    if (mr->curIndex == mr->resultsNum) {
      base->atEOF = 1;
      return ITERATOR_EOF;
    }
    cur_id = mr->idsList[mr->curIndex];
  }
  // Set the item that we skipped to it in hit.
  base->lastDocId = cur_id;
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    base->atEOF = 1;
  }
  return (cur_id == docId) ? ITERATOR_OK : ITERATOR_NOTFOUND;
}

static void MR_Free(QueryIterator *self) {
  MetricIterator *mr = (MetricIterator *)self;
  if (mr == NULL) {
    return;
  }
  IndexResult_Free(mr->base.current);

  array_free(mr->idsList);
  array_free(mr->metricList);

  rm_free(mr);
}

QueryIterator *IT_V2(NewMetricIterator)(t_docId *ids_list, double *metric_list, Metric metric_type, bool yields_metric) {
  MetricIterator *mi = rm_new(MetricIterator);
  mi->base.lastDocId = 0;
  mi->base.atEOF = 0;
  mi->idsList = ids_list;
  mi->metricList = metric_list;
  mi->resultsNum = array_len(ids_list);
  mi->curIndex = 0;

  QueryIterator *ri = &mi->base;
  ri->type = METRIC_ITERATOR;
  ri->current = NewMetricResult();
  mi->type = metric_type;
  ri->Read = MR_Read;
  ri->SkipTo = MR_SkipTo;
  // If we interested in yielding score
  /*if (yields_metric) {
    ri->Read = MR_Read_With_Yield;
    ri->SkipTo = MR_SkipTo_With_Yield;
  } else {
    ri->Read = MR_Read;
    ri->SkipTo = MR_SkipTo;
  }*/
  ri->Rewind = MR_Rewind;
  ri->Free = MR_Free;
  ri->NumEstimated = MR_NumEstimated;

  return ri;
}