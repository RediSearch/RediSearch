/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "metric_iterator.h"
#include "vector_index.h"

static size_t MR_NumEstimated(IndexIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  return mr->resultsNum;
}

static void MR_Rewind(IndexIterator *base) {
  if (base->isAborted) return; // Not allowed to rewind an aborted iterator
  MetricIterator *mr = (MetricIterator *)base;
  base->LastDocId = 0;
  mr->curIndex = 0;
  IITER_CLEAR_EOF(base);
}

static int MR_Read(IndexIterator *base, RSIndexResult **hit) {
  if (!base->isValid) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = (MetricIterator *)base;

  // Set the item that we read in the current RSIndexResult
  *hit = mr->base.current;
  (*hit)->docId = base->LastDocId = mr->idsList[mr->curIndex];
  (*hit)->num.value = mr->metricList[mr->curIndex];

  // Advance the current index in the doc ids array, so it will point to the next id to be returned.
  // If we reached the total number of results, iterator is depleted.
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    IITER_SET_EOF(base);
  }
  return INDEXREAD_OK;
}

static int MR_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  if (!base->isValid) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = (MetricIterator *)base;
  t_docId cur_id = mr->idsList[mr->curIndex];
  while(cur_id < docId) {
    // consider binary search for next value (skip exponentially to 2,4,8,...).
    mr->curIndex++;
    if (mr->curIndex == mr->resultsNum) {
      IITER_SET_EOF(base);
      return INDEXREAD_EOF;
    }
    cur_id = mr->idsList[mr->curIndex];
  }
  // Set the item that we skipped to it in hit.
  *hit = mr->base.current;
  (*hit)->docId = base->LastDocId = cur_id;
  (*hit)->num.value = mr->metricList[mr->curIndex];
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    IITER_SET_EOF(&mr->base);
  }
  return (cur_id == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}

static void SetYield(IndexIterator *mr, RSIndexResult **hit) {
  ResultMetrics_Reset(*hit);
  ResultMetrics_Add(*hit, mr->ownKey, RS_NumVal((*hit)->num.value));
}

static int MR_Read_With_Yield(IndexIterator *mr, RSIndexResult **hit) {
  int rc = MR_Read(mr, hit);
  if (INDEXREAD_OK == rc) {
    SetYield(mr, hit);
  }
  return rc;
}

static int MR_SkipTo_With_Yield(IndexIterator *mr, t_docId docId, RSIndexResult **hit) {
  int rc = MR_SkipTo(mr, docId, hit);
  if (INDEXREAD_OK == rc || INDEXREAD_NOTFOUND == rc) {
    SetYield(mr, hit);
  }
  return rc;
}

static void MR_Free(IndexIterator *self) {
  MetricIterator *mr = (MetricIterator *)self;
  if (mr == NULL) {
    return;
  }
  IndexResult_Free(mr->base.current);

  array_free(mr->idsList);
  array_free(mr->metricList);

  rm_free(mr);
}

IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list, Metric metric_type, bool yields_metric) {
  MetricIterator *mi = rm_new(MetricIterator);
  mi->base.LastDocId = 0;
  mi->base.isValid = 1;
  mi->base.isAborted = 0;
  mi->idsList = ids_list;
  mi->metricList = metric_list;
  mi->resultsNum = array_len(ids_list);
  mi->curIndex = 0;

  IndexIterator *ri = &mi->base;
  ri->type = METRIC_ITERATOR;
  ri->current = NewMetricResult();
  ri->ownKey = NULL;

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
