/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "metric_iterator.h"
#include "vector_index.h"

static int MR_HasNext(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->base.isValid;
}

static size_t MR_Len(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->resultsNum;
}

static void MR_Abort(void *ctx) {
  MetricIterator *mr = ctx;
  IITER_SET_EOF(&mr->base);
}

static t_docId MR_LastDocId(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->lastDocId;
}

static void MR_Rewind(void *ctx) {
  MetricIterator *mr = ctx;
  mr->lastDocId = 0;
  mr->curIndex = 0;
  IITER_CLEAR_EOF(&mr->base);
}

static int MR_Read(void *ctx, RSIndexResult **hit) {
  if (!MR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = ctx;

  // Set the item that we read in the current RSIndexResult
  *hit = mr->base.current;
  (*hit)->docId = mr->lastDocId = mr->idsList[mr->curIndex];
  (*hit)->num.value = mr->metricList[mr->curIndex];

  // Advance the current index in the doc ids array, so it will point to the next id to be returned.
  // If we reached the total number of results, iterator is depleted.
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    IITER_SET_EOF(&mr->base);
  }
  return INDEXREAD_OK;
}

static int MR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  if (!MR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = ctx;
  t_docId cur_id = mr->idsList[mr->curIndex];
  while(cur_id < docId) {
    // consider binary search for next value (skip exponentially to 2,4,8,...).
    mr->curIndex++;
    if (mr->curIndex == mr->resultsNum) {
      mr->lastDocId = cur_id;
      IITER_SET_EOF(&mr->base);
      return INDEXREAD_EOF;
    }
    cur_id = mr->idsList[mr->curIndex];
  }
  // Set the item that we skipped to it in hit.
  *hit = mr->base.current;
  (*hit)->docId = mr->lastDocId = cur_id;
  (*hit)->num.value = mr->metricList[mr->curIndex];
  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    IITER_SET_EOF(&mr->base);
  }
  return (cur_id == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}

static void SetYield(void *ctx, RSIndexResult **hit) {
  MetricIterator *mr = ctx;
  ResultMetrics_Reset(*hit);
  ResultMetrics_Add(*hit, mr->base.ownKey, RS_NumVal((*hit)->num.value));
}

static int MR_Read_With_Yield(void *ctx, RSIndexResult **hit) {
  int rc = MR_Read(ctx, hit);
  if (INDEXREAD_OK == rc) {
    SetYield(ctx, hit);
  }
  return rc;
}

static int MR_SkipTo_With_Yield(void *ctx, t_docId docId, RSIndexResult **hit) {
  int rc = MR_SkipTo(ctx, docId, hit);
  if (INDEXREAD_OK == rc || INDEXREAD_NOTFOUND == rc) {
    SetYield(ctx, hit);
  }
  return rc;
}

static void MR_Free(IndexIterator *self) {
  MetricIterator *mr = self->ctx;
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
  mi->lastDocId = 0;
  mi->base.isValid = 1;
  mi->idsList = ids_list;
  mi->metricList = metric_list;
  mi->resultsNum = array_len(ids_list);
  mi->curIndex = 0;

  IndexIterator *ri = &mi->base;
  ri->ctx = mi;
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
  ri->HasNext = MR_HasNext;
  ri->NumEstimated = ri->Len = MR_Len;
  ri->Abort = MR_Abort;
  ri->LastDocId = MR_LastDocId;

  return ri;
}
