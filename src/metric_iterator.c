#include "metric_iterator.h"
#include "vector_index.h"

static int MR_HasNext(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->base.isValid;
}

static size_t MR_Len(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->results_num;
}

static void MR_Abort(void *ctx) {
  MetricIterator *mr = ctx;
  mr->base.isValid = 0;
}

static t_docId MR_LastDocId(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->lastDocId;
}

static void MR_VecDistance_Rewind(void *ctx) {
  MetricIterator *mr = ctx;
  VecSimQueryResult_IteratorReset(mr->iter);
  mr->lastDocId = 0;
  mr->base.isValid = 1;
}

static int MR_VecDistance_Read(void *ctx, RSIndexResult **hit) {
  MetricIterator *mr = ctx;
  if (!MR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  VecSimQueryResult_Iterator *iter = (VecSimQueryResult_Iterator *)mr->iter;
  if (!VecSimQueryResult_IteratorHasNext(iter)) {
    mr->base.isValid = 0;
    return INDEXREAD_EOF;
  }
  VecSimQueryResult *res = VecSimQueryResult_IteratorNext(iter);
  // Set the item that we read in the current RSIndexResult
  *hit = mr->base.current;
  (*hit)->docId = mr->lastDocId = VecSimQueryResult_GetId(res);
  (*hit)->metric.value = VecSimQueryResult_GetScore(res);
  (*hit)->metric.metricField = mr->metricField;
  return INDEXREAD_OK;
}

static int MR_VecDistance_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  MetricIterator *mr = ctx;
  VecSimQueryResult_Iterator *iter = (VecSimQueryResult_Iterator *)mr->iter;
  while(VecSimQueryResult_IteratorHasNext(iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    // Set the item that we skipped to it in hit.
    *hit = mr->base.current;
    (*hit)->docId = mr->lastDocId = id;
    (*hit)->metric.value = VecSimQueryResult_GetScore(res);
    (*hit)->metric.metricField = mr->metricField;
    return INDEXREAD_OK;
  }
  mr->base.isValid = 0;
  return INDEXREAD_EOF;
}

static void MR_VecDistance_Free(IndexIterator *self) {
  MetricIterator *mr = self->ctx;
  if (mr == NULL) {
    return;
  }
  IndexResult_Free(mr->base.current);

  VecSimQueryResult_FreeArray(mr->list);
  VecSimQueryResult_IteratorFree(mr->iter);

  rm_free(mr);
}


static void InitByMetricType(MetricIterator *mi, Metric metricType) {
  switch (metricType) {
    case VECTOR_DISTANCE:
      mi->base.Read = MR_VecDistance_Read;
      mi->base.SkipTo = MR_VecDistance_SkipTo;
      mi->base.Rewind = MR_VecDistance_Rewind;
      mi->base.Free = MR_VecDistance_Free;
      mi->results_num = VecSimQueryResult_ArrayLen((VecSimQueryResult *)mi->list);
      break;
    default:
      break;  // Error
  }
}

IndexIterator *NewMetricIterator(MetricResult_List list, MetricResult_Iterator iter,
                                 const char *metric_name, Metric metric_type) {
  MetricIterator *mi = rm_new(MetricIterator);
  mi->lastDocId = 0;
  mi->metricField = metric_name;
  mi->base.isValid = 1;
  mi->list = list;
  mi->iter = iter;

  IndexIterator *ri = &mi->base;
  ri->ctx = mi;
  ri->type = METRIC_ITERATOR;
  ri->mode = MODE_SORTED;
  ri->current = NewMetricResult();

  mi->type = metric_type;
  InitByMetricType(mi, metric_type);

  ri->HasNext = MR_HasNext;
  ri->NumEstimated = ri->Len = MR_Len;
  ri->Abort = MR_Abort;
  ri->LastDocId = MR_LastDocId;

  return ri;
}
