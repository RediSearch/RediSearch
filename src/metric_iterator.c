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
  mr->base.isValid = 0;
}

static t_docId MR_LastDocId(void *ctx) {
  MetricIterator *mr = ctx;
  return mr->lastDocId;
}

static void MR_Rewind(void *ctx) {
  MetricIterator *mr = ctx;
  mr->lastDocId = 0;
  mr->curIndex = 0;
  mr->base.isValid = 1;
}

static int MR_Read(void *ctx, RSIndexResult **hit) {
  if (!MR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = ctx;

  // Set the item that we read in the current RSIndexResult
  *hit = mr->base.current;
  (*hit)->docId = mr->lastDocId = mr->idsList[mr->curIndex];
  (*hit)->metric.value = mr->metricList[mr->curIndex];
  (*hit)->metric.metricField = mr->fieldName;

  mr->curIndex++;
  if (mr->curIndex == mr->resultsNum) {
    mr->base.isValid = 0;
  }
  return INDEXREAD_OK;
}

static int MR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  if (!MR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  MetricIterator *mr = ctx;
  t_docId cur_id = mr->idsList[mr->curIndex];
  while(mr->curIndex < mr->resultsNum) {
    if (docId > cur_id) {
      // consider binary search for next value
      cur_id = mr->idsList[++mr->curIndex];
      continue;
    }
    // Set the item that we skipped to it in hit.
    *hit = mr->base.current;
    (*hit)->docId = mr->lastDocId = cur_id;
    (*hit)->metric.value = mr->metricList[mr->curIndex];
    (*hit)->metric.metricField = mr->fieldName;
    mr->curIndex++;
    if (mr->curIndex == mr->resultsNum) {
      mr->base.isValid = 0;
    }
    return INDEXREAD_OK;
  }
  mr->base.isValid = 0;
  return INDEXREAD_EOF;
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

IndexIterator *NewMetricIterator(t_docId *ids_list, double *metric_list,
                                 const char *field_name, Metric metric_type) {
  MetricIterator *mi = rm_new(MetricIterator);
  mi->lastDocId = 0;
  mi->fieldName = field_name;
  mi->base.isValid = 1;
  mi->idsList = ids_list;
  mi->metricList = metric_list;
  mi->resultsNum = array_len(ids_list);
  mi->curIndex = 0;

  IndexIterator *ri = &mi->base;
  ri->ctx = mi;
  ri->type = METRIC_ITERATOR;
  ri->mode = MODE_SORTED;
  ri->current = NewMetricResult();

  mi->type = metric_type;

  ri->Read = MR_Read;
  ri->SkipTo = MR_SkipTo;
  ri->Rewind = MR_Rewind;
  ri->Free = MR_Free;
  ri->HasNext = MR_HasNext;
  ri->NumEstimated = ri->Len = MR_Len;
  ri->Abort = MR_Abort;
  ri->LastDocId = MR_LastDocId;

  return ri;
}
