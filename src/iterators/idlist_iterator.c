/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "idlist_iterator.h"

static inline void setEof(QueryIterator *base, bool value) {
  base->atEOF = value;
}

static inline bool isEof(const QueryIterator *base) {
  return base->atEOF;
}

static size_t IL_NumEstimated(QueryIterator *base) {
  IdListIterator *it = (IdListIterator *)base;
  return (size_t)it->size;
}

/* Read the next entry from the iterator, into hit *e.
*  Returns ITERATOR_EOF if at the end */
static IteratorStatus IL_Read(QueryIterator *base) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(base) || it->offset >= it->size) {
    setEof(base, true);
    return ITERATOR_EOF;
  }

  base->lastDocId = it->docIds[it->offset++];
  base->current->docId = base->lastDocId;
  return ITERATOR_OK;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
* matches */
static IteratorStatus IL_SkipTo(QueryIterator *base, t_docId docId) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(base)) {
    return ITERATOR_EOF;
  }
  if (it->offset >= it->size || docId > it->docIds[it->size - 1]) {
    setEof(base, true);
    return ITERATOR_EOF;
  }

  uint64_t top = it->size, bottom = it->offset;
  uint64_t i;
  t_docId did;
  while (bottom < top) {
    i = (bottom + top) / 2;
    did = it->docIds[i];

    if (did == docId) {
      break;
    }
    if (docId < did) {
      top = i;
    } else {
      bottom = i + 1;
    }
  }
  if (did < docId) {
    did = it->docIds[++i];
  }
  it->offset = i + 1;
  base->current->docId = base->lastDocId = did;
  return docId == base->lastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
}

/* release the iterator's context and free everything needed */
static void IL_Free(QueryIterator *self) {
  IdListIterator *it = (IdListIterator *)self;
  IndexResult_Free(self->current);
  rm_free(it->docIds);
  rm_free(self);
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;
  if (*d1 < *d2) return -1;
  if (*d1 > *d2) return 1;
  return 0;
}

static void IL_Rewind(QueryIterator *base) {
  IdListIterator *il = (IdListIterator *)base;
  setEof(base, false);
  base->lastDocId = 0;
  base->current->docId = 0;
  il->offset = 0;
}

QueryIterator *IT_V2(NewIdListIterator) (t_docId *ids, t_offset num, double weight) {
  // Assume the ids are not null and num > 0 otherwise these Iterator would not be created, avoid validation
  // first sort the ids, so the caller will not have to deal with it
  IdListIterator *it = rm_new(IdListIterator);
  it->size = num;
  it->docIds = ids;
  it->offset = 0;
  QueryIterator *ret = &it->base;

  setEof(ret, false);
  ret->current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  ret->lastDocId = 0;
  ret->type = ID_LIST_ITERATOR;
  ret->NumEstimated = IL_NumEstimated;
  ret->Free = IL_Free;
  ret->Read = IL_Read;
  ret->SkipTo = IL_SkipTo;
  ret->Rewind = IL_Rewind;
  return ret;
}

static void SetYield(QueryIterator *base, double value) {
  base->current->num.value = value;
  if (!base->current->metrics) {
    ResultMetrics_Add(base->current, NULL, RS_NumVal(value));
  } else {
    ResultMetrics_UpdateDouble(base->current, NULL, value);
  }
}

static IteratorStatus MR_Read(QueryIterator *base) {
  MetricIterator *mr = (MetricIterator *)base;
  IdListIterator *it = &mr->base;
  IteratorStatus rc = IL_Read(base);
  if (ITERATOR_OK == rc) {
    SetYield(base, mr->metricList[it->offset - 1]);
  }
  return rc;
}

static IteratorStatus MR_SkipTo(QueryIterator *base, t_docId docId) {
  MetricIterator *mr = (MetricIterator *)base;
  IdListIterator *it = &mr->base;
  int rc = IL_SkipTo(base, docId);
  if (ITERATOR_OK == rc || ITERATOR_NOTFOUND == rc) {
    SetYield(base, mr->metricList[it->offset - 1]);
  }
  return rc;
}

static void MR_Free(QueryIterator *self) {
  MetricIterator *mi = (MetricIterator *)self;
  IdListIterator *it = &mi->base;
  QueryIterator *base = &it->base;
  IndexResult_Free(base->current);
  rm_free(it->docIds);
  rm_free(mi->metricList);
  rm_free(mi);
}

QueryIterator *IT_V2(NewMetricIterator)(t_docId *docIds, double *metric_list, size_t num_results, Metric metric_type) {
  QueryIterator *ret;
  MetricIterator *mi = rm_new(MetricIterator);
  IdListIterator *it = &mi->base;
  ret = &it->base;
  mi->type = metric_type;
  mi->metricList = metric_list;
  it->docIds = docIds;
  it->size = num_results;
  it->offset = 0;

  ret->lastDocId = 0;
  setEof(ret, false);
  ret->type = METRIC_ITERATOR;
  ret->current = NewMetricResult();
  ret->Read = MR_Read;
  ret->SkipTo = MR_SkipTo;
  ret->Rewind = IL_Rewind;
  ret->Free = MR_Free;
  ret->NumEstimated = IL_NumEstimated;
  return ret;
}
