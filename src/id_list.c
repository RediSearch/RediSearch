#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"

typedef struct {
  IndexIterator base;
  t_docId *docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;
} IdListIterator;

static inline void setEof(IdListIterator *it, int value) {
  it->base.isValid = !value;
}

static inline int isEof(const IdListIterator *it) {
  return !it->base.isValid;
}

typedef struct {
  IndexCriteriaTester base;
  t_docId *docIds;
  t_offset size;
} ILCriteriaTester;

static int cmp_docids(const void *p1, const void *p2);

static int IL_Test(struct IndexCriteriaTester *ct, t_docId id) {
  ILCriteriaTester *lct = (ILCriteriaTester *)ct;
  return bsearch((void *)id, lct->docIds, (size_t)lct->size, sizeof(t_docId), cmp_docids) != NULL;
}

static void IL_TesterFree(struct IndexCriteriaTester *ct) {
  ILCriteriaTester *lct = (ILCriteriaTester *)ct;
  rm_free(lct->docIds);
  rm_free(lct);
}

IndexCriteriaTester *IL_GetCriteriaTester(void *ctx) {
  IdListIterator *it = ctx;
  ILCriteriaTester *ct = rm_malloc(sizeof(*ct));
  ct->docIds = rm_malloc(sizeof(t_docId) * it->size);
  memcpy(ct->docIds, it->docIds, it->size);
  ct->size = it->size;
  ct->base.Test = IL_Test;
  ct->base.Free = IL_TesterFree;
  return &ct->base;
}

size_t IL_NumEstimated(void *ctx) {
  IdListIterator *it = ctx;
  return (size_t)it->size;
}

/* Read the next entry from the iterator, into hit *e.
 *  Returns INDEXREAD_EOF if at the end */
int IL_Read(void *ctx, RSIndexResult **r) {
  IdListIterator *it = ctx;
  if (isEof(it) || it->offset >= it->size) {
    setEof(it, 1);
    return INDEXREAD_EOF;
  }

  it->lastDocId = it->docIds[it->offset++];

  // TODO: Filter here
  it->base.current->docId = it->lastDocId;
  *r = it->base.current;
  return INDEXREAD_OK;
}

void IL_Abort(void *ctx) {
  ((IdListIterator *)ctx)->base.isValid = 0;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int IL_SkipTo(void *ctx, t_docId docId, RSIndexResult **r) {
  IdListIterator *it = ctx;
  if (isEof(it) || it->offset >= it->size) {
    return INDEXREAD_EOF;
  }

  if (docId > it->docIds[it->size - 1]) {
    it->base.isValid = 0;
    return INDEXREAD_EOF;
  }

  t_offset top = it->size - 1, bottom = it->offset;
  t_offset i = bottom;

  while (bottom <= top) {

    t_docId did = it->docIds[i];

    if (did == docId) {
      break;
    }
    if (docId < did) {
      if (i == 0) break;
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }
  it->offset = i + 1;
  if (it->offset >= it->size) {
    setEof(it, 1);
  }

  it->lastDocId = it->docIds[i];
  it->base.current->docId = it->lastDocId;

  *r = it->base.current;

  if (it->lastDocId == docId) {
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
}

/* the last docId read */
t_docId IL_LastDocId(void *ctx) {
  return ((IdListIterator *)ctx)->lastDocId;
}

/* release the iterator's context and free everything needed */
void IL_Free(struct indexIterator *self) {
  IdListIterator *it = self->ctx;
  IndexResult_Free(it->base.current);
  if (it->docIds) {
    rm_free(it->docIds);
  }
  rm_free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t IL_Len(void *ctx) {
  return (size_t)((IdListIterator *)ctx)->size;
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;

  return (int)(*d1 - *d2);
}

void IL_Rewind(void *p) {
  IdListIterator *il = p;
  setEof(il, 0);
  il->lastDocId = 0;
  il->base.current->docId = 0;
  il->offset = 0;
}

IndexIterator *NewIdListIterator(t_docId *ids, t_offset num, double weight) {

  // first sort the ids, so the caller will not have to deal with it
  qsort(ids, (size_t)num, sizeof(t_docId), cmp_docids);

  IdListIterator *it = rm_new(IdListIterator);

  it->size = num;
  it->docIds = rm_calloc(num, sizeof(t_docId));
  if (num > 0) memcpy(it->docIds, ids, num * sizeof(t_docId));
  setEof(it, 0);
  it->lastDocId = 0;
  it->base.current = NewVirtualResult(weight);
  it->base.current->fieldMask = RS_FIELDMASK_ALL;

  it->offset = 0;

  IndexIterator *ret = &it->base;
  ret->ctx = it;
  ret->type = ID_LIST_ITERATOR;
  ret->GetCriteriaTester = IL_GetCriteriaTester;
  ret->NumEstimated = IL_NumEstimated;
  ret->Free = IL_Free;
  ret->LastDocId = IL_LastDocId;
  ret->Len = IL_Len;
  ret->Read = IL_Read;
  ret->SkipTo = IL_SkipTo;
  ret->Abort = IL_Abort;
  ret->Rewind = IL_Rewind;
  ret->mode = MODE_SORTED;

  ret->HasNext = NULL;
  return ret;
}
