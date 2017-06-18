#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"
#include "id_list.h"

/* Read the next entry from the iterator, into hit *e.
*  Returns INDEXREAD_EOF if at the end */
int IL_Read(void *ctx, RSIndexResult **r) {
  IdListIterator *it = ctx;
  if (it->atEOF || it->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  it->lastDocId = it->docIds[it->offset];
  ++it->offset;
  if (it->offset == it->size) {
    it->atEOF = 1;
  }
  // TODO: Filter here
  it->res->docId = it->lastDocId;
  *r = it->res;
  // AggregateResult_AddChild(r, &it->res);

  return INDEXREAD_OK;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int IL_SkipTo(void *ctx, u_int32_t docId, RSIndexResult **r) {
  IdListIterator *it = ctx;
  if (it->atEOF || it->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  if (docId > it->docIds[it->size - 1]) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  t_offset top = it->size - 1, bottom = it->offset;
  t_offset i = bottom;
  t_offset newi;

  while (bottom < top) {
    t_docId did = it->docIds[i];
    if (did == docId) {
      break;
    }
    if (docId <= did) {
      top = i;
    } else {
      bottom = i;
    }
    newi = (bottom + top) / 2;
    if (newi == i) {
      break;
    }
    i = newi;
  }
  it->offset = i + 1;
  if (it->offset == it->size) {
    it->atEOF = 1;
  }

  it->lastDocId = it->docIds[i];
  it->res->docId = it->lastDocId;

  *r = it->res;

  // printf("lastDocId: %d, docId%d\n", it->lastDocId, docId);
  if (it->lastDocId == docId) {
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
}

/* the last docId read */
t_docId IL_LastDocId(void *ctx) {
  return ((IdListIterator *)ctx)->lastDocId;
}

/* can we continue iteration? */
int IL_HasNext(void *ctx) {
  return !((IdListIterator *)ctx)->atEOF;
}

RSIndexResult *IL_Current(void *ctx) {
  return ((IdListIterator *)ctx)->res;
}

/* release the iterator's context and free everything needed */
void IL_Free(struct indexIterator *self) {
  IdListIterator *it = self->ctx;
  IndexResult_Free(it->res);
  rm_free(it->docIds);
  rm_free(it);
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

IndexIterator *NewIdListIterator(t_docId *ids, t_offset num) {

  // first sort the ids, so the caller will not have to deal with it
  qsort(ids, (size_t)num, sizeof(t_docId), cmp_docids);
  IdListIterator *it = rm_new(IdListIterator);

  it->size = num;
  it->docIds = rm_calloc(num, sizeof(t_docId));
  if (num > 0) memcpy(it->docIds, ids, num * sizeof(t_docId));
  it->atEOF = 0;
  it->lastDocId = 0;
  it->res = NewVirtualResult();
  it->res->fieldMask = RS_FIELDMASK_ALL;

  it->offset = 0;

  IndexIterator *ret = rm_new(IndexIterator);
  ret->ctx = it;
  ret->Free = IL_Free;
  ret->HasNext = IL_HasNext;
  ret->LastDocId = IL_LastDocId;
  ret->Len = IL_Len;
  ret->Read = IL_Read;
  ret->Current = IL_Current;
  ret->SkipTo = IL_SkipTo;
  return ret;
}
