/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "idlist_iterator.h"

static inline void setEof(IdListIterator *it, int value) {
  it->base.atEOF = value;
}

static inline int isEof(const IdListIterator *it) {
  return it->base.atEOF;
}

static size_t IL_NumEstimated(QueryIterator *base) {
  IdListIterator *it = (IdListIterator *)base;
  return (size_t)it->size;
}
 
/* Read the next entry from the iterator, into hit *e.
*  Returns ITERATOR_EOF if at the end */
static IteratorStatus IL_Read(QueryIterator *base) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(it) || it->offset >= it->size) {
    setEof(it, 1);
    return ITERATOR_EOF;
  }

  base->lastDocId = it->docIds[it->offset++];
  it->base.current->docId = base->lastDocId;
  return ITERATOR_OK;
}
 
/* Skip to a docid, potentially reading the entry into hit, if the docId
* matches */
static IteratorStatus IL_SkipTo(QueryIterator *base, t_docId docId) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(it) || it->offset >= it->size) {
    return ITERATOR_EOF;
  }

  if (docId > it->docIds[it->size - 1]) {
    setEof(it, 1);
    return ITERATOR_EOF;
  }

  int64_t top = it->size - 1, bottom = it->offset;
  int64_t i;
  t_docId did;
  while (bottom <= top) {
    i = (bottom + top) / 2;
    did = it->docIds[i];

    if (did == docId) {
      break;
    }
    if (docId < did) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
  }
  if (did < docId) did = it->docIds[++i];
  it->offset = i + 1;
  if (it->offset >= it->size) {
    setEof(it, 1);
  }

  it->base.current->docId = it->base.lastDocId = did;
  return docId == did ? ITERATOR_OK : ITERATOR_NOTFOUND;
}

/* release the iterator's context and free everything needed */
static void IL_Free(QueryIterator *self) {
  IdListIterator *it = (IdListIterator *)self;
  IndexResult_Free(it->base.current);
  if (it->docIds) {
      rm_free(it->docIds);
  }
  rm_free(self);
}
 
static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;

  return (int)(*d1 - *d2);
}

static void IL_Rewind(QueryIterator *base) {
  IdListIterator *il = (IdListIterator *)base;
  setEof(il, 0);
  il->base.lastDocId = 0;
  il->base.current->docId = 0;
  il->offset = 0;
}
 
QueryIterator *IT_V2(NewIdListIterator) (t_docId *ids, t_offset num, double weight) {
  // Assume the ids are not null and num > 0 otherwise these Iterator would not be created, avoid validation
  // first sort the ids, so the caller will not have to deal with it
  
  qsort(ids, (size_t)num, sizeof(t_docId), cmp_docids);

  IdListIterator *it = rm_new(IdListIterator);

  it->size = num;
  it->docIds = rm_calloc(num, sizeof(t_docId));
  if (num > 0) memcpy(it->docIds, ids, num * sizeof(t_docId));
  setEof(it, 0);
  it->base.current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  it->base.lastDocId = 0;

  it->offset = 0;

  QueryIterator *ret = &it->base;
  ret->type = ID_LIST_ITERATOR;
  ret->NumEstimated = IL_NumEstimated;
  ret->Free = IL_Free;
  ret->Read = IL_Read;
  ret->SkipTo = IL_SkipTo;
  ret->Rewind = IL_Rewind;

  return ret;
}