/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"

typedef struct {
  IndexIterator base;
  t_docId *docIds;
  t_offset size;
  t_offset offset;
} IdListIterator;

static inline void setEof(IdListIterator *it, int value) {
  it->base.isValid = !value;
}

static inline int isEof(const IdListIterator *it) {
  return !it->base.isValid;
}

size_t IL_NumEstimated(IndexIterator *base) {
  IdListIterator *it = (IdListIterator *)base;
  return (size_t)it->size;
}

/* Read the next entry from the iterator, into hit *e.
 *  Returns INDEXREAD_EOF if at the end */
int IL_Read(IndexIterator *base, RSIndexResult **r) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(it) || it->offset >= it->size) {
    setEof(it, 1);
    return INDEXREAD_EOF;
  }

  base->LastDocId = it->docIds[it->offset++];

  // TODO: Filter here
  it->base.current->docId = base->LastDocId;
  *r = it->base.current;
  return INDEXREAD_OK;
}

void IL_Abort(IndexIterator *base) {
  IITER_SET_EOF(base);
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int IL_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **r) {
  IdListIterator *it = (IdListIterator *)base;
  if (isEof(it) || it->offset >= it->size) {
    return INDEXREAD_EOF;
  }

  if (docId > it->docIds[it->size - 1]) {
    it->base.isValid = 0;
    return INDEXREAD_EOF;
  }

  t_offset top = it->size - 1, bottom = it->offset;
  t_offset i = 0;

  while (bottom <= top) {
    i = (bottom + top) / 2;
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
  }
  it->offset = i + 1;
  if (it->offset >= it->size) {
    setEof(it, 1);
  }

  it->base.current->docId = base->LastDocId = it->docIds[i];
  *r = it->base.current;

  return docId == it->docIds[i] ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}

/* release the iterator's context and free everything needed */
void IL_Free(IndexIterator *self) {
  IdListIterator *it = (IdListIterator *)self;
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

void IL_Rewind(IndexIterator *base) {
  IdListIterator *il = (IdListIterator *)base;
  setEof(il, 0);
  base->LastDocId = 0;
  il->base.current->docId = 0;
  il->offset = 0;
}

IndexIterator *NewIdListIterator(t_docId *ids, t_offset num, double weight) {
  IdListIterator *it = rm_new(IdListIterator);

  it->size = num;
  it->docIds = ids;
  setEof(it, 0);
  it->base.current = NewVirtualResult(weight, RS_FIELDMASK_ALL);

  it->offset = 0;

  IndexIterator *ret = &it->base;
  ret->type = ID_LIST_ITERATOR;
  ret->LastDocId = 0;
  ret->NumEstimated = IL_NumEstimated;
  ret->Free = IL_Free;
  ret->Read = IL_Read;
  ret->SkipTo = IL_SkipTo;
  ret->Abort = IL_Abort;
  ret->Rewind = IL_Rewind;

  ret->HasNext = NULL;
  return ret;
}
