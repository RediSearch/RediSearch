#include "list_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

typedef struct {
  IndexIterator base;
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  t_offset size;
} ListIterator;

static inline void setEof(ListIterator *it, int value) {
  it->base.isValid = !value;
}

static inline int isEof(const ListIterator *it) {
  return !it->base.isValid;
}

static int LR_Read(void *ctx, RSIndexResult **hit) {
  ListIterator *lr = ctx;
  if (isEof(lr) || !VecSimQueryResult_IteratorHasNext(lr->iter)) {
    setEof(lr, 1);
    return INDEXREAD_EOF;
  }

  VecSimQueryResult *res = VecSimQueryResult_IteratorNext(lr->iter);
  lr->base.current->docId = lr->lastDocId = VecSimQueryResult_GetId(res);
  // save distance on RSIndexResult
  lr->base.current->num.value = VecSimQueryResult_GetScore(res);
  *hit = lr->base.current;

  return INDEXREAD_OK;
}

static int LR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  ListIterator *lr = ctx;
  while(VecSimQueryResult_IteratorHasNext(lr->iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(lr->iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    lr->base.current->docId = id;
    lr->lastDocId = id;
    lr->base.current->num.value = VecSimQueryResult_GetScore(res);
    *hit = lr->base.current;

    return INDEXREAD_OK;
  }
  setEof(lr, 1);
  return INDEXREAD_EOF;
}

void ListIterator_Free(struct indexIterator *self) {
  ListIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }

  IndexResult_Free(it->base.current);
  // free iterator
  if (it->iter) {
    VecSimQueryResult_IteratorFree(it->iter);
  }
  if (it->list) {
    VecSimQueryResult_Free(it->list);
  }
  rm_free(it);
}

static size_t LR_NumEstimated(void *ctx) {
  ListIterator *lr = ctx;
  return VecSimQueryResult_Len(lr->list);
}

static size_t LR_LastDocId(void *ctx) {
  ListIterator *lr = ctx;
  return lr->lastDocId;
}

static size_t LR_NumDocs(void *ctx) {
  ListIterator *lr = ctx;
  return VecSimQueryResult_Len(lr->list);
}

static void LR_Abort(void *ctx) {
  ListIterator *lr = ctx;
  setEof(lr, 1);
}

static void LR_Rewind(void *ctx) {
  ListIterator *lr = ctx;
  VecSimQueryResult_IteratorFree(lr->iter);
  lr->iter = VecSimQueryResult_List_GetIterator(lr->list);
  lr->lastDocId = 0;
  setEof(lr, 0);
}

static int LR_HasNext(void *ctx) {
  ListIterator *lr = ctx;
  return VecSimQueryResult_IteratorHasNext(lr->iter);
}

IndexIterator *NewListIterator(void *list, size_t len) {
  ListIterator *li = rm_malloc(sizeof(*li));
  li->lastDocId = 0;
  li->size = len;
  li->list = list;
  li->iter = VecSimQueryResult_List_GetIterator(li->list);
  li->base.isValid = 1;

  IndexIterator *ri = &li->base;
  ri->ctx = li;
  ri->mode = MODE_SORTED;
  ri->type = LIST_ITERATOR;
  ri->NumEstimated = LR_NumEstimated;
  ri->GetCriteriaTester = NULL; // TODO:remove from all project
  ri->Read = LR_Read;
  ri->SkipTo = LR_SkipTo;
  ri->LastDocId = LR_LastDocId;
  ri->Free = ListIterator_Free;
  ri->Len = LR_NumDocs;
  ri->Abort = LR_Abort;
  ri->Rewind = LR_Rewind;
  ri->HasNext = LR_HasNext;
  ri->current = NewDistanceResult();

  return ri;
}
