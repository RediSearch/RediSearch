#include "list_reader.h"
#include "VecSim/vecsim.h"

typedef struct {
  IndexIterator base;
  VecSimQueryResult *list; // TODO: make generic
  t_docId lastDocId;
  t_offset size;
  t_offset offset;
} ListIterator;

static inline void setEof(ListIterator *it, int value) {
  it->base.isValid = !value;
}

static inline int isEof(const ListIterator *it) {
  return !it->base.isValid;
}

static int LR_Read(void *ctx, RSIndexResult **hit) {
  ListIterator *lr = ctx;
  if (isEof(lr) || lr->offset >= lr->size) {
    setEof(lr, 1);
    return INDEXREAD_EOF;
  }

  lr->base.current->docId = lr->lastDocId = lr->list[lr->offset].id;
  // save distance on RSIndexResult
  lr->base.current->num.value = lr->list[lr->offset].score;// ? 1 / lr->list[lr->offset].score : 1;
  *hit = lr->base.current;
  ++lr->offset;

  return INDEXREAD_OK;
}

static int LR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  ListIterator *lr = ctx;
  while(lr->offset < lr->size) {
    if (docId < lr->list[lr->offset].id) {
      ++lr->offset; // consider binary search for next value
      continue;
    }

    lr->base.current->docId = lr->lastDocId = lr->list[lr->offset].id;
    lr->base.current->num.value = lr->list[lr->offset].score;
    *hit = lr->base.current;
    ++lr->offset;

    return INDEXREAD_OK;
  }
  return INDEXREAD_EOF;
}

void ListIterator_Free(struct indexIterator *self) {
  ListIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }

  IndexResult_Free(it->base.current);
  if (it->list) {
    VecSimQueryResult_Free(it->list);
  }
  rm_free(it);
}

static size_t LR_NumEstimated(void *ctx) {
  ListIterator *lr = ctx;
  return lr->size;
}

static size_t LR_LastDocId(void *ctx) {
  ListIterator *lr = ctx;
  return lr->list[lr->size - 1].id;
}

static size_t LR_NumDocs(void *ctx) {
  ListIterator *lr = ctx;
  return lr->size;
}

static void LR_Abort(void *ctx) {
  ListIterator *lr = ctx;
  lr->offset = lr->size;
}

static void LR_Rewind(void *ctx) {
  ListIterator *lr = ctx;
  lr->offset = 0;
}

static int LR_HasNext(void *ctx) {
  ListIterator *lr = ctx;
  return lr->offset < lr->size;
}

IndexIterator *NewListIterator(void *list, size_t len) {

  ListIterator *li = rm_malloc(sizeof(*li));
  li->lastDocId = 0;
  li->offset = 0;
  li->size = len;
  li->list = list;
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
