#include "list_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"
#include "VecSim/query_result_struct.h"
#include "util/heap.h"

typedef struct {
  IndexIterator base;
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  t_offset size;
} ListIterator;

typedef struct {
  IndexIterator base;
  IndexIterator *childIt;
  bool BF_MODE;
  VecSimBatchIterator *batchIterator;
  size_t vecIndexSize;
  size_t k;
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  t_offset listSize;
  size_t returnedResCount;
  heap_t *topResults;
} HybridIterator;

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
  ListIterator *li = rm_new(ListIterator);
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

static int cmpVecSimRes(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;

  if (e1->num.value < e2->num.value) {
    return 1;
  } else if (e1->num.value > e2->num.value) {
    return -1;
  }
  return 0;
}

// todo: decide how to handle SkipTo... rewind the child it?
static int HR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  while(VecSimQueryResult_IteratorHasNext(hr->iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(hr->iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    hr->base.current->docId = id;
    hr->lastDocId = id;
    hr->base.current->num.value = VecSimQueryResult_GetScore(res);
    *hit = hr->base.current;
    return INDEXREAD_OK;
  }
  hr->base.isValid = 0;
  return INDEXREAD_EOF;
}

static int HR_Read(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  // Assuming the results for BF_MODE are stored upon initializing the iterator.
  if (!hr->base.isValid || (!VecSimQueryResult_IteratorHasNext(hr->iter) && hr->BF_MODE)) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  // Batch mode
  if (heap_size(hr->topResults) > 0) {
    *hit = heap_poll(hr->topResults);
    hr->returnedResCount++;
    return INDEXREAD_OK;
    // Try to get another batch
  } else if (!VecSimBatchIterator_HasNext(hr->batchIterator) || hr->returnedResCount == hr->k) {
      hr->base.isValid = false;
      return INDEXREAD_EOF;
  }
  // Reset the result list and iterator with the next batch.
  if (hr->iter) {
    VecSimQueryResult_IteratorFree(hr->iter);
  }
  if (hr->list) {
    VecSimQueryResult_Free(hr->list);
  }
  VecSimQueryResult_List next_batch = VecSimBatchIterator_Next(hr->batchIterator, hr->k, BY_ID);
  hr->list = next_batch;
  VecSimQueryResult_Iterator *iter = VecSimQueryResult_List_GetIterator(next_batch);
  hr->iter = iter;

  // Initialize heap with more results, maximum size is the number of results left to return.
  unsigned int heap_size = hr->k - hr->returnedResCount;
  float upper_bound = INFINITY;

  // Go over both iterators.
  hr->childIt->Rewind(hr->childIt);
  RSIndexResult *cur_res;
  while (hr->childIt->isValid && hr->base.isValid) {
    // found a match
    if (hr->lastDocId == hr->childIt->current->docId) {
      if (heap_count(hr->topResults) < heap_size) {
        // insert to heap
        heap_offerx(hr->topResults, cur_res);
        RSIndexResult *top = heap_peek(hr->topResults);
        upper_bound = (float)top->num.value;
      } else if (hr->base.current->num.value < upper_bound) {
        // replace with the worst candidate.
        heap_replace(hr->topResults, cur_res);
        RSIndexResult *top = heap_peek(hr->topResults);
        upper_bound = (float)top->num.value;
      }
      hr->childIt->Read(hr, &cur_res);
      // Otherwise, advance one of the iterators
    } else if (hr->lastDocId < hr->childIt->current->docId) {
      hr->base.SkipTo(hr, hr->childIt->current->docId, &cur_res);
    } else {
      hr->childIt->SkipTo(hr->childIt, hr->lastDocId, &cur_res);
    }
  }
  // Return some result from the heap
  if (heap_count(hr->topResults) == 0) {
    hr->base.isValid = 0;
    return INDEXREAD_EOF;
  }
  *hit = heap_poll(hr->topResults);
  return INDEXREAD_OK;
}

static bool UseBF(size_t T, size_t vec_index_size) {
  return (float)T < (0.05 * (float)vec_index_size);
}

IndexIterator *NewHybridVectorIteratorImpl(VecSimBatchIterator *batch_it, size_t vec_index_size, size_t k, IndexIterator *child_it) {
  HybridIterator *hi = rm_new(HybridIterator);
  hi->lastDocId = 0;

  //Todo: apply heuristics for BF mode
  if (UseBF(child_it->NumEstimated(child_it), vec_index_size)) {
    hi->BF_MODE = true;
    // Go over child_it results, compute distances, sort and store results in hi->list
    // can we do it here, or should it happen in the first call to "read"?
  } else {
    hi->batchIterator = batch_it;
    //Todo: apply heuristics (batch_size = k / (vec_index_size*child_it->NumEstimated(child_it)))
    size_t batch_size = k;
    hi->list = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
  }
  hi->listSize = VecSimQueryResult_Len(hi->list);
  hi->iter = VecSimQueryResult_List_GetIterator(hi->list);
  hi->base.isValid = 1;
  hi->topResults = rm_malloc(heap_sizeof(k));
  heap_init(hi->topResults, cmpVecSimRes, NULL, k);

  IndexIterator *ri = &hi->base;
  ri->ctx = hi;
  ri->mode = MODE_UNSORTED;
  ri->type = HYBRID_ITERATOR;
  ri->NumEstimated = HR_NumEstimated;
  ri->GetCriteriaTester = NULL; // TODO:remove from all project
  ri->Read = HR_Read;
  ri->SkipTo = HR_SkipTo;
  ri->LastDocId = HR_LastDocId;
  ri->Free = HybridIterator_Free;
  ri->Len = HR_NumDocs;
  ri->Abort = HR_Abort;
  ri->Rewind = HR_Rewind;
  ri->HasNext = HR_HasNext;
  ri->current = NewDistanceResult();

  return ri;
}
