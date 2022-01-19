#include "hybrid_reader.h"
#include "util/heap.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

typedef enum {
  STANDARD_KNN,
  HYBRID_ADHOC_BF,
  HYBRID_BATCHES
} VecSearchMode;

typedef struct {
  IndexIterator base;
  VecSimIndex *index;
  TopKVectorQuery query;
  IndexIterator *childIt;
  VecSearchMode mode;
  bool RESULTS_PREPARED;
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  size_t returnedResCount;
  heap_t *topResults;  // sorted by score (max heap).
  heap_t *orderedResults;  // sorted by id (min heap) - do we need it?
} HybridIterator;

static int cmpVecSimResByScore(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;

  if (e1->num.value < e2->num.value) {
    return 1;
  } else if (e1->num.value > e2->num.value) {
    return -1;
  }
  return 0;
}

static int cmpVecSimResById(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;

  if (e1->docId > e2->docId) {
    return 1;
  } else if (e1->docId < e2->docId) {
    return -1;
  }
  return 0;
}

// Simulate the logic of "SkipTo", but it is limited to the results in a specific batch.
static int HR_SkipToInBatch(VecSimQueryResult_Iterator *iter, t_docId docId, RSIndexResult *hit) {
  while(VecSimQueryResult_IteratorHasNext(iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    hit->docId = id;
    hit->num.value = VecSimQueryResult_GetScore(res);
    return INDEXREAD_OK;
  }
  return INDEXREAD_EOF;
}


static void prepareResults(HybridIterator *hr) {
  if (hr->mode == STANDARD_KNN) {
    hr->list =
        VecSimIndex_TopKQuery(hr->index, hr->query.vector, hr->query.k, NULL, hr->query.order);
    hr->iter = VecSimQueryResult_List_GetIterator(hr->list);
    return;
  }

  if (hr->mode == HYBRID_ADHOC_BF) {
    // Go over child_it results, compute distances, sort and store results in topResults.
    return;
  }
  // Batch mode
  VecSimBatchIterator *batch_it = VecSimBatchIterator_New(hr->index, hr->query.vector);
  float upper_bound = INFINITY;
  while (VecSimBatchIterator_HasNext(batch_it)) {
    size_t batch_size = hr->query.k;  // add heuristics here
    hr->lastDocId = 0;
    VecSimQueryResult_List next_batch = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
    VecSimQueryResult_Iterator *iter = VecSimQueryResult_List_GetIterator(next_batch);

    // Go over both iterators.
    hr->childIt->Rewind(hr->childIt);
    RSIndexResult *cur_res;
    while (hr->childIt->isValid && VecSimQueryResult_IteratorHasNext(iter)) {
      // found a match
      if (hr->lastDocId == hr->childIt->current->docId) {
        if (heap_count(hr->topResults) < hr->query.k) {
          // todo: should we allocate the res?
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
        HR_SkipToInBatch(iter, hr->childIt->current->docId, cur_res);
        hr->lastDocId = cur_res->docId;
      } else {
        hr->childIt->SkipTo(hr->childIt, hr->lastDocId, &cur_res);
      }
    }
    if (heap_count(hr->topResults) == hr->query.k) {
      break;
    }
  }
}

static int HR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  while(heap_count(hr->orderedResults) > 0) {
    RSIndexResult *res = heap_poll(hr->orderedResults);
    t_docId id = res->docId;
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    hr->lastDocId = id;
    hr->base.current = res;
    *hit = hr->base.current;
    return INDEXREAD_OK;
  }
  hr->base.isValid = 0;
  return INDEXREAD_EOF;
}

static int HR_Read(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!hr->RESULTS_PREPARED) {
    prepareResults(hr);
    hr->RESULTS_PREPARED = true;
  }
  if (!hr->base.isValid) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  if (hr->mode == HYBRID_BATCHES || hr->mode == HYBRID_ADHOC_BF) {
    if (heap_count(hr->topResults) > 0) {
      hr->base.current = heap_poll(hr->topResults);
      *hit = hr->base.current;
      return INDEXREAD_OK;
    }
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  if (!VecSimQueryResult_IteratorHasNext(hr->iter)) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  VecSimQueryResult *res = VecSimQueryResult_IteratorNext(hr->iter);
  hr->base.current->docId = hr->lastDocId = VecSimQueryResult_GetId(res);
  // save distance on RSIndexResult
  hr->base.current->num.value = VecSimQueryResult_GetScore(res);
  *hit = hr->base.current;
  return INDEXREAD_OK;
}


static bool UseBF(size_t T, TopKVectorQuery query, VecSimIndex *index) {
  return (float)T < (0.05 * (float)VecSimIndex_IndexSize(index));
}

static size_t HR_NumEstimated(void *ctx) {
  HybridIterator *hr = ctx;
  return hr->query.k;
}

static size_t HR_Len(void *ctx) {
  HybridIterator *hr = ctx;
  return heap_count(hr->orderedResults);
}

static void HR_Abort(void *ctx) {
  HybridIterator *hr = ctx;
  hr->base.isValid = 0;
}

static size_t HR_LastDocId(void *ctx) {
  HybridIterator *hr = ctx;
  return hr->lastDocId;
}

static void HR_Rewind(void *ctx) {
  HybridIterator *hr = ctx;
  hr->RESULTS_PREPARED = false;
  hr->lastDocId = 0;
  hr->base.isValid = 1;
}

static int HR_HasNext(void *ctx) {
  HybridIterator *hr = ctx;
  if (!hr->base.isValid) return false;
  if (hr->mode == STANDARD_KNN && hr->RESULTS_PREPARED) {
    return VecSimQueryResult_IteratorHasNext(hr->iter);
  }
  if (hr->RESULTS_PREPARED) {
    return heap_count(hr->topResults) > 0;
  }
  // Otherwise, this is called before we prepared the results. There is at least one result
  // if the child_it at least one result.
  return hr->childIt->Len(hr->childIt) > 0;
}

void HybridIterator_Free(struct indexIterator *self) {
  HybridIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }
  IndexResult_Free(it->base.current);
  if (it->mode == HYBRID_ADHOC_BF || it->mode == HYBRID_BATCHES) {
    heap_free(it->topResults);
  } else if (it->mode == STANDARD_KNN) {
    if (it->list) VecSimQueryResult_Free(it->list);
    if (it->iter) VecSimQueryResult_IteratorFree(it->iter);
  } else {
    RedisModule_Assert(false); // Error
  }
  rm_free(it);
}

IndexIterator *NewHybridVectorIteratorImpl(VecSimIndex *index, TopKVectorQuery query, IndexIterator *child_it) {
  HybridIterator *hi = rm_new(HybridIterator);
  hi->lastDocId = 0;
  hi->childIt = child_it;
  hi->RESULTS_PREPARED = false;
  hi->index = index;
  hi->query = query;

  //Todo: apply heuristics for BF mode
  if (child_it == NULL) {
    hi->mode = STANDARD_KNN;
    hi->list = NULL;
    hi->iter = NULL;
  } else if (UseBF(child_it->NumEstimated(child_it), query, index)) {
    hi->mode = HYBRID_ADHOC_BF;
  } else {
    hi->mode = HYBRID_BATCHES;
    //Todo: apply heuristics (batch_size = k / (vec_index_size*child_it->NumEstimated(child_it)))
  }
  hi->base.isValid = 1;
  hi->topResults = rm_malloc(heap_sizeof(query.k));
  heap_init(hi->topResults, cmpVecSimResByScore, NULL, query.k);

  IndexIterator *ri = &hi->base;
  ri->ctx = hi;
  ri->mode = MODE_SORTED;
  ri->type = HYBRID_ITERATOR;
  ri->NumEstimated = HR_NumEstimated;
  ri->GetCriteriaTester = NULL; // TODO:remove from all project
  ri->Read = HR_Read;
  //ri->SkipTo = HR_SkipTo;
  ri->SkipTo = NULL; // As long as we return results by score, this has no meaning.
  ri->LastDocId = HR_LastDocId;
  ri->Free = HybridIterator_Free;
  ri->Len = HR_Len;
  ri->Abort = HR_Abort;
  ri->Rewind = HR_Rewind;
  ri->HasNext = HR_HasNext;
  ri->current = NewDistanceResult();

  return ri;
}
