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
  VecSimQueryParams *runtimeParams;   // evaluated runtime params.
  IndexIterator *childIt;
  VecSearchMode mode;
  bool RESULTS_PREPARED;
  VecSimQueryResult_List list;
  VecSimQueryResult_Iterator *iter;
  t_docId lastDocId;
  RSIndexResult **returnedResults; // Save the pointers to be freed in clean-up.
  char *scoreField;  // to use by the sorter, for distinguishing between different vector fields.
  heap_t *topResults;  // sorted by score (max heap).
  heap_t *orderedResults;  // sorted by id (min heap) - do we need it?
} HybridIterator;

#define VECTOR_RESULT(p) p->agg.children[0]

void prepareResults(HybridIterator *hr); // forward declaration

static int cmpVecSimResByScore(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;
  double score1 = VECTOR_RESULT(e1)->dist.distance, score2 = VECTOR_RESULT(e2)->dist.distance;
  if (score1 < score2) {
    return -1;
  } else if (score1 > score2) {
    return 1;
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
static int HR_SkipToInBatch(void *ctx, t_docId docId, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  while(VecSimQueryResult_IteratorHasNext(hr->iter)) {
    VecSimQueryResult *res = VecSimQueryResult_IteratorNext(hr->iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    // Set the item that we skipped to it in hit.
    (*hit)->docId = VecSimQueryResult_GetId(res);
    (*hit)->dist.distance = VecSimQueryResult_GetScore(res);
    (*hit)->dist.scoreField = hr->scoreField;
    return INDEXREAD_OK;
  }
  if (hr->mode == STANDARD_KNN) {
    hr->base.isValid = false;
  }
  return INDEXREAD_EOF;
}

// Simulate the logic of "Read", but it is limited to the results in a specific batch.
static int HR_ReadInBatch(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (hr->mode == STANDARD_KNN && !hr->RESULTS_PREPARED) {
    prepareResults(hr);
    hr->RESULTS_PREPARED = true;
  }
  if (!VecSimQueryResult_IteratorHasNext(hr->iter)) {
    if (hr->mode == STANDARD_KNN) {
      hr->base.isValid = 0;
    }
    return INDEXREAD_EOF;
  }
  if (hr->mode == STANDARD_KNN) {
    *hit = hr->base.current;
  }
  VecSimQueryResult *res = VecSimQueryResult_IteratorNext(hr->iter);
  // Set the item that we read in the current RSIndexResult
  (*hit)->docId = VecSimQueryResult_GetId(res);
  (*hit)->dist.distance = VecSimQueryResult_GetScore(res);
  (*hit)->dist.scoreField = hr->scoreField;
  return INDEXREAD_OK;
}

static void alternatingIterate(HybridIterator *hr, VecSimQueryResult_Iterator *vecsim_iter, float *upper_bound) {
  RSIndexResult *cur_vec_res = NewDistanceResult(), *cur_child_res;
  hr->childIt->Read(hr->childIt->ctx, &cur_child_res);
  HR_ReadInBatch(hr, &cur_vec_res);
  while (hr->childIt->isValid) {
    if (cur_vec_res->docId == cur_child_res->docId) {
      // Found a match - check if it should be added to the results heap.
      if (heap_count(hr->topResults) >= hr->query.k && cur_vec_res->dist.distance >= *upper_bound) {
        // Skip the result and get the next one, since it was not better
        // than the k results that we already have.
        hr->childIt->Read(hr->childIt->ctx, &cur_child_res);
        HR_ReadInBatch(hr, &cur_vec_res);
        continue;
      }
      // Otherwise, set the vector and child results as the children
      // before insert result to the heap.
      AggregateResult_AddChild(hr->base.current, cur_vec_res);
      AggregateResult_AddChild(hr->base.current, cur_child_res);
      // todo: can we reuse memory sometimes instead of deep copying as the sorter does?
      RSIndexResult *cur_res = IndexResult_DeepCopy(hr->base.current);
      if (heap_count(hr->topResults) >= hr->query.k) {
        // Remove and release the worst result to replace it with the new one.
        RSIndexResult *top_res = heap_poll(hr->topResults);
        IndexResult_Free(top_res);
      }
      // Insert to heap, update the distance upper bound.
      heap_offerx(hr->topResults, cur_res);
      RSIndexResult *top = heap_peek(hr->topResults);
      *upper_bound = VECTOR_RESULT(top)->dist.distance;
      // Reset the current result.
      AggregateResult_Reset(hr->base.current);
      hr->childIt->Read(hr->childIt->ctx, &cur_child_res);
      HR_ReadInBatch(hr, &cur_vec_res);
    }
    // Otherwise, advance the iterator pointing to the lower id.
    else if (cur_vec_res->docId > cur_child_res->docId && hr->childIt->isValid) {
      int rc = hr->childIt->SkipTo(hr->childIt->ctx, cur_vec_res->docId, &cur_child_res);
      if (rc == INDEXREAD_NOTFOUND) {
        hr->childIt->Read(hr->childIt->ctx, &cur_child_res);
      }
    } else if (VecSimQueryResult_IteratorHasNext(vecsim_iter)){
      HR_SkipToInBatch(hr, cur_child_res->docId, &cur_vec_res);
    } else {
      break; // both iterators are depleted.
    }
  }
  IndexResult_Free(cur_vec_res);
}

void prepareResults(HybridIterator *hr) {
  if (hr->mode == STANDARD_KNN) {
    hr->list =
        VecSimIndex_TopKQuery(hr->index, hr->query.vector, hr->query.k, hr->runtimeParams, hr->query.order);
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
    size_t batch_size = hr->query.k;  // todo: add heuristics here
    if (hr->list) {
      VecSimQueryResult_Free(hr->list);
    }
    if (hr->iter) {
      VecSimQueryResult_IteratorFree(hr->iter);
    }
    // Get the next batch.
    hr->list = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
    hr->iter = VecSimQueryResult_List_GetIterator(hr->list);
    hr->childIt->Rewind(hr->childIt->ctx);

    // Go over both iterators and save mutual results in the heap.
    alternatingIterate(hr, hr->iter, &upper_bound);
    if (heap_count(hr->topResults) == hr->query.k) {
      break;
    }
  }
  VecSimBatchIterator_Free(batch_it);
}

static int HR_ReadUnsorted(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  // This callback should be called only for hybrid queries.
  RedisModule_Assert(hr->mode != STANDARD_KNN);
  if (!hr->RESULTS_PREPARED) {
    prepareResults(hr);
    hr->RESULTS_PREPARED = true;
  }
  if (!hr->base.isValid) {
    return INDEXREAD_EOF;
  }
  if (heap_count(hr->topResults) > 0) {
    hr->base.current = heap_poll(hr->topResults);
    *hit = hr->base.current;
    hr->returnedResults = array_append(hr->returnedResults, *hit);
    return INDEXREAD_OK;
  }
  hr->base.isValid = false;
  return INDEXREAD_EOF;
}

static bool UseBF(size_t T, TopKVectorQuery query, VecSimIndex *index) {
  // todo: have more sophisticated heuristics here.
  return (float)T < (0.05 * (float)VecSimIndex_IndexSize(index));
}

static size_t HR_NumEstimated(void *ctx) {
  HybridIterator *hr = ctx;
  return hr->query.k;
}

static size_t HR_Len(void *ctx) {
  HybridIterator *hr = ctx;
  if (!hr->RESULTS_PREPARED) {
    prepareResults(hr);
  }
  if (hr->mode == STANDARD_KNN) {
    return VecSimQueryResult_Len(hr->list);
  }
  return heap_count(hr->topResults);
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
  // if the child_it has at least one result.
  return hr->childIt->Len(hr->childIt) > 0;
}

void HybridIterator_Free(struct indexIterator *self) {
  HybridIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }
  rm_free(it->runtimeParams);
  while (heap_count(it->topResults) > 0) {
    IndexResult_Free(heap_poll(it->topResults));
  }
  heap_free(it->topResults);
  for (int i = 0; i < (int)array_len(it->returnedResults) - 1; i++) {
    IndexResult_Free(it->returnedResults[i]);
  }
  array_free(it->returnedResults);
  IndexResult_Free(it->base.current); // the last returned result is stored in current.
  if (it->list) VecSimQueryResult_Free(it->list);
  if (it->iter) VecSimQueryResult_IteratorFree(it->iter);
  if (it->childIt) {
    it->childIt->Free(it->childIt);
  }
  rm_free(it);
}

IndexIterator *NewHybridVectorIteratorImpl(VecSimIndex *index, char *score_field, TopKVectorQuery query, VecSimQueryParams *qParams, IndexIterator *child_it) {
  HybridIterator *hi = rm_new(HybridIterator);
  hi->lastDocId = 0;
  hi->childIt = child_it;
  hi->RESULTS_PREPARED = false;
  hi->index = index;
  hi->query = query;
  hi->runtimeParams = qParams;
  hi->scoreField = score_field;
  hi->list = NULL;
  hi->iter = NULL;
  hi->returnedResults = array_new(RSIndexResult *, query.k);

  if (child_it == NULL) {
    hi->mode = STANDARD_KNN;
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
  ri->type = HYBRID_ITERATOR;
  ri->mode = MODE_UNSORTED;
  ri->NumEstimated = HR_NumEstimated;
  ri->GetCriteriaTester = NULL; // TODO:remove from all project
  ri->LastDocId = HR_LastDocId;
  ri->Free = HybridIterator_Free;
  ri->Len = HR_Len;
  ri->Abort = HR_Abort;
  ri->Rewind = HR_Rewind;
  ri->HasNext = HR_HasNext;
  ri->SkipTo = NULL; // As long as we return results by score (unsorted by id), this has no meaning.
  if (hi->mode == STANDARD_KNN) {
    ri->current = NewDistanceResult();
    ri->Read = HR_ReadInBatch;
  } else {
    ri->current = NewHybridResult();
    ri->Read = HR_ReadUnsorted;
  }
  return ri;
}
