#include <math.h>
#include "hybrid_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

#define VECTOR_RESULT(p) p->agg.children[0]

static void prepareResults(HybridIterator *hr); // forward declaration

static int cmpVecSimResByScore(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;
  double score1 = VECTOR_RESULT(e1)->dist.distance, score2 = VECTOR_RESULT(e2)->dist.distance;
  if (score1 < score2) {
    return -1;
  } else if (score1 > score2) {
    return 1;
  }
  return e1->docId < e2->docId;
}

// To use in the future, if we will need to sort results by id.
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
    (*hit)->docId = id;
    (*hit)->dist.distance = VecSimQueryResult_GetScore(res);
    (*hit)->dist.scoreField = hr->scoreField;
    return INDEXREAD_OK;
  }
  return INDEXREAD_EOF;
}

// Simulate the logic of "Read", but it is limited to the results in a specific batch.
static int HR_ReadInBatch(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!VecSimQueryResult_IteratorHasNext(hr->iter)) {
    return INDEXREAD_EOF;
  }
  VecSimQueryResult *res = VecSimQueryResult_IteratorNext(hr->iter);
  // Set the item that we read in the current RSIndexResult
  (*hit)->docId = VecSimQueryResult_GetId(res);
  (*hit)->dist.distance = VecSimQueryResult_GetScore(res);
  (*hit)->dist.scoreField = hr->scoreField;
  return INDEXREAD_OK;
}

static void insertResultToHeap(HybridIterator *hr, RSIndexResult *res, RSIndexResult *child_res,
                               RSIndexResult *vec_res, float *upper_bound) {
  AggregateResult_AddChild(res, vec_res);
  AggregateResult_AddChild(res, child_res);
  // todo: can we avoid deep copy and reuse memory sometimes (as the sorter does)?
  RSIndexResult *hit = IndexResult_DeepCopy(res);
  if (heap_count(hr->topResults) >= hr->query.k) {
    // Remove and release the worst result to replace it with the new one.
    RSIndexResult *top_res = heap_poll(hr->topResults);
    IndexResult_Free(top_res);
  }
  // Insert to heap, update the distance upper bound.
  heap_offerx(hr->topResults, hit);
  RSIndexResult *top = heap_peek(hr->topResults);
  *upper_bound = VECTOR_RESULT(top)->dist.distance;
  // Reset the current result and advance both "sub-iterators".
  AggregateResult_Reset(res);
}

static void alternatingIterate(HybridIterator *hr, VecSimQueryResult_Iterator *vecsim_iter, float *upper_bound) {
  RSIndexResult *cur_vec_res = NewDistanceResult();
  RSIndexResult *cur_child_res;  // This will use the memory of hr->child->current.
  RSIndexResult *cur_res = hr->base.current;

  hr->child->Read(hr->child->ctx, &cur_child_res);
  HR_ReadInBatch(hr, &cur_vec_res);
  while (IITER_HAS_NEXT(hr->child)) {
    if (cur_vec_res->docId == cur_child_res->docId) {
      // Found a match - check if it should be added to the results heap.
      if (heap_count(hr->topResults) < hr->query.k || cur_vec_res->dist.distance < *upper_bound) {
        // Otherwise, set the vector and child results as the children the res
        // and insert result to the heap.
        insertResultToHeap(hr, cur_res, cur_child_res, cur_vec_res, upper_bound);
      }
      int ret_child = hr->child->Read(hr->child->ctx, &cur_child_res);
      int ret_vec = HR_ReadInBatch(hr, &cur_vec_res);
      if (ret_child != INDEXREAD_OK || ret_vec != INDEXREAD_OK) break;
    }
    // Otherwise, advance the iterator pointing to the lower id.
    else if (cur_vec_res->docId > cur_child_res->docId && IITER_HAS_NEXT(hr->child)) {
      int rc = hr->child->SkipTo(hr->child->ctx, cur_vec_res->docId, &cur_child_res);
      if (rc == INDEXREAD_EOF) break; // no more results left.
      // It may be the case where we skipped to an invalid result (in NOT iterator for example),
      // so we read to get the next valid result.
      // If we passed cur_vec_res->docId, we found the next valid id of the child.
      if (rc == INDEXREAD_NOTFOUND && cur_child_res->docId <= cur_vec_res->docId) {
        rc = HR_ReadInBatch(hr, &cur_vec_res);
        if (rc == INDEXREAD_EOF) break;
      }
    } else if (VecSimQueryResult_IteratorHasNext(vecsim_iter)){
      int rc = HR_SkipToInBatch(hr, cur_child_res->docId, &cur_vec_res);
      if (rc == INDEXREAD_EOF) break;
    } else {
      break; // both iterators are depleted.
    }
  }
  IndexResult_Free(cur_vec_res);
}

void computeDistances(HybridIterator *hr) {
  float upper_bound = INFINITY;
  RSIndexResult *cur_res = hr->base.current;
  RSIndexResult *cur_child_res;  // This will use the memory of hr->child->current.
  RSIndexResult *cur_vec_res = NewDistanceResult();

  while (hr->child->Read(hr->child->ctx, &cur_child_res) != INDEXREAD_EOF) {
    float dist = (float)VecSimIndex_GetDistanceFrom(hr->index, cur_child_res->docId, hr->query.vector);
    if (heap_count(hr->topResults) < hr->query.k || dist < upper_bound) {
      // Populate the vector result.
      cur_vec_res->docId = cur_child_res->docId;
      cur_vec_res->dist.distance = dist;
      cur_vec_res->dist.scoreField = hr->scoreField;
      insertResultToHeap(hr, cur_res, cur_child_res, cur_vec_res, &upper_bound);
    }
  }
}

static void prepareResults(HybridIterator *hr) {
    if (hr->mode == STANDARD_KNN) {
      hr->list =
          VecSimIndex_TopKQuery(hr->index, hr->query.vector, hr->query.k, &(hr->runtimeParams), hr->query.order);
      hr->iter = VecSimQueryResult_List_GetIterator(hr->list);
    return;
  }

  if (hr->mode == HYBRID_ADHOC_BF) {
    // Go over child_it results, compute distances, sort and store results in topResults.
    computeDistances(hr);
    return;
  }
  // Batches mode.
  VecSimBatchIterator *batch_it = VecSimBatchIterator_New(hr->index, hr->query.vector);
  float upper_bound = INFINITY;
  while (VecSimBatchIterator_HasNext(batch_it)) {
    hr->numIterations++;
    size_t vec_index_size = VecSimIndex_IndexSize(hr->index);
    size_t n_res_left = hr->query.k - heap_count(hr->topResults);
    size_t batch_size = n_res_left * ((float)vec_index_size / hr->child->NumEstimated(hr->child->ctx));
    if (hr->list) {
      VecSimQueryResult_Free(hr->list);
    }
    if (hr->iter) {
      VecSimQueryResult_IteratorFree(hr->iter);
    }
    // Get the next batch.
    hr->list = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
    hr->iter = VecSimQueryResult_List_GetIterator(hr->list);
    hr->child->Rewind(hr->child->ctx);

    // Go over both iterators and save mutual results in the heap.
    alternatingIterate(hr, hr->iter, &upper_bound);
    if (heap_count(hr->topResults) == hr->query.k) {
      break;
    }
  }
  VecSimBatchIterator_Free(batch_it);
}

static int HR_HasNext(void *ctx) {
  HybridIterator *hr = ctx;
  return hr->base.isValid;
}

// In KNN mode, the results will return sorted by ascending order of the distance
// (better score first), while in hybrid mode, the results will return in descending order.
static int HR_ReadHybridUnsorted(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!hr->resultsPrepared) {
    prepareResults(hr);
    hr->resultsPrepared = true;
  }
  if (!HR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  if (heap_count(hr->topResults) == 0) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  *hit = heap_poll(hr->topResults);
  hr->returnedResults = array_append(hr->returnedResults, *hit);
  hr->lastDocId = (*hit)->docId;
  return INDEXREAD_OK;
}

static int HR_ReadKnnUnsorted(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!hr->resultsPrepared) {
    prepareResults(hr);
    hr->resultsPrepared = true;
  }
  if (!HR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  *hit = hr->base.current;
  if (HR_ReadInBatch(hr, hit) == INDEXREAD_EOF) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  hr->lastDocId = (*hit)->docId;
  return INDEXREAD_OK;
}

static bool UseBF(size_t T, KNNVectorQuery query, VecSimIndex *index) {
  // todo: have more sophisticated heuristics here...
  //return (float)T <= (0.05 * (float)VecSimIndex_IndexSize(index));
  return false;
}

static size_t HR_NumEstimated(void *ctx) {
  HybridIterator *hr = ctx;
  size_t vec_res_num = MIN(hr->query.k, VecSimIndex_IndexSize(hr->index));
  if (hr->child == NULL) return vec_res_num;
  return MIN(vec_res_num, hr->child->NumEstimated(hr->child->ctx));
}

static size_t HR_Len(void *ctx) {
  return HR_NumEstimated(ctx);
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
  hr->resultsPrepared = false;
  hr->numIterations = 0;
  if (hr->list) {
    VecSimQueryResult_Free(hr->list);
    hr->list = NULL;
  }
  if (hr->iter) {
    VecSimQueryResult_IteratorFree(hr->iter);
    hr->iter = NULL;
  }
  hr->lastDocId = 0;
  hr->base.isValid = 1;

  if (hr->mode != STANDARD_KNN) {
    // Clean the saved and returned results (in case of HYBRID mode).
    while (heap_count(hr->topResults) > 0) {
      IndexResult_Free(heap_poll(hr->topResults));
    }
    for (size_t i = 0; i < array_len(hr->returnedResults); i++) {
      IndexResult_Free(hr->returnedResults[i]);
    }
    array_clear(hr->returnedResults);
    hr->child->Rewind(hr->child->ctx);
  }
}

void HybridIterator_Free(struct indexIterator *self) {
  HybridIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }
  if (it->mode != STANDARD_KNN) {
    while (heap_count(it->topResults) > 0) {
      IndexResult_Free(heap_poll(it->topResults));
    }
    heap_free(it->topResults);
    for (int i = 0; i < (int)array_len(it->returnedResults); i++) {
      IndexResult_Free(it->returnedResults[i]);
    }
    array_free(it->returnedResults);
  }
  IndexResult_Free(it->base.current);
  if (it->list) VecSimQueryResult_Free(it->list);
  if (it->iter) VecSimQueryResult_IteratorFree(it->iter);
  if (it->child) {
    it->child->Free(it->child);
  }
  rm_free(it);
}

IndexIterator *NewHybridVectorIterator(VecSimIndex *index, char *score_field, KNNVectorQuery query, VecSimQueryParams qParams, IndexIterator *child_it) {
  HybridIterator *hi = rm_new(HybridIterator);
  hi->lastDocId = 0;
  hi->child = child_it;
  hi->resultsPrepared = false;
  hi->index = index;
  hi->query = query;
  hi->runtimeParams = qParams;
  hi->scoreField = score_field;
  hi->base.isValid = 1;
  hi->list = NULL;
  hi->iter = NULL;
  hi->numIterations = 0;

  if (child_it == NULL) {
    hi->mode = STANDARD_KNN;
  } else if (UseBF(child_it->NumEstimated(child_it->ctx), query, index)) {
    hi->mode = HYBRID_ADHOC_BF;
  } else {
    hi->mode = HYBRID_BATCHES;
    //Todo: apply heuristics (batch_size = k / (vec_index_size*child_it->NumEstimated(child_it)))
  }
  if (hi->mode != STANDARD_KNN) {
    hi->topResults = rm_malloc(heap_sizeof(query.k));
    heap_init(hi->topResults, cmpVecSimResByScore, NULL, query.k);
    hi->returnedResults = array_new(RSIndexResult *, query.k);
  }

  IndexIterator *ri = &hi->base;
  ri->ctx = hi;
  ri->type = HYBRID_ITERATOR;
  ri->mode = MODE_SORTED;  // Since this iterator is always the root, we currently don't return the
                           // results sorted by id as an optimization (this can be modified in the future).
  ri->NumEstimated = HR_NumEstimated;
  ri->GetCriteriaTester = NULL; // TODO:remove from all project
  ri->LastDocId = HR_LastDocId;
  ri->Free = HybridIterator_Free;
  ri->Len = HR_Len;            // Not clear what is the definition of this, currently returns
  ri->Abort = HR_Abort;
  ri->Rewind = HR_Rewind;
  ri->HasNext = HR_HasNext;
  ri->SkipTo = NULL; // As long as we return results by score (unsorted by id), this has no meaning.
  if (hi->mode == STANDARD_KNN) {
    ri->Read = HR_ReadKnnUnsorted;
    ri->current = NewDistanceResult();
  } else {
    ri->Read = HR_ReadHybridUnsorted;
    ri->current = NewHybridResult();
  }
  return ri;
}
