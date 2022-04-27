#include <math.h>
#include "hybrid_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

#define VECTOR_RESULT(p) (p->type == RSResultType_Distance ? p : p->agg.children[0])

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

  RSIndexResult *hit;
  // If we ignore the document score, hit is single node of type DISTANCE.
  if (hr->ignoreScores) {
    if (heap_count(hr->topResults) < hr->query.k) {
      hit = NewDistanceResult();
    } else {
      hit = heap_poll(hr->topResults); // Reuse the memory of the worst result and replace it.
    }
    *hit = *vec_res; // Shallow copy.
  } else {
    // Otherwise, first child is the vector distance, and the second contains a subtree with
    // the terms that the scorer will use later on in the pipeline.
    AggregateResult_AddChild(res, vec_res);
    AggregateResult_AddChild(res, child_res);
    hit = IndexResult_DeepCopy(res);
    if (heap_count(hr->topResults) >= hr->query.k) {
      // Remove and release the worst result to replace it with the new one.
      RSIndexResult *top_res = heap_poll(hr->topResults);
      IndexResult_Free(top_res);
    }
  }
  // Insert to heap, update the distance upper bound.
  heap_offerx(hr->topResults, hit);
  RSIndexResult *top = heap_peek(hr->topResults);
  *upper_bound = VECTOR_RESULT(top)->dist.distance;
  // Reset the current result.
  AggregateResult_Reset(res);
}

static void alternatingIterate(HybridIterator *hr, VecSimQueryResult_Iterator *vecsim_iter,
                               float *upper_bound) {
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
  void *qvector = hr->query.vector;

  if (hr->indexMetric == VecSimMetric_Cosine) {
    qvector = rm_malloc(hr->dimension * VecSimType_sizeof(hr->vecType));
    memcpy(qvector, hr->query.vector, hr->dimension * VecSimType_sizeof(hr->vecType));
    VecSim_Normalize(qvector, hr->dimension, hr->vecType);
  }

  while (hr->child->Read(hr->child->ctx, &cur_child_res) != INDEXREAD_EOF) {
    float dist = (float)VecSimIndex_GetDistanceFrom(hr->index, cur_child_res->docId, qvector);
    // If this id is not in the vector index (since it was deleted), dist will return as NaN.
    if (isnanf(dist)) {
      continue;
    }
    if (heap_count(hr->topResults) < hr->query.k || dist < upper_bound) {
      // Populate the vector result.
      cur_vec_res->docId = cur_child_res->docId;
      cur_vec_res->dist.distance = dist;
      cur_vec_res->dist.scoreField = hr->scoreField;
      insertResultToHeap(hr, cur_res, cur_child_res, cur_vec_res, &upper_bound);
    }
  }
  if (qvector != hr->query.vector) {
    rm_free(qvector);
  }
  IndexResult_Free(cur_vec_res);
}

static void prepareResults(HybridIterator *hr) {
    if (hr->searchMode == VECSIM_STANDARD_KNN) {
      hr->list =
          VecSimIndex_TopKQuery(hr->index, hr->query.vector, hr->query.k, &(hr->runtimeParams), hr->query.order);
      hr->iter = VecSimQueryResult_List_GetIterator(hr->list);
    return;
  }

  if (hr->searchMode == VECSIM_HYBRID_ADHOC_BF) {
    // Go over child_it results, compute distances, sort and store results in topResults.
    computeDistances(hr);
    return;
  }
  // Batches mode.
  if (hr->child->NumEstimated(hr->child->ctx) == 0) {
    return;
  }
  VecSimBatchIterator *batch_it = VecSimBatchIterator_New(hr->index, hr->query.vector);
  float upper_bound = INFINITY;
  size_t child_num_estimated = hr->child->NumEstimated(hr->child->ctx);
  if (child_num_estimated > VecSimIndex_IndexSize(hr->index)) {
    child_num_estimated = VecSimIndex_IndexSize(hr->index);
  }
  size_t child_upper_bound = child_num_estimated;
  while (VecSimBatchIterator_HasNext(batch_it)) {
    hr->numIterations++;
    size_t vec_index_size = VecSimIndex_IndexSize(hr->index);
    size_t n_res_left = hr->query.k - heap_count(hr->topResults);
    // Since NumEstimated(child) is an upper bound, it can be higher than index size, then we increase
    // batch and make sure that it is at least one.
    size_t batch_size = n_res_left * ((float)vec_index_size / child_num_estimated) + 1;
    RedisModule_Log(NULL, "warning", "Batch size is: %zu", batch_size);
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
    size_t new_results_cur_batch = heap_count(hr->topResults) - (hr->query.k - n_res_left);
    RedisModule_Log(NULL, "warning", "Found %zu results in iteration number %zu", new_results_cur_batch, hr->numIterations);
    if (heap_count(hr->topResults) == hr->query.k) {
      break;
    }
    // Re-evaluate the child num estimated results and the hybrid policy based on the current batch.
    // size_t new_results_cur_batch = heap_count(hr->topResults) - (hr->query.k - n_res_left);
    // This is the ratio between index_size to child results size as reflected by this batch.
    float cur_ratio = (float)new_results_cur_batch / n_res_left;
    // Child estimated number of results as reflected by this batch.
    size_t cur_child_num_estimated = cur_ratio * VecSimIndex_IndexSize(hr->index);
    // Conclude the new estimation of the child res num as the average between the old
    // and new estimation (get the accumulated estimation).
    child_num_estimated = (child_num_estimated + cur_child_num_estimated) / 2;
    if (child_num_estimated > child_upper_bound) {
      child_num_estimated = child_upper_bound;
    }
    RedisModule_Log(NULL, "warning", "Expected number of results that pass the filters: %zu", child_num_estimated);
    if (VecSimIndex_PreferAdHocSearch(hr->index, child_num_estimated, hr->query.k)) {
      // Change policy.
      RedisModule_Log(NULL, "warning", "Switched to ad-hoc BF");
      VecSimBatchIterator_Free(batch_it);
      hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
      // Clean the saved results, and restart the hybrid search in ad-hoc BF mode.
      while (heap_count(hr->topResults) > 0) {
        IndexResult_Free(heap_poll(hr->topResults));
      }
      hr->child->Rewind(hr->child->ctx);
      computeDistances(hr);
      return;
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

  if (hr->searchMode == VECSIM_HYBRID_ADHOC_BF || hr->searchMode == VECSIM_HYBRID_BATCHES) {
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
  if (it->searchMode == VECSIM_HYBRID_ADHOC_BF || it->searchMode == VECSIM_HYBRID_BATCHES) {
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

IndexIterator *NewHybridVectorIterator(HybridIteratorParams hParams) {
  HybridIterator *hi = rm_new(HybridIterator);
  hi->lastDocId = 0;
  hi->child = hParams.childIt;
  hi->resultsPrepared = false;
  hi->index = hParams.index;
  hi->dimension = hParams.dim;
  hi->vecType = hParams.elementType;
  hi->indexMetric = hParams.spaceMetric;
  hi->query = hParams.query;
  hi->runtimeParams = hParams.qParams;
  hi->scoreField = hParams.vectorScoreField;
  hi->base.isValid = 1;
  hi->list = NULL;
  hi->iter = NULL;
  hi->numIterations = 0;
  hi->ignoreScores = hParams.ignoreDocScore;

  if (hParams.childIt == NULL) {
    hi->searchMode = VECSIM_STANDARD_KNN;
  } else {
    // hi->searchMode is VECSIM_HYBRID_ADHOC_BF || VECSIM_HYBRID_BATCHES
    // Get the estimated number of results that pass the child "sub-query filter". Note that
    // this is an upper bound, and might even be larger than the total vector index size.
    size_t subset_size = hParams.childIt->NumEstimated(hParams.childIt->ctx);
    // IITER_INVALID_NUM_ESTIMATED_RESULTS is the default (invalid) value for indicating invalid intersection iterator.
    if (subset_size == IITER_INVALID_NUM_ESTIMATED_RESULTS) {
      rm_free(hi);
      return NULL;
    }
    if (subset_size > VecSimIndex_IndexSize(hParams.index)) {
      subset_size = VecSimIndex_IndexSize(hParams.index);
    }
    RedisModule_Log(NULL, "warning", "Expected number of results that pass the filters: %zu", subset_size);
    // Use a pre-defined heuristics that determines which approach should be faster.
    if (VecSimIndex_PreferAdHocSearch(hParams.index, subset_size, hParams.query.k)) {
      hi->searchMode = VECSIM_HYBRID_ADHOC_BF;
      RedisModule_Log(NULL, "warning", "Selected ad-hoc BF policy");
    } else {
      hi->searchMode = VECSIM_HYBRID_BATCHES;
      RedisModule_Log(NULL, "warning", "Selected batches policy");
    }
    hi->topResults = rm_malloc(heap_sizeof(hParams.query.k));
    heap_init(hi->topResults, cmpVecSimResByScore, NULL, hParams.query.k);
    hi->returnedResults = array_new(RSIndexResult *, hParams.query.k);
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
  if (hi->searchMode == VECSIM_STANDARD_KNN) {
    ri->Read = HR_ReadKnnUnsorted;
    ri->current = NewDistanceResult();
  } else {
    // Hybrid query - save the RSIndexResult subtree which is not the vector distance only if required.
    ri->Read = HR_ReadHybridUnsorted;
    if (hParams.ignoreDocScore) {
      ri->current = NewDistanceResult();
    } else {
      ri->current = NewHybridResult();
    }
  }
  return ri;
}
