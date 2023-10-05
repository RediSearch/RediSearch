/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <math.h>
#include "hybrid_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

#define VECTOR_RESULT(p) (p->type == RSResultType_Metric ? p : p->agg.children[0])

static VecSimQueryReply_Code prepareResults(HybridIterator *hr); // forward declaration

static int cmpVecSimResByScore(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;
  double score1 = VECTOR_RESULT(e1)->num.value, score2 = VECTOR_RESULT(e2)->num.value;
  if (score1 < score2) {
    return -1;
  } else if (score1 > score2) {
    return 1;
  }
  return e1->docId < e2->docId;
}

// To use in the future, if we will need to sort results by id.
// static int cmpVecSimResById(const void *p1, const void *p2, const void *udata) {
//   const RSIndexResult *e1 = p1, *e2 = p2;

//   if (e1->docId > e2->docId) {
//     return 1;
//   } else if (e1->docId < e2->docId) {
//     return -1;
//   }
//   return 0;
// }

// Simulate the logic of "SkipTo", but it is limited to the results in a specific batch.
static int HR_SkipToInBatch(void *ctx, t_docId docId, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  while(VecSimQueryReply_IteratorHasNext(hr->iter)) {
    VecSimQueryResult *res = VecSimQueryReply_IteratorNext(hr->iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    // Set the item that we skipped to it in hit.
    (*hit)->docId = id;
    (*hit)->num.value = VecSimQueryResult_GetScore(res);
    return INDEXREAD_OK;
  }
  return INDEXREAD_EOF;
}

// Simulate the logic of "Read", but it is limited to the results in a specific batch.
static int HR_ReadInBatch(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!VecSimQueryReply_IteratorHasNext(hr->iter)) {
    return INDEXREAD_EOF;
  }
  VecSimQueryResult *res = VecSimQueryReply_IteratorNext(hr->iter);
  // Set the item that we read in the current RSIndexResult
  (*hit)->docId = VecSimQueryResult_GetId(res);
  (*hit)->num.value = VecSimQueryResult_GetScore(res);
  return INDEXREAD_OK;
}

static void insertResultToHeap_Metric(HybridIterator *hr, RSIndexResult *child_res, RSIndexResult **vec_res, double *upper_bound) {

  ResultMetrics_Concat(*vec_res, child_res); // Pass child metrics, if there are any
  ResultMetrics_Add(*vec_res, hr->base.ownKey, RS_NumVal((*vec_res)->num.value));

  if (hr->topResults->count < hr->query.k) {
    // Insert to heap, allocate new memory for the next result.
    mmh_insert(hr->topResults, *vec_res);
    *vec_res = NewMetricResult();
  } else {
    // Replace the worst result and reuse its memory.
    *vec_res = mmh_exchange_max(hr->topResults, *vec_res);
    IndexResult_Clear(*vec_res); // Reuse
  }
  // Set new upper bound.
  RSIndexResult *worst = mmh_peek_max(hr->topResults);
  *upper_bound = worst->num.value;
}

static void insertResultToHeap_Aggregate(HybridIterator *hr, RSIndexResult *res, RSIndexResult *child_res,
                                         RSIndexResult *vec_res, double *upper_bound) {

  AggregateResult_AddChild(res, vec_res);
  AggregateResult_AddChild(res, child_res);
  RSIndexResult *hit = IndexResult_DeepCopy(res);
  AggregateResult_Reset(res); // Reset the current result.
  ResultMetrics_Add(hit, hr->base.ownKey, RS_NumVal(vec_res->num.value));

  if (hr->topResults->count < hr->query.k) {
    mmh_insert(hr->topResults, hit);
  } else {
    IndexResult_Free(mmh_exchange_max(hr->topResults, hit));
  }
  // Set new upper bound.
  RSIndexResult *worst = mmh_peek_max(hr->topResults);
  *upper_bound = worst->agg.children[0]->num.value;
}

static void insertResultToHeap(HybridIterator *hr, RSIndexResult *res, RSIndexResult *child_res,
                               RSIndexResult **vec_res, double *upper_bound) {
  if (hr->ignoreScores) {
    // If we ignore the document score, insert a single node of type DISTANCE.
    insertResultToHeap_Metric(hr, child_res, vec_res, upper_bound);
  } else {
    // Otherwise, first child is the vector distance, and the second contains a subtree with
    // the terms that the scorer will use later on in the pipeline.
    insertResultToHeap_Aggregate(hr, res, child_res, *vec_res, upper_bound);
  }
}

static void alternatingIterate(HybridIterator *hr, VecSimQueryReply_Iterator *vecsim_iter,
                               double *upper_bound) {
  RSIndexResult *cur_vec_res = NewMetricResult();
  RSIndexResult *cur_child_res;  // This will use the memory of hr->child->current.
  RSIndexResult *cur_res = hr->base.current;

  hr->child->Read(hr->child->ctx, &cur_child_res);
  HR_ReadInBatch(hr, &cur_vec_res);
  while (IITER_HAS_NEXT(hr->child)) {
    if (cur_vec_res->docId == cur_child_res->docId) {
      // Found a match - check if it should be added to the results heap.
      if (hr->topResults->count < hr->query.k || cur_vec_res->num.value < *upper_bound) {
        // Otherwise, set the vector and child results as the children the res
        // and insert result to the heap.
        insertResultToHeap(hr, cur_res, cur_child_res, &cur_vec_res, upper_bound);
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
    } else if (VecSimQueryReply_IteratorHasNext(vecsim_iter)){
      int rc = HR_SkipToInBatch(hr, cur_child_res->docId, &cur_vec_res);
      if (rc == INDEXREAD_EOF) break;
    } else {
      break; // both iterators are depleted.
    }
  }
  IndexResult_Free(cur_vec_res);
}

static VecSimQueryReply_Code computeDistances(HybridIterator *hr) {
  double upper_bound = INFINITY;
  VecSimQueryReply_Code rc = VecSim_QueryReply_OK;
  RSIndexResult *cur_res = hr->base.current;
  RSIndexResult *cur_child_res;  // This will use the memory of hr->child->current.
  RSIndexResult *cur_vec_res = NewMetricResult();
  void *qvector = hr->query.vector;

  if (hr->indexMetric == VecSimMetric_Cosine) {
    qvector = rm_malloc(hr->dimension * VecSimType_sizeof(hr->vecType));
    memcpy(qvector, hr->query.vector, hr->dimension * VecSimType_sizeof(hr->vecType));
    VecSim_Normalize(qvector, hr->dimension, hr->vecType);
  }

  VecSimTieredIndex_AcquireSharedLocks(hr->index);
  while (hr->child->Read(hr->child->ctx, &cur_child_res) != INDEXREAD_EOF) {
    if (TimedOut_WithCtx(&hr->timeoutCtx)) {
      rc = VecSim_QueryReply_TimedOut;
      VecSimTieredIndex_ReleaseSharedLocks(hr->index);
      break;
    }
    double metric = VecSimIndex_GetDistanceFrom_Unsafe(hr->index, cur_child_res->docId, qvector);
    // If this id is not in the vector index (since it was deleted), metric will return as NaN.
    if (isnan(metric)) {
      continue;
    }
    if (hr->topResults->count < hr->query.k || metric < upper_bound) {
      // Populate the vector result.
      cur_vec_res->docId = cur_child_res->docId;
      cur_vec_res->num.value = metric;
      insertResultToHeap(hr, cur_res, cur_child_res, &cur_vec_res, &upper_bound);
    }
  }
  VecSimTieredIndex_ReleaseSharedLocks(hr->index);
  if (qvector != hr->query.vector) {
    rm_free(qvector);
  }
  IndexResult_Free(cur_vec_res);
  return rc;
}

// Review the estimated child results num, and returns true if hybrid policy should change.
static bool reviewHybridSearchPolicy(HybridIterator *hr, size_t n_res_left, size_t child_upper_bound,
                                     size_t *child_num_estimated) {

  // If user asked explicitly to run in batches with a fixed batch size, continue immediately
  // to the next batch without revisiting the hybrid policy.
  if ((VecSimSearchMode)hr->runtimeParams.searchMode == VECSIM_HYBRID_BATCHES && hr->runtimeParams.batchSize) {
    return false;
  }
  // Re-evaluate the child num estimated results and the hybrid policy based on the current batch.
  size_t new_results_cur_batch = (hr->topResults->count) - (hr->query.k - n_res_left);
  // This is the ratio between index_size to child results size as reflected by this batch.
  float cur_ratio = (float)new_results_cur_batch / n_res_left;
  // Child estimated number of results as reflected by this batch.
  size_t cur_child_num_estimated = cur_ratio * VecSimIndex_IndexSize(hr->index);
  // Conclude the new estimation of the child res num as the average between the old
  // and new estimation (get the accumulated estimation).
  *child_num_estimated = (*child_num_estimated + cur_child_num_estimated) / 2;
  if (*child_num_estimated > child_upper_bound) {
    *child_num_estimated = child_upper_bound;
  }
  if ((VecSimSearchMode)hr->runtimeParams.searchMode == VECSIM_HYBRID_BATCHES) {
    return false;
  }
  return VecSimIndex_PreferAdHocSearch(hr->index, *child_num_estimated, hr->query.k, false);
}

static VecSimQueryReply_Code prepareResults(HybridIterator *hr) {
  if (hr->searchMode == VECSIM_STANDARD_KNN) {
    hr->reply = VecSimIndex_TopKQuery(hr->index, hr->query.vector, hr->query.k, &(hr->runtimeParams), hr->query.order);
    hr->iter = VecSimQueryReply_GetIterator(hr->reply);
    return VecSimQueryReply_GetCode(hr->reply);
  }

  if (hr->searchMode == VECSIM_HYBRID_ADHOC_BF) {
    // Go over child_it results, compute distances, sort and store results in topResults.
    return computeDistances(hr);
  }
  // Batches mode.
  if (hr->child->NumEstimated(hr->child->ctx) == 0) {
    return VecSim_QueryReply_OK;
  }
  VecSimBatchIterator *batch_it = VecSimBatchIterator_New(hr->index, hr->query.vector, &hr->runtimeParams);
  double upper_bound = INFINITY;
  VecSimQueryReply_Code code = VecSim_QueryReply_OK;
  size_t child_num_estimated = hr->child->NumEstimated(hr->child->ctx);
  // Since NumEstimated(child) is an upper bound, it can be higher than index size.
  if (child_num_estimated > VecSimIndex_IndexSize(hr->index)) {
    child_num_estimated = VecSimIndex_IndexSize(hr->index);
  }
  size_t child_upper_bound = child_num_estimated;
  while (VecSimBatchIterator_HasNext(batch_it)) {
    hr->numIterations++;
    size_t vec_index_size = VecSimIndex_IndexSize(hr->index);
    size_t n_res_left = hr->query.k - hr->topResults->count;
    // If user requested explicitly a batch size, use it. Otherwise, compute optimal batch size
    // based on the ratio between child_num_estimated and the index size.
    size_t batch_size = hr->runtimeParams.batchSize;
    if (batch_size == 0) {
      batch_size = n_res_left * ((float)vec_index_size / child_num_estimated) + 1;
    }
    VecSimQueryReply_Free(hr->reply);
    VecSimQueryReply_IteratorFree(hr->iter);
    hr->iter = NULL;
    // Get the next batch.
    hr->reply = VecSimBatchIterator_Next(batch_it, batch_size, BY_ID);
    code = VecSimQueryReply_GetCode(hr->reply);
    if (VecSim_QueryReply_TimedOut == code) {
      break;
    }
    hr->iter = VecSimQueryReply_GetIterator(hr->reply);
    hr->child->Rewind(hr->child->ctx);

    // Go over both iterators and save mutual results in the heap.
    alternatingIterate(hr, hr->iter, &upper_bound);
    if (hr->topResults->count == hr->query.k) {
      break;
    }

    if (reviewHybridSearchPolicy(hr, n_res_left, child_num_estimated, &child_num_estimated)) {
      // Change policy from batches to AD-HOC BF.
      hr->searchMode = VECSIM_HYBRID_BATCHES_TO_ADHOC_BF;
      // Clean the saved results, and restart the hybrid search in ad-hoc BF mode.
      mmh_clear(hr->topResults);
      hr->child->Rewind(hr->child->ctx);
      code = computeDistances(hr);
      break;
    }
  }
  VecSimBatchIterator_Free(batch_it);
  return code;
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
    hr->resultsPrepared = true;
    if (prepareResults(hr) == VecSim_QueryReply_TimedOut) {
      return INDEXREAD_TIMEOUT;
    }
  }
  if (!HR_HasNext(ctx)) {
    return INDEXREAD_EOF;
  }
  if (hr->topResults->count == 0) {
    hr->base.isValid = false;
    return INDEXREAD_EOF;
  }
  *hit = mmh_pop_min(hr->topResults);
  hr->returnedResults = array_append(hr->returnedResults, *hit);
  hr->lastDocId = (*hit)->docId;
  return INDEXREAD_OK;
}

static int HR_ReadKnnUnsorted(void *ctx, RSIndexResult **hit) {
  HybridIterator *hr = ctx;
  if (!hr->resultsPrepared) {
    hr->resultsPrepared = true;
    if (prepareResults(hr) == VecSim_QueryReply_TimedOut) {
      return INDEXREAD_TIMEOUT;
    }
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
  ResultMetrics_Reset(*hit);
  ResultMetrics_Add(*hit, hr->base.ownKey, RS_NumVal((*hit)->num.value));
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

static t_docId HR_LastDocId(void *ctx) {
  HybridIterator *hr = ctx;
  return hr->lastDocId;
}

static void HR_Rewind(void *ctx) {
  HybridIterator *hr = ctx;
  hr->resultsPrepared = false;
  hr->numIterations = 0;
  VecSimQueryReply_Free(hr->reply);
  VecSimQueryReply_IteratorFree(hr->iter);
  hr->reply = NULL;
  hr->iter = NULL;
  hr->lastDocId = 0;
  hr->base.isValid = 1;

  if (hr->searchMode == VECSIM_HYBRID_ADHOC_BF || hr->searchMode == VECSIM_HYBRID_BATCHES) {
    // Clean the saved and returned results (in case of HYBRID mode).
    mmh_clear(hr->topResults);
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
  if (it->topResults) {   // Iterator is in one of the hybrid modes.
    mmh_free(it->topResults);
  }
  if (it->returnedResults) {   // Iterator is in one of the hybrid modes.
    array_free_ex(it->returnedResults, IndexResult_Free(*(RSIndexResult **)ptr));
  }
  IndexResult_Free(it->base.current);
  VecSimQueryReply_Free(it->reply);
  VecSimQueryReply_IteratorFree(it->iter);
  if (it->child) {
    it->child->Free(it->child);
  }
  rm_free(it);
}

IndexIterator *NewHybridVectorIterator(HybridIteratorParams hParams, QueryError *status) {
  // If searchMode is out of the expected range.
  if (hParams.qParams.searchMode < 0 || hParams.qParams.searchMode >= VECSIM_LAST_SEARCHMODE) {
    QueryError_SetErrorFmt(status, QUERY_EGENERIC, "Creating new hybrid vector iterator has failed");
  }

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
  hi->reply = NULL;
  hi->iter = NULL;
  hi->topResults = NULL;
  hi->returnedResults = NULL;
  hi->numIterations = 0;
  hi->ignoreScores = hParams.ignoreDocScore;
  hi->timeoutCtx = (TimeoutCtx){ .timeout = hParams.timeout, .counter = 0 };
  hi->runtimeParams.timeoutCtx = &hi->timeoutCtx;

  if (hParams.childIt == NULL || hParams.query.k == 0) {
    // If there is no child iterator, or the query is going to return 0 results, we can use simple KNN.
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
    // If user asks explicitly for a policy - use it.
    if (hParams.qParams.searchMode) {
      hi->searchMode = (VecSimSearchMode)hParams.qParams.searchMode;
    } else {
      // Use a pre-defined heuristics that determines which approach should be faster.
      if (VecSimIndex_PreferAdHocSearch(hParams.index, subset_size, hParams.query.k, true)) {
        hi->searchMode = VECSIM_HYBRID_ADHOC_BF;
      } else {
        hi->searchMode = VECSIM_HYBRID_BATCHES;
      }
    }
    hi->topResults = mmh_init_with_size(hParams.query.k, cmpVecSimResByScore, NULL, (mmh_free_func)IndexResult_Free);
    hi->returnedResults = array_new(RSIndexResult *, hParams.query.k);
  }

  IndexIterator *ri = &hi->base;
  ri->ctx = hi;
  // This will be changed later to a valid RLookupKey if there is no syntax error in the query,
  // by the creation of the metrics loader results processor.
  ri->ownKey = NULL;
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
    ri->current = NewMetricResult();
  } else {
    // Hybrid query - save the RSIndexResult subtree which is not the vector distance only if required.
    ri->Read = HR_ReadHybridUnsorted;
    if (hParams.ignoreDocScore) {
      ri->current = NewMetricResult();
    } else {
      ri->current = NewHybridResult();
    }
  }
  return ri;
}
