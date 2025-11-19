/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <math.h>
#include "hybrid_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"
#include "wildcard_iterator.h"
#include "query.h"

#define VECTOR_SCORE(p) (p->data.tag == RSResultData_Metric ? IndexResult_NumValue(p) : IndexResult_NumValue(AggregateResult_Get(IndexResult_AggregateRef(p), 0)))

static int cmpVecSimResByScore(const void *p1, const void *p2, const void *udata) {
  const RSIndexResult *e1 = p1, *e2 = p2;
  double score1 = VECTOR_SCORE(e1), score2 = VECTOR_SCORE(e2);
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
// Returns ITERATOR_OK even if the result is not found, as it is expected to be used in a loop.
static IteratorStatus HR_SkipToInBatch(HybridIterator *hr, t_docId docId, RSIndexResult *result) {
  while(VecSimQueryReply_IteratorHasNext(hr->iter)) {
    VecSimQueryResult *res = VecSimQueryReply_IteratorNext(hr->iter);
    t_docId id = VecSimQueryResult_GetId(res);
    if (docId > id) {
      // consider binary search for next value
      continue;
    }
    // Set the item that we skipped to it in hit.
    result->docId = id;
    IndexResult_SetNumValue(result, VecSimQueryResult_GetScore(res));
    return ITERATOR_OK;
  }
  return ITERATOR_EOF;
}

// Simulate the logic of "Read", but it is limited to the results in a specific batch.
static IteratorStatus HR_ReadInBatch(HybridIterator *hr, RSIndexResult *out) {
  if (!VecSimQueryReply_IteratorHasNext(hr->iter)) {
    return ITERATOR_EOF;
  }
  VecSimQueryResult *res = VecSimQueryReply_IteratorNext(hr->iter);
  // Set the item that we read in the current RSIndexResult
  out->docId = VecSimQueryResult_GetId(res);
  IndexResult_SetNumValue(out, VecSimQueryResult_GetScore(res));
  return ITERATOR_OK;
}

static void insertResultToHeap_Metric(HybridIterator *hr, RSIndexResult *child_res, RSIndexResult **vec_res, double *upper_bound) {

  RSYieldableMetric_Concat(&(*vec_res)->metrics, child_res->metrics); // Pass child metrics, if there are any
  ResultMetrics_Add(*vec_res, hr->ownKey, RSValue_NewNumber(IndexResult_NumValue(*vec_res)));

  if (hr->topResults->count < hr->query.k) {
    // Insert to heap, allocate new memory for the next result.
    mmh_insert(hr->topResults, *vec_res);
    *vec_res = NewMetricResult();
  } else {
    // Replace the worst result and reuse its memory.
    *vec_res = mmh_exchange_max(hr->topResults, *vec_res);
    ResultMetrics_Free((*vec_res)->metrics); // Reuse
    (*vec_res)->metrics = NULL;
  }
  // Set new upper bound.
  RSIndexResult *worst = mmh_peek_max(hr->topResults);
  *upper_bound = IndexResult_NumValue(worst);
}

static void insertResultToHeap_Aggregate(HybridIterator *hr, RSIndexResult *child_res,
                                         RSIndexResult *vec_res, double *upper_bound) {

  RSIndexResult *res = NewHybridResult();
  AggregateResult_AddChild(res, IndexResult_DeepCopy(vec_res));
  AggregateResult_AddChild(res, IndexResult_DeepCopy(child_res));
  res->data.hybrid_metric.tag = RSAggregateResult_Owned; // Mark as copy, so when we free it, it will also free its children.
  ResultMetrics_Add(res, hr->ownKey, RSValue_NewNumber(IndexResult_NumValue(vec_res)));

  if (hr->topResults->count < hr->query.k) {
    mmh_insert(hr->topResults, res);
  } else {
    IndexResult_Free(mmh_exchange_max(hr->topResults, res));
  }
  // Set new upper bound.
  RSIndexResult *worst = mmh_peek_max(hr->topResults);
  const RSAggregateResult *agg = IndexResult_AggregateRef(worst);
  const RSIndexResult *first = AggregateResult_Get(agg, 0);
  *upper_bound = IndexResult_NumValue(first);
}

static void insertResultToHeap(HybridIterator *hr, RSIndexResult *child_res,
                               RSIndexResult **vec_res, double *upper_bound) {
  if (hr->canTrimDeepResults) {
    // If we ignore the document score, insert a single node of type DISTANCE.
    insertResultToHeap_Metric(hr, child_res, vec_res, upper_bound);
  } else {
    // Otherwise, first child is the vector distance, and the second contains a subtree with
    // the terms that the scorer will use later on in the pipeline.
    insertResultToHeap_Aggregate(hr, child_res, *vec_res, upper_bound);
  }
}

static void alternatingIterate(HybridIterator *hr, VecSimQueryReply_Iterator *vecsim_iter,
                               double *upper_bound) {
  RSIndexResult *cur_vec_res = NewMetricResult();

  IteratorStatus child_status = hr->child->Read(hr->child);
  IteratorStatus vec_status = HR_ReadInBatch(hr, cur_vec_res);
  while (child_status == ITERATOR_OK && vec_status == ITERATOR_OK) {
    if (cur_vec_res->docId == hr->child->lastDocId) {
      // Found a match - check if it should be added to the results heap.
      if (hr->topResults->count < hr->query.k || IndexResult_NumValue(cur_vec_res) < *upper_bound) {
        // Otherwise, set the vector and child results as the children the res
        // and insert result to the heap.
        insertResultToHeap(hr, hr->child->current, &cur_vec_res, upper_bound);
      }
      child_status = hr->child->Read(hr->child);
      vec_status = HR_ReadInBatch(hr, cur_vec_res);
    }
    // Otherwise, advance the iterator pointing to the lower id.
    else if (cur_vec_res->docId > hr->child->lastDocId) {
      child_status = hr->child->SkipTo(hr->child, cur_vec_res->docId);
      // We don't need to distinguish between ITERATOR_OK and ITERATOR_NOTFOUND here
      if (child_status == ITERATOR_NOTFOUND) child_status = ITERATOR_OK;
    } else if (VecSimQueryReply_IteratorHasNext(vecsim_iter)){
      vec_status = HR_SkipToInBatch(hr, hr->child->lastDocId, cur_vec_res);
    } else {
      break; // both iterators are depleted.
    }
  }
  IndexResult_Free(cur_vec_res);
}

static VecSimQueryReply_Code computeDistances(HybridIterator *hr) {
  double upper_bound = INFINITY;
  VecSimQueryReply_Code rc = VecSim_QueryReply_OK;
  RSIndexResult *cur_vec_res = NewMetricResult();
  void *qvector = hr->query.vector;

  if (hr->indexMetric == VecSimMetric_Cosine) {
    qvector = rm_malloc(hr->dimension * VecSimType_sizeof(hr->vecType));
    memcpy(qvector, hr->query.vector, hr->dimension * VecSimType_sizeof(hr->vecType));
    VecSim_Normalize(qvector, hr->dimension, hr->vecType);
  }

  VecSimTieredIndex_AcquireSharedLocks(hr->index);
  IteratorStatus child_status;
  while ((child_status = hr->child->Read(hr->child)) != ITERATOR_EOF) {
    if (child_status == ITERATOR_TIMEOUT || TimedOut_WithCtx(&hr->timeoutCtx)) {
      rc = VecSim_QueryReply_TimedOut;
      break;
    }
    RS_ASSERT(child_status == ITERATOR_OK);
    double metric = VecSimIndex_GetDistanceFrom_Unsafe(hr->index, hr->child->lastDocId, qvector);
    // If this id is not in the vector index (since it was deleted), metric will return as NaN.
    if (isnan(metric)) {
      continue;
    }
    if (hr->topResults->count < hr->query.k || metric < upper_bound) {
      // Populate the vector result.
      cur_vec_res->docId = hr->child->lastDocId;
      IndexResult_SetNumValue(cur_vec_res, metric);
      insertResultToHeap(hr, hr->child->current, &cur_vec_res, &upper_bound);
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
  if (hr->child->NumEstimated(hr->child) == 0) {
    return VecSim_QueryReply_OK;
  }
  VecSimBatchIterator *batch_it = VecSimBatchIterator_New(hr->index, hr->query.vector, &hr->runtimeParams);
  double upper_bound = INFINITY;
  VecSimQueryReply_Code code = VecSim_QueryReply_OK;
  size_t child_num_estimated = hr->child->NumEstimated(hr->child);
  // Since NumEstimated(child) is an upper bound, it can be higher than index size.
  if (child_num_estimated > VecSimIndex_IndexSize(hr->index)) {
    child_num_estimated = VecSimIndex_IndexSize(hr->index);
  }
  size_t child_upper_bound = child_num_estimated;
  // Track maximum batch size
  hr->maxBatchSize = hr->runtimeParams.batchSize;
  while (VecSimBatchIterator_HasNext(batch_it)) {
    hr->numIterations++;
    size_t vec_index_size = VecSimIndex_IndexSize(hr->index);
    size_t n_res_left = hr->query.k - hr->topResults->count;
    // If user requested explicitly a batch size, use it. Otherwise, compute optimal batch size
    // based on the ratio between child_num_estimated and the index size.
    size_t batch_size = hr->runtimeParams.batchSize;
    if (batch_size == 0) {
      batch_size = n_res_left * ((float)vec_index_size / child_num_estimated) + 1;
      // If given by the user, it's constant, otherwise update the maximum batch size.
      if (batch_size > hr->maxBatchSize) {
        hr->maxBatchSize = batch_size;
        hr->maxBatchIteration = hr->numIterations - 1;  // Zero-based
      }
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
    hr->child->Rewind(hr->child);

    // Go over both iterators and save mutual results in the heap.
    alternatingIterate(hr, hr->iter, &upper_bound);
    if (hr->topResults->count == hr->query.k) {
      break;
    }

    if (reviewHybridSearchPolicy(hr, n_res_left, child_num_estimated, &child_num_estimated)) {
      // Change policy from batches to AD-HOC BF.
      VecSimBatchIterator_Free(batch_it);
      hr->searchMode = VECSIM_HYBRID_BATCHES_TO_ADHOC_BF;
      // Clean the saved results, and restart the hybrid search in ad-hoc BF mode.
      mmh_clear(hr->topResults);
      hr->child->Rewind(hr->child);
      return computeDistances(hr);
    }
  }
  VecSimBatchIterator_Free(batch_it);
  return code;
}

// In KNN mode, the results will return sorted by ascending order of the distance
// (better score first), while in hybrid mode, the results will return in descending order.
static IteratorStatus HR_ReadHybridUnsortedSingle(HybridIterator *hr) {
  if (hr->base.atEOF) {
    return ITERATOR_EOF;
  }
  if (hr->topResults->count == 0) {
    hr->base.atEOF = true;
    return ITERATOR_EOF;
  }
  if (hr->base.current) {
    IndexResult_Free(hr->base.current);
  }
  hr->base.current = mmh_pop_min(hr->topResults);

  const t_fieldIndex fieldIndex = hr->filterCtx.field.index;
  if (hr->sctx && fieldIndex != RS_INVALID_FIELD_INDEX
      && !DocTable_CheckFieldExpirationPredicate(&hr->sctx->spec->docs, hr->base.current->docId, fieldIndex, hr->filterCtx.predicate, &hr->sctx->time.current)) {
    return ITERATOR_NOTFOUND;
  }
  hr->base.lastDocId = hr->base.current->docId;
  return ITERATOR_OK;
}

static IteratorStatus HR_ReadHybridUnsorted(QueryIterator *ctx) {
  HybridIterator *hr = (HybridIterator *)ctx;
  if (!hr->resultsPrepared) {
    hr->resultsPrepared = true;
    if (prepareResults(hr) == VecSim_QueryReply_TimedOut) {
      return ITERATOR_TIMEOUT;
    }
  }

  IteratorStatus rc;
  do {
    rc = HR_ReadHybridUnsortedSingle(hr);
    if (TimedOut_WithCtx(&hr->timeoutCtx)) {
      return ITERATOR_TIMEOUT;
    }
  } while (rc == ITERATOR_NOTFOUND);
  return rc;
}

static IteratorStatus HR_ReadKnnUnsortedSingle(HybridIterator *hr) {
  if (hr->base.atEOF) {
    return ITERATOR_EOF;
  }
  if (HR_ReadInBatch(hr, hr->base.current) == ITERATOR_EOF) {
    hr->base.atEOF = true;
    return ITERATOR_EOF;
  }

  const t_fieldIndex fieldIndex = hr->filterCtx.field.index;
  if (hr->sctx && fieldIndex != RS_INVALID_FIELD_INDEX
      && !DocTable_CheckFieldExpirationPredicate(&hr->sctx->spec->docs, hr->base.current->docId, fieldIndex, hr->filterCtx.predicate, &hr->sctx->time.current)) {
    return ITERATOR_NOTFOUND;
  }

  hr->base.lastDocId = hr->base.current->docId;
  ResultMetrics_Add(hr->base.current, hr->ownKey, RSValue_NewNumber(IndexResult_NumValue(hr->base.current)));
  return ITERATOR_OK;
}

static IteratorStatus HR_ReadKnnUnsorted(QueryIterator *ctx) {
  HybridIterator *hr = (HybridIterator *)ctx;
  if (!hr->resultsPrepared) {
    hr->resultsPrepared = true;
    if (prepareResults(hr) == VecSim_QueryReply_TimedOut) {
      return ITERATOR_TIMEOUT;
    }
    ctx->current = NewMetricResult(); // Initialize the current result.
  }

  IteratorStatus rc;
  do {
    rc = HR_ReadKnnUnsortedSingle(hr);
    if (TimedOut_WithCtx(&hr->timeoutCtx)) {
      return ITERATOR_TIMEOUT;
    }
  } while (rc == ITERATOR_NOTFOUND);
  return rc;
}

static size_t HR_NumEstimated(QueryIterator *ctx) {
  HybridIterator *hr = (HybridIterator *)ctx;
  size_t vec_res_num = MIN(hr->query.k, VecSimIndex_IndexSize(hr->index));
  if (hr->child == NULL) return vec_res_num;
  return MIN(vec_res_num, hr->child->NumEstimated(hr->child));
}

static void HR_Rewind(QueryIterator *ctx) {
  HybridIterator *hr = (HybridIterator *)ctx;
  hr->resultsPrepared = false;
  hr->numIterations = 0;
  hr->maxBatchSize = 0;
  hr->maxBatchIteration = 0;
  VecSimQueryReply_Free(hr->reply);
  VecSimQueryReply_IteratorFree(hr->iter);
  hr->reply = NULL;
  hr->iter = NULL;
  hr->base.lastDocId = 0;
  hr->base.atEOF = false;
  if (hr->base.current) {
    IndexResult_Free(hr->base.current);
    hr->base.current = NULL;
  }

  if (hr->searchMode == VECSIM_HYBRID_ADHOC_BF || hr->searchMode == VECSIM_HYBRID_BATCHES) {
    // Clean the saved and returned results (in case of HYBRID mode).
    mmh_clear(hr->topResults);
    hr->child->Rewind(hr->child);
  }
}

void HybridIterator_Free(QueryIterator *self) {
  HybridIterator *it = (HybridIterator *)self;
  if (it == NULL) {
    return;
  }

  // Invalidate the handle if it exists
  if (it->keyHandle) {
    it->keyHandle->is_valid = false;
  }

  if (it->topResults) {   // Iterator is in one of the hybrid modes.
    mmh_free(it->topResults);
  }
  if (it->base.current) {
    IndexResult_Free(it->base.current);
    it->base.current = NULL;
  }
  VecSimQueryReply_Free(it->reply);
  VecSimQueryReply_IteratorFree(it->iter);
  if (it->child) {
    it->child->Free(it->child);
  }
  rm_free(it);
}

static QueryIterator* HybridIteratorReducer(HybridIteratorParams *hParams) {
  QueryIterator* ret = NULL;
  if (hParams->childIt && hParams->childIt->type == EMPTY_ITERATOR) {
    ret = hParams->childIt;
  } else if (hParams->childIt && IsWildcardIterator(hParams->childIt)) {
    hParams->childIt->Free(hParams->childIt);
    hParams->qParams.searchMode = STANDARD_KNN;
    hParams->childIt = NULL;
  }
  return ret;
}

// Revalidate the hybrid iterator.
// If we already have the results prepared, we are OK, and if not, we didn't execute the query yet so we are also OK.
// Only if we have a child iterator, and it aborted, we need to abort the hybrid iterator.
// If the child iterator is OK or MOVED, we are OK whether we have results prepared or not.
static ValidateStatus HR_Revalidate(QueryIterator *ctx) {
  HybridIterator *hr = (HybridIterator *)ctx;
  if (hr->child && hr->child->Revalidate(hr->child) == VALIDATE_ABORTED) {
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

QueryIterator *NewHybridVectorIterator(HybridIteratorParams hParams, QueryError *status) {
  // If searchMode is out of the expected range.
  RS_ASSERT(hParams.qParams.searchMode >= 0 && hParams.qParams.searchMode < VECSIM_LAST_SEARCHMODE);
  QueryIterator* ri = HybridIteratorReducer(&hParams);
  if (ri) {
    return ri;
  }

  HybridIterator *hi = rm_new(HybridIterator);
  // This will be changed later to a valid RLookupKey if there is no syntax error in the query,
  // by the creation of the metrics loader results processor.
  hi->ownKey = NULL;
  hi->keyHandle = NULL; // Will be set later if this iterator is used for metrics
  hi->child = hParams.childIt;
  hi->resultsPrepared = false;
  hi->index = hParams.index;
  hi->dimension = hParams.dim;
  hi->vecType = hParams.elementType;
  hi->indexMetric = hParams.spaceMetric;
  hi->query = hParams.query;
  hi->runtimeParams = hParams.qParams;
  hi->scoreField = hParams.vectorScoreField;
  hi->reply = NULL;
  hi->iter = NULL;
  hi->topResults = NULL;
  hi->numIterations = 0;
  hi->maxBatchSize = 0;
  hi->maxBatchIteration = 0;
  hi->canTrimDeepResults = hParams.canTrimDeepResults;
  hi->timeoutCtx = (TimeoutCtx){ .timeout = hParams.timeout, .counter = 0 };
  hi->runtimeParams.timeoutCtx = &hi->timeoutCtx;
  hi->sctx = hParams.sctx;
  hi->filterCtx = *hParams.filterCtx;

  if (hParams.childIt == NULL || hParams.query.k == 0) {
    // If there is no child iterator, or the query is going to return 0 results, we can use simple KNN.
    hi->searchMode = VECSIM_STANDARD_KNN;
  } else {
    // hi->searchMode is VECSIM_HYBRID_ADHOC_BF || VECSIM_HYBRID_BATCHES
    // Get the estimated number of results that pass the child "sub-query filter". Note that
    // this is an upper bound, and might even be larger than the total vector index size.
    size_t subset_size = hParams.childIt->NumEstimated(hParams.childIt);
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
  }

  ri = &hi->base;
  ri->type = HYBRID_ITERATOR;
  ri->atEOF = false;
  ri->lastDocId = 0;
  ri->current = NULL;
  ri->NumEstimated = HR_NumEstimated;
  ri->Free = HybridIterator_Free;
  ri->Rewind = HR_Rewind;
  ri->Revalidate = HR_Revalidate;
  ri->SkipTo = NULL; // As long as this iterator is always at the root, this is not needed.
  if (hi->searchMode == VECSIM_STANDARD_KNN) {
    ri->Read = HR_ReadKnnUnsorted;
  } else {
    // Hybrid query - save the RSIndexResult subtree which is not the vector distance only if required.
    ri->Read = HR_ReadHybridUnsorted;
  }
  return ri;
}
