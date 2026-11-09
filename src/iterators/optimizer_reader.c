/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "iterators/optimizer_reader.h"

#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/param.h>

#include "iterators_ffi.h"
#include "rqe_iterator_type.h"
#include "types_ffi.h"
#include "doc_table.h"
#include "field.h"
#include "field_spec.h"
#include "index_result_rs.h"
#include "inverted_index.h"
#include "iterator_api.h"
#include "numeric_filter.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "sorting_vector_ffi.h"
#include "spec.h"
#include "value_ffi.h"

/* A sort key's value in a result's sorting vector, or NULL when the document has none, which is
 * how the sorter's row lookup reads it. */
static const RSValue *sortKeyValue(const RSIndexResult *res, int16_t sortIdx) {
  const RSSortingVector *sv = &res->dmd->sortVector;
  if (sortIdx < 0 || (size_t)sortIdx >= RSSortingVector_Length(sv)) {
    return NULL;
  }
  const RSValue *value = RSSortingVector_Get(sv, sortIdx);
  return value == RSValue_NullStatic() ? NULL : value;
}

/* Whether a pooled result stands for a document without a value for the sortby field. */
static inline bool lacksSortValue(const OptimizerIterator *it, const RSIndexResult *res) {
  return it->missingSortValue[res - it->resArr];
}

/* Orders results the way the pipeline's sorter ranks them (`SearchResult_CmpByFields`), returning a
 * positive value when `v1` ranks after `v2`: by the numeric key's indexed value, then by each later
 * sort key, a missing value ranking last in either direction, then by document id in the last
 * key's direction. The heap keeps only `limit` results, so it has to agree with the sorter on which
 * of the results tied on the numeric key those are. */
static int cmpBySortKeys(const void *v1, const void *v2, const void *udata) {
  const OptimizerIterator *it = udata;
  const QOptimizer *opt = it->optim;
  const RSIndexResult *res1 = v1;
  const RSIndexResult *res2 = v2;

  bool missing1 = lacksSortValue(it, res1);
  bool missing2 = lacksSortValue(it, res2);
  if (missing1 != missing2) {
    return missing1 ? 1 : -1;
  }
  if (!missing1) {
    double num1 = IndexResult_NumValue(res1);
    double num2 = IndexResult_NumValue(res2);
    if (num1 != num2) {
      int rc = num1 > num2 ? 1 : -1;
      return opt->asc ? rc : -rc;
    }
  }

  for (size_t i = 0; i < opt->numTieBreakKeys; i++) {
    const RSValue *value1 = sortKeyValue(res1, opt->tieBreakSortIdx[i]);
    const RSValue *value2 = sortKeyValue(res2, opt->tieBreakSortIdx[i]);
    if (!value1 || !value2) {
      if (value1 == value2) continue;
      return value1 ? -1 : 1;
    }
    int rc = RSValue_Cmp(value1, value2, NULL);
    if (rc) {
      return (opt->sortAscMap & (1ULL << (i + 1))) ? rc : -rc;
    }
  }

  bool lastAscending = opt->sortAscMap & (1ULL << opt->numTieBreakKeys);
  int rc = res1->docId < res2->docId ? -1 : 1;
  return lastAscending ? rc : -rc;
}

static inline double getSuccessRatio(const OptimizerIterator *optIt) {
  if (optIt->lastLimitEstimate <= 0) {
    return 0;
  }
  double resultsCollectedSinceLast = heap_count(optIt->heap) - optIt->heapOldSize;
  return resultsCollectedSinceLast / optIt->lastLimitEstimate;
}

/* Whether a numeric window limited to `limit` postings spans everything left of the range: a limit
 * of 0 does not limit the window at all, and SIZE_MAX is what a rewind asks for to get the rest.
 * Comparing with the index's document count instead would not do, since postings of deleted
 * documents stay in the range until garbage collection. */
static inline bool windowSpansRest(size_t limit) {
  return limit == 0 || limit == SIZE_MAX;
}

/* Whether the walk has covered the whole sortby range: the window just drained spanned everything
 * left of it, or held nothing because the walk is already past its end. */
static inline bool walkExhausted(OptimizerIterator *it) {
  return it->windowSpansRest || it->numericIter->NumEstimated(it->numericIter) == 0;
}

static size_t OPT_NumEstimated(const QueryIterator *self) {
  const OptimizerIterator *opt = (const OptimizerIterator *)self;
  return MIN(opt->child->NumEstimated(opt->child) ,
              opt->numericIter->NumEstimated(opt->numericIter));
}

// TODO: handle MOVED better
static ValidateStatus OPT_Validate(QueryIterator *self, struct IndexSpec *spec) {
  OptimizerIterator *opt = (OptimizerIterator *)self;
  ValidateStatus childStatus = opt->child->Revalidate(opt->child, spec);
  // A timeout is passed up as such: unlike an abort, it says the query ran out of time and its
  // results are partial, which only the caller can report.
  if (childStatus == VALIDATE_TIMEOUT) {
    return VALIDATE_TIMEOUT;
  }
  if (childStatus != VALIDATE_OK) {
    return VALIDATE_ABORTED;
  }
  ValidateStatus numericStatus = opt->numericIter->Revalidate(opt->numericIter, spec);
  if (numericStatus == VALIDATE_TIMEOUT) {
    return VALIDATE_TIMEOUT;
  }
  if (numericStatus != VALIDATE_OK) {
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static QueryIterator *OPT_ProfileChildren(QueryIterator *base) {
  OptimizerIterator *oi = (OptimizerIterator *)base;
  if (oi->child) {
    oi->child = IntoProfiled(oi->child);
  }
  return base;
}

static void OPT_Rewind(QueryIterator *self) {
  OptimizerIterator *optIt = (OptimizerIterator *)self;
  QOptimizer *qOpt = optIt->optim;
  heap_t *heap = optIt->heap;
  QueryIterator *child = optIt->child;

  // rewind child iterator
  child->Rewind(child);

  // update numeric filter with old iterator result estimation
  // used to skip ranges when creating new numeric iterator
  QueryIterator *numeric = optIt->numericIter;
  NumericFilter *nf = qOpt->nf;
  nf->offset += numeric->NumEstimated(numeric);
  numeric->Free(numeric);
  optIt->numericIter = NULL;

  double successRatio = getSuccessRatio(optIt);

  // very low success, or enough windows already: get all remaining results
  if (successRatio < 0.01 || optIt->numIterations >= 3) {
    nf->limit = SIZE_MAX;
  } else {
    // Size the next window to hold the missing results at the rate the drained
    // window actually hit at. observedEstimate restates that rate as the
    // document count the estimator divides by. successRatio is at least 0.01 here.
    int resultsMissing = heap_size(heap) - heap_count(heap);
    size_t observedEstimate = successRatio * optIt->numDocs;
    optIt->lastLimitEstimate = nf->limit =
      QOptimizer_EstimateLimit(optIt->numDocs, observedEstimate, resultsMissing);
  }

  optIt->windowSpansRest = windowSpansRest(nf->limit);

  // create new numeric filter
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = optIt->numericFieldIndex}, .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};
  optIt->numericIter = NewNumericFilterIterator(qOpt->sctx, qOpt->nf, INDEXFLD_T_NUMERIC, optIt->config, &filterCtx);

  optIt->heapOldSize = heap_count(heap);
  optIt->numIterations++;
}

void OptimizerIterator_Free(QueryIterator *self) {
  OptimizerIterator *it = (OptimizerIterator *)self;
  if (it == NULL) {
    return;
  }

  if(it->flags & OPTIM_OWN_NF) {
    NumericFilter_Free(it->optim->nf);
    it->optim->nf = NULL;
  }

  if (it->child) {
    it->child->Free(it->child);
  }

  if (it->numericIter) {
    it->numericIter->Free(it->numericIter);
  }

  // we always use the array as RSResultData_Numeric. no need for IndexResult_Free
  rm_free(it->resArr);
  rm_free(it->missingSortValue);
  IndexResult_Free(it->missingTemplate);
  heap_free(it->heap);

  rm_free(it);
}

IteratorStatus OPT_ReadYield(QueryIterator *self) {
  OptimizerIterator *it = (OptimizerIterator *)self;
  if (heap_count(it->heap) > 0) {
    self->current = heap_poll(it->heap);
    self->lastDocId = self->current->docId;
    return ITERATOR_OK;
  }
  return ITERATOR_EOF;
}

/* Whether a document already holds a heap slot. Slots before `pooledResult` are taken while the
 * heap fills; once it is full, every slot but the spare `pooledResult` is. */
static bool heapHoldsDoc(const OptimizerIterator *it, t_docId docId) {
  size_t taken = heap_count(it->heap) < heap_size(it->heap) ? (size_t)heap_count(it->heap)
                                                            : (size_t)heap_size(it->heap) + 1;
  for (size_t i = 0; i < taken; i++) {
    const RSIndexResult *res = &it->resArr[i];
    if (res != it->pooledResult && res->docId == docId) {
      return true;
    }
  }
  return false;
}

/* Offer the result in `pooledResult`, whose `dmd` is borrowed, to the heap. A document holding
 * several values for the sortby field can be reached again by a later window, which covers other
 * values of it, so such a document is only collected once. */
static void offerToHeap(OptimizerIterator *it) {
  if (it->optim->sortFieldMayHoldManyValues && heapHoldsDoc(it, it->pooledResult->docId)) {
    DMD_Return(it->pooledResult->dmd);
    return;
  }

  // heap is not full. insert
  if (heap_count(it->heap) < heap_size(it->heap)) {
    heap_offer(&it->heap, it->pooledResult);
    it->pooledResult++;

    // heap is full. try to replace
  } else {
    RSIndexResult *tempRes = heap_peek(it->heap);
    if (it->cmp(tempRes, it->pooledResult, it) > 0) {
      heap_replace(it->heap, it->pooledResult);
      it->pooledResult = tempRes;
    }
    DMD_Return(it->pooledResult->dmd);
  }
}

/* Fill the heap's remaining slots with the child's matches that have no value for the sortby field,
 * which the sorter ranks after every match that has one. The walk never reaches them, since it only
 * visits documents in the field's numeric index; a document whose value has expired counts as
 * having none, as it does for the sorter. */
static void collectMatchesWithoutSortValue(OptimizerIterator *it) {
  QOptimizer *opt = it->optim;
  QueryIterator *child = it->child;
  child->Rewind(child);

  NumericFilter *wholeField =
      NewNumericFilter(-INFINITY, INFINITY, 1, 1, opt->asc, it->numericFieldIndex, NULL);
  FieldFilterContext filterCtx = {
      .field = {.index_tag = FieldMaskOrIndex_Index, .index = it->numericFieldIndex},
      .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};
  QueryIterator *valued =
      NewNumericFilterIterator(opt->sctx, wholeField, INDEXFLD_T_NUMERIC, it->config, &filterCtx);
  bool valuedExhausted = valued == NULL;

  // Every match is offered, not only enough to fill the heap: among themselves these rank by the
  // later sort keys and the document id, so a later match can still displace an earlier one.
  while (child->Read(child) == ITERATOR_OK) {
    t_docId docId = child->lastDocId;
    bool hasValue = false;
    if (!valuedExhausted) {
      if (valued->lastDocId < docId) {
        IteratorStatus rc = valued->SkipTo(valued, docId);
        valuedExhausted = rc != ITERATOR_OK && rc != ITERATOR_NOTFOUND;
        hasValue = rc == ITERATOR_OK;
      } else {
        hasValue = valued->lastDocId == docId;
      }
    }
    if (hasValue) {
      continue;
    }

    const RSDocumentMetadata *dmd = DocTable_Borrow(&opt->sctx->spec->docs, docId);
    if (!dmd) {
      continue;
    }
    *it->pooledResult = *it->missingTemplate;
    it->pooledResult->docId = docId;
    it->pooledResult->dmd = dmd;
    it->missingSortValue[it->pooledResult - it->resArr] = true;
    offerToHeap(it);
  }

  if (valued) {
    valued->Free(valued);
  }
  NumericFilter_Free(wholeField);
}

IteratorStatus OPT_Read(QueryIterator *self) {
  IteratorStatus rc1, rc2;
  OptimizerIterator *it = (OptimizerIterator *)self;
  QOptimizer *opt = it->optim;

  QueryIterator *child = it->child;
  QueryIterator *numeric = it->numericIter;
  RSIndexResult *childRes = NULL;
  RSIndexResult *numericRes = NULL;

  it->hitCounter = 0;

  while (1) {

    while (1) {
      // get next result
      if (numericRes == NULL || childRes->docId == numericRes->docId) {
        rc1 = child->Read(child);
        if (rc1 == ITERATOR_EOF) break;
        rc2 = numeric->SkipTo(numeric, child->lastDocId);
      } else if (childRes->docId > numericRes->docId) {
        rc2 = numeric->SkipTo(numeric, childRes->docId);
      } else {
        rc1 = child->SkipTo(child, numericRes->docId);
      }

      if (rc1 == ITERATOR_EOF || rc2 == ITERATOR_EOF) {
        break;
      }
      childRes = child->current;
      numericRes = numeric->current;

      it->hitCounter++;
      if (childRes->docId == numericRes->docId) {
        self->lastDocId = childRes->docId;

        // copy the numeric result for the sorting heap
        if (numericRes->data.tag == RSResultData_Numeric) {
          *it->pooledResult = *numericRes;
        } else {
          const RSAggregateResult *agg = IndexResult_AggregateRef(numericRes);
          const RSIndexResult *child = AggregateResult_Get(agg, 0);
          RS_LOG_ASSERT(child->data.tag == RSResultData_Numeric, "???");
          *it->pooledResult = *(child);
        }

        // handle expired results
        const RSDocumentMetadata *dmd = DocTable_Borrow(&opt->sctx->spec->docs, childRes->docId);
        if (!dmd) {
          continue;
        }
        it->pooledResult->dmd = dmd;
        it->missingSortValue[it->pooledResult - it->resArr] = false;
        offerToHeap(it);
      }
    }

    // Not enough results: widen the window and walk on until the walk covers the whole range.
    // The child's matches can all lie past the windows walked so far, whatever the child's
    // estimate or the hit rate so far suggest, so stopping any earlier returns an incomplete page.
    if (heap_size(it->heap) > heap_count(it->heap) && !walkExhausted(it)) {
      OPT_Rewind(&it->base);
      childRes = numericRes = NULL;
      // rewind was successful, continue iteration
      if (it->numericIter != NULL) {
        numeric = it->numericIter;
        it->hitCounter = 0;
        it->numIterations++;
        continue;
      }
    }

    if (heap_count(it->heap) < heap_size(it->heap)) {
      collectMatchesWithoutSortValue(it);
    }

    it->base.Read = OPT_ReadYield;
    return OPT_ReadYield(self);
  }
}

QueryIterator *NewOptimizerIterator(QOptimizer *qOpt, QueryIterator *root, IteratorsConfig *config) {
  // Check for overflow in result array allocation: (limit + 1) * sizeof(RSIndexResult)
  size_t resArr_alloc_size;
  if (__builtin_add_overflow(qOpt->limit, 1, &resArr_alloc_size)) {
    return NULL;
  }
  if (__builtin_mul_overflow(resArr_alloc_size, sizeof(RSIndexResult), &resArr_alloc_size)) {
    return NULL;
  }

  // Check for overflow in heap allocation: (limit * sizeof(void*)) + sizeof(heap_t)
  // Note: We only check the multiplication overflow.
  // The test for overflow due to the addition of sizeof(heap_t) is not needed
  // because sizeof(RSIndexResult) > sizeof(void*), so any limit that would
  // cause the heap addition to overflow would have already triggered the resArr
  // multiplication overflow check above.
  size_t heap_array_size;
  if (__builtin_mul_overflow((size_t)qOpt->limit, sizeof(void *), &heap_array_size)) {
    return NULL;
  }

  OptimizerIterator *oi = rm_calloc(1, sizeof(*oi));
  oi->child = root;
  oi->optim = qOpt;

  oi->cmp = cmpBySortKeys;
  oi->resArr = rm_malloc(resArr_alloc_size);
  oi->pooledResult = oi->resArr;
  oi->missingSortValue = rm_calloc(qOpt->limit + 1, sizeof(*oi->missingSortValue));
  oi->missingTemplate = NewNumericResult();
  oi->heap = rm_malloc(heap_sizeof(qOpt->limit));
  heap_init(oi->heap, oi->cmp, oi, qOpt->limit);

  oi->numDocs = qOpt->sctx->spec->docs.size;
  oi->childEstimate = root->NumEstimated(root);

  const FieldSpec *field = IndexSpec_GetFieldWithLength(qOpt->sctx->spec, qOpt->fieldName, strlen(qOpt->fieldName));
  // if there is no numeric range query but sortby, create a Numeric Filter, over the range the
  // query confines the sortby field to if it does
  if (!qOpt->nf) {
    const SortFieldBounds *bounds = &qOpt->impliedBounds;
    qOpt->nf = qOpt->hasImpliedBounds
                   ? NewNumericFilter(bounds->min, bounds->max, bounds->minInclusive,
                                      bounds->maxInclusive, qOpt->asc, field->index, NULL)
                   : NewNumericFilter(-INFINITY, INFINITY, 1, 1, qOpt->asc, field->index, NULL);
    oi->flags |= OPTIM_OWN_NF;
  }
  oi->lastLimitEstimate = qOpt->nf->limit =
    QOptimizer_EstimateLimit(oi->numDocs, oi->childEstimate, qOpt->limit);
  oi->windowSpansRest = windowSpansRest(qOpt->nf->limit);

  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = field->index}, .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};
  oi->numericFieldIndex = field->index;
  // Disk specs are filtered out in QOptimizer_Iterators — OPT_Read and
  // numDocs both consult spec->docs, which is empty on disk.
  RS_ASSERT(!qOpt->sctx->spec->diskSpec);
  oi->numericIter = NewNumericFilterIterator(qOpt->sctx, qOpt->nf, INDEXFLD_T_NUMERIC, config, &filterCtx);
  if (!oi->numericIter) {
    // The sortby field has no numeric index, so no document has a value for it. A range lifted out
    // of the query on that field matches nothing; otherwise the query alone decides the matches,
    // which all tie on the field, so it runs unoptimized.
    bool liftedRange = !(oi->flags & OPTIM_OWN_NF);
    if (!liftedRange) {
      oi->child = NULL;
      OptimizerIterator_Free(&oi->base);
      return root;
    }
    OptimizerIterator_Free(&oi->base);
    return NewEmptyIterator();
  }

  oi->config = config;

  QueryIterator *ri = &oi->base;
  ri->type = OPTIMUS_ITERATOR;
  ri->atEOF = false;
  ri->lastDocId = 0;
  ri->NumEstimated = OPT_NumEstimated;
  ri->Free = OptimizerIterator_Free;
  ri->Rewind = OPT_Rewind;
  ri->Revalidate = OPT_Validate;
  ri->SkipTo = NULL;            // The iterator is always on top and and Read() is called
  ri->Read = OPT_Read;
  ri->ProfileChildren = OPT_ProfileChildren;
  ri->PrintProfile = Optimus_PrintProfile;
  ri->current = NULL;

  return &oi->base;
}

// Accessors for profile printing.
const QueryIterator *OptimizerIterator_GetChild(const QueryIterator *it) {
  RS_ASSERT(it->type == OPTIMUS_ITERATOR);
  const OptimizerIterator *oi = (const OptimizerIterator *)it;
  return oi->child;
}

const char *OptimizerIterator_GetOptimizationType(const QueryIterator *it) {
  RS_ASSERT(it->type == OPTIMUS_ITERATOR);
  const OptimizerIterator *oi = (const OptimizerIterator *)it;
  return QOptimizer_PrintType(oi->optim);
}

