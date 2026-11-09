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

/* Orders results the way the pipeline's sorter ranks them (`SearchResult_CmpByFields`), returning a
 * positive value when `v1` ranks after `v2`: by the numeric key's indexed value, then by each later
 * sort key, a missing value ranking last in either direction, then by document id in the last
 * key's direction. The heap keeps only `limit` results, so it has to agree with the sorter on which
 * of the results tied on the numeric key those are. */
static int cmpBySortKeys(const void *v1, const void *v2, const void *udata) {
  const QOptimizer *opt = udata;
  const RSIndexResult *res1 = v1;
  const RSIndexResult *res2 = v2;

  double num1 = IndexResult_NumValue(res1);
  double num2 = IndexResult_NumValue(res2);
  if (num1 != num2) {
    int rc = num1 > num2 ? 1 : -1;
    return opt->asc ? rc : -rc;
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

/* Whether a numeric window limited to `limit` documents spans everything left of the range; a
 * limit of 0 does not limit the window at all. */
static inline bool windowSpansRest(size_t limit, size_t numDocs) {
  return limit == 0 || limit >= numDocs;
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
    nf->limit = optIt->numDocs;
  } else {
    // Size the next window to hold the missing results at the rate the drained
    // window actually hit at. observedEstimate restates that rate as the
    // document count the estimator divides by. successRatio is at least 0.01 here.
    int resultsMissing = heap_size(heap) - heap_count(heap);
    size_t observedEstimate = successRatio * optIt->numDocs;
    optIt->lastLimitEstimate = nf->limit =
      QOptimizer_EstimateLimit(optIt->numDocs, observedEstimate, resultsMissing);
  }

  optIt->windowSpansRest = windowSpansRest(nf->limit, optIt->numDocs);

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
  }

  it->child->Free(it->child);

  if (it->numericIter) {
    it->numericIter->Free(it->numericIter);
  }

  // we always use the array as RSResultData_Numeric. no need for IndexResult_Free
  rm_free(it->resArr);
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

        // heap is not full. insert
        if (heap_count(it->heap) < heap_size(it->heap)) {
          heap_offer(&it->heap, it->pooledResult);
          it->pooledResult++;

        // heap is full. try to replace
        } else {
          RSIndexResult *tempRes = heap_peek(it->heap);
          if (it->cmp(tempRes, it->pooledResult, opt) > 0) {
            heap_replace(it->heap, it->pooledResult);
            it->pooledResult = tempRes;
          }
          DMD_Return(it->pooledResult->dmd);
        }
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
  oi->heap = rm_malloc(heap_sizeof(qOpt->limit));
  heap_init(oi->heap, oi->cmp, qOpt, qOpt->limit);

  oi->numDocs = qOpt->sctx->spec->docs.size;
  oi->childEstimate = root->NumEstimated(root);

  const FieldSpec *field = IndexSpec_GetFieldWithLength(qOpt->sctx->spec, qOpt->fieldName, strlen(qOpt->fieldName));
  // if there is no numeric range query but sortby, create a Numeric Filter
  if (!qOpt->nf) {
    qOpt->nf = NewNumericFilter(-INFINITY, INFINITY, 1, 1, qOpt->asc, field->index, NULL);
    oi->flags |= OPTIM_OWN_NF;
  }
  oi->lastLimitEstimate = qOpt->nf->limit =
    QOptimizer_EstimateLimit(oi->numDocs, oi->childEstimate, qOpt->limit);
  oi->windowSpansRest = windowSpansRest(qOpt->nf->limit, oi->numDocs);

  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = field->index}, .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};
  oi->numericFieldIndex = field->index;
  // Disk specs are filtered out in QOptimizer_Iterators — OPT_Read and
  // numDocs both consult spec->docs, which is empty on disk.
  RS_ASSERT(!qOpt->sctx->spec->diskSpec);
  oi->numericIter = NewNumericFilterIterator(qOpt->sctx, qOpt->nf, INDEXFLD_T_NUMERIC, config, &filterCtx);
  if (!oi->numericIter) {
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

