/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "iterators/optimizer_reader.h"
#include "iterators/empty_iterator.h"

int cmpAsc(const void *v1, const void *v2, const void *udata) {
  RSIndexResult *res1 = (RSIndexResult *)v1;
  RSIndexResult *res2 = (RSIndexResult *)v2;

  if (IndexResult_NumValue(res1) > IndexResult_NumValue(res2)) return 1;
  if (IndexResult_NumValue(res1) < IndexResult_NumValue(res2)) return -1;
  return res1->docId < res2->docId ? -1 : 1;
}

int cmpDesc(const void *v1, const void *v2, const void *udata) {
  RSIndexResult *res1 = (RSIndexResult *)v1;
  RSIndexResult *res2 = (RSIndexResult *)v2;

  if (IndexResult_NumValue(res1) > IndexResult_NumValue(res2)) return -1;
  if (IndexResult_NumValue(res1) < IndexResult_NumValue(res2)) return 1;
  return res1->docId < res2->docId ? -1 : 1;
}

static inline double getSuccessRatio(const OptimizerIterator *optIt) {
  double resultsCollectedSinceLast = heap_count(optIt->heap) - optIt->heapOldSize;
  return resultsCollectedSinceLast / optIt->lastLimitEstimate;
}


static size_t OPT_NumEstimated(QueryIterator *self) {
  OptimizerIterator *opt = (OptimizerIterator *)self;
  return MIN(opt->child->NumEstimated(opt->child) ,
              opt->numericIter->NumEstimated(opt->numericIter));
}

// TODO: handle MOVED better
static ValidateStatus OPT_Validate(QueryIterator *self) {
  OptimizerIterator *opt = (OptimizerIterator *)self;
  if (opt->child->Revalidate(opt->child) != VALIDATE_OK) {
    return VALIDATE_ABORTED;
  }
  if (opt->numericIter->Revalidate(opt->numericIter) != VALIDATE_OK) {
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
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

  // very low success, lets get all remaining results
  if (successRatio < 0.01 || optIt->numIterations == 3) {
    nf->limit = optIt->numDocs;
  } else {
    int resultsMissing = heap_size(heap) - heap_count(heap);
    size_t limitEstimate = QOptimizer_EstimateLimit(optIt->numDocs, optIt->childEstimate, resultsMissing);
    optIt->lastLimitEstimate = nf->limit = limitEstimate * successRatio;
  }

  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = optIt->numericFieldIndex}, .predicate = FIELD_EXPIRATION_DEFAULT};
  // create new numeric filter
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
          if (it->cmp(tempRes, it->pooledResult, NULL) > 0) {
            heap_replace(it->heap, it->pooledResult);
            it->pooledResult = tempRes;
          }
          DMD_Return(it->pooledResult->dmd);
        }
      }
    }

    // Not enough result, try to rewind
    if (heap_size(it->heap) > heap_count(it->heap) && it->offset < it->childEstimate) {
      if (getSuccessRatio(it) < 1) {
        OPT_Rewind(&it->base);
        childRes = numericRes = NULL;
        // rewind was successful, continue iteration
        if (it->numericIter != NULL) {
          numeric = it->numericIter;
          it->hitCounter = 0;
          it->numIterations++;
          continue;
        }
      } else {
        RedisModule_Log(RSDummyContext, "verbose", "Not enough results collected, but success ratio is %f", getSuccessRatio(it));
        RedisModule_Log(RSDummyContext, "debug", "Heap size: %d, heap count: %d, offset: %ld, childEstimate: %ld",
                                        heap_size(it->heap), heap_count(it->heap), it->offset, it->childEstimate);
      }
    }

    it->base.Read = OPT_ReadYield;
    return OPT_ReadYield(self);
  }
}

QueryIterator *NewOptimizerIterator(QOptimizer *qOpt, QueryIterator *root, IteratorsConfig *config) {
  OptimizerIterator *oi = rm_calloc(1, sizeof(*oi));
  oi->child = root;
  oi->optim = qOpt;

  oi->cmp = qOpt->asc ? cmpAsc : cmpDesc;
  oi->resArr = rm_malloc((qOpt->limit + 1) * sizeof(RSIndexResult));
  oi->pooledResult = oi->resArr;
  oi->heap = rm_malloc(heap_sizeof(qOpt->limit));
  heap_init(oi->heap, oi->cmp, NULL, qOpt->limit);

  oi->numDocs = qOpt->sctx->spec->docs.size;
  oi->childEstimate = root->NumEstimated(root);

  const FieldSpec *field = IndexSpec_GetFieldWithLength(qOpt->sctx->spec, qOpt->fieldName, strlen(qOpt->fieldName));
  // if there is no numeric range query but sortby, create a Numeric Filter
  if (!qOpt->nf) {
    qOpt->nf = NewNumericFilter(-INFINITY, INFINITY, 1, 1, qOpt->asc, field);
    oi->flags |= OPTIM_OWN_NF;
  }
  oi->lastLimitEstimate = qOpt->nf->limit =
    QOptimizer_EstimateLimit(oi->numDocs, oi->childEstimate, qOpt->limit);

  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = field->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
  oi->numericFieldIndex = field->index;
  oi->numericIter = NewNumericFilterIterator(qOpt->sctx, qOpt->nf, INDEXFLD_T_NUMERIC, config, &filterCtx);
  if (!oi->numericIter) {
    OptimizerIterator_Free(&oi->base);
    return NewEmptyIterator();
  }

  oi->offset = oi->numericIter->NumEstimated(oi->numericIter);
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
  ri->current = NULL;

  return &oi->base;
}
