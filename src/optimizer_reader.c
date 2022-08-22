#include "optimizer_reader.h"
#include "aggregate/aggregate.h"

#define IITER_CURRENT_RECORD(ii) ((ii)->current ? (ii)->current : 0)

int cmpAsc(const void *v1, const void *v2, const void *udata) {
  double d1 = *((double *)v1);
  double d2 = *((double *)v2);
  return d1 > d2 ? 1 : d1 < d2 ? -1 : 0;
}

int cmpDesc(const void *v1, const void *v2, const void *udata) {
  return -cmpAsc(v1, v2, udata);
}


static size_t OPT_NumEstimated(void *ctx) {
  OptimizerIterator *opt = ctx;
  return MIN(opt->childIter->NumEstimated(opt->childIter->ctx) ,
              opt->numericIter->NumEstimated(opt->numericIter->ctx));
}

static size_t OPT_Len(void *ctx) {
  return OPT_NumEstimated(ctx);
}

static void OPT_Abort(void *ctx) {
  OptimizerIterator *opt = ctx;
  opt->base.isValid = 0;
}

static t_docId OPT_LastDocId(void *ctx) {
  OptimizerIterator *opt = ctx;
  return opt->lastDocId;
}

//TODO: not completed
static void OPT_Rewind(void *ctx) {
  OptimizerIterator *opt = ctx;
  IndexIterator *child = opt->childIter;
  child->Rewind(child->ctx);

  IndexIterator *numeric = opt->numericIter;
  numeric->Free(numeric);
  numeric = opt->numericIter = NewNumericFilterIterator(NULL, opt->optim.nf, NULL, INDEXFLD_T_NUMERIC); // TODO:
  opt->offset += numeric->NumEstimated(numeric->ctx);
  opt->optim.nf->offset += numeric->NumEstimated(numeric->ctx);
}

static int OPT_HasNext(void *ctx) {
  OptimizerIterator *opt = ctx;
  return opt->base.isValid;
}

void OptimizerIterator_Free(struct indexIterator *self) {
  OptimizerIterator *it = self->ctx;
  if (it == NULL) {
    return;
  }
  if (it->heap) {   // Iterator is in one of the hybrid modes.
    while (heap_count(it->heap) > 0) {
      IndexResult_Free(heap_poll(it->heap));
    }
    heap_free(it->heap);
  }
  it->childIter->Free(it->childIter);
  it->numericIter->Free(it->numericIter);
  rm_free(it);
}

int OPT_ReadYield(void *ctx, RSIndexResult **e) {
  OptimizerIterator *it = ctx;
  *e = heap_poll(it->heap);
  return (*e == NULL) ? INDEXREAD_EOF : INDEXREAD_OK;
}

int OPT_Read(void *ctx, RSIndexResult **e) {
  OptimizerIterator *it = ctx;
  IndexIterator *child = it->childIter;
  IndexIterator *numeric = it->numericIter;

  if (it->pooledResult == NULL) {
    it->pooledResult = rm_calloc(1, sizeof(*it->pooledResult));
  } else {
    // nothing to clean
  }

  while (1) {
    AggregateResult_Reset(it->base.current);

    RSIndexResult *childRes; //= IITER_CURRENT_RECORD(child);
    RSIndexResult *numericRes;// = IITER_CURRENT_RECORD(numeric);
    // skip to the next
    int rc = INDEXREAD_OK;

    int rc1 = child->Read(child->ctx, &childRes);
    int rc2 = numeric->SkipTo(numeric->ctx, childRes->docId, &numericRes);

    while (1) {
      if (rc1 == INDEXREAD_EOF || rc2 == INDEXREAD_EOF) {
        break;
      }
      if (childRes->docId == numericRes->docId) {
        it->lastDocId = childRes->docId;
        
        // heap is not full. insert
        if (heap_count(it->heap) < heap_size(it->heap)) {
          *it->pooledResult = *numericRes;
          heap_offer(it->heap, it->pooledResult);
          it->pooledResult = NULL;
        // heap is full. try to replace
        } else if (heap_cmp_root(it->heap, numericRes)) {
          RSIndexResult *tempRes = heap_peek(it->heap);
          *it->pooledResult = *numericRes; 
          heap_replace(it->heap, it->pooledResult);
          it->pooledResult = tempRes;
        }

      } else if (childRes->docId > numericRes->docId) {
        int rc2 = numeric->SkipTo(numeric->ctx, childRes->docId, &numericRes);
      } else {
        int rc1 = child->SkipTo(child->ctx, childRes->docId, &childRes);
      }
    }

    // if heap is full or TODO no more rewinds
    if (heap_size(it->heap) == heap_count(it->heap)) {
      it->base.Read = OPT_ReadYield;
      return OPT_ReadYield(ctx, e);
    }

    // use heuristic to decide next step
    // rewind


}







IndexIterator *NewOptimizerIterator(QOptimizer *q_opt, IndexIterator *root, IndexIterator *numeric) {
  OptimizerIterator *oi = rm_malloc(sizeof(*oi));
  oi->childIter = root;
  oi->numericIter = numeric;
  oi->offset = numeric->NumEstimated(numeric->ctx);

  oi->optim = *q_opt;
  oi->heap = rm_malloc(heap_sizeof(q_opt->limit));
  heap_init(oi->heap, q_opt->asc ? cmpAsc : cmpDesc, NULL, q_opt->limit);

  IndexIterator *ri = &oi->base;
  ri->ctx = oi;
  ri->type = OPTIMUS_ITERATOR;
  ri->mode = MODE_SORTED; 

  ri->NumEstimated = OPT_NumEstimated;
  ri->GetCriteriaTester = NULL;
  ri->LastDocId = OPT_LastDocId;
  ri->Free = OptimizerIterator_Free;
  ri->Len = OPT_Len;
  ri->Abort = OPT_Abort;
  ri->Rewind = OPT_Rewind;
  ri->HasNext = OPT_HasNext;
  ri->SkipTo = NULL;            // The iterator is always on top and and Read() is called
  ri->Read = OPT_Read;
  ri->current = NewNumericResult();

  return &oi->base;
}