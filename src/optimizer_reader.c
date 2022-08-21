#include "optimizer_reader.h"
#include "aggregate/aggregate.h"

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

int OPT_Read(void *ctx, RSIndexResult **e) {

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
  ri->SkipTo = NULL;            // The iterator is always on top and on Read() is called
  ri->Read = OPT_Read;
  ri->current = NewNumericResult();

  return &oi->base;
}