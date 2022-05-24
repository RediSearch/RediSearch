
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct fvCtx {
  const RLookupKey *retprop;   // The key to return
  const RLookupKey *sortprop;  // The key to sort by
  RSValue *value;              // Value to return
  RSValue *sortval;            // Top sorted value
  int ascending;
};

struct FVReducer {
  Reducer base;
  const RLookupKey *sortprop;  // The property the value is sorted by
  int ascending;
};

//---------------------------------------------------------------------------------------------

static void *fvNewInstance(Reducer *rbase) {
  FVReducer *parent = (FVReducer *)rbase;
  BlkAlloc *ba = &parent->base.alloc;
  fvCtx *fv = BlkAlloc_Alloc(ba, sizeof(*fv), 1024 * sizeof(*fv));  // malloc(sizeof(*ctr));
  fv->retprop = parent->base.srckey;
  fv->sortprop = parent->sortprop;
  fv->ascending = parent->ascending;

  fv->value = NULL;
  fv->sortval = NULL;
  return fv;
}

//---------------------------------------------------------------------------------------------

static int fvAdd_noSort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  if (fvx->value) {
    return 1;
  }

  RSValue *val = RLookup_GetItem(fvx->retprop, srcrow);
  if (!val) {
    fvx->value = RS_NullVal();
    return 1;
  }
  fvx->value = val->IncrRef();
  return 1;
}

//---------------------------------------------------------------------------------------------

static int fvAdd_sort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  RSValue *val = RLookup_GetItem(fvx->retprop, srcrow);
  if (!val) {
    return 1;
  }

  RSValue *curSortval = RLookup_GetItem(fvx->sortprop, srcrow);
  if (!curSortval) {
    curSortval = &RS_StaticNull;
  }

  if (!fvx->sortval) {
    // No current value: assign value and continue
    fvx->value = val->IncrRef();
    fvx->sortval = curSortval->IncrRef();
    return 1;
  }

  int rc = (fvx->ascending ? -1 : 1) * RSValue::Cmp(curSortval, fvx->sortval, NULL);
  int isnull = fvx->sortval->IsNull();

  if (!fvx->value || (!isnull && rc > 0) || (isnull && rc < 0)) {
    RSVALUE_REPLACE(&fvx->sortval, curSortval);
    RSVALUE_REPLACE(&fvx->value, val);
  }

  return 1;
}

//---------------------------------------------------------------------------------------------

static RSValue *fvFinalize(Reducer *parent, void *ctx) {
  fvCtx *fvx = ctx;
  return fvx->value->IncrRef();
}

//---------------------------------------------------------------------------------------------

static void fvFreeInstance(Reducer *parent, void *p) {
  fvCtx *fvx = p;
  RSVALUE_CLEARVAR(fvx->value);
  RSVALUE_CLEARVAR(fvx->sortval);
}

//---------------------------------------------------------------------------------------------

Reducer *RDCRFirstValue_New(const ReducerOptions *options) {
  FVReducer *fvr = rm_calloc(1, sizeof(*fvr));
  fvr->ascending = 1;

  if (!options->GetKey(&fvr->base.srckey)) {
    rm_free(fvr);
    return NULL;
  }

  if (options->args->AdvanceIfMatch("BY")) {
    // Get the next field...
    if (!options->GetKey(&fvr->sortprop)) {
      rm_free(fvr);
      return NULL;
    }
    if (options->args->AdvanceIfMatch("ASC")) {
      fvr->ascending = 1;
    } else if (options->args->AdvanceIfMatch("DESC")) {
      fvr->ascending = 0;
    }
  }

  if (!options->EnsureArgsConsumed()) {
    rm_free(fvr);
    return NULL;
  }

  Reducer *rbase = &fvr->base;

  rbase->Add = fvr->sortprop ? fvAdd_sort : fvAdd_noSort;
  rbase->Finalize = fvFinalize;
  rbase->Free = Reducer_GenericFree;
  rbase->FreeInstance = fvFreeInstance;
  rbase->NewInstance = fvNewInstance;
  return rbase;
}

///////////////////////////////////////////////////////////////////////////////////////////////
