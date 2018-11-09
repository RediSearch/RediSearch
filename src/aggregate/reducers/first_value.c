#include <aggregate/reducer.h>

typedef struct {
  const RLookupKey *property;
  const RLookupKey *sortby;
  RSValue *value;
  RSValue *sortval;
  int ascending;
} fvCtx;

typedef struct {
  Reducer base;
  const RLookupKey *sortprop;
  int ascending;
} FVReducer;

static void *fvNewInstance(Reducer *rbase) {
  FVReducer *parent = (FVReducer *)rbase;
  BlkAlloc *ba = &parent->base.alloc;
  fvCtx *fv = BlkAlloc_Alloc(ba, sizeof(*fv), 1024 * sizeof(*fv));  // malloc(sizeof(*ctr));
  fv->property = parent->base.srckey;
  fv->sortby = parent->sortprop;
  fv->ascending = parent->ascending;
  fv->value = NULL;
  fv->sortby = NULL;
  return fv;
}

static int fvAdd_noSort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  if (fvx->value) {
    return 1;
  }

  RSValue *val = RLookup_GetItem(fvx->property, srcrow);
  if (!val) {
    return 1;
  }
  fvx->value = RSValue_IncrRef(val);
  return 1;
}

static int fvAdd_sort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  RSValue *val = RLookup_GetItem(fvx->property, srcrow);
  if (!val) {
    return 1;
  }

  RSValue *curSortval = RLookup_GetItem(fvx->sortby, srcrow);
  if (!curSortval) {
    curSortval = &RS_StaticNull;
  }

  if (!fvx->sortby) {
    // No current value: assign value and continue
    fvx->value = RSValue_IncrRef(val);
    fvx->sortval = RSValue_IncrRef(curSortval);
    return 1;
  }

  int rc = (fvx->ascending ? -1 : 1) * RSValue_Cmp(curSortval, fvx->sortval);
  int isnull = RSValue_IsNull(fvx->sortval);

  if (!fvx->value || (!isnull && rc > 0) || (isnull && rc < 0)) {
    RSVALUE_REPLACE(&fvx->sortval, curSortval);
    RSVALUE_REPLACE(&fvx->value, val);
    RSValue_Decref(fvx->sortval);
  }

  return 1;
}

static RSValue *fvFinalize(Reducer *parent, void *ctx) {
  fvCtx *fvx = ctx;
  return RSValue_IncrRef(fvx->value);
}

static void fvFreeInstance(Reducer *parent, void *p) {
  fvCtx *fvx = p;
  RSVALUE_CLEARVAR(fvx->value);
  RSVALUE_CLEARVAR(fvx->sortval);
}

Reducer *RDCRFirstValue_New(const ReducerOptions *options) {
  FVReducer *fvr = calloc(1, sizeof(*fvr));
  fvr->ascending = 1;

  if (!ReducerOpts_GetKey(options, &fvr->base.srckey)) {
    free(fvr);
    return NULL;
  }
  if (!AC_AdvanceIfMatch(options->args, "BY")) {
    // Get the next field...
    if (!ReducerOptions_GetKey(options, &fvr->sortprop)) {
      free(fvr);
      return NULL;
    }
    if (!AC_IsAtEnd(options->args)) {
      // ASC/DESC
      if (AC_AdvanceIfMatch(options->args, "ASC")) {
        fvr->ascending = 1;
      } else if (AC_AdvanceIfMatch(options->args, "DESC")) {
        fvr->ascending = 0;
      } else {
        QueryError_FmtUnknownArg(options->status, options->args, options->name);
      }
    }
  }
  Reducer *rbase = &fvr->base;

  rbase->Add = fvr->sortprop ? fvAdd_sort : fvAdd_noSort;
  rbase->Finalize = fvFinalize;
  rbase->Free = Reducer_GenericFree;
  rbase->FreeInstance = fvFreeInstance;
  rbase->NewInstance = fvNewInstance;
  return rbase;
}