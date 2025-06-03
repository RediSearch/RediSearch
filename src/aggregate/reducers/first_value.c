/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>

typedef struct {
  const RLookupKey *retprop;   // The key to return
  const RLookupKey *sortprop;  // The key to sort by
  RSValue *value;              // Value to return
  RSValue *sortval;            // Top sorted value
  int ascending;
} fvCtx;

typedef struct {
  Reducer base;
  const RLookupKey *sortprop;  // The property the value is sorted by
  int ascending;
} FVReducer;

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

static int fvAdd_noSort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  if (fvx->value) {
    return 1;
  }

  RSValue *val = RLookup_GetItem(fvx->retprop, srcrow);
  if (!val) val = RS_NullVal();
  fvx->value = RSValue_IncrRef(val);
  return 1;
}

static int fvAdd_sort(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  fvCtx *fvx = ctx;
  RSValue *val = RLookup_GetItem(fvx->retprop, srcrow);
  if (!val) val = RS_NullVal();

  RSValue *curSortval = RLookup_GetItem(fvx->sortprop, srcrow);
  if (!curSortval) curSortval = RS_NullVal();

  if (!fvx->sortval) {
    // This is the first value we see
    fvx->value = RSValue_IncrRef(val);
    fvx->sortval = RSValue_IncrRef(curSortval);
  } else if (RSValue_IsNull(curSortval)) {
    // If the current value is null, we don't need to do anything
  } else if (RSValue_IsNull(fvx->sortval)) {
    // If the best value is null, replace it with the current value (which is not null)
    RSVALUE_REPLACE(&fvx->sortval, curSortval);
    RSVALUE_REPLACE(&fvx->value, val);
  } else {
    // If both values are not null, compare them and replace if necessary
    int rc = RSValue_Cmp(curSortval, fvx->sortval, NULL);
    if (fvx->ascending ? rc < 0 : rc > 0) {
      RSVALUE_REPLACE(&fvx->sortval, curSortval);
      RSVALUE_REPLACE(&fvx->value, val);
    }
  }

  return 1;
}

static RSValue *fvFinalize(Reducer *parent, void *ctx) {
  fvCtx *fvx = ctx;
  if (fvx->value) {
    return RSValue_IncrRef(fvx->value);
  } else {
    return RS_NullVal();
  }
}

static void fvFreeInstance(Reducer *parent, void *p) {
  fvCtx *fvx = p;
  RSVALUE_CLEARVAR(fvx->value);
  RSVALUE_CLEARVAR(fvx->sortval);
}

Reducer *RDCRFirstValue_New(const ReducerOptions *options) {
  FVReducer *fvr = rm_calloc(1, sizeof(*fvr));
  fvr->ascending = 1;

  if (!ReducerOpts_GetKey(options, &fvr->base.srckey)) {
    rm_free(fvr);
    return NULL;
  }

  if (AC_AdvanceIfMatch(options->args, "BY")) {
    // Get the next field...
    if (!ReducerOptions_GetKey(options, &fvr->sortprop)) {
      rm_free(fvr);
      return NULL;
    }
    if (AC_AdvanceIfMatch(options->args, "ASC")) {
      fvr->ascending = 1;
    } else if (AC_AdvanceIfMatch(options->args, "DESC")) {
      fvr->ascending = 0;
    }
  }

  if (!ReducerOpts_EnsureArgsConsumed(options)) {
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
