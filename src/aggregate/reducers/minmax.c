/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <aggregate/reducer.h>
#include <float.h>

typedef struct {
  double val;
} minmaxCtx;

static int minAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  minmaxCtx *m = ctx;
  double val;
  RSValue *v = RLookup_GetItem(r->srckey, srcrow);
  if (RSValue_ToNumber(v, &val)) {
    m->val = MIN(m->val, val);
  }
  return 1;
}

static int maxAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  minmaxCtx *m = ctx;
  double val;
  RSValue *v = RLookup_GetItem(r->srckey, srcrow);
  if (RSValue_ToNumber(v, &val)) {
    m->val = MAX(m->val, val);
  }
  return 1;
}

static void *minmaxNewInstance(Reducer *r) {
  minmaxCtx *m = BlkAlloc_Alloc(&r->alloc, sizeof(*m), 1024);
  m->val = r->Add == maxAdd ? -INFINITY : INFINITY;
  return m;
}

static RSValue *minmaxFinalize(Reducer *parent, void *instance) {
  minmaxCtx *ctx = instance;
  return RS_NumVal(ctx->val);
}

typedef int (*ReducerAddFunc)(Reducer *, void *, const RLookupRow *);

static Reducer *newMinMax(const ReducerOptions *options, ReducerAddFunc modeAdd) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->NewInstance = minmaxNewInstance;
  r->Add = modeAdd;
  r->Finalize = minmaxFinalize;
  r->Free = Reducer_GenericFree;
  r->reducerId = modeAdd == minAdd ? REDUCER_T_MIN : REDUCER_T_MAX;
  return r;
}

Reducer *RDCRMin_New(const ReducerOptions *options) {
  return newMinMax(options, minAdd);
}

Reducer *RDCRMax_New(const ReducerOptions *options) {
  return newMinMax(options, maxAdd);
}
