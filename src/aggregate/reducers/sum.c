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
  size_t count;
  double total;
} sumCtx;

typedef struct {
  Reducer base;
  bool isAvg;
} SumReducer;

#define BLOCK_SIZE 32 * sizeof(sumCtx)

static void *sumNewInstance(Reducer *r) {
  sumCtx *ctx = BlkAlloc_Alloc(&r->alloc, sizeof(*ctx), BLOCK_SIZE);
  ctx->count = 0;
  ctx->total = 0;
  return ctx;
}

static int sumAdd(Reducer *r, void *instance, const RLookupRow *row) {
  sumCtx *ctr = instance;
  double d;
  const RSValue *v = RLookup_GetItem(r->srckey, row);
  if (RSValue_ToNumber(v, &d)) {
    ctr->total += d;
    ctr->count++;
  }
  return 1;
}

static RSValue *sumFinalize(Reducer *baseparent, void *instance) {
  sumCtx *ctr = instance;
  SumReducer *parent = (SumReducer *)baseparent;
  double v = NAN;
  if (ctr->count) {
    if (parent->isAvg) {
      v = ctr->total / ctr->count;
    } else {
      v = ctr->total;
    }
  }
  return RS_NumVal(v);
}

static Reducer *newReducerCommon(const ReducerOptions *options, bool isAvg) {
  SumReducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->base.srckey)) {
    rm_free(r);
    return NULL;
  }
  r->base.NewInstance = sumNewInstance;
  r->base.Add = sumAdd;
  r->base.Finalize = sumFinalize;
  r->base.Free = Reducer_GenericFree;
  r->isAvg = isAvg;
  return &r->base;
}

Reducer *RDCRSum_New(const ReducerOptions *options) {
  return newReducerCommon(options, false);
}

Reducer *RDCRAvg_New(const ReducerOptions *options) {
  return newReducerCommon(options, true);
}
