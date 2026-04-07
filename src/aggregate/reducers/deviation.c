/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <aggregate/reducer.h>  // for Reducer, Reducer_GenericFree, ...
#include <math.h>               // for sqrt
#include <stdint.h>             // for uint32_t
#include <string.h>             // for memset, NULL, size_t

#include "rlookup_rs.h"         // for RLookupRow_Get, RSValue, RLookupRow
#include "rmalloc.h"            // for rm_calloc, rm_free
#include "util/block_alloc.h"   // for BlkAlloc_Alloc
#include "value/value.h"        // for RSValue_ToNumber, RSValue_ArrayItem

typedef struct {
  size_t n;
  double M, S;
} devCtx;

#define BLOCK_SIZE 1024 * sizeof(devCtx)

static void *stddevNewInstance(Reducer *rbase) {
  devCtx *dctx = BlkAlloc_Alloc(&rbase->alloc, sizeof(*dctx), BLOCK_SIZE);
  memset(dctx, 0, sizeof(*dctx));
  return dctx;
}

static void stddevAddInternal(devCtx *dctx, double d) {
  // https://www.johndcook.com/blog/standard_deviation/
  dctx->n++;
  if (dctx->n == 1) {
    dctx->M = d;
    dctx->S = 0.0;
  } else {
    double newM = dctx->M + (d - dctx->M) / dctx->n;
    double newS = dctx->S + (d - dctx->M) * (d - newM);

    // set up for next iteration
    dctx->M = newM;
    dctx->S = newS;
  }
}

static int stddevAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  devCtx *dctx = ctx;
  double d;
  RSValue *v = RLookupRow_Get(r->srckey, srcrow);
  if (v) {
    if (!RSValue_IsArray(v)) {
      if (RSValue_ToNumber(v, &d)) {
        stddevAddInternal(dctx, d);
      }
    } else {
      uint32_t sz = RSValue_ArrayLen(v);
      for (uint32_t i = 0; i < sz; i++) {
        if (RSValue_ToNumber(RSValue_ArrayItem(v, i), &d)) {
          stddevAddInternal(dctx, d);
        }
      }
    }
  }
  return 1;
}

static RSValue *stddevFinalize(Reducer *parent, void *instance) {
  devCtx *dctx = instance;
  double variance = ((dctx->n > 1) ? dctx->S / (dctx->n - 1) : 0.0);
  double stddev = sqrt(variance);
  return RSValue_NewNumber(stddev);
}

Reducer *RDCRStdDev_New(const ReducerOptions *options) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOptions_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = stddevAdd;
  r->Finalize = stddevFinalize;
  r->Free = Reducer_GenericFree;
  r->NewInstance = stddevNewInstance;
  r->reducerId = REDUCER_T_STDDEV;
  return r;
}
